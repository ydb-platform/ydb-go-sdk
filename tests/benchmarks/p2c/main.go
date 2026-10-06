//go:build darwin || linux

// Command p2c compares SDK revisions against a separate-process mock gRPC cluster.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime/pprof"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_testing "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type settings struct {
	nodes    int
	scenario string
	rate     int
	duration time.Duration
	workers  int
	delay    time.Duration
	server   bool
	profile  string
}

const (
	pinnedScenario    = "pinned"
	slowScenario      = "slow"
	temporaryScenario = "temporary"
	streamsScenario   = "streams"
)

type result struct {
	Nodes              int       `json:"nodes"`
	Scenario           string    `json:"scenario"`
	Rate               int       `json:"rate"`
	DurationSeconds    float64   `json:"durationSeconds"`
	Requests           int       `json:"requests"`
	Errors             int       `json:"errors"`
	CompletedInWindow  int       `json:"completedInWindow"`
	Throughput         float64   `json:"throughput"`
	P50MS              float64   `json:"p50Ms"`
	P95MS              float64   `json:"p95Ms"`
	P99MS              float64   `json:"p99Ms"`
	RPCP95MS           float64   `json:"rpcP95Ms"`
	DispatchP95MS      float64   `json:"dispatchP95Ms"`
	ClientCPUSeconds   float64   `json:"clientCpuSeconds"`
	CallsByNode        []int64   `json:"callsByNode"`
	MeanInflightByNode []float64 `json:"meanInflightByNode"`
	MaxInflightByNode  []int64   `json:"maxInflightByNode"`
	PhaseP95MS         []float64 `json:"phaseP95Ms"`
}

func main() {
	s := settings{}
	flag.IntVar(&s.nodes, "nodes", 9, "Number of mock nodes")
	flag.StringVar(&s.scenario, "scenario", "equal", "equal, slow, temporary, streams or pinned")
	flag.IntVar(&s.rate, "rate", 2700, "Offered unary RPC/s")
	flag.DurationVar(&s.duration, "duration", 6*time.Second, "Measurement duration")
	flag.IntVar(&s.workers, "workers", 4, "Concurrent unary handlers per mock node")
	flag.DurationVar(&s.delay, "delay", 8*time.Millisecond, "Service time on a healthy node")
	flag.BoolVar(&s.server, "server", false, "Run the mock cluster subprocess")
	flag.StringVar(&s.profile, "cpuprofile", "", "Write client CPU profile during offered load")
	flag.Parse()
	if err := run(s); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(s settings) error {
	if s.nodes < 1 || s.rate < 1 || s.workers < 1 || s.duration <= 0 || s.delay <= 0 {
		return errors.New("nodes, rate, workers, duration and delay must be positive")
	}
	switch s.scenario {
	case "equal", slowScenario, temporaryScenario, streamsScenario, pinnedScenario:
	default:
		return fmt.Errorf("unknown scenario %q", s.scenario)
	}
	if s.server {
		return serve(s)
	}
	r, err := measure(s)
	if err != nil {
		return err
	}

	return json.NewEncoder(os.Stdout).Encode(r)
}

type observations struct {
	calls []atomic.Int64
	load  []atomic.Int64
}

func (o *observations) trace() *trace.Driver {
	return &trace.Driver{OnConnInvoke: func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
		if info.Method != trace.Method(grpc_testing.TestService_EmptyCall_FullMethodName) {
			return nil
		}
		i := int(info.Endpoint.NodeID()) - 1
		o.calls[i].Add(1)
		o.load[i].Add(1)

		return func(trace.DriverConnInvokeDoneInfo) { o.load[i].Add(-1) }
	}}
}

func startCluster(s settings) ([]string, func(), error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.duration+15*time.Second)
	command := exec.CommandContext(ctx, executable, "-server", "-nodes", fmt.Sprint(s.nodes),
		"-scenario", s.scenario, "-workers", fmt.Sprint(s.workers), "-delay", s.delay.String())
	command.Stderr = os.Stderr
	out, err := command.StdoutPipe()
	if err != nil {
		cancel()

		return nil, nil, err
	}
	in, err := command.StdinPipe()
	if err != nil {
		cancel()

		return nil, nil, err
	}
	if err = command.Start(); err != nil {
		cancel()

		return nil, nil, err
	}
	stop := func() {
		defer cancel()
		_ = in.Close()
		if err := command.Wait(); err != nil {
			fmt.Fprintln(os.Stderr, "mock server:", err)
		}
	}
	var addresses []string
	if err = json.NewDecoder(out).Decode(&addresses); err != nil {
		stop()

		return nil, nil, err
	}

	return addresses, stop, nil
}

func measure(s settings) (result, error) {
	addresses, stop, err := startCluster(s)
	if err != nil {
		return result{}, err
	}
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), s.duration+10*time.Second)
	defer cancel()
	o := &observations{calls: make([]atomic.Int64, s.nodes), load: make([]atomic.Int64, s.nodes)}
	cfg := config.New(config.WithEndpoint(addresses[0]), config.WithDatabase("/benchmark"),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		config.WithBalancer(balancers.WithMaxConnections(balancers.RandomChoice(), 0)),
		config.WithTrace(*o.trace()))
	pool := conn.NewPool(ctx, cfg)
	defer func() { _ = pool.RemoveRef(context.Background()) }()
	b, err := balancer.New(ctx, cfg, pool)
	if err != nil {
		return result{}, err
	}
	defer func() { _ = b.Close(context.Background()) }()
	client := grpc_testing.NewTestServiceClient(b)
	rpcCtx := conn.WithoutWrapping(ctx)
	warm := metadata.NewOutgoingContext(rpcCtx, metadata.Pairs("x-p2c-warm", "1"))
	for i := range s.nodes {
		if _, err = client.EmptyCall(balancers.WithNodeID(warm, uint32(i+1)), &grpc_testing.Empty{}); err != nil {
			return result{}, err
		}
		o.calls[i].Store(0)
	}
	if s.scenario == streamsScenario {
		for range 2 {
			streamCtx, stopStream := context.WithCancel(balancers.WithNodeID(rpcCtx, 1))
			defer stopStream()
			stream, streamErr := client.FullDuplexCall(streamCtx)
			if streamErr != nil {
				return result{}, streamErr
			}
			if streamErr = stream.Send(&grpc_testing.StreamingOutputCallRequest{}); streamErr != nil {
				return result{}, streamErr
			}
			if _, streamErr = stream.Recv(); streamErr != nil {
				return result{}, streamErr
			}
		}
		o.load[0].Add(2)
		defer o.load[0].Add(-2)
	}

	return runLoad(rpcCtx, s, client, o)
}

type sample struct {
	latency  time.Duration
	rpc      time.Duration
	dispatch time.Duration
	err      error
	inWindow bool
	phase    int
}

//nolint:funlen
func runLoad(ctx context.Context, s settings, client grpc_testing.TestServiceClient, o *observations) (result, error) {
	if s.profile != "" {
		file, err := os.Create(s.profile)
		if err != nil {
			return result{}, err
		}
		defer file.Close()
		if err := pprof.StartCPUProfile(file); err != nil {
			return result{}, err
		}
		defer pprof.StopCPUProfile()
	}
	count := int(float64(s.rate) * s.duration.Seconds())
	samples := make([]sample, count)
	mean := make([]float64, s.nodes)
	maximum := make([]int64, s.nodes)
	stopSampling := make(chan struct{})
	samplingDone := make(chan struct{})
	var sampleCount int
	go func() {
		defer close(samplingDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				sampleCount++
				for i := range o.load {
					value := o.load[i].Load()
					mean[i] += float64(value)
					maximum[i] = max(maximum[i], value)
				}
			}
		}
	}()
	before, err := cpuSeconds()
	if err != nil {
		close(stopSampling)
		<-samplingDone

		return result{}, err
	}
	started := time.Now()
	ends := started.Add(s.duration)
	slowCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-p2c-slow", "1"))
	var wg sync.WaitGroup
	for i := range samples {
		scheduled := started.Add(time.Duration(int64(i) * int64(time.Second) / int64(s.rate)))
		if delay := time.Until(scheduled); delay > 0 {
			time.Sleep(delay)
		}
		phase := min(2, int(scheduled.Sub(started)*3/s.duration))
		callCtx := ctx
		if s.scenario == temporaryScenario && phase == 1 {
			callCtx = slowCtx
		}
		if s.scenario == pinnedScenario && i%10 != 0 {
			callCtx = balancers.WithNodeID(callCtx, uint32(i%s.nodes+1))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			began := time.Now()
			callCtx, cancel := context.WithDeadline(callCtx, scheduled.Add(time.Second))
			defer cancel()
			_, callErr := client.EmptyCall(callCtx, &grpc_testing.Empty{})
			finished := time.Now()
			samples[i] = sample{
				latency: finished.Sub(scheduled), rpc: finished.Sub(began), dispatch: began.Sub(scheduled),
				err: callErr, inWindow: !finished.After(ends), phase: phase,
			}
		}()
	}
	wg.Wait()
	after, err := cpuSeconds()
	close(stopSampling)
	<-samplingDone
	if err != nil {
		return result{}, err
	}
	r := summarize(samples)
	r.Nodes, r.Scenario, r.Rate = s.nodes, s.scenario, s.rate
	r.DurationSeconds = s.duration.Seconds()
	r.Throughput = float64(r.CompletedInWindow) / r.DurationSeconds
	r.ClientCPUSeconds = after - before
	r.CallsByNode = make([]int64, s.nodes)
	for i := range o.calls {
		r.CallsByNode[i] = o.calls[i].Load()
		if sampleCount > 0 {
			mean[i] /= float64(sampleCount)
		}
	}
	r.MeanInflightByNode, r.MaxInflightByNode = mean, maximum

	return r, nil
}

func summarize(samples []sample) result {
	r := result{Requests: len(samples), PhaseP95MS: make([]float64, 3)}
	latency := make([]float64, 0, len(samples))
	rpc := make([]float64, 0, len(samples))
	dispatch := make([]float64, 0, len(samples))
	phases := make([][]float64, 3)
	for _, sample := range samples {
		if sample.err != nil {
			r.Errors++

			continue
		}
		if sample.inWindow {
			r.CompletedInWindow++
		}
		ms := float64(sample.latency) / float64(time.Millisecond)
		latency = append(latency, ms)
		rpc = append(rpc, float64(sample.rpc)/float64(time.Millisecond))
		dispatch = append(dispatch, float64(sample.dispatch)/float64(time.Millisecond))
		phases[sample.phase] = append(phases[sample.phase], ms)
	}
	r.P50MS, r.P95MS, r.P99MS = percentile(latency, 0.5), percentile(latency, 0.95), percentile(latency, 0.99)
	r.RPCP95MS, r.DispatchP95MS = percentile(rpc, 0.95), percentile(dispatch, 0.95)
	for i := range phases {
		r.PhaseP95MS[i] = percentile(phases[i], 0.95)
	}

	return r
}

func percentile(values []float64, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	slices.Sort(values)
	index := max(0, int(math.Ceil(float64(len(values))*fraction))-1)

	return values[min(index, len(values)-1)]
}

func cpuSeconds() (float64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}

	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6, nil
}
