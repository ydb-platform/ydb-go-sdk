//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Defaults for TestTopicDirectWriteLoad. They target the preprod cluster
// requested in the task; override any of them via the matching env var below.
const (
	directWriteLoadDefaultEndpoint      = "grpc://ydb-ru.yandex.net:2135/?database=/ru/kikimr/preprod/alexandr268-test"
	directWriteLoadDefaultTopic         = "topics/zkpo-direct-write-load"
	directWriteLoadDefaultMinPartitions = int64(10)
	directWriteLoadDefaultMaxPartitions = int64(1000)
	directWriteLoadDefaultPartitionID   = int64(0)
	directWriteLoadDefaultWriters       = 5
	directWriteLoadDefaultDuration      = 10 * time.Minute
	directWriteLoadDefaultBatch         = 1000
	directWriteLoadDefaultPayload       = 256
)

// Env vars recognized by TestTopicDirectWriteLoad.
const (
	envDirectWriteLoadEnable        = "YDB_TOPIC_DIRECT_WRITE_LOAD"                // set to non-empty to run the test
	envDirectWriteLoadEndpoint      = "YDB_TOPIC_DIRECT_WRITE_LOAD_DSN"            // overrides connection string
	envDirectWriteLoadTopic         = "YDB_TOPIC_DIRECT_WRITE_LOAD_TOPIC"          // overrides topic path
	envDirectWriteLoadMinPartitions = "YDB_TOPIC_DIRECT_WRITE_LOAD_MIN_PARTITIONS" // min partitions (initial)
	envDirectWriteLoadMaxPartitions = "YDB_TOPIC_DIRECT_WRITE_LOAD_MAX_PARTITIONS" // max partitions (autosplit cap)
	envDirectWriteLoadPartitionID   = "YDB_TOPIC_DIRECT_WRITE_LOAD_PARTITION_ID"   // partition every writer pins to
	envDirectWriteLoadWriters       = "YDB_TOPIC_DIRECT_WRITE_LOAD_WRITERS"        // number of parallel writers
	envDirectWriteLoadDuration      = "YDB_TOPIC_DIRECT_WRITE_LOAD_DURATION"       // how long to keep writing (e.g. "10m")
	envDirectWriteLoadBatch         = "YDB_TOPIC_DIRECT_WRITE_LOAD_BATCH"          // messages per Write call
	envDirectWriteLoadPayload       = "YDB_TOPIC_DIRECT_WRITE_LOAD_BYTES"          // payload size in bytes
)

// TestTopicDirectWriteLoad spins up N writers with direct-write enabled
// against a topic on a real cluster, verifies each writer ends up bound to
// a specific partition's node, and then pumps messages through them in
// parallel. The default endpoint points at the Yandex preprod cluster
// requested in the task — override via YDB_TOPIC_DIRECT_WRITE_LOAD_DSN.
// Auth uses YDB_ACCESS_TOKEN_CREDENTIALS.
func TestTopicDirectWriteLoad(t *testing.T) {
	// if os.Getenv(envDirectWriteLoadEnable) == "" {
	// 	t.Skipf("set %s=1 to run this load stub", envDirectWriteLoadEnable)
	// }

	endpoint := envOr(envDirectWriteLoadEndpoint, directWriteLoadDefaultEndpoint)
	topicPath := envOr(envDirectWriteLoadTopic, directWriteLoadDefaultTopic)
	minPartitions := envInt64(t, envDirectWriteLoadMinPartitions, directWriteLoadDefaultMinPartitions)
	maxPartitions := envInt64(t, envDirectWriteLoadMaxPartitions, directWriteLoadDefaultMaxPartitions)
	partitionID := envInt64(t, envDirectWriteLoadPartitionID, directWriteLoadDefaultPartitionID)
	numWriters := envInt(t, envDirectWriteLoadWriters, directWriteLoadDefaultWriters)
	duration := envDuration(t, envDirectWriteLoadDuration, directWriteLoadDefaultDuration)
	batchSize := envInt(t, envDirectWriteLoadBatch, directWriteLoadDefaultBatch)
	payloadSize := envInt(t, envDirectWriteLoadPayload, directWriteLoadDefaultPayload)
	require.Greater(t, batchSize, 0, "batch size must be > 0")
	require.Greater(t, numWriters, 0, "writers count must be > 0")
	require.Greater(t, minPartitions, int64(0), "min partitions must be > 0")
	require.GreaterOrEqual(t, maxPartitions, minPartitions, "max partitions must be >= min")
	require.GreaterOrEqual(t, partitionID, int64(0), "partition id must be >= 0")
	require.Greater(t, duration, time.Duration(0), "duration must be > 0")

	t.Logf("direct-write load: endpoint=%s topic=%s partitions=%d..%d pinPartition=%d writers=%d duration=%s batch=%d payloadBytes=%d",
		endpoint, topicPath, minPartitions, maxPartitions, partitionID, numWriters, duration, batchSize, payloadSize,
	)

	// Give the context some slack on top of the requested write duration so
	// the init / DescribeTopic / topic recreate / final close all fit.
	ctx, cancel := context.WithTimeout(context.Background(), duration+5*time.Minute)
	defer cancel()

	// Real TCP peer of the *most recently bound* StreamWrite session. Set by
	// the gRPC stream interceptor on the first RecvMsg of each stream. With
	// sequential writer init, the value right after writer i's first-connect
	// callback is writer i's peer — we snapshot it into the per-writer slot.
	var currentPeerAddr atomic.Pointer[string]

	// Stream interceptor: wraps every StreamWrite client stream so the first
	// RecvMsg (= InitResponse) records the gRPC peer the stream was actually
	// bound to. peer.FromContext is reliable only after the underlying
	// SubConn is selected, which happens by the time the server sends back
	// its first frame — that's what we hook on.
	streamPeerInterceptor := func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || !strings.Contains(method, "StreamWrite") {
			return stream, err
		}
		return &peerLoggingStream{
			ClientStream: stream,
			method:       method,
			t:            t,
			peerSink:     &currentPeerAddr,
		}, nil
	}

	db, err := ydb.Open(ctx, endpoint,
		ydb.With(config.WithGrpcOptions(
			grpc.WithChainStreamInterceptor(streamPeerInterceptor),
		)),
		ydb.WithAccessTokenCredentials("y1__xCWs-eRpdT-ARi-ByDPn_UC9dXiBjEBEnpgJpRZHJdd1MeooPQ"),
		ydb.WithTraceTopic(trace.Topic{
			OnWriterInitStream: func(_ trace.TopicWriterInitStreamStartInfo) func(trace.TopicWriterInitStreamDoneInfo) {
				return func(done trace.TopicWriterInitStreamDoneInfo) {
					// Never let trace logging take down the test; SDK can pass
					// a typed-nil endpoint and any method call would panic.
					defer func() {
						if p := recover(); p != nil {
							t.Logf("writer init stream trace panic: %v", p)
						}
					}()
					addr, nodeID := endpointAddrNode(done.Endpoint)
					if done.Error != nil {
						t.Logf("writer init stream FAILED: err=%v nodeID=%d address=%s",
							done.Error, nodeID, addr)
						return
					}
					t.Logf("writer init stream OK: session=%s nodeID=%d address=%s",
						done.SessionID, nodeID, addr)
				}
			},
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(context.Background()) })

	// Make the topic path absolute so Drop/Create/StartWriter all see the
	// same name regardless of how the user passed it.
	fullTopicPath := topicPath
	if !strings.HasPrefix(fullTopicPath, "/") {
		fullTopicPath = path.Join(db.Name(), fullTopicPath)
	}

	// (Re)create the topic with the requested partition count so each run
	// starts from a clean state. Drop is best-effort — it fails on the very
	// first run when the topic does not exist yet.
	_ = db.Topic().Drop(ctx, fullTopicPath)
	require.NoError(t, db.Topic().Create(ctx, fullTopicPath,
		// Start at minPartitions; YDB grows up to maxPartitions under load.
		topicoptions.CreateWithMinActivePartitions(minPartitions),
		topicoptions.CreateWithMaxActivePartitions(maxPartitions),
		// Advertise gzip alongside raw so the writer can negotiate it.
		topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw, topictypes.CodecGzip),
		// Auto-split by write-speed: split a partition once sustained write
		// load crosses UpUtilizationPercent of its per-partition limit,
		// merge back when it falls under DownUtilizationPercent. Stabilization
		// window keeps YDB from flapping on short bursts. ScaleUpAndDown
		// exercises both halves of the rebind logic in the SDK.
		// topicoptions.CreateWithAutoPartitioningSettings(topictypes.AutoPartitioningSettings{
		// 	AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyScaleUp,
		// 	AutoPartitioningWriteSpeedStrategy: topictypes.AutoPartitioningWriteSpeedStrategy{
		// 		StabilizationWindow:  time.Minute,
		// 		UpUtilizationPercent: 5,
		// 	},
		// }),
	))
	t.Logf("(re)created topic %s: partitions=%d..%d, autosplit by write-speed; spinning up %d writers",
		fullTopicPath, minPartitions, maxPartitions, numWriters)

	ts := time.Now().UnixNano()
	producerPrefix := fmt.Sprintf("direct-write-load-%d", ts)

	// One multi-writer with partition-by-PartitionID routing and direct write
	// enabled for every per-partition child writer it spawns. Each Write() call
	// from the pump goroutines carries Message.PartitionID; the multi-writer
	// spins up (or reuses) a per-partition writer for that PartitionID, and
	// because WithMultiWriterDirectWrite is on, that child writer binds its
	// gRPC stream directly to the node hosting the partition.
	mw, err := db.Topic().StartWriter(fullTopicPath,
		topicoptions.WithWriterCodec(topictypes.CodecGzip),
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithWriterPartitionByPartitionID(),
			topicoptions.WithMultiWriterDirectWrite(true),
			topicoptions.WithProducerIDPrefix(producerPrefix),
			topicoptions.WithWriterIdleTimeout(2*time.Minute),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mw.Close(context.Background()) })
	require.NoError(t, mw.WaitInit(ctx))
	t.Logf("multi-writer ready: producerIDPrefix=%s; %d pump goroutines, partition=goroutineIdx %% %d",
		producerPrefix, numWriters, minPartitions)

	payload := strings.Repeat("x", payloadSize)

	start := time.Now()
	deadline := start.Add(duration)
	var wg sync.WaitGroup
	totalSent := make([]int64, numWriters)
	for i := 0; i < numWriters; i++ {
		// Round-robin pump goroutines across the initial partitions so each
		// child writer (and thus each direct-write binding) gets exercised.
		targetPartition := int64(i) % minPartitions
		wg.Add(1)
		go func(idx int, pid int64) {
			defer wg.Done()
			totalSent[idx] = pumpToPartition(ctx, t, mw, idx, pid, deadline, batchSize, payload, start)
		}(i, targetPartition)
	}
	wg.Wait()

	_ = partitionID // kept around for env-compat; unused in multi-writer mode

	elapsed := time.Since(start)
	var total int64
	for _, n := range totalSent {
		total += n
	}
	t.Logf("done: total=%d messages of %d bytes across %d writers in %s (%.0f msg/s, %.2f MiB/s)",
		total, payloadSize, numWriters, elapsed,
		float64(total)/elapsed.Seconds(),
		float64(total*int64(payloadSize))/elapsed.Seconds()/(1024*1024),
	)
}

// pumpToPartition sends batches through the multi-writer with each message's
// PartitionID set to pid, until the deadline elapses or ctx is cancelled.
// Logs progress every ~30 seconds. Returns the total number of messages this
// goroutine has sent. The actual TCP peer of the per-partition child writer
// shows up in the topic trace (writer init stream OK lines) plus the gRPC
// stream interceptor — they fire per session, not per pump goroutine.
func pumpToPartition(
	ctx context.Context,
	t testing.TB,
	mw *topicwriter.Writer,
	idx int,
	pid int64,
	deadline time.Time,
	batchSize int,
	payload string,
	start time.Time,
) int64 {
	const progressEvery = 30 * time.Second
	nextLog := start.Add(progressEvery)

	var sent int64
	batch := make([]topicwriter.Message, 0, batchSize)
	for {
		if ctx.Err() != nil {
			return sent
		}
		if time.Now().After(deadline) {
			return sent
		}

		batch = batch[:0]
		for j := 0; j < batchSize; j++ {
			batch = append(batch, topicwriter.Message{
				PartitionID: pid,
				Data:        strings.NewReader(payload),
			})
		}

		err := mw.Write(ctx, batch...)
		require.NoErrorf(t, err, "pump-%d (partition=%d) write failed after %d messages",
			idx, pid, sent)

		sent += int64(len(batch))

		if now := time.Now(); !now.Before(nextLog) {
			elapsed := now.Sub(start)
			remaining := time.Until(deadline)
			if remaining < 0 {
				remaining = 0
			}
			t.Logf("pump-%d sent %d in %s (%.0f msg/s) partition=%d (≈ %s left)",
				idx, sent, elapsed.Truncate(time.Second),
				float64(sent)/elapsed.Seconds(),
				pid,
				remaining.Truncate(time.Second),
			)
			nextLog = now.Add(progressEvery)
		}
	}
}

// endpointAddrNode extracts (address, nodeID) from a trace.EndpointInfo, safe
// against both untyped-nil interfaces and typed-nil pointers stuffed into the
// interface (a known footgun: `done.Endpoint != nil` returns true for a typed
// nil, then calling any method panics). Uses reflect to detect typed nil.
// peerLoggingStream wraps a grpc.ClientStream and records the real TCP peer
// the stream is bound to. peer.FromContext is reliable only once the
// underlying SubConn is selected; that happens by the time the server sends
// back its first frame, so we hook the *first* RecvMsg. Subsequent recvs
// just delegate. This is the only way to confirm whether direct-write's
// endpoint.WithNodeID binding actually landed on the requested host.
type peerLoggingStream struct {
	grpc.ClientStream
	method   string
	t        testing.TB
	peerSink *atomic.Pointer[string]
	once     sync.Once
}

func (s *peerLoggingStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	s.once.Do(func() {
		addr := "<unknown>"
		if p, ok := peer.FromContext(s.ClientStream.Context()); ok && p.Addr != nil {
			addr = p.Addr.String()
		}
		s.peerSink.Store(&addr)
		s.t.Logf("StreamWrite peer (first frame): method=%s peer=%s", s.method, addr)
	})
	return err
}

func endpointAddrNode(ep trace.EndpointInfo) (string, uint32) {
	if ep == nil {
		return "<no endpoint>", 0
	}
	v := reflect.ValueOf(ep)
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Chan, reflect.Func, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return "<no endpoint>", 0
		}
	}
	return ep.Address(), ep.NodeID()
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func envDuration(t testing.TB, name string, fallback time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(v)
	require.NoErrorf(t, err, "invalid %s=%q: must be a Go duration (e.g. 10m, 1h)", name, v)
	return parsed
}

func envInt(t testing.TB, name string, fallback int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	require.NoErrorf(t, err, "invalid %s=%q: must be int", name, v)
	return parsed
}

func envInt64(t testing.TB, name string, fallback int64) int64 {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	require.NoErrorf(t, err, "invalid %s=%q: must be int64", name, v)
	return parsed
}
