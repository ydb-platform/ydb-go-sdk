package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	sdkconfig "github.com/ydb-platform/ydb-go-sdk/v3/config"
)

type reportConfig struct {
	RunID                  string      `json:"run_id"`
	Label                  string      `json:"label,omitempty"`
	Mode                   writerMode  `json:"mode"`
	Routing                routingMode `json:"routing"`
	AutoSeqNo              bool        `json:"auto_seq_no"`
	Duration               string      `json:"duration"`
	Warmup                 string      `json:"warmup"`
	TransactionTimeout     string      `json:"transaction_timeout"`
	Concurrency            int         `json:"concurrency"`
	MessagesPerTx          int         `json:"messages_per_transaction"`
	MessageSize            int         `json:"message_size_bytes"`
	LatencySampleEvery     int         `json:"latency_sample_every"`
	MaxErrors              int         `json:"max_errors"`
	SkipTableWrite         bool        `json:"skip_table_write"`
	ProducerIDPrefix       string      `json:"producer_id_prefix,omitempty"`
	StableProducerSlots    bool        `json:"stable_producer_slots"`
	QueryRetries           bool        `json:"query_retries"`
	DirectWrite            bool        `json:"direct_write"`
	CPUProfile             string      `json:"cpu_profile,omitempty"`
	HeapProfile            string      `json:"heap_profile,omitempty"`
	AutoSplit              bool        `json:"auto_split"`
	AutoSplitMaxPartitions int64       `json:"auto_split_max_partitions,omitempty"`
	AutoSplitWriteSpeed    int64       `json:"auto_split_write_speed_bytes_per_second,omitempty"`
	AutoSplitBurstBytes    int64       `json:"auto_split_burst_bytes,omitempty"`
	AutoSplitUpUtilization int         `json:"auto_split_up_utilization_percent,omitempty"`
	AutoSplitStabilization string      `json:"auto_split_stabilization_window,omitempty"`
	AutoSplitPollInterval  string      `json:"auto_split_poll_interval,omitempty"`
}

type topicReport struct {
	Path                        string  `json:"path"`
	ActivePartitionCount        int     `json:"active_partition_count"`
	ActivePartitionIDs          []int64 `json:"active_partition_ids"`
	InitialActivePartitionCount int     `json:"initial_active_partition_count"`
	InitialActivePartitionIDs   []int64 `json:"initial_active_partition_ids"`
	TotalPartitionCount         int     `json:"total_partition_count"`
	AutoSplitEnabled            bool    `json:"auto_split_enabled"`
	AutoSplitObserved           bool    `json:"auto_split_observed"`
	FirstSplitAfterMilliseconds float64 `json:"first_split_after_ms,omitempty"`
	TopologyDescribeCalls       uint64  `json:"topology_describe_calls"`
	TopologyLastError           string  `json:"topology_last_error,omitempty"`
}

type topicTopology struct {
	ActivePartitionIDs []int64
	TotalPartitions    int
}

type topologyObservation struct {
	Initial         topicTopology
	Final           topicTopology
	SplitObserved   bool
	FirstSplitAfter time.Duration
	Polls           uint64
	LastError       string
}

type topologyRecorder struct {
	startedAt time.Time
	mu        sync.Mutex
	result    topologyObservation
}

func newTopologyRecorder(startedAt time.Time, initial topicTopology) *topologyRecorder {
	initial = cloneTopicTopology(initial)

	return &topologyRecorder{
		startedAt: startedAt,
		result: topologyObservation{
			Initial: initial,
			Final:   cloneTopicTopology(initial),
		},
	}
}

func (r *topologyRecorder) record(now time.Time, topology topicTopology) {
	r.mu.Lock()
	defer r.mu.Unlock()

	topology = cloneTopicTopology(topology)
	r.result.Final = topology
	r.result.Polls++
	if !r.result.SplitObserved && len(topology.ActivePartitionIDs) > len(r.result.Initial.ActivePartitionIDs) {
		r.result.SplitObserved = true
		r.result.FirstSplitAfter = now.Sub(r.startedAt)
	}
}

func (r *topologyRecorder) recordError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.result.Polls++
	r.result.LastError = err.Error()
}

func (r *topologyRecorder) snapshot() topologyObservation {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := r.result
	result.Initial = cloneTopicTopology(result.Initial)
	result.Final = cloneTopicTopology(result.Final)

	return result
}

func cloneTopicTopology(topology topicTopology) topicTopology {
	return topicTopology{
		ActivePartitionIDs: append([]int64(nil), topology.ActivePartitionIDs...),
		TotalPartitions:    topology.TotalPartitions,
	}
}

type tableReport struct {
	Path    string `json:"path,omitempty"`
	Enabled bool   `json:"enabled"`
}

type buildReport struct {
	SDKVersion  string `json:"sdk_version"`
	GoVersion   string `json:"go_version"`
	GOOS        string `json:"goos"`
	GOARCH      string `json:"goarch"`
	CPUs        int    `json:"cpus"`
	VCSRevision string `json:"vcs_revision,omitempty"`
	VCSModified bool   `json:"vcs_modified"`
}

type memorySnapshot struct {
	HeapAlloc   uint64
	HeapObjects uint64
	TotalAlloc  uint64
	Mallocs     uint64
	NumGC       uint32
}

type memoryReport struct {
	AllocatedBytes     uint64 `json:"allocated_bytes"`
	Mallocs            uint64 `json:"mallocs"`
	GCCycles           uint32 `json:"gc_cycles"`
	HeapBeforeBytes    uint64 `json:"heap_before_bytes"`
	HeapAtStopBytes    uint64 `json:"heap_at_stop_bytes"`
	HeapAfterGCBytes   uint64 `json:"heap_after_gc_bytes"`
	HeapObjectsBefore  uint64 `json:"heap_objects_before"`
	HeapObjectsAtStop  uint64 `json:"heap_objects_at_stop"`
	HeapObjectsAfterGC uint64 `json:"heap_objects_after_gc"`
}

type benchmarkReport struct {
	SchemaVersion  int                     `json:"schema_version"`
	Implementation string                  `json:"implementation"`
	GeneratedAt    time.Time               `json:"generated_at"`
	Build          buildReport             `json:"build"`
	Config         reportConfig            `json:"config"`
	Topic          topicReport             `json:"topic"`
	Table          tableReport             `json:"table"`
	Result         phaseReport             `json:"result"`
	Lifecycle      instrumentationSnapshot `json:"lifecycle"`
	RuntimeMemory  memoryReport            `json:"runtime_memory"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	cfg, err := parseConfig(args, stderr)
	if err != nil {
		return err
	}

	rootContext, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	metrics := &instrumentation{}
	openContext, cancelOpen := context.WithTimeout(rootContext, cfg.TransactionTimeout)
	db, err := openDatabase(openContext, cfg, metrics)
	cancelOpen()
	if err != nil {
		return err
	}
	defer func() {
		closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelClose()
		if closeErr := db.Close(closeContext); closeErr != nil {
			_, _ = fmt.Fprintf(stderr, "close YDB driver: %v\n", closeErr)
		}
	}()

	if cfg.Prepare {
		prepareContext, cancelPrepare := context.WithTimeout(rootContext, cfg.TransactionTimeout)
		err = prepareSchema(prepareContext, db, cfg)
		cancelPrepare()
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(
			stderr,
			"prepared topic=%q table=%q partitions=%d\n",
			cfg.TopicPath,
			cfg.TablePath,
			cfg.PreparePartitions,
		)
	}
	if cfg.PrepareOnly {
		return nil
	}

	describeContext, cancelDescribe := context.WithTimeout(rootContext, cfg.TransactionTimeout)
	description, err := db.Topic().Describe(describeContext, cfg.TopicPath)
	cancelDescribe()
	if err != nil {
		return fmt.Errorf("describe topic %q: %w", cfg.TopicPath, err)
	}
	initialTopology, err := topicTopologyFromDescription(description)
	if err != nil {
		return err
	}
	if cfg.AutoSplit {
		if err := validateAutoSplitTopic(description, cfg); err != nil {
			return err
		}
	}
	activePartitionIDs := initialTopology.ActivePartitionIDs

	var (
		topologyDB     *ydb.Driver
		cancelMonitor  context.CancelFunc
		monitorDone    chan struct{}
		recorder       = newTopologyRecorder(time.Now(), initialTopology)
		monitorCleaned bool
	)
	if cfg.AutoSplit {
		monitorOpenContext, cancelMonitorOpen := context.WithTimeout(rootContext, cfg.TransactionTimeout)
		topologyDB, err = openDatabase(monitorOpenContext, cfg, nil)
		cancelMonitorOpen()
		if err != nil {
			return fmt.Errorf("open topology monitor connection: %w", err)
		}
		recorder = newTopologyRecorder(time.Now(), initialTopology)
		monitorContext, cancel := context.WithCancel(rootContext)
		cancelMonitor = cancel
		monitorDone = make(chan struct{})
		go func() {
			defer close(monitorDone)
			monitorTopicTopology(
				monitorContext,
				topologyDB,
				cfg.TopicPath,
				cfg.AutoSplitPollInterval,
				recorder,
			)
		}()
		defer func() {
			if monitorCleaned {
				return
			}
			cancelMonitor()
			<-monitorDone
			closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelClose()
			_ = topologyDB.Close(closeContext)
		}()
	}
	payload := makePayload(cfg.MessageSize)
	sequences := make([]atomic.Uint64, cfg.Concurrency)
	_, _ = fmt.Fprintf(
		stderr,
		"benchmark mode=%s routing=%s concurrency=%d messages_per_tx=%d message_size=%d active_partitions=%d\n",
		cfg.Mode,
		cfg.Routing,
		cfg.Concurrency,
		cfg.MessagesPerTx,
		cfg.MessageSize,
		len(activePartitionIDs),
	)

	if cfg.Warmup > 0 {
		_, _ = fmt.Fprintf(stderr, "warmup for %s\n", cfg.Warmup)
		warmupStats, warmupErr := runPhase(
			rootContext,
			db,
			cfg,
			activePartitionIDs,
			payload,
			cfg.Warmup,
			sequences,
		)
		if warmupErr != nil {
			return fmt.Errorf(
				"warmup failed after %d committed transactions: %w; first error: %s",
				warmupStats.Committed,
				warmupErr,
				warmupStats.FirstError,
			)
		}
		_, _ = fmt.Fprintf(stderr, "warmup committed=%d\n", warmupStats.Committed)
	}

	runtime.GC()
	memoryBefore := readMemorySnapshot()
	lifecycleBefore := metrics.snapshot()
	stopCPUProfile, err := startCPUProfile(cfg.CPUProfile)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(stderr, "measurement for %s\n", cfg.Duration)
	measurement, measurementErr := runPhase(
		rootContext,
		db,
		cfg,
		activePartitionIDs,
		payload,
		cfg.Duration,
		sequences,
	)
	if err := stopCPUProfile(); err != nil && measurementErr == nil {
		measurementErr = err
	}
	memoryAtStop := readMemorySnapshot()
	if cfg.AutoSplit {
		cancelMonitor()
		<-monitorDone
		finalDescribeContext, cancelFinalDescribe := context.WithTimeout(rootContext, cfg.TransactionTimeout)
		finalDescription, finalDescribeErr := topologyDB.Topic().Describe(finalDescribeContext, cfg.TopicPath)
		cancelFinalDescribe()
		if finalDescribeErr != nil {
			recorder.recordError(fmt.Errorf("describe final topic topology: %w", finalDescribeErr))
		} else {
			finalTopology, topologyErr := topicTopologyFromDescription(finalDescription)
			if topologyErr != nil {
				recorder.recordError(topologyErr)
			} else {
				recorder.record(time.Now(), finalTopology)
			}
		}
		closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
		closeErr := topologyDB.Close(closeContext)
		cancelClose()
		monitorCleaned = true
		if closeErr != nil && measurementErr == nil {
			measurementErr = fmt.Errorf("close topology monitor connection: %w", closeErr)
		}
	}
	topology := recorder.snapshot()
	runtime.GC()
	memoryAfterGC := readMemorySnapshot()
	lifecycle := metrics.snapshot().subtract(lifecycleBefore)
	if err := writeHeapProfile(cfg.HeapProfile); err != nil && measurementErr == nil {
		measurementErr = err
	}

	report := makeReport(
		cfg,
		topology,
		measurement,
		lifecycle,
		makeMemoryReport(memoryBefore, memoryAtStop, memoryAfterGC),
	)
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("encode benchmark report: %w", err)
	}
	if measurementErr != nil {
		return measurementErr
	}

	return nil
}

func openDatabase(ctx context.Context, cfg config, metrics *instrumentation) (*ydb.Driver, error) {
	options := make([]ydb.Option, 0, 3)
	if metrics != nil {
		options = append(
			options,
			ydb.With(sdkconfig.WithGrpcOptions(
				grpc.WithChainUnaryInterceptor(metrics.unaryClientInterceptor),
				grpc.WithChainStreamInterceptor(metrics.streamClientInterceptor),
			)),
			ydb.WithTraceTopic(metrics.topicTrace()),
		)
	}
	if cfg.Anonymous {
		options = append(options, ydb.WithAnonymousCredentials())
	} else {
		options = append(options, environ.WithEnvironCredentials())
	}

	db, err := ydb.Open(ctx, cfg.DSN, options...)
	if err != nil {
		return nil, fmt.Errorf("open YDB driver: %w", err)
	}

	return db, nil
}

func makeReport(
	cfg config,
	topology topologyObservation,
	measurement phaseStats,
	lifecycle instrumentationSnapshot,
	memory memoryReport,
) benchmarkReport {
	return benchmarkReport{
		SchemaVersion:  1,
		Implementation: "go_sdk",
		GeneratedAt:    time.Now().UTC(),
		Build:          currentBuildReport(),
		Config: reportConfig{
			RunID:                  cfg.RunID,
			Label:                  cfg.Label,
			Mode:                   cfg.Mode,
			Routing:                cfg.Routing,
			AutoSeqNo:              cfg.AutoSeqNo,
			Duration:               cfg.Duration.String(),
			Warmup:                 cfg.Warmup.String(),
			TransactionTimeout:     cfg.TransactionTimeout.String(),
			Concurrency:            cfg.Concurrency,
			MessagesPerTx:          cfg.MessagesPerTx,
			MessageSize:            cfg.MessageSize,
			LatencySampleEvery:     cfg.LatencySampleEvery,
			MaxErrors:              cfg.MaxErrors,
			SkipTableWrite:         cfg.SkipTableWrite,
			ProducerIDPrefix:       cfg.ProducerIDPrefix,
			StableProducerSlots:    cfg.ProducerIDPrefix != "",
			QueryRetries:           cfg.QueryRetries,
			DirectWrite:            false,
			CPUProfile:             cfg.CPUProfile,
			HeapProfile:            cfg.HeapProfile,
			AutoSplit:              cfg.AutoSplit,
			AutoSplitMaxPartitions: cfg.AutoSplitMaxPartitions,
			AutoSplitWriteSpeed:    cfg.AutoSplitWriteSpeed,
			AutoSplitBurstBytes:    cfg.AutoSplitBurstBytes,
			AutoSplitUpUtilization: cfg.AutoSplitUpUtilization,
			AutoSplitStabilization: cfg.AutoSplitStabilization.String(),
			AutoSplitPollInterval:  cfg.AutoSplitPollInterval.String(),
		},
		Topic: topicReport{
			Path:                        cfg.TopicPath,
			ActivePartitionCount:        len(topology.Final.ActivePartitionIDs),
			ActivePartitionIDs:          topology.Final.ActivePartitionIDs,
			InitialActivePartitionCount: len(topology.Initial.ActivePartitionIDs),
			InitialActivePartitionIDs:   topology.Initial.ActivePartitionIDs,
			TotalPartitionCount:         topology.Final.TotalPartitions,
			AutoSplitEnabled:            cfg.AutoSplit,
			AutoSplitObserved:           topology.SplitObserved,
			FirstSplitAfterMilliseconds: float64(topology.FirstSplitAfter) / float64(time.Millisecond),
			TopologyDescribeCalls:       topology.Polls,
			TopologyLastError:           topology.LastError,
		},
		Table: tableReport{
			Path:    cfg.TablePath,
			Enabled: !cfg.SkipTableWrite,
		},
		Result:        measurement.report(cfg.SkipTableWrite),
		Lifecycle:     lifecycle,
		RuntimeMemory: memory,
	}
}

func currentBuildReport() buildReport {
	report := buildReport{
		SDKVersion: ydb.Version,
		GoVersion:  runtime.Version(),
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
		CPUs:       runtime.NumCPU(),
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return report
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			report.VCSRevision = setting.Value
		case "vcs.modified":
			report.VCSModified, _ = strconv.ParseBool(setting.Value)
		}
	}

	return report
}

func readMemorySnapshot() memorySnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	return memorySnapshot{
		HeapAlloc:   stats.HeapAlloc,
		HeapObjects: stats.HeapObjects,
		TotalAlloc:  stats.TotalAlloc,
		Mallocs:     stats.Mallocs,
		NumGC:       stats.NumGC,
	}
}

func makeMemoryReport(before, atStop, afterGC memorySnapshot) memoryReport {
	return memoryReport{
		AllocatedBytes:     atStop.TotalAlloc - before.TotalAlloc,
		Mallocs:            atStop.Mallocs - before.Mallocs,
		GCCycles:           atStop.NumGC - before.NumGC,
		HeapBeforeBytes:    before.HeapAlloc,
		HeapAtStopBytes:    atStop.HeapAlloc,
		HeapAfterGCBytes:   afterGC.HeapAlloc,
		HeapObjectsBefore:  before.HeapObjects,
		HeapObjectsAtStop:  atStop.HeapObjects,
		HeapObjectsAfterGC: afterGC.HeapObjects,
	}
}

func startCPUProfile(path string) (func() error, error) {
	if path == "" {
		return func() error { return nil }, nil
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create CPU profile %q: %w", path, err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("start CPU profile %q: %w", path, err)
	}

	return func() error {
		pprof.StopCPUProfile()
		if err := file.Close(); err != nil {
			return fmt.Errorf("close CPU profile %q: %w", path, err)
		}

		return nil
	}, nil
}

func writeHeapProfile(path string) error {
	if path == "" {
		return nil
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create heap profile %q: %w", path, err)
	}
	if err := pprof.WriteHeapProfile(file); err != nil {
		_ = file.Close()

		return fmt.Errorf("write heap profile %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close heap profile %q: %w", path, err)
	}

	return nil
}
