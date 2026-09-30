package transactionalwriterbenchmark

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	sdkconfig "github.com/ydb-platform/ydb-go-sdk/v3/config"
)

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

func finishTopologyMonitor(
	ctx context.Context,
	cfg config,
	db *ydb.Driver,
	recorder *topologyRecorder,
	cancel context.CancelFunc,
	done <-chan struct{},
) error {
	cancel()
	<-done

	describeContext, cancelDescribe := context.WithTimeout(ctx, cfg.TransactionTimeout)
	description, describeErr := db.Topic().Describe(describeContext, cfg.TopicPath)
	cancelDescribe()
	if describeErr != nil {
		recorder.recordError(fmt.Errorf("describe final topic topology: %w", describeErr))
	} else {
		topology, topologyErr := topicTopologyFromDescription(description)
		if topologyErr != nil {
			recorder.recordError(topologyErr)
		} else {
			recorder.record(time.Now(), topology)
		}
	}

	closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelClose()

	return db.Close(closeContext)
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
	options = append(options, ydb.WithAnonymousCredentials())

	db, err := ydb.Open(ctx, cfg.DSN, options...)
	if err != nil {
		return nil, fmt.Errorf("open YDB driver: %w", err)
	}

	return db, nil
}
