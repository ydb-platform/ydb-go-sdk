package research_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cucumber/godog"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/tests/server_contracts/internal/grpcclient"
)

const (
	defaultConnectionString = "grpc://localhost:2136/local"
	scenarioTimeout         = 45 * time.Second
	emptyTopicStepPattern   = `^an empty topic(?: with ([0-9]+) partitions)?` +
		`( with paused auto partitioning)?(?: with consumer "([^"]+)" for observation)?$`
)

type worldContextKey struct{}

type researchWorld struct {
	cancel context.CancelFunc

	conn     *grpcclient.Conn
	observer *grpcObserver
	research *streamWriteResearch

	topicPath string
	consumer  string
	created   bool
}

var scenarioCounter atomic.Uint64

func initializeScenario() func(*godog.ScenarioContext) {
	return func(sc *godog.ScenarioContext) {
		sc.Before(func(ctx context.Context, scenario *godog.Scenario) (context.Context, error) {
			scenarioCtx, cancel := context.WithTimeout(ctx, scenarioTimeout)
			world := &researchWorld{cancel: cancel}
			godog.Logf(scenarioCtx, "Server scenario:")
			for line := range strings.SplitSeq(formatResearchScenario(scenario), "\n") {
				godog.Logf(scenarioCtx, "%s", line)
			}
			godog.Logf(scenarioCtx, "")

			return context.WithValue(scenarioCtx, worldContextKey{}, world), nil
		})

		sc.After(func(ctx context.Context, _ *godog.Scenario, scenarioErr error) (context.Context, error) {
			world, err := worldFromContext(ctx)
			if err != nil {
				return ctx, err
			}
			if scenarioErr != nil {
				godog.Logf(ctx, "Scenario error: %v", scenarioErr)
				godog.Logf(ctx, "Protocol transcript:\n%s", world.Transcript())
			}

			return ctx, world.Close()
		})

		sc.Step(emptyTopicStepPattern, stepEmptyTopic)
		initializeConcurrentTransactionSteps(sc)
		initializeStreamWriteSteps(sc)
		initializeStreamReadSteps(sc)
		initializeTopicPartitionSteps(sc)
		initializeSplitObservationSteps(sc)
		initializeContractSteps(sc)
		initializeSplitContractSteps(sc)
	}
}

func formatResearchScenario(scenario *godog.Scenario) string {
	lines := []string{"Scenario: " + scenario.Name}
	for _, step := range scenario.Steps {
		lines = append(lines, "  * "+step.Text)
		if step.Argument == nil || step.Argument.DataTable == nil {
			continue
		}
		table := step.Argument.DataTable
		widths := make([]int, 0)
		for _, row := range table.Rows {
			for column, cell := range row.Cells {
				for len(widths) <= column {
					widths = append(widths, 0)
				}
				if len(cell.Value) > widths[column] {
					widths[column] = len(cell.Value)
				}
			}
		}
		for _, row := range table.Rows {
			cells := make([]string, 0, len(row.Cells))
			for column, cell := range row.Cells {
				cells = append(cells, fmt.Sprintf("%-*s", widths[column], cell.Value))
			}
			lines = append(lines, "    | "+strings.Join(cells, " | ")+" |")
		}
	}

	return strings.Join(lines, "\n")
}

func stepEmptyTopic(ctx context.Context, partitions, paused, consumer string) error {
	world, err := worldFromContext(ctx)
	if err != nil {
		return err
	}

	partitionCount, err := parseTopicPartitionCount(partitions)
	if err != nil {
		return err
	}

	return world.CreateTopic(ctx, partitionCount, paused != "", consumer)
}

func parseTopicPartitionCount(value string) (int64, error) {
	if value == "" {
		return 1, nil
	}
	count, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse topic partition count: %w", err)
	}
	if count < 1 {
		return 0, errors.New("topic partition count must be positive")
	}

	return count, nil
}

func worldFromContext(ctx context.Context) (*researchWorld, error) {
	world, ok := ctx.Value(worldContextKey{}).(*researchWorld)
	if !ok || world == nil {
		return nil, errors.New("research world is missing from scenario context")
	}

	return world, nil
}

func (w *researchWorld) CreateTopic(ctx context.Context, partitionCount int64, paused bool, consumer string) error {
	if w.conn != nil {
		return errors.New("topic fixture is already initialized")
	}

	w.observer = newGRPCObserver()
	conn, err := grpcclient.Open(connectionString(),
		grpc.WithChainUnaryInterceptor(w.observer.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(w.observer.StreamClientInterceptor()),
	)
	if err != nil {
		return err
	}
	w.conn = conn

	w.consumer = consumer

	name := fmt.Sprintf("topic-research-%d-%d-%d", os.Getpid(), time.Now().UnixNano(), scenarioCounter.Add(1))
	w.topicPath = path.Join(conn.Database, name)
	request := &Ydb_Topic.CreateTopicRequest{
		Path:            w.topicPath,
		OperationParams: &Ydb_Operations.OperationParams{OperationMode: Ydb_Operations.OperationParams_SYNC},
		PartitioningSettings: &Ydb_Topic.PartitioningSettings{
			MinActivePartitions: partitionCount, MaxActivePartitions: partitionCount,
		},
	}
	if paused {
		request.PartitioningSettings.AutoPartitioningSettings = &Ydb_Topic.AutoPartitioningSettings{
			Strategy: Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_PAUSED,
		}
	}
	if consumer != "" {
		request.Consumers = []*Ydb_Topic.Consumer{{Name: consumer}}
	}
	response, err := Ydb_Topic_V1.NewTopicServiceClient(conn).CreateTopic(ctx, request)
	if err != nil {
		return fmt.Errorf("create topic %q: %w", w.topicPath, err)
	}
	if err := grpcclient.DecodeOperation("CreateTopic", response.GetOperation(), nil); err != nil {
		return err
	}

	w.created = true

	return nil
}

func (w *researchWorld) Close() error {
	if w.cancel != nil {
		defer w.cancel()
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var cleanupErr error
	if w.research != nil {
		cleanupErr = errors.Join(cleanupErr, w.research.Close(cleanupCtx))
	}
	if w.conn != nil && w.created {
		response, err := Ydb_Topic_V1.NewTopicServiceClient(w.conn).DropTopic(cleanupCtx, &Ydb_Topic.DropTopicRequest{
			Path:            w.topicPath,
			OperationParams: &Ydb_Operations.OperationParams{OperationMode: Ydb_Operations.OperationParams_SYNC},
		})
		if err == nil {
			err = grpcclient.DecodeOperation("DropTopic", response.GetOperation(), nil)
		}
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("drop topic %q: %w", w.topicPath, err))
		}
		w.created = false
	}
	if w.conn != nil {
		cleanupErr = errors.Join(cleanupErr, w.conn.Close())
		w.conn = nil
	}

	return cleanupErr
}

func (w *researchWorld) Transcript() string {
	parts := make([]string, 0, 2)
	if w.observer != nil {
		parts = append(parts, w.observer.Transcript())
	}
	if w.research != nil {
		parts = append(parts, w.research.Transcript())
	}

	return strings.Join(parts, "\n")
}

func connectionString() string {
	if value := os.Getenv("YDB_CONNECTION_STRING"); value != "" {
		return value
	}

	return defaultConnectionString
}

func (w *researchWorld) DescribeTopic(ctx context.Context, includeStats bool) (*Ydb_Topic.DescribeTopicResult, error) {
	response, err := Ydb_Topic_V1.NewTopicServiceClient(w.conn).DescribeTopic(ctx, &Ydb_Topic.DescribeTopicRequest{
		Path: w.topicPath, IncludeStats: includeStats,
		OperationParams: &Ydb_Operations.OperationParams{OperationMode: Ydb_Operations.OperationParams_SYNC},
	})
	if err != nil {
		return nil, err
	}
	result := &Ydb_Topic.DescribeTopicResult{}
	if err := grpcclient.DecodeOperation("DescribeTopic", response.GetOperation(), result); err != nil {
		return nil, err
	}

	return result, nil
}
