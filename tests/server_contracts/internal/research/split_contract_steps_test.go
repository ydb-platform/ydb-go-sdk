package research_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/cucumber/godog"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
)

func initializeSplitContractSteps(sc *godog.ScenarioContext) {
	sc.Step(`^contract: AlterTopic completed successfully$`, contractAlterSucceeded)
	sc.Step(`^contract: StreamWrite "([^"]+)" terminated with (\w+)$`, contractStreamTerminated)
	sc.Step(`^contract: DescribeTopic eventually shows inactive partition (\d+) `+
		`with active children \[([0-9, ]+)\] after writer rejection$`,
		contractChildrenDiscovered)
}

func contractAlterSucceeded(ctx context.Context) error {
	research, err := researchFromContext(ctx)
	if err != nil {
		return err
	}
	operation := research.lastAlterResponse.GetOperation()
	if !operation.GetReady() || operation.GetStatus() != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf("AlterTopic did not succeed: %v", operation)
	}

	return nil
}

func contractStreamTerminated(ctx context.Context, name, status string) error {
	session, err := writeSessionFromContext(ctx, name)
	if err != nil {
		return err
	}
	code, ok := Ydb.StatusIds_StatusCode_value[status]
	if !ok || code == int32(Ydb.StatusIds_SUCCESS) {
		return fmt.Errorf("invalid terminal status %q", status)
	}
	session.takePendingResponses()
	for {
		if _, err := session.receive(ctx); err != nil {
			if !errors.Is(err, io.EOF) {
				return fmt.Errorf("StreamWrite %s: expected EOF after %s: %w", name, status, err)
			}

			break
		}
	}
	responses := session.contractResponses()
	if len(responses) < 2 || responses[0].GetInitResponse() == nil {
		return fmt.Errorf("StreamWrite %s: no initialized session before termination", name)
	}
	for _, response := range responses[:len(responses)-1] {
		if response.GetStatus() != Ydb.StatusIds_SUCCESS {
			return fmt.Errorf("StreamWrite %s: unexpected earlier failure: %v", name, response)
		}
	}
	last := responses[len(responses)-1]
	if last.GetStatus() != Ydb.StatusIds_StatusCode(code) || last.GetServerMessage() != nil {
		return fmt.Errorf("StreamWrite %s: expected terminal %s, got %v", name, status, last)
	}

	return nil
}

func contractChildrenDiscovered(ctx context.Context, parent int64, childrenText string) error {
	children, err := parseReadPartitionIDs(childrenText)
	if err != nil {
		return err
	}
	research, err := researchFromContext(ctx)
	if err != nil {
		return err
	}
	observation := research.splitObservation
	if observation == nil || observation.writeFailed.IsZero() ||
		observation.writeResponse.GetStatus() != Ydb.StatusIds_OVERLOADED {
		return errors.New("split observation has no OVERLOADED writer rejection")
	}
	operation := observation.alterResponse.GetOperation()
	if !operation.GetReady() || operation.GetStatus() != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf("split AlterTopic did not succeed: %v", operation)
	}
	ticker := time.NewTicker(observation.sampleInterval)
	defer ticker.Stop()
	for {
		if splitChildrenDiscovered(observation, parent, children) {
			return nil
		}
		select {
		case <-ticker.C:
			if _, err := research.sampleTopicDescription(ctx, observation); err != nil {
				return err
			}
		case <-ctx.Done():
			return fmt.Errorf("children %v of partition %d did not appear after writer rejection: %w",
				children, parent, ctx.Err())
		}
	}
}

func splitChildrenDiscovered(observation *splitObservation, parent int64, children []int64) bool {
	for _, sample := range observation.samples {
		if sample.started.After(observation.writeFailed) && hasSplitTopology(sample.result, parent, children) {
			return true
		}
	}

	return false
}

func hasSplitTopology(description *Ydb_Topic.DescribeTopicResult, parent int64, children []int64) bool {
	foundParent := false
	foundChildren := make(map[int64]bool)
	for _, partition := range description.GetPartitions() {
		if partition.GetPartitionId() == parent {
			actual := slices.Clone(partition.GetChildPartitionIds())
			expected := slices.Clone(children)
			slices.Sort(actual)
			slices.Sort(expected)
			foundParent = !partition.GetActive() && slices.Equal(actual, expected)
		}
		if slices.Contains(children, partition.GetPartitionId()) && partition.GetActive() &&
			len(partition.GetChildPartitionIds()) == 0 && slices.Contains(partition.GetParentPartitionIds(), parent) {
			foundChildren[partition.GetPartitionId()] = true
		}
	}

	return foundParent && len(foundChildren) == len(children)
}
