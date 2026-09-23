//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xhash"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TestTopicTransactionalMultiWriter_ManualPartitionIncrease characterizes a change
// that cannot be detected through write errors: the original partition stays active.
// Transactional writers reuse their client's cached topology, so both writers keep
// routing the same key to the original partition.
func TestTopicTransactionalMultiWriter_ManualPartitionIncrease(t *testing.T) {
	for _, sameClient := range []bool{true, false} {
		for _, waitServerAck := range []bool{true, false} {
			t.Run(fmt.Sprintf("same_client=%t/sync=%t", sameClient, waitServerAck), func(t *testing.T) {
				scope := newScope(t)
				ctx, cancel := context.WithTimeout(scope.Ctx, time.Minute)
				defer cancel()

				admin := scope.Driver()
				topicPath := scope.TopicPath(
					topicoptions.CreateWithMinActivePartitions(1),
					topicoptions.CreateWithMaxActivePartitions(1),
					topicoptions.CreateWithAutoPartitioningSettings(topictypes.AutoPartitioningSettings{
						AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyDisabled,
					}),
				)
				var describeCalls atomic.Int64
				writerDB := scope.driverNamed("writer", ydb.With(config.WithGrpcOptions(
					grpc.WithChainUnaryInterceptor(countTopicDescriptions(&describeCalls)),
				)))
				alterClient := admin.Topic()
				if sameClient {
					alterClient = writerDB.Topic()
				}
				key := keyForSecondKafkaPartition()
				attempts := 0

				err := writerDB.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
					attempts++
					writer, err := writerDB.Topic().StartTransactionalWriter(tx, topicPath,
						partitionChangeWriterOptions("before-alter", waitServerAck)...)
					if err != nil {
						return err
					}
					if err := writer.WaitInit(ctx); err != nil {
						return err
					}
					beforeAlter := describeCalls.Load()
					if err := alterClient.Alter(ctx, topicPath,
						topicoptions.AlterWithMinActivePartitions(2),
						topicoptions.AlterWithMaxActivePartitions(2),
					); err != nil {
						return err
					}
					// Use a separate client for observation; its Describe calls must not
					// refresh the writer client's view in a future shared implementation.
					waitKafkaHashTopicReady(ctx, t, admin.Topic(), topicPath, 2)
					if err := writer.Write(ctx, topicwriter.Message{
						Key: key, Data: strings.NewReader("before-alter"),
					}); err != nil {
						return err
					}
					t.Logf("DescribeTopic calls while the existing writer writes after Alter: %d",
						describeCalls.Load()-beforeAlter)

					return nil
				}, query.WithLazyTx(false))
				require.NoError(t, err)
				require.Equal(t, 1, attempts, "the old partition must accept the transaction without retries")

				err = writerDB.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
					writer, err := writerDB.Topic().StartTransactionalWriter(tx, topicPath,
						partitionChangeWriterOptions("after-alter", waitServerAck)...)
					if err != nil {
						return err
					}

					return writer.Write(ctx, topicwriter.Message{
						Key: key, Data: strings.NewReader("after-alter"),
					})
				}, query.WithLazyTx(false))
				require.NoError(t, err)

				messages, err := readMessagesDetailed(ctx, admin.Topic(), topicPath,
					scope.TopicConsumerName(), 2, 15*time.Second, false)
				require.NoError(t, err)
				byPayload := make(map[string]receivedTopicMessage, len(messages))
				for _, message := range messages {
					byPayload[string(message.payload)] = message
				}
				require.Len(t, byPayload, 2)
				require.EqualValues(t, 0, byPayload["before-alter"].partitionID)
				require.EqualValues(t, 0, byPayload["after-alter"].partitionID)
				require.Empty(t, byPayload["before-alter"].producerID)
				require.Empty(t, byPayload["after-alter"].producerID)
				t.Logf("same key %q, sequential commits: partition %d / producer %q -> "+
					"partition %d / producer %q; DescribeTopic calls: %d",
					key, byPayload["before-alter"].partitionID, byPayload["before-alter"].producerID,
					byPayload["after-alter"].partitionID, byPayload["after-alter"].producerID, describeCalls.Load())
			})
		}
	}
}

func TestTopicPartitionChanges_ManualDecreaseRejected(t *testing.T) {
	scope := newScope(t)
	ctx, cancel := context.WithTimeout(scope.Ctx, 30*time.Second)
	defer cancel()
	client := scope.Driver().Topic()
	topicPath := scope.TopicPath(
		topicoptions.CreateWithMinActivePartitions(2),
		topicoptions.CreateWithAutoPartitioningSettings(topictypes.AutoPartitioningSettings{
			AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyDisabled,
		}),
	)

	err := client.Alter(ctx, topicPath, topicoptions.AlterWithMinActivePartitions(1))
	require.Error(t, err)
	t.Logf("decreasing the partition count with automatic partitioning disabled: %v", err)
	description, err := client.Describe(ctx, topicPath)
	require.NoError(t, err)
	require.Equal(t, 2, countPartitionsVisibleToKafkaHash(description.Partitions))
}

// TestTopicTransactionalMultiWriter_ManualSplit uses Alter rather than load to
// exercise a writer whose partition becomes inactive between WaitInit and Write.
func TestTopicTransactionalMultiWriter_ManualSplit(t *testing.T) {
	if os.Getenv("YDB_VERSION") != "nightly" {
		t.Skip("manual splitting requires a server with AlterTopic-driven splitting; tested on nightly")
	}
	for _, targetPartitions := range []int{2, 4} {
		for _, beforeInit := range []bool{true, false} {
			for _, waitServerAck := range []bool{true, false} {
				name := fmt.Sprintf("partitions=%d/before_init=%t/sync=%t", targetPartitions, beforeInit, waitServerAck)
				t.Run(name, func(t *testing.T) {
					scope := newScope(t)
					ctx, cancel := context.WithTimeout(scope.Ctx, time.Minute)
					defer cancel()
					admin := scope.Driver()
					topicPath := scope.TopicPath(
						topicoptions.CreateWithMinActivePartitions(1),
						topicoptions.CreateWithMaxActivePartitions(int64(targetPartitions)),
						topicoptions.CreateWithAutoPartitioningSettings(topictypes.AutoPartitioningSettings{
							AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyPaused,
						}),
					)
					var describeCalls atomic.Int64
					firstDescription := make(chan struct{}, 1)
					resumeDescription := make(chan struct{})
					if !beforeInit {
						close(resumeDescription)
					}
					writerDB := scope.driverNamed("writer", ydb.WithTraceTopic(partitionChangeTrace(t)),
						ydb.With(config.WithGrpcOptions(
							grpc.WithChainUnaryInterceptor(countTopicDescriptions(&describeCalls),
								holdFirstTopicDescription(firstDescription, resumeDescription)),
						)))
					attempts := 0
					var afterSplit topictypes.TopicDescription

					err := writerDB.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
						attempts++
						writer, err := writerDB.Topic().StartTransactionalWriter(tx, topicPath,
							topicoptions.WithWriterWaitServerAck(waitServerAck),
							topicoptions.WithWriteToManyPartitions(
								topicoptions.WithProducerIDPrefix("manual-split"),
								topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser()),
							),
						)
						if err != nil {
							return err
						}
						if beforeInit && attempts == 1 {
							select {
							case <-firstDescription:
							case <-ctx.Done():
								return ctx.Err()
							}
						} else if err := writer.WaitInit(ctx); err != nil {
							return err
						}
						if attempts == 1 {
							if err := admin.Topic().Alter(ctx, topicPath,
								topicoptions.AlterWithMinActivePartitions(int64(targetPartitions))); err != nil {
								return err
							}
							afterSplit = waitTopicActivePartitions(ctx, t, admin.Topic(), topicPath, targetPartitions)
							t.Logf("Alter applied before Write: %s", partitionChangeSummary(afterSplit.Partitions))
							if beforeInit {
								close(resumeDescription)
							}
						}

						return writer.Write(ctx, topicwriter.Message{
							Key: "same-key", Data: strings.NewReader("after-split"),
						})
					}, query.WithLazyTx(false))
					t.Logf("transaction attempts=%d, DescribeTopic calls=%d, error=%v", attempts, describeCalls.Load(), err)
					require.NoError(t, err)
					require.Greater(t, describeCalls.Load(), int64(1), "an inactive partition must trigger a new description")

					messages, err := readMessagesDetailed(ctx, admin.Topic(), topicPath,
						scope.TopicConsumerName(), 1, 15*time.Second, true)
					require.NoError(t, err)
					chooser := topicoptions.BoundPartitionChooser()
					for _, partition := range afterSplit.Partitions {
						if partition.Active && len(partition.ChildPartitionIDs) == 0 {
							require.NoError(t, chooser.AddNewPartitions(partition))
						}
					}
					expectedPartition, err := chooser.ChoosePartition(topicwriter.Message{Key: "same-key"})
					require.NoError(t, err)
					require.Equal(t, expectedPartition, messages[0].partitionID)
					require.Equal(t, "after-split", string(messages[0].payload))
				})
			}
		}
	}
}

// TestTopicPartitionChanges_PartitionKeyDoesNotValidateTarget shows why a stale
// key-to-partition mapping cannot always be detected through server write errors.
func TestTopicPartitionChanges_PartitionKeyDoesNotValidateTarget(t *testing.T) {
	scope := newScope(t)
	ctx, cancel := context.WithTimeout(scope.Ctx, 30*time.Second)
	defer cancel()
	db := scope.Driver()
	topicPath := scope.TopicPath(
		topicoptions.CreateWithMinActivePartitions(2),
		topicoptions.CreateWithMaxActivePartitions(2),
		topicoptions.CreateWithAutoPartitioningSettings(topictypes.AutoPartitioningSettings{
			AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyPaused,
		}),
	)
	description, err := db.Topic().Describe(ctx, topicPath)
	require.NoError(t, err)
	var left, right topictypes.PartitionInfo
	for _, partition := range description.Partitions {
		if len(partition.FromBound) == 0 {
			left = partition
		} else {
			right = partition
		}
	}
	if len(right.FromBound) == 0 {
		t.Skip("this server does not expose partition boundaries")
	}

	err = db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		writer, err := db.Topic().StartTransactionalWriter(tx, topicPath,
			topicoptions.WithWriterPartitionID(left.PartitionID),
			topicoptions.WithWriterWaitServerAck(true),
		)
		if err != nil {
			return err
		}

		return writer.Write(ctx, topicwriter.Message{
			Data: strings.NewReader("explicit-partition"),
			// This is the same metadata field populated by BoundPartitionChooser.
			Metadata: map[string][]byte{"__partition_key": right.FromBound},
		})
	}, query.WithLazyTx(false))
	require.NoError(t, err)
	messages, err := readMessagesDetailed(ctx, db.Topic(), topicPath,
		scope.TopicConsumerName(), 1, 10*time.Second, false)
	require.NoError(t, err)
	require.Equal(t, left.PartitionID, messages[0].partitionID)
	require.Equal(t, right.FromBound, messages[0].metadata["__partition_key"])
	t.Logf("commit accepted a key from partition %d in explicitly selected partition %d",
		right.PartitionID, left.PartitionID)
}

// TestTopicTransactionalMultiWriter_KeyOrderAcrossManualSplit checks committed
// transaction order across parent and child partitions without producer IDs.
func TestTopicTransactionalMultiWriter_KeyOrderAcrossManualSplit(t *testing.T) {
	if os.Getenv("YDB_VERSION") != "nightly" {
		t.Skip("manual splitting requires a server with AlterTopic-driven splitting; tested on nightly")
	}
	scope := newScope(t)
	ctx, cancel := context.WithTimeout(scope.Ctx, time.Minute)
	defer cancel()
	db := scope.Driver()
	topicPath := scope.TopicPath(
		topicoptions.CreateWithMinActivePartitions(1),
		topicoptions.CreateWithMaxActivePartitions(2),
		topicoptions.CreateWithAutoPartitioningSettings(topictypes.AutoPartitioningSettings{
			AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyPaused,
		}),
	)
	for index := range 2 {
		err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			writer, err := db.Topic().StartTransactionalWriter(tx, topicPath,
				topicoptions.WithWriteToManyPartitions(
					topicoptions.WithProducerIDPrefix(fmt.Sprintf("key-order-%d", index)),
					topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser()),
				),
			)
			if err != nil {
				return err
			}

			return writer.Write(ctx, topicwriter.Message{
				Key: "same-key", Data: strings.NewReader(fmt.Sprint(index)),
			})
		}, query.WithLazyTx(false))
		require.NoError(t, err)
		if index == 0 {
			require.NoError(t, db.Topic().Alter(ctx, topicPath, topicoptions.AlterWithMinActivePartitions(2)))
			waitTopicActivePartitions(ctx, t, db.Topic(), topicPath, 2)
		}
	}

	messages, err := readMessagesDetailed(ctx, db.Topic(), topicPath,
		scope.TopicConsumerName(), 2, 15*time.Second, true)
	require.NoError(t, err)
	require.Equal(t, "0", string(messages[0].payload))
	require.Equal(t, "1", string(messages[1].payload))
	require.NotEqual(t, messages[0].partitionID, messages[1].partitionID)
	require.Empty(t, messages[0].producerID)
	require.Empty(t, messages[1].producerID)
}

func partitionChangeTrace(t testing.TB) trace.Topic {
	return trace.Topic{
		OnWriterReceiveGRPCMessage: func(info trace.TopicWriterReceiveGRPCMessageInfo) {
			if info.Message != nil && info.Message.GetStatus() != Ydb.StatusIds_SUCCESS {
				t.Logf("StreamWrite error: stream=%s session=%s status=%s issues=%v",
					info.TopicStreamInternalID, info.SessionID, info.Message.GetStatus(), info.Message.GetIssues())
			}
		},
	}
}

func partitionChangeSummary(partitions []topictypes.PartitionInfo) string {
	var result strings.Builder
	for _, partition := range partitions {
		fmt.Fprintf(&result, "id=%d active=%t range=[%x,%x) parents=%v children=%v; ",
			partition.PartitionID, partition.Active, partition.FromBound, partition.ToBound,
			partition.ParentPartitionIDs, partition.ChildPartitionIDs)
	}

	return result.String()
}

func waitTopicActivePartitions(ctx context.Context, t testing.TB, client topic.Client,
	topicPath string, count int,
) topictypes.TopicDescription {
	t.Helper()
	var last topictypes.TopicDescription
	var lastErr error
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		last, lastErr = client.Describe(ctx, topicPath)
		active := 0
		for _, partition := range last.Partitions {
			if partition.Active && len(partition.ChildPartitionIDs) == 0 {
				active++
			}
		}
		if lastErr == nil && active == count {
			return last
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %d active partitions: %v; last Describe error: %v, partitions: %+v",
				count, ctx.Err(), lastErr, last.Partitions)
		case <-ticker.C:
		}
	}
}

func countTopicDescriptions(count *atomic.Int64) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn,
		invoke grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		if strings.HasSuffix(method, "/DescribeTopic") {
			count.Add(1)
		}

		return invoke(ctx, method, req, reply, conn, opts...)
	}
}

// holdFirstTopicDescription delays delivery of a real server response. No server
// statuses or partition records are synthesized by this interceptor.
func holdFirstTopicDescription(ready chan<- struct{}, resume <-chan struct{}) grpc.UnaryClientInterceptor {
	var held atomic.Bool
	return func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn,
		invoke grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		err := invoke(ctx, method, req, reply, conn, opts...)
		if err != nil || !strings.HasSuffix(method, "/DescribeTopic") || held.Swap(true) {
			return err
		}
		ready <- struct{}{}
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func partitionChangeWriterOptions(prefix string, waitServerAck bool) []topicoptions.WriterOption {
	return []topicoptions.WriterOption{
		topicoptions.WithWriterWaitServerAck(waitServerAck),
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithProducerIDPrefix(prefix),
			topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
		),
	}
}

func keyForSecondKafkaPartition() string {
	for i := 0; ; i++ {
		key := fmt.Sprintf("partition-change-%d", i)
		if xhash.Murmur2Hash32([]byte(key), 0)%2 == 1 {
			return key
		}
	}
}
