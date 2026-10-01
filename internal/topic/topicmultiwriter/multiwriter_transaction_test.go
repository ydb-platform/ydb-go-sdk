package topicmultiwriter

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestMultiWriterWithTransaction_Write_SetsTx(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))

	multiWriter := newTestMultiWriterWithBasicWriter(
		t,
		func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
			return stubClient.Describe(ctx, path)
		},
	)

	require.NoError(t, multiWriter.WaitInit(ctx))

	stubTxn := newStubTopicTransaction("test-txn")

	wrapped := NewTopicMultiWriterTransaction(multiWriter, stubTxn, nil)

	messages := []topicwriterinternal.PublicMessage{
		{
			Data:  bytes.NewReader([]byte("a")),
			SeqNo: 1,
			Key:   "k1",
		},
		{
			Data:  bytes.NewReader([]byte("b")),
			SeqNo: 2,
			Key:   "k2",
		},
	}

	require.NoError(t, wrapped.Write(ctx, messages))

	for i := range messages {
		require.Same(t, stubTxn, messages[i].Tx, "message %d", i)
	}

	require.NoError(t, multiWriter.Close(ctx))
}

func TestTransactionalMultiWriterDeduplication(t *testing.T) {
	for _, tc := range []struct {
		name              string
		prefix            string
		wantProducerID    string
		wantExplicitSeqNo bool
	}{
		{name: "without producer ID"},
		{name: "with producer ID prefix", prefix: "producer", wantProducerID: "producer-1"},
		{
			name: "explicit sequence numbers", prefix: "producer",
			wantProducerID: "producer-1", wantExplicitSeqNo: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := xtest.Context(t)
			stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription(t))
			options := []topicwriterinternal.PublicWriterOption{
				topicwriterinternal.WithTransactionMode(),
				topicwriterinternal.WithTopic("test/topic"),
				topicwriterinternal.WithAutosetCreatedTime(false),
			}
			if tc.wantExplicitSeqNo {
				options = append(options, topicwriterinternal.WithAutoSetSeqNo(false))
			}
			cfg := topicwriterinternal.NewWriterReconnectorConfig(options...)
			mwCfg := MultiWriterConfig{ProducerIDPrefix: tc.prefix}
			factory := &transactionalWriterFactory{
				writersFactory: newStubWritersFactory(t, stubs.StubWriterTypeBasic, tc.prefix, nil, 0),
				created:        make(chan topicwriterinternal.WriterReconnectorConfig, 1),
			}
			withWritersFactory(factory)(&mwCfg)
			writer, err := NewMultiWriter(stubClient.Describe, &cfg, &mwCfg)
			require.NoError(t, err)
			require.NoError(t, writer.WaitInit(ctx))
			require.Equal(t, tc.prefix, mwCfg.ProducerIDPrefix)
			require.Equal(t, tc.wantProducerID, writer.orchestrator.writerPool.getProducerID(1))
			require.Equal(t, !tc.wantExplicitSeqNo, cfg.AutoSetSeqNo)

			err = writer.Write(ctx, []topicwriterinternal.PublicMessage{{
				Data: bytes.NewReader([]byte("message")), PartitionID: 1,
			}})
			if tc.wantExplicitSeqNo {
				require.ErrorIs(t, err, ErrNoSeqNo)
				err = writer.Write(ctx, []topicwriterinternal.PublicMessage{{
					Data: bytes.NewReader([]byte("message")), PartitionID: 1, SeqNo: 1,
				}})
				require.NoError(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, writer.Close(ctx))
			select {
			case directCfg := <-factory.created:
				require.Equal(t, tc.wantProducerID, directCfg.ProducerID())
				require.False(t, directCfg.AutoSetSeqNo)
				require.Equal(t, cfg.AutoSetSeqNo && tc.prefix != "", directCfg.RequestLastSeqNo)
				require.Equal(t, topic.PublicRetryDecisionStop, directCfg.RetrySettings.CheckError(
					topic.PublicCheckErrorRetryArgs{Error: errors.New("session failed")},
				))
			case <-ctx.Done():
				t.Fatal("partition writer was not created")
			}
		})
	}
}

func TestTransactionalMultiWriterDoesNotOpenSessionForSeqNo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bg := background.NewWorker(ctx, "test multiwriter")
	defer func() {
		cancel()
		_ = bg.Close(context.Background(), nil)
	}()

	cfg := topicwriterinternal.NewWriterReconnectorConfig(
		topicwriterinternal.WithTransactionMode(),
		topicwriterinternal.WithTopic("test/topic"),
	)
	mwCfg := MultiWriterConfig{}
	withWritersFactory(newStubWritersFactory(t, stubs.StubWriterTypeBasic, "", nil, 0))(&mwCfg)
	orchestrator := newOrchestrator(ctx, cancel, nil, bg, &cfg, &mwCfg)
	chooser := partitionchooser.NewByPartitionIDPartitionChooser()
	partition := topictypes.PartitionInfo{PartitionID: 1, Active: true}
	require.NoError(t, chooser.AddNewPartitions(partition))
	orchestrator.partitionChooser = chooser
	orchestrator.partitions[1] = &PartitionInfo{PartitionInfo: partition}

	err := orchestrator.pushMessage(ctx, message{MessageWithDataContent: topicwritercommon.NewMessageDataWithContent(
		topicwriterinternal.PublicMessage{Data: bytes.NewReader([]byte("message")), PartitionID: 1},
		topicwritercommon.NewMultiEncoder(),
	)})
	require.NoError(t, err)
	require.Zero(t, orchestrator.getWritersCount())
}

// stubTopicTransaction is a minimal [tx.Transaction] for MultiWriterWithTransaction tests.
type stubTopicTransaction struct {
	tx.Identifier

	sessionID string
}

func newStubTopicTransaction(id string) *stubTopicTransaction {
	return &stubTopicTransaction{
		Identifier: tx.ID(id),
		sessionID:  "test-session",
	}
}

func (s *stubTopicTransaction) UnLazy(context.Context) error {
	return nil
}

func (s *stubTopicTransaction) SessionID() string {
	return s.sessionID
}

func (s *stubTopicTransaction) NodeID() uint32 {
	return 0
}

func (*stubTopicTransaction) OnBeforeCommit(tx.OnTransactionBeforeCommit) {}

func (*stubTopicTransaction) OnCompleted(tx.OnTransactionCompletedFunc) {}

func (*stubTopicTransaction) Rollback(context.Context) error {
	return nil
}

type transactionalWriterFactory struct {
	writersFactory

	created chan topicwriterinternal.WriterReconnectorConfig
}

func (f *transactionalWriterFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	f.created <- cfg

	return f.writersFactory.Create(cfg)
}
