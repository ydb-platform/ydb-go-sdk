package topicmultiwriter

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// stubTopicTransaction is a minimal tx.Transaction for MultiWriterWithTransaction tests.
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
