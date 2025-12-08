//go:build integration
// +build integration

package integration

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

type TopicReaderUpdateOffsetsSuite struct {
	suite.Suite

	scope  *scopeT
	writer *topicwriter.Writer
	reader *topicreader.Reader
	driver *ydb.Driver
}

func (t *TopicReaderUpdateOffsetsSuite) SetupTest() {
	t.scope = newScope(t.T())
	t.writer = t.scope.TopicWriter()
	t.reader = t.scope.TopicReader()
	t.driver = t.scope.Driver()
}

func TestBatchTxStorage(t *testing.T) {
	suite.Run(t, new(TopicReaderUpdateOffsetsSuite))
}

// Helper methods for test setup

func (t *TopicReaderUpdateOffsetsSuite) testContext() context.Context {
	return t.scope.Ctx
}

func (t *TopicReaderUpdateOffsetsSuite) TestSingleTransaction() {
	ctx := t.testContext()

	t.writeMessage(ctx, "1")

	var batch *topicreader.Batch

	var once sync.Once
	err := t.driver.Query().DoTx(ctx, func(ctx context.Context, tr query.TxActor) error {
		once.Do(func() {
			t.deleteTxSession(ctx, tr.(tx.Transaction))
		})

		var err error
		batch, err = t.reader.PopMessagesBatchTx(ctx, tr)
		return err
	})

	t.Require().NoError(err)
	t.Require().Len(batch.Messages, 1)
	t.MsgEqualString("1", batch.Messages[0])
}

func (t *TopicReaderUpdateOffsetsSuite) TestSeveralReads() {
	ctx := t.testContext()

	t.writeMessage(ctx, "1")

	err := t.driver.Query().DoTx(ctx, func(ctx context.Context, tr query.TxActor) error {
		_, err := t.reader.PopMessagesBatchTx(ctx, tr)
		t.Require().NoError(err)

		t.writeMessage(ctx, "2")

		_, err = t.reader.PopMessagesBatchTx(ctx, tr)
		t.Require().NoError(err)

		t.writeMessage(ctx, "3")

		return nil
	})
	t.Require().NoError(err)

	msg, err := t.reader.ReadMessage(ctx)
	t.Require().NoError(err)
	t.MsgEqualString("3", msg)
}

func (t *TopicReaderUpdateOffsetsSuite) TestSeveralTransactions() {
	ctx := t.testContext()

	t.writeMessage(ctx, "1")

	var (
		once1 sync.Once
		once2 sync.Once
	)

	err := t.driver.Query().DoTx(ctx, func(ctx context.Context, tr query.TxActor) error {
		once1.Do(func() {
			t.deleteTxSession(ctx, tr.(tx.Transaction))
		})

		_, err := t.reader.PopMessagesBatchTx(ctx, tr)
		return err
	})
	t.NoError(err)

	t.writeMessage(ctx, "2")

	var batch *topicreader.Batch
	err = t.driver.Query().DoTx(ctx, func(ctx context.Context, tr query.TxActor) error {
		once2.Do(func() {
			t.deleteTxSession(ctx, tr.(tx.Transaction))
		})

		var err error
		batch, err = t.reader.PopMessagesBatchTx(ctx, tr)
		return err
	})
	t.Require().NoError(err)

	t.MsgEqualString("2", batch.Messages[0])
}

// Helper methods

func (t *TopicReaderUpdateOffsetsSuite) writeMessage(ctx context.Context, msg string) {
	err := t.writer.Write(ctx, topicwriter.Message{Data: strings.NewReader(msg)})
	t.NoError(err)
}

func (t *TopicReaderUpdateOffsetsSuite) MsgEqualString(expected string, msg *topicreader.Message) {
	t.T().Helper()

	var actual string

	topicsugar.ReadMessageDataWithCallback(msg, func(data []byte) error {
		actual = string(data)
		return nil
	})

	t.Equal(expected, actual)
}

func (t *TopicReaderUpdateOffsetsSuite) deleteTxSession(ctx context.Context, tx tx.Transaction) {
	t.deleteSession(ctx, tx.SessionID())
}

func (t *TopicReaderUpdateOffsetsSuite) deleteSession(ctx context.Context, sessionID string) {
	_, err := Ydb_Query_V1.NewQueryServiceClient(ydb.GRPCConn(t.driver)).
		DeleteSession(ctx, &Ydb_Query.DeleteSessionRequest{
			SessionId: sessionID,
		})
	t.NoError(err)
}
