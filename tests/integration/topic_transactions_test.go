//go:build integration
// +build integration

package integration

import (
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestTopicReadInTransaction(t *testing.T) {
	if os.Getenv("YDB_VERSION") != "nightly" && version.Lt(os.Getenv("YDB_VERSION"), "25.0") {
		t.Skip("require enables transactions for topics")
	}
	scope := newScope(t)
	ctx := scope.Ctx
	require.NoError(t, scope.TopicWriter().Write(ctx, topicwriter.Message{Data: strings.NewReader("asd")}))
	scope.Logf("topic message written")

	require.NoError(t, scope.Driver().Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		reader := scope.TopicReaderNamed("first")
		scope.Logf("trying to pop a batch")
		batch, err := reader.PopMessagesBatchTx(ctx, tx)
		scope.Logf("pop a batch result: %v", err)
		if err != nil {
			return err
		}
		content := string(xtest.Must(io.ReadAll(batch.Messages[0])))
		require.Equal(t, "asd", content)
		_ = reader.Close(ctx)
		return nil
	}))

	scope.Logf("first pop messages done")

	scope.Logf("writting second message")
	require.NoError(t, scope.TopicWriter().Write(ctx, topicwriter.Message{Data: strings.NewReader("bbb")}))

	require.NoError(t, scope.Driver().Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		reader := scope.TopicReaderNamed("second")
		// err := tx.Exec(ctx, "SELECT 1", query.WithCommit())
		err := tx.Exec(ctx, "SELECT 1")
		if err != nil {
			return err
		}

		scope.Logf("trying second pop batch")
		batch, err := reader.PopMessagesBatchTx(ctx, tx)
		scope.Logf("second pop batch result: %v", err)
		if err != nil {
			return err
		}
		content := string(xtest.Must(io.ReadAll(batch.Messages[0])))
		require.Equal(t, "bbb", content)
		return nil
	}))
}

func TestWriteInTransaction(t *testing.T) {
	if os.Getenv("YDB_VERSION") != "nightly" && version.Lt(os.Getenv("YDB_VERSION"), "25.0") {
		t.Skip("require enables transactions for topics")
	}

	t.Run("OK", func(t *testing.T) {
		scope := newScope(t)
		reader := scope.TopicReader()

		driver := scope.DriverWithGRPCLogging()

		const writeTime = time.Second
		protectCtx, cancel := context.WithTimeout(scope.Ctx, writeTime*10)
		defer cancel()

		deadline := time.Now().Add(writeTime)
		transactionsCount := 0
		for {
			err := driver.Query().DoTx(scope.Ctx, func(ctx context.Context, tx query.TxActor) error {
				writer, err := driver.Topic().StartTransactionalWriter(tx, scope.TopicPath())
				if err != nil {
					return err
				}

				return writer.Write(ctx, topicwriter.Message{Data: strings.NewReader(strconv.Itoa(transactionsCount))})
			})
			require.NoError(t, err)
			transactionsCount++
			if time.Now().After(deadline) {
				break
			}
		}

		for i := 0; i < transactionsCount; i++ {
			mess, err := reader.ReadMessage(protectCtx)
			require.NoError(t, err)

			contentBytes, _ := io.ReadAll(mess)
			content := string(contentBytes)
			require.Equal(t, strconv.Itoa(i), content)
		}
		t.Logf("transactions count: %v", transactionsCount)
	})

	t.Run("Rollback", func(t *testing.T) {
		scope := newScope(t)
		reader := scope.TopicReader()

		driver := scope.Driver()

		const writeTime = time.Second
		protectCtx, cancel := context.WithTimeout(scope.Ctx, writeTime*10)
		defer cancel()

		deadline := time.Now().Add(writeTime)
		transactionsCount := 0
		testErr := errors.New("test")
		for {
			err := driver.Query().DoTx(scope.Ctx, func(ctx context.Context, tx query.TxActor) error {
				writer, err := driver.Topic().StartTransactionalWriter(tx, scope.TopicPath())
				if err != nil {
					return err
				}

				require.NoError(t, writer.Write(ctx, topicwriter.Message{Data: strings.NewReader(strconv.Itoa(transactionsCount))}))
				return testErr
			})
			require.ErrorIs(t, err, testErr)
			transactionsCount++
			if time.Now().After(deadline) {
				break
			}
		}

		protectCtx, cancel = context.WithTimeout(scope.Ctx, time.Millisecond*10)
		defer cancel()

		mess, err := reader.ReadMessage(protectCtx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		_ = mess
		t.Logf("transactions count: %v", transactionsCount)
	})
}
