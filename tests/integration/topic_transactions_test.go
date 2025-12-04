//go:build integration
// +build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
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

func TestTopicReaderTLIIssue1797(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	db := scope.Driver()

	tablePath := scope.TablePath()

	writer := scope.TopicWriter()
	reader := scope.TopicReader()

	scope.Require.NoError(writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("1")}))

	messageReaded := make(chan bool)
	valueInserted := make(chan bool)

	go func() {
		<-messageReaded
		db.Query().Exec(ctx, fmt.Sprintf("INSERT INTO `%s` (id) VALUES (2)", tablePath))
		close(valueInserted)
	}()

	attempts := 0
	scope.Require.NoError(db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		attempts++

		row, err := tx.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tablePath))
		scope.Require.NoError(err)
		var cnt uint64
		scope.Require.NoError(row.Scan(&cnt))
		fmt.Println("table items count", cnt)

		_, err = reader.PopMessagesBatchTx(ctx, tx)
		scope.Require.NoError(err)
		if attempts == 1 {
			close(messageReaded)
		}

		<-valueInserted
		err = tx.Exec(ctx, fmt.Sprintf("UPSERT INTO `%s` (id) VALUES (3)", tablePath))
		if err != nil {
			t.Log("UPSERT value 3 failed:", err)

			return err
		}

		return nil
	}))
}

func TestTopicReaderUpdateOffsetsIssue(t *testing.T) {
	scope := newScope(t)
	ctx, cancel := context.WithTimeout(scope.Ctx, 10*time.Second)
	defer cancel()

	db := scope.Driver()

	writer := scope.TopicWriter()
	reader := scope.TopicReader()

	scope.Require.NoError(writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("1")}))

	sessionIDAtStart := ""
	scope.Require.NoError(db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		sss := tx.(interface{ SessionID() string })
		if sessionIDAtStart == "" {
			sessionIDAtStart = sss.SessionID()

			scope.DeleteSession(ctx, sessionIDAtStart)
		}

		batch, err := reader.PopMessagesBatchTx(ctx, tx)
		scope.Require.NoError(err)
		scope.Require.Len(batch.Messages, 1)

		return nil
	}))

	t.Run("several reads", func(t *testing.T) {
		scope.Require.NoError(db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			sss := tx.(interface{ SessionID() string })
			if sessionIDAtStart == "" {
				sessionIDAtStart = sss.SessionID()

				scope.DeleteSession(ctx, sessionIDAtStart)
			}

			batch, err := reader.PopMessagesBatchTx(ctx, tx)
			scope.Require.NoError(err)
			scope.Require.Len(batch.Messages, 1)

			scope.Require.NoError(writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("2")}))

			return nil
		}))

		scope.Require.NoError(writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("3")}))

		scope.Require.NoError(db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			sss := tx.(interface{ SessionID() string })
			if sessionIDAtStart == "" {
				sessionIDAtStart = sss.SessionID()

				scope.DeleteSession(ctx, sessionIDAtStart)
			}

			batch, err := reader.PopMessagesBatchTx(ctx, tx)
			scope.Require.NoError(err)
			scope.Require.Len(batch.Messages, 1)

			log.Println(batch.Messages[0].Offset)

			//scope.Require.NoError(writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("2")}))

			return nil
		}))
	})
}

func TestTopicWriterTLI(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	db := scope.Driver()

	tablePath := scope.TablePath()

	messageWritten := make(chan bool)
	valueInserted := make(chan bool)

	go func() {
		<-messageWritten
		db.Query().Exec(ctx, fmt.Sprintf("INSERT INTO `%s` (id) VALUES (2)", tablePath))
		close(valueInserted)
	}()

	attempts := 0
	scope.Require.NoError(db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		attempts++

		row, err := tx.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tablePath))
		scope.Require.NoError(err)
		var cnt uint64
		scope.Require.NoError(row.Scan(&cnt))
		fmt.Println("table items count", cnt)

		txWriter, err := scope.Driver().Topic().StartTransactionalWriter(tx, scope.TopicPath())
		scope.Require.NoError(err)

		err = txWriter.Write(ctx, topicwriter.Message{Data: strings.NewReader("test")})
		scope.Require.NoError(err)
		if attempts == 1 {
			close(messageWritten)
		}

		<-valueInserted
		err = tx.Exec(ctx, fmt.Sprintf("UPSERT INTO `%s` (id) VALUES (3)", tablePath))
		if err != nil {
			t.Log("UPSERT value 3 failed:", err)
			return err
		}

		return nil
	}))

	// Check retries
	scope.Require.Greater(attempts, 1)

	// Verify the message was written
	batch, err := scope.TopicReader().ReadMessagesBatch(ctx)
	scope.Require.NoError(err)
	scope.Require.Len(batch.Messages, 1)
	content, err := io.ReadAll(batch.Messages[0])
	scope.Require.NoError(err)
	scope.Require.Equal("test", string(content))
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
