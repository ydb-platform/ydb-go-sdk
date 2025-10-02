//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestTLI(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	db := scope.Driver()
	tablePath := scope.TablePath()

	t1 := make(chan string)
	t2 := make(chan string)
	//await := make(chan struct{})

	//var done struct{}

	wg := sync.WaitGroup{}

	// t1
	wg.Go(func() {
		err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			for yql := range t1 {
				err := tx.Exec(ctx, yql)

				require.NoError(t, err)
			}

			return nil
		})
		require.NoError(t, err)
	})

	// t2
	wg.Go(func() {
		err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			for yql := range t2 {
				err := tx.Exec(ctx, yql)

				require.NoError(t, err)
			}

			return nil
		})
		require.NoError(t, err)
	})

	//<-await
	t2 <- fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (2, 'y')", tablePath)
	t2 <- fmt.Sprintf("INSERT INTO `%s` (id, val) VALUES (2, 'y')", tablePath)
	t1 <- fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (2, 'x')", tablePath)
	t2 <- fmt.Sprintf("UPSERT INTO `%s` (id, val) VALUES (2, 'y')", tablePath)
	//<-await

	close(t1)
	close(t2)

	wg.Wait()
}

func TestTopicReaderTLI(t *testing.T) {
	os.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "info")
	os.Setenv("GRPC_GO_LOG_VERBOSITY_LEVEL", "99")
	scope := newScope(t)
	ctx := scope.Ctx
	db := scope.DriverWithLogs()

	tablePath := scope.TablePath()

	writer := scope.TopicWriter()
	reader := scope.TopicReader()

	messages := make([]topicwriter.Message, 500)
	for i := range 500 {
		messages[i] = topicwriter.Message{Data: strings.NewReader("1")}
	}

	scope.Require.NoError(writer.Write(ctx, messages...))

	messageReaded := make(chan bool)
	valueInserted := make(chan bool)

	go func() {
		<-messageReaded
		err := db.Query().Exec(ctx, fmt.Sprintf("INSERT INTO `%s` (id) VALUES (2)", tablePath))
		require.NoError(t, err)
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

	t.Log("attempts", attempts)

	scope.Require.NoError(writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("2")}))

	scope.Require.NoError(db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		t.Log("ATTEMPT!!!!!------")
		_, err := reader.PopMessagesBatchTx(ctx, tx)
		return err
	}))
}
