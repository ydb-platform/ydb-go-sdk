package test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// TestTxWriterUnLazyRace is a regression test for a data race on
// internal/tx.LazyID.v reported in production logs:
//
//	WRITE: (*LazyID).SetTxID -> (*Transaction).UnLazy ->
//	       (*WriterWithTransaction).Write
//	READ:  (*Transaction).ID -> createWriteRequest ->
//	       (*SingleStreamWriter).sendMessagesFromQueueToStreamLoop
//
// The race fires when several transactional topic writers, all sharing one
// lazy transaction created by DoTx, materialize that transaction concurrently
// from independent goroutines. The first writer to win the race calls
// SetTxID; another writer's send loop is meanwhile reading the same id via
// tx.ID() to fill in the WriteRequest.Tx field.
//
// xtest.TestManyTimes loops the scenario so the non-deterministic race is
// reliably surfaced under `go test -race`.
func TestTxWriterUnLazyRace(t *testing.T) {
	srv := mock.Server(t)

	ctx := context.Background()

	db, err := ydb.Open(ctx, srv.ConnString(), ydb.WithAnonymousCredentials())
	require.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	xtest.TestManyTimes(t, func(t testing.TB) {
		const writers = 5

		err = db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			start := make(chan struct{})

			var wg sync.WaitGroup
			errs := make([]error, writers)

			for i := range writers {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					w, startErr := db.Topic().StartTransactionalWriter(tx, "mock-topic")
					if startErr != nil {
						errs[i] = startErr

						return
					}

					<-start

					errs[i] = w.Write(ctx, topicwriter.Message{
						Data: strings.NewReader("payload"),
					})
				}(i)
			}

			close(start)
			wg.Wait()

			for _, e := range errs {
				if e != nil {
					return e
				}
			}

			return nil
		}, query.WithLazyTx(true))
		require.NoError(t, err)
	})
}
