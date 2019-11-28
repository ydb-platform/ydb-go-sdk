package table

import (
	"context"
	"testing"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/testutil"
)

func TestStatementRemoveNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prepared := make(chan struct{}, 1)
	b := StubBuilder{
		T: t,
		Handler: methodHandlers{
			testutil.TablePrepareDataQuery: func(req, res interface{}) error {
				prepared <- struct{}{}
				return nil
			},
			testutil.TableExecuteDataQuery: func(req, res interface{}) error {
				return &ydb.OpError{
					Reason: ydb.StatusNotFound,
				}
			},
		},
	}
	s, err := b.CreateSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertPrepared := func() {
		const timeout = 500 * time.Millisecond
		select {
		case <-prepared:
		case <-time.After(timeout):
			t.Fatalf("no prepare after %s", timeout)
		}
	}
	assertNotPrepared := func() {
		const timeout = 500 * time.Millisecond
		select {
		case <-prepared:
			t.Fatalf("unexpected prepare")
		case <-time.After(timeout):
		}
	}
	prepare := func() *Statement {
		const yql = "SOME YQL TEXT"
		stmt, err := s.Prepare(ctx, yql)
		if err != nil {
			t.Fatal(err)
		}
		return stmt
	}

	stmt1 := prepare()
	assertPrepared()

	prepare()
	assertNotPrepared() // Used from cache.

	_, _, _ = stmt1.Execute(ctx, TxControl(), nil)
	prepare()
	assertPrepared()
}
