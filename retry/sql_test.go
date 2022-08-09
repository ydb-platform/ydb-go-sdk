package retry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type mock struct{}

func (c mock) Commit() error {
	return nil
}

func (c mock) Rollback() error {
	return nil
}

func (c mock) Open(name string) (driver.Conn, error) {
	return c, nil
}

func (c mock) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

func (c mock) Close() error {
	return nil
}

func (c mock) Begin() (driver.Tx, error) {
	return c, nil
}

func (c mock) Connect(ctx context.Context) (driver.Conn, error) {
	return c, nil
}

func (c mock) Driver() driver.Driver {
	return c
}

func (c mock) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c, nil
}

func TestDoTxAbortedTLI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	db := sql.OpenDB(mock{})
	var counter int
	if err := DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		counter++
		if counter > 10 {
			return nil
		}
		return xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_ABORTED),
			xerrors.WithIssues([]*Ydb_Issue.IssueMessage{
				{IssueCode: 2001, Message: "Transaction locks invalidated"},
			}),
		)
	}); err != nil {
		t.Errorf("retry operation failed: %v", err)
	}
	if counter <= 1 {
		t.Errorf("nothing attempts: %d", counter)
	}
}
