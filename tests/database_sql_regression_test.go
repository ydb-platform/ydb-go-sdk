//go:build !fast
// +build !fast

package tests

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestRegressionCloud109307(t *testing.T) {
	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 42*time.Second)
	defer cancel()

	for i := int64(1); ; i++ {
		if ctx.Err() != nil {
			break
		}

		if err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			//nolint:gosec
			if rand.Int31n(3) == 0 {
				return badconn.Map(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)))
			}
			var rows *sql.Rows
			rows, err = tx.QueryContext(ctx, `
				DECLARE $i AS Int64;

				SELECT $i;
			`, sql.Named("i", i))
			if err != nil {
				return err
			}
			defer rows.Close()
			if !rows.Next() {
				return errors.New("no rows")
			}
			var result interface{}
			if err = rows.Scan(&result); err != nil {
				return err
			}
			if result.(int64)%100 == 0 {
				t.Logf("result: %+v\n", result)
			}
			return rows.Err()
		}, retry.WithTxOptions(&sql.TxOptions{
			Isolation: sql.LevelSnapshot,
			ReadOnly:  true,
		}), retry.WithDoTxRetryOptions(
			retry.WithIdempotent(true),
		)); err != nil {
			if ctx.Err() == nil {
				t.Fatalf("error: %+v\n", err)
			}
		}
	}
}
