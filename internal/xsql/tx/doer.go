package tx

import (
	"context"
	"database/sql"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

type TxOperationFunc func(context.Context, *sql.Tx) error

// TxDoer contains options for retrying transactions.
type TxDoer struct {
	DB      *sql.DB
	Options *sql.TxOptions
}

// Do starts a transaction and calls f with it. If f() call returns a retryable
// error, it repeats it accordingly to retry configuration that TxDoer's DB
// driver holds.
//
// Note that callers should mutate state outside f carefully and keeping in
// mind that f could be called again even if no error returned – transaction
// commitment can be failed:
//
//   var results []int
//   ydb.DoTx(x, db, TxOperationFunc(func(x context.Context, tx *conn.Tx) error {
//       // Reset resulting slice to prevent duplicates when retry occurred.
//       results = results[:0]
//
//       rows, err := tx.QueryContext(...)
//       if err != nil {
//           // handle error
//       }
//       for rows.Next() {
//           results = append(results, ...)
//       }
//       return rows.Err()
//   }))
func (d TxDoer) Do(ctx context.Context, f TxOperationFunc) (err error) {
	return xerrors.Map(retry.Retry(ctx, func(ctx context.Context) (err error) {
		return d.do(ctx, f)
	}))
}

func (d TxDoer) do(ctx context.Context, f TxOperationFunc) error {
	tx, err := d.DB.BeginTx(ctx, d.Options)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	err = f(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// DoTx is a shortcut for calling Do(x, f) on initialized TxDoer with DB
// field set to given db.
func DoTx(ctx context.Context, db *sql.DB, f TxOperationFunc) error {
	return (TxDoer{DB: db}).Do(ctx, f)
}
