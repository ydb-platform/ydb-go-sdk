package ydbsql

import (
	"context"
	"database/sql/driver"

	"github.com/yandex-cloud/ydb-go-sdk"
)

// retryer contains logic of retrying operations failed with retriable errors.
type retryer struct {
	// MaxRetries is a number of maximum attempts to retry a failed operation.
	// If MaxRetries is zero then no attempts will be made.
	MaxRetries int

	// RetryChecker contains options of mapping errors to retry mode.
	RetryChecker ydb.RetryChecker

	// Backoff is a selected backoff policy.
	// If backoff is nil, then the DefaultBackoff is used.
	Backoff ydb.Backoff
}

// Do calls op.Do until it return nil or not retriable error.
func (r *retryer) do(ctx context.Context, f func(context.Context) error) (err error) {
	var m ydb.RetryMode
	for i := 0; i <= r.MaxRetries; i++ {
		if err = f(ctx); err == nil {
			return nil
		}
		if err == driver.ErrBadConn {
			// ErrBadConn may be returned by f() when we are within transaction
			// execution.
			//
			// Thus we may retry whole transaction by calling f() again.
			continue
		}
		if m = r.RetryChecker.Check(err); !m.Retriable() {
			return err
		}
		if m.MustDeleteSession() {
			// BadSession error may be returned by direct Query()/Exec() calls.
			// Not within transaction execution.
			//
			// Because connection represents single session, we (actually,
			// database/sql package) must return immediately and probably
			// create new one session.
			return err
		}
		if m.MustBackoff() {
			if e := ydb.WaitBackoff(ctx, r.Backoff, i); e != nil {
				// Return original error to make it possible to lay on for the
				// client.
				return err
			}
		}
	}
	return err
}
