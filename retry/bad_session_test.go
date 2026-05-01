package retry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// badSessionConn simulates a query-service driver connection whose underlying
// session has been invalidated (BAD_SESSION).  It implements driver.Validator
// so database/sql can detect the dead session and discard it instead of
// returning it to the connection pool.
type badSessionConn struct {
	queryErr error
}

var (
	_ driver.Conn           = (*badSessionConn)(nil)
	_ driver.ConnBeginTx    = (*badSessionConn)(nil)
	_ driver.QueryerContext = (*badSessionConn)(nil)
	_ driver.Validator      = (*badSessionConn)(nil)
)

func (c *badSessionConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *badSessionConn) Close() error                        { return nil }
func (c *badSessionConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (c *badSessionConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return c, nil
}

func (c *badSessionConn) Commit() error   { return nil }
func (c *badSessionConn) Rollback() error { return nil }

func (c *badSessionConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return nil, c.queryErr
}

// IsValid returns false, simulating a session that has been invalidated by a
// BAD_SESSION error from the server.  database/sql calls this before caching
// the connection; returning false causes the connection to be discarded rather
// than returned to the pool.
func (c *badSessionConn) IsValid() bool { return false }

// badSessionConnector creates badSessionConn instances and counts how many
// were created so tests can assert that dead sessions are not reused.
type badSessionConnector struct {
	queryErr error
	created  int
}

var _ driver.Connector = (*badSessionConnector)(nil)

func (bc *badSessionConnector) Open(string) (driver.Conn, error) { return nil, driver.ErrSkip }

func (bc *badSessionConnector) Connect(_ context.Context) (driver.Conn, error) {
	bc.created++

	return &badSessionConn{queryErr: bc.queryErr}, nil
}

func (bc *badSessionConnector) Driver() driver.Driver { return bc }

// TestDoDiscardsDeadSessionAfterBadSessionError is a regression test for the
// case where database/sql kept reusing the same invalidated query-service
// session across retry attempts (causing 800+ identical BAD_SESSION errors).
//
// The root cause was that xsql.Conn did not implement driver.Validator, so
// database/sql had no way to detect that the session was dead and continued
// returning the same connection from its pool on every retry.
//
// Implementing driver.Validator on xsql.Conn so that returning false from
// IsValid() causes database/sql to discard the connection instead of pooling it.
//
// This test verifies the behavior at the database/sql level using a mock
// connector whose connections always report IsValid() == false (dead session).
// With the fix in place, each retry.Do attempt must obtain a fresh connection;
// without the fix the mock would be asked to create only one connection (the
// dead session reused every time).
func TestDoDiscardsDeadSessionAfterBadSessionError(t *testing.T) {
	const maxAttempts = 5

	// Use a retryable error that is NOT driver.ErrBadConn and does NOT wrap
	// the *sql.Conn as invalid (so retry.Do's own mustDeleteConn check returns
	// false). Only database/sql's IsValid() check can cause the connection to
	// be discarded — this is what we are testing here.
	retryableErr := xerrors.Retryable(errors.New("simulated BAD_SESSION error"))

	bc := &badSessionConnector{queryErr: retryableErr}
	db := sql.OpenDB(bc)
	defer func() { _ = db.Close() }()

	attempts := 0
	_ = Do(
		context.Background(), db,
		func(ctx context.Context, cc *sql.Conn) error {
			attempts++
			if attempts >= maxAttempts {
				return nil // stop retrying; the operation succeeds on the last attempt
			}
			rows, err := cc.QueryContext(ctx, "SELECT 1")
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()

			return rows.Err()
		},
		WithDoRetryOptions(
			WithFastBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
			WithSlowBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
			WithIdempotent(true),
		),
	)

	require.Equal(t, maxAttempts, attempts, "retry loop should have run exactly %d times", maxAttempts)

	// Each attempt must use a fresh connection because IsValid() == false
	// causes database/sql to discard the dead session instead of pooling it.
	//
	// If IsValid() were not implemented (the pre-fix state), database/sql would
	// return the same dead connection from the pool every time, and bc.created
	// would equal 1 instead of maxAttempts.
	require.Equal(t, maxAttempts, bc.created,
		"expected one new connection per attempt (%d total), but only %d were created; "+
			"this likely means IsValid() is not being checked and the dead session is reused",
		maxAttempts, bc.created,
	)
}
