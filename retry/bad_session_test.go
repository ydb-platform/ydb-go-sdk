package retry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
)

// badSessionConn simulates a query-service driver connection whose underlying
// session has been invalidated (BAD_SESSION).  It implements driver.Validator
// so database/sql can detect the dead session and discard it instead of
// returning it to the connection pool.
type (
	badSessionConn struct {
		beginErr       error
		beginTxCounter int
		onBeginTx      func(beginTxCounter int)
	}
	badSessionConnValidator struct {
		driver.Conn
	}
)

func (badSessionConnValidator) IsValid() bool {
	return false
}

var (
	_ driver.Conn           = (*badSessionConn)(nil)
	_ driver.ConnBeginTx    = (*badSessionConn)(nil)
	_ driver.QueryerContext = (*badSessionConn)(nil)
	_ driver.Validator      = (*badSessionConnValidator)(nil)
)

func (c *badSessionConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *badSessionConn) Close() error {
	return nil
}

func (c *badSessionConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *badSessionConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.beginTxCounter++

	c.onBeginTx(c.beginTxCounter)

	return nil, c.beginErr
}

func (c *badSessionConn) Commit() error   { return nil }
func (c *badSessionConn) Rollback() error { return nil }

func (c *badSessionConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return nil, c.beginErr
}

// badSessionConnector creates badSessionConn instances and counts how many
// were connectCounter so tests can assert that dead sessions are not reused.
type badSessionConnector struct {
	driver.Conn

	connectCounter int
	onConnect      func(createdCounter int)
}

var _ driver.Connector = (*badSessionConnector)(nil)

func (bc *badSessionConnector) Open(string) (driver.Conn, error) { return nil, driver.ErrSkip }

func (bc *badSessionConnector) Connect(_ context.Context) (driver.Conn, error) {
	bc.connectCounter++

	bc.onConnect(bc.connectCounter)

	return bc.Conn, nil
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
	const maxAttempts = 830 // https://github.com/ydb-platform/ydb-go-sdk/issues/2108
	cases := []struct {
		name            string
		err             error
		expConnectCount int
		expBeginTxCount int
	}{
		{
			name:            "retryable error with no validator",
			err:             xerrors.Retryable(errors.New("simulated BAD_SESSION error")),
			expConnectCount: 1,
			expBeginTxCount: maxAttempts,
		},
		{
			name:            "BAD_SESSION error with no validator",
			err:             xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
			expConnectCount: 1,
			expBeginTxCount: maxAttempts,
		},
		{
			name:            "SESSION_BUSY error with no validator",
			err:             xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY)),
			expConnectCount: 1,
			expBeginTxCount: maxAttempts,
		},
		{
			name:            "mapped retryable error with no validator",
			err:             badconn.Map(xerrors.Retryable(errors.New("simulated BAD_SESSION error"))),
			expConnectCount: 1,
			expBeginTxCount: maxAttempts,
		},
		{
			name:            "mapped BAD_SESSION error with no validator",
			err:             badconn.Map(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))),
			expConnectCount: maxAttempts,
			expBeginTxCount: maxAttempts,
		},
		{
			name:            "mapped SESSION_BUSY error with no validator",
			err:             badconn.Map(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY))),
			expConnectCount: maxAttempts,
			expBeginTxCount: maxAttempts,
		},
	}
	t.Run("WithoutValidator", func(t *testing.T) {
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())

				conn := &badSessionConn{
					beginErr: tt.err,
					onBeginTx: func(beginTxCounter int) {
						if beginTxCounter >= maxAttempts {
							cancel()
						}
					},
				}

				bc := &badSessionConnector{
					Conn: conn,
					onConnect: func(connectCounter int) {
						if connectCounter >= maxAttempts {
							cancel()
						}
					},
				}

				db := sql.OpenDB(bc)
				defer func() { _ = db.Close() }()

				err := DoTx(ctx, db,
					func(ctx context.Context, tx *sql.Tx) error {
						return nil
					},
					WithDoTxRetryOptions(
						WithFastBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
						WithSlowBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
						WithIdempotent(true),
					),
				)

				assert.ErrorIs(t, err, context.Canceled)
				assert.Equal(t, tt.expConnectCount, bc.connectCounter)
				assert.Equal(t, tt.expBeginTxCount, conn.beginTxCounter)
			})
		}
	})
	t.Run("WithValidator", func(t *testing.T) {
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())

				conn := &badSessionConn{
					beginErr: tt.err,
					onBeginTx: func(beginTxCounter int) {
						if beginTxCounter >= maxAttempts {
							cancel()
						}
					},
				}

				bc := &badSessionConnector{
					Conn: &badSessionConnValidator{
						Conn: conn,
					},
					onConnect: func(connectCounter int) {
						if connectCounter >= maxAttempts {
							cancel()
						}
					},
				}

				db := sql.OpenDB(bc)
				defer func() { _ = db.Close() }()

				err := DoTx(ctx, db,
					func(ctx context.Context, tx *sql.Tx) error {
						return nil
					},
					WithDoTxRetryOptions(
						WithFastBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
						WithSlowBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
						WithIdempotent(true),
					),
				)

				assert.ErrorIs(t, err, context.Canceled)
				assert.Equal(t, maxAttempts, bc.connectCounter)
				assert.Equal(t, maxAttempts, conn.beginTxCounter)
			})
		}
	})
}
