package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// deadSessionCommonConn implements common.Conn for dead-session pool-reuse tests.
// IsValid() always returns false, simulating a query-service session that was
// invalidated by a BAD_SESSION error.
type deadSessionCommonConn struct {
	beginTxFn func() error
}

var _ common.Conn = (*deadSessionCommonConn)(nil)

func (c *deadSessionCommonConn) IsValid() bool  { return false }
func (c *deadSessionCommonConn) ID() string     { return "dead-test-session" }
func (c *deadSessionCommonConn) NodeID() uint32 { return 0 }

func (c *deadSessionCommonConn) Ping(_ context.Context) error { return nil }
func (c *deadSessionCommonConn) Close() error                 { return nil }

func (c *deadSessionCommonConn) Query(
	_ context.Context, _ string, _ *params.Params,
) (common.Rows, error) {
	return nil, c.beginTxFn()
}

func (c *deadSessionCommonConn) Exec(
	_ context.Context, _ string, _ *params.Params,
) (driver.Result, error) {
	return nil, c.beginTxFn()
}

func (c *deadSessionCommonConn) Explain(
	_ context.Context, _ string, _ *params.Params,
) (string, string, error) {
	return "", "", nil
}

func (c *deadSessionCommonConn) BeginTx(_ context.Context, _ driver.TxOptions) (common.Tx, error) {
	return nil, c.beginTxFn()
}

// deadSessionXsqlConnector is a driver.Connector that creates real xsql.Conn
// instances backed by a deadSessionCommonConn. It records how many connections
// were created so tests can assert that dead sessions are discarded rather than
// reused.
type deadSessionXsqlConnector struct {
	cc             *deadSessionCommonConn
	sharedConnCfg  *Connector
	connectCounter int
	onConnect      func(int)
}

var (
	_ driver.Connector = (*deadSessionXsqlConnector)(nil)
	_ driver.Driver    = (*deadSessionXsqlConnector)(nil)
)

func (c *deadSessionXsqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.connectCounter++
	if c.onConnect != nil {
		c.onConnect(c.connectCounter)
	}

	return &Conn{
		processor: QUERY,
		cc:        c.cc,
		ctx:       ctx,
		connector: c.sharedConnCfg,
	}, nil
}

func (c *deadSessionXsqlConnector) Open(string) (driver.Conn, error) { return nil, driver.ErrSkip }

func (c *deadSessionXsqlConnector) Driver() driver.Driver { return c }

// TestXsqlConnIsValidPreventsDeadSessionPoolReuse is a regression test for
// https://github.com/ydb-platform/ydb-go-sdk/issues/2108
// (800+ repeated BAD_SESSION errors on the same invalidated session).
//
// Root cause: xsql.Conn did not implement driver.Validator, so database/sql
// had no way to detect that a session was dead and kept pooling (and reusing)
// the same dead connection across all retry attempts.
//
// Fix: xsql.Conn.IsValid() delegates to the underlying common.Conn.IsValid().
// When it returns false, database/sql discards the connection instead of
// pooling it, forcing a fresh Connect() on the next attempt.
//
// The test uses real xsql.Conn objects so it directly exercises the fix in
// internal/xsql/conn.go:IsValid(). Without the fix (IsValid() absent from
// xsql.Conn), connectCounter would be 1 (same dead session reused 830 times)
// and the assertion would fail. With the fix, connectCounter equals
// maxAttempts (one fresh connection per retry).
func TestXsqlConnIsValidPreventsDeadSessionPoolReuse(t *testing.T) {
	const maxAttempts = 830 // https://github.com/ydb-platform/ydb-go-sdk/issues/2108

	cases := []struct {
		name       string
		beginTxErr error
	}{
		{
			name:       "retryable error",
			beginTxErr: xerrors.Retryable(errors.New("simulated BAD_SESSION error")),
		},
		{
			name:       "BAD_SESSION",
			beginTxErr: xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
		},
		{
			name:       "SESSION_BUSY",
			beginTxErr: xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY)),
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())

			beginTxCounter := 0
			cc := &deadSessionCommonConn{
				beginTxFn: func() error {
					beginTxCounter++
					if beginTxCounter >= maxAttempts {
						cancel()
					}

					return tt.beginTxErr
				},
			}

			// sharedConnCfg holds the configuration used by all xsql.Conn instances
			// created during the test. It is not used as the outer driver.Connector
			// (that role belongs to deadSessionXsqlConnector), so its own Connect()
			// path is never invoked here.
			sharedConnCfg := &Connector{
				trace:     &trace.DatabaseSQL{},
				bindings:  newMockBindings(),
				clock:     clockwork.NewRealClock(),
				processor: QUERY,
				done:      make(chan struct{}),
			}

			bc := &deadSessionXsqlConnector{
				cc:            cc,
				sharedConnCfg: sharedConnCfg,
			}

			db := sql.OpenDB(bc)
			defer func() { _ = db.Close() }()

			err := retry.DoTx(ctx, db,
				func(ctx context.Context, tx *sql.Tx) error {
					return nil
				},
				retry.WithDoTxRetryOptions(
					retry.WithFastBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
					retry.WithSlowBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
					retry.WithIdempotent(true),
				),
			)

			assert.ErrorIs(t, err, context.Canceled)

			// Each retry must create a fresh xsql.Conn because
			// xsql.Conn.IsValid() returns false (delegates to cc.IsValid()),
			// causing database/sql to discard the dead connection rather than
			// pooling it for the next attempt.
			//
			// If IsValid() were removed from xsql.Conn, database/sql would pool
			// the same connection every time → connectCounter == 1 (regression).
			assert.Equal(t, maxAttempts, bc.connectCounter)
			assert.Equal(t, maxAttempts, beginTxCounter)
		})
	}
}
