package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync/atomic"
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
func (c *deadSessionCommonConn) Close(context.Context) error  { return nil }

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
// Fix: xsql.Conn checks the underlying common.Conn before starting work and
// implements driver.Validator so database/sql discards invalid connections.
//
// The test uses real xsql.Conn objects so it directly exercises the fix in
// internal/xsql/conn.go:IsValid(). No BeginTx RPC must reach a known-dead
// session, and every database/sql retry must request a fresh connection.
func TestXsqlConnIsValidPreventsDeadSessionPoolReuse(t *testing.T) {
	const maxAttempts = 830 // https://github.com/ydb-platform/ydb-go-sdk/issues/2108

	ctx, cancel := context.WithCancel(t.Context())
	beginTxCounter := 0
	cc := &deadSessionCommonConn{
		beginTxFn: func() error {
			beginTxCounter++

			return nil
		},
	}
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
		onConnect: func(attempt int) {
			if attempt >= maxAttempts {
				cancel()
			}
		},
	}
	db := sql.OpenDB(bc)
	t.Cleanup(func() { _ = db.Close() })

	err := retry.DoTx(ctx, db,
		func(context.Context, *sql.Tx) error { return nil },
		retry.WithDoTxRetryOptions(
			retry.WithFastBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
			retry.WithSlowBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
			retry.WithIdempotent(true),
		),
	)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, maxAttempts, bc.connectCounter)
	assert.Zero(t, beginTxCounter)
}

type asyncShutdownCommonConn struct {
	nodeID           uint32
	valid            atomic.Bool
	execCalls        atomic.Int64
	invalidExecCalls atomic.Int64
}

var _ common.Conn = (*asyncShutdownCommonConn)(nil)

func newAsyncShutdownCommonConn(nodeID uint32) *asyncShutdownCommonConn {
	c := &asyncShutdownCommonConn{nodeID: nodeID}
	c.valid.Store(true)

	return c
}

func (c *asyncShutdownCommonConn) IsValid() bool  { return c.valid.Load() }
func (c *asyncShutdownCommonConn) ID() string     { return "async-shutdown-session" }
func (c *asyncShutdownCommonConn) NodeID() uint32 { return c.nodeID }

func (c *asyncShutdownCommonConn) Ping(context.Context) error  { return nil }
func (c *asyncShutdownCommonConn) Close(context.Context) error { return nil }

func (c *asyncShutdownCommonConn) Query(
	context.Context, string, *params.Params,
) (common.Rows, error) {
	return nil, nil
}

func (c *asyncShutdownCommonConn) Exec(
	context.Context, string, *params.Params,
) (driver.Result, error) {
	c.execCalls.Add(1)
	if !c.valid.Load() {
		c.invalidExecCalls.Add(1)

		return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
	}

	return driver.RowsAffected(0), nil
}

func (c *asyncShutdownCommonConn) Explain(
	context.Context, string, *params.Params,
) (string, string, error) {
	return "", "", nil
}

func (c *asyncShutdownCommonConn) BeginTx(context.Context, driver.TxOptions) (common.Tx, error) {
	return nil, nil
}

type asyncShutdownConnector struct {
	sharedConnCfg *Connector
	connections   []*asyncShutdownCommonConn
}

var (
	_ driver.Connector = (*asyncShutdownConnector)(nil)
	_ driver.Driver    = (*asyncShutdownConnector)(nil)
)

func (c *asyncShutdownConnector) Connect(ctx context.Context) (driver.Conn, error) {
	cc := newAsyncShutdownCommonConn(uint32(42 + len(c.connections)))
	c.connections = append(c.connections, cc)

	conn := &Conn{
		processor: QUERY,
		cc:        cc,
		ctx:       ctx,
		connector: c.sharedConnCfg,
	}

	return c.sharedConnCfg.registerConn(conn), nil
}

func (c *asyncShutdownConnector) Open(string) (driver.Conn, error) { return nil, driver.ErrSkip }
func (c *asyncShutdownConnector) Driver() driver.Driver            { return c }

func TestXsqlConnResetSessionDiscardsSessionInvalidatedWhileIdle(t *testing.T) {
	connector := &asyncShutdownConnector{
		sharedConnCfg: &Connector{
			trace:     &trace.DatabaseSQL{},
			bindings:  newMockBindings(),
			clock:     clockwork.NewRealClock(),
			processor: QUERY,
			done:      make(chan struct{}),
		},
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })

	_, err := db.ExecContext(t.Context(), "SELECT 1")
	assert.NoError(t, err)
	assert.Len(t, connector.connections, 1)

	// Simulate endpoint pessimization after database/sql has already put the
	// connection into its idle pool.
	first := connector.connections[0]
	connector.sharedConnCfg.OnConnBanned(first.NodeID())

	_, err = db.ExecContext(t.Context(), "SELECT 1")
	assert.NoError(t, err)
	assert.Len(t, connector.connections, 2)
	assert.EqualValues(t, 1, first.execCalls.Load())
	assert.Zero(t, first.invalidExecCalls.Load())
}
