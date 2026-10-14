package kv

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"xorm.io/xorm"
	"xorm.io/xorm/core"

	"slo/internal/framework"
	"slo/internal/generator"
)

func TestTableAdvancePreservesUnstoredCancellationAfterResponse(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			res := &unaryRowsFixture{rows: []generator.Row{dataFixture(1)}}
			var ctx context.Context
			var cancel context.CancelFunc
			if errors.Is(cause, context.Canceled) {
				ctx, cancel = context.WithCancel(context.Background())
				cancel() // response already exists; no advance has occurred
			} else {
				ctx, cancel = context.WithDeadline(context.Background(), time.Unix(0, 0))
			}
			defer cancel()
			rows, err := ReadTableRows(ctx, res)
			if !errors.Is(err, cause) || rows != nil || res.Err() != nil || res.advances != 1 {
				t.Fatalf("advance lost original cause: rows=%v err=%v stored=%v advances=%d", rows, err, res.Err(), res.advances)
			}
		})
	}
}

func TestTableRowsDrainBeforeCheckingData(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []generator.Row
		err  error
	}{
		{name: "healthy", rows: []generator.Row{dataFixture(1)}},
		{name: "empty"},
		{name: "extra", rows: []generator.Row{dataFixture(1), dataFixture(2)}},
		{name: "trailing error", rows: []generator.Row{dataFixture(1)}, err: errDataSDK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := &unaryRowsFixture{rows: tc.rows, trailing: tc.err}
			rows, err := ReadTableRows(context.Background(), res)
			if !errors.Is(err, tc.err) {
				t.Fatalf("iterator error = %v, want %v", err, tc.err)
			}
			if tc.err != nil {
				return
			}
			if len(rows) != len(tc.rows) || res.advances != 2 {
				t.Fatalf("incomplete iteration: %d rows, %d advances", len(rows), res.advances)
			}
			if err := CheckRows([]generator.Row{dataFixture(1)}, rows); (err == nil) != (tc.name == "healthy") {
				t.Fatalf("completed-result verdict = %v", err)
			}
		})
	}
}

func TestTableAdvanceCancellationRemainsCausalShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := &unaryRowsFixture{rows: []generator.Row{dataFixture(1)}}
	called := false
	err := framework.RunWorkers(ctx, 1, framework.NewRateLimiter(1), func(ctx context.Context) error {
		called = true
		cancel() // successful response received, immediately before first advance
		_, err := ReadTableRows(ctx, res)

		return err
	})
	if err != nil || !called || res.advances != 1 {
		t.Fatalf("planned shutdown = %v, called=%v advances=%d", err, called, res.advances)
	}
}

func TestXORMRowsMaterializeTrailingRowsAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rows     []generator.Row
		trailing error
		closeErr error
	}{
		{name: "healthy", rows: []generator.Row{dataFixture(1)}},
		{name: "empty"},
		{name: "extra", rows: []generator.Row{dataFixture(1), dataFixture(2)}},
		{name: "duplicate", rows: []generator.Row{dataFixture(1), dataFixture(1)}},
		{name: "trailing error", rows: []generator.Row{dataFixture(1)}, trailing: errDataSDK},
		{name: "close error", rows: []generator.Row{dataFixture(1)}, closeErr: errDataSDK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &xormRowsFixture{rows: tc.rows, trailing: tc.trailing, closeErr: tc.closeErr}
			raw := sql.OpenDB(&xormConnectorFixture{fixture: fixture})
			t.Cleanup(func() { _ = raw.Close() })
			engine, err := xorm.NewEngineWithDB("ydb", "grpc://unused:2136/unused", core.FromDB(raw))
			if err != nil {
				t.Fatal(err)
			}
			rows, err := ReadXORMRows(context.Background(), engine, 1)
			wantErr := tc.trailing
			if wantErr == nil {
				wantErr = tc.closeErr
			}
			if !errors.Is(err, wantErr) || !fixture.closed || fixture.nextCalls != len(tc.rows)+1 {
				t.Fatalf("XORM result: err=%v want=%v closed=%v advances=%d", err, wantErr, fixture.closed, fixture.nextCalls)
			}
			if strings.Contains(strings.ToUpper(fixture.query), "LIMIT") || len(fixture.args) != 2 {
				t.Fatalf("XORM query did not select complete controlled key: %q %v", fixture.query, fixture.args)
			}
			for _, arg := range fixture.args {
				if arg.Value != int64(1) {
					t.Fatalf("wrong controlled-key argument: %v", arg)
				}
			}
			if wantErr != nil {
				return
			}
			if len(rows) != len(tc.rows) {
				t.Fatalf("XORM discarded trailing rows: got %d, want %d", len(rows), len(tc.rows))
			}
			for i := range rows {
				if err := CheckRow(tc.rows[i], rows[i]); err != nil {
					t.Fatal(err)
				}
			}
			if err := CheckRows([]generator.Row{dataFixture(1)}, rows); (err == nil) != (tc.name == "healthy") {
				t.Fatalf("completed XORM result verdict = %v", err)
			}
		})
	}
}

type unaryRowsFixture struct {
	result.BaseResult

	rows     []generator.Row
	trailing error
	stored   error
	advances int
	index    int
}

func (r *unaryRowsFixture) NextResultSetErr(ctx context.Context, _ ...string) error {
	r.advances++
	if r.advances > 1 {
		return io.EOF
	}

	return ctx.Err() // actual unary SDK behavior: not stored in Err
}

func (r *unaryRowsFixture) NextResultSet(ctx context.Context, columns ...string) bool {
	return r.NextResultSetErr(ctx, columns...) == nil
}

func (r *unaryRowsFixture) Err() error { return r.stored }

func (r *unaryRowsFixture) CurrentResultSet() result.Set { return &untruncatedSetFixture{} }

func (r *unaryRowsFixture) NextRow() bool {
	if r.index == len(r.rows) {
		r.stored = r.trailing

		return false
	}
	r.index++

	return true
}

func (r *unaryRowsFixture) ScanNamed(values ...named.Value) error {
	row := CloneRow(r.rows[r.index-1])
	for _, value := range values {
		switch value.Name {
		case "id":
			*value.Value.(*uint64) = row.ID
		case "payload_str":
			*value.Value.(**string) = row.PayloadStr
		case "payload_double":
			*value.Value.(**float64) = row.PayloadDouble
		case "payload_timestamp":
			*value.Value.(**time.Time) = row.PayloadTimestamp
		case "payload_hash":
			*value.Value.(**uint64) = row.PayloadHash
		}
	}

	return nil
}

type untruncatedSetFixture struct{ result.Set }

func (*untruncatedSetFixture) Truncated() bool { return false }

type xormConnectorFixture struct{ fixture *xormRowsFixture }

func (c *xormConnectorFixture) Connect(context.Context) (driver.Conn, error) {
	return &xormConnFixture{fixture: c.fixture}, nil
}

func (c *xormConnectorFixture) Driver() driver.Driver { return &xormDriverFixture{connector: c} }

type xormDriverFixture struct{ connector *xormConnectorFixture }

func (d *xormDriverFixture) Open(string) (driver.Conn, error) {
	return d.connector.Connect(context.Background())
}

type xormConnFixture struct {
	driver.Conn

	fixture *xormRowsFixture
}

func (*xormConnFixture) Close() error { return nil }

func (c *xormConnFixture) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.fixture.query = query
	c.fixture.args = args

	return c.fixture, nil
}

type xormRowsFixture struct {
	rows      []generator.Row
	trailing  error
	closeErr  error
	closed    bool
	nextCalls int
	index     int
	query     string
	args      []driver.NamedValue
}

func (*xormRowsFixture) Columns() []string {
	return []string{"hash", "id", "payload_str", "payload_hash", "payload_double", "payload_timestamp"}
}

func (r *xormRowsFixture) Close() error {
	r.closed = true

	return r.closeErr
}

func (r *xormRowsFixture) Next(values []driver.Value) error {
	r.nextCalls++
	if r.index == len(r.rows) {
		if r.trailing != nil {
			return r.trailing
		}

		return io.EOF
	}
	row := r.rows[r.index]
	r.index++
	values[0], values[1] = int64(row.Hash), int64(row.ID)
	values[2], values[3] = *row.PayloadStr, int64(*row.PayloadHash)
	values[4], values[5] = *row.PayloadDouble, *row.PayloadTimestamp

	return nil
}
