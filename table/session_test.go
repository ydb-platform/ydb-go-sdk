package table

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Scheme"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Table"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
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

func TestSessionKeepAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		status Ydb_Table.KeepAliveResult_SessionStatus
		e      error
	)
	b := StubBuilder{
		T: t,
		Handler: methodHandlers{
			testutil.TableKeepAlive: func(req, res interface{}) error {
				r, _ := res.(*Ydb_Table.KeepAliveResult)
				r.SessionStatus = status

				return e
			},
		},
	}
	s, err := b.CreateSession(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		e = errors.New("any error")
		_, err := s.KeepAlive(ctx)
		if err == nil {
			t.Fatal(err)
		}
	}
	{
		status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_READY, nil
		info, err := s.KeepAlive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != SessionReady {
			t.Fatalf("Result %v differ from, expectd %v", info.Status, SessionReady)
		}
	}
	{
		status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY, nil
		info, err := s.KeepAlive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != SessionBusy {
			t.Fatalf("Result %v differ from, expectd %v", info.Status, SessionBusy)
		}
	}
}

func TestSessionDescribeTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		result *Ydb_Table.DescribeTableResult
		e      error
	)
	b := StubBuilder{
		T: t,
		Handler: methodHandlers{
			testutil.TableDescribeTable: func(req, res interface{}) error {
				r, _ := res.(*Ydb_Table.DescribeTableResult)
				r.Reset()
				proto.Merge(r, result)
				return e
			},
		},
	}
	s, err := b.CreateSession(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		e = errors.New("any error")
		_, err = s.DescribeTable(ctx, "")
		if err == nil {
			t.Fatal(err)
		}
	}
	{
		e = nil
		expect := Description{
			Name:       "testName",
			PrimaryKey: []string{"testKey"},
			Columns: []Column{
				{
					Name: "testColumn",
					Type: ydb.Void(),
				},
			},
			KeyRanges: []KeyRange{
				{
					From: nil,
					To:   ydb.Int64Value(100500),
				},
				{
					From: ydb.Int64Value(100500),
					To:   nil,
				},
			},
		}
		result = &Ydb_Table.DescribeTableResult{
			Self: &Ydb_Scheme.Entry{
				Name:                 expect.Name,
				Owner:                "",
				Type:                 0,
				EffectivePermissions: nil,
				Permissions:          nil,
			},
			Columns: []*Ydb_Table.ColumnMeta{
				{
					Name:   expect.Columns[0].Name,
					Type:   internal.TypeToYDB(expect.Columns[0].Type),
					Family: "",
				},
			},
			PrimaryKey: expect.PrimaryKey,
			ShardKeyBounds: []*Ydb.TypedValue{
				internal.ValueToYDB(expect.KeyRanges[0].To),
			},
			Indexes:    nil,
			TableStats: nil,
		}

		d, err := s.DescribeTable(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(d, expect) {
			t.Fatalf("Result %+v differ from, expectd %+v", d, expect)
		}
	}
}
