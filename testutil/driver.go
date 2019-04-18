package testutil

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk/internal"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/grpc/Ydb_Table_V1"
)

var ErrNotImplemented = errors.New("testutil: not implemented")

type MethodCode uint

func (m MethodCode) String() string {
	return codeToString[m]
}

const (
	UnknownMethod MethodCode = iota
	TableCreateSession
	TableDeleteSession
	TableKeepAlive
	TableCreateTable
	TableDropTable
	TableAlterTable
	TableCopyTable
	TableDescribeTable
	TableExplainDataQuery
	TablePrepareDataQuery
	TableExecuteDataQuery
	TableExecuteSchemeQuery
	TableBeginTransaction
	TableCommitTransaction
	TableRollbackTransaction
	TableDescribeTableOptions
	TableStreamReadTable
)

var grpcMethodToCode = map[string]MethodCode{
	Ydb_Table_V1.CreateSession:        TableCreateSession,
	Ydb_Table_V1.DeleteSession:        TableDeleteSession,
	Ydb_Table_V1.KeepAlive:            TableKeepAlive,
	Ydb_Table_V1.CreateTable:          TableCreateTable,
	Ydb_Table_V1.DropTable:            TableDropTable,
	Ydb_Table_V1.AlterTable:           TableAlterTable,
	Ydb_Table_V1.CopyTable:            TableCopyTable,
	Ydb_Table_V1.DescribeTable:        TableDescribeTable,
	Ydb_Table_V1.ExplainDataQuery:     TableExplainDataQuery,
	Ydb_Table_V1.PrepareDataQuery:     TablePrepareDataQuery,
	Ydb_Table_V1.ExecuteDataQuery:     TableExecuteDataQuery,
	Ydb_Table_V1.ExecuteSchemeQuery:   TableExecuteSchemeQuery,
	Ydb_Table_V1.BeginTransaction:     TableBeginTransaction,
	Ydb_Table_V1.CommitTransaction:    TableCommitTransaction,
	Ydb_Table_V1.RollbackTransaction:  TableRollbackTransaction,
	Ydb_Table_V1.DescribeTableOptions: TableDescribeTableOptions,
	Ydb_Table_V1.StreamReadTable:      TableStreamReadTable,
}

var codeToString = map[MethodCode]string{
	TableCreateSession:        lastSegment(Ydb_Table_V1.CreateSession),
	TableDeleteSession:        lastSegment(Ydb_Table_V1.DeleteSession),
	TableKeepAlive:            lastSegment(Ydb_Table_V1.KeepAlive),
	TableCreateTable:          lastSegment(Ydb_Table_V1.CreateTable),
	TableDropTable:            lastSegment(Ydb_Table_V1.DropTable),
	TableAlterTable:           lastSegment(Ydb_Table_V1.AlterTable),
	TableCopyTable:            lastSegment(Ydb_Table_V1.CopyTable),
	TableDescribeTable:        lastSegment(Ydb_Table_V1.DescribeTable),
	TableExplainDataQuery:     lastSegment(Ydb_Table_V1.ExplainDataQuery),
	TablePrepareDataQuery:     lastSegment(Ydb_Table_V1.PrepareDataQuery),
	TableExecuteDataQuery:     lastSegment(Ydb_Table_V1.ExecuteDataQuery),
	TableExecuteSchemeQuery:   lastSegment(Ydb_Table_V1.ExecuteSchemeQuery),
	TableBeginTransaction:     lastSegment(Ydb_Table_V1.BeginTransaction),
	TableCommitTransaction:    lastSegment(Ydb_Table_V1.CommitTransaction),
	TableRollbackTransaction:  lastSegment(Ydb_Table_V1.RollbackTransaction),
	TableDescribeTableOptions: lastSegment(Ydb_Table_V1.DescribeTableOptions),
	TableStreamReadTable:      lastSegment(Ydb_Table_V1.StreamReadTable),
}

func setField(name string, dst interface{}, value interface{}) {
	x := reflect.ValueOf(dst).Elem()
	t := x.Type()
	f, ok := t.FieldByName(name)
	if !ok {
		panic(fmt.Sprintf(
			"ydb/testutil: struct %s has no field %q",
			t, name,
		))
	}
	v := reflect.ValueOf(value)
	if f.Type.Kind() != v.Type().Kind() {
		panic(fmt.Sprintf(
			"ydb/testutil: struct %s field %q is type of %s, not %s",
			t, name, f.Type, v.Type(),
		))
	}
	x.FieldByName(f.Name).Set(v)
}

type TableCreateSessionResult struct {
	R interface{}
}

func (t TableCreateSessionResult) SetSessionID(id string) {
	setField("SessionId", t.R, id)
}

type Driver struct {
	OnCall       func(ctx context.Context, code MethodCode, req, res interface{}) error
	OnStreamRead func(ctx context.Context, code MethodCode, req, res interface{}, process func(error)) error
	OnClose      func() error
}

func (d *Driver) Call(ctx context.Context, op internal.Operation) error {
	if d.OnCall == nil {
		return ErrNotImplemented
	}
	method, req, res := internal.Unwrap(op)
	code := grpcMethodToCode[method]

	// NOTE: req and res may be converted to testutil inner structs, which are
	// mirrors of grpc api envelopes.
	return d.OnCall(ctx, code, req, res)
}

func (d *Driver) StreamRead(ctx context.Context, op internal.StreamOperation) error {
	if d.OnStreamRead == nil {
		return ErrNotImplemented
	}
	method, req, res, processor := internal.UnwrapStreamOperation(op)
	code := grpcMethodToCode[method]

	return d.OnStreamRead(ctx, code, req, res, processor)
}

func (d *Driver) Close() error {
	if d.OnClose == nil {
		return ErrNotImplemented
	}
	return d.OnClose()
}

func lastSegment(m string) string {
	s := strings.Split(m, "/")
	return s[len(s)-1]
}
