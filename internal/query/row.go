package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ query.Row = (*row)(nil)

type row struct {
	ctx   context.Context //nolint:containedctx
	trace *trace.Query

	indexedScanner scanner.IndexedScanner
	namedScanner   scanner.NamedScanner
	structScanner  scanner.StructScanner
}

func NewRow(ctx context.Context, columns []*Ydb.Column, v *Ydb.Value, t *trace.Query) (*row, error) {
	data := scanner.Data(columns, v.GetItems())

	return &row{
		ctx:            ctx,
		trace:          t,
		indexedScanner: scanner.Indexed(data),
		namedScanner:   scanner.Named(data),
		structScanner:  scanner.Struct(data),
	}, nil
}

func (r row) Scan(dst ...interface{}) (err error) {
	var (
		ctx    = r.ctx
		onDone = trace.QueryOnRowScan(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.row.Scan"),
		)
	)
	defer func() {
		onDone(err)
	}()

	return r.indexedScanner.Scan(dst...)
}

func (r row) ScanNamed(dst ...scanner.NamedDestination) (err error) {
	var (
		ctx    = r.ctx
		onDone = trace.QueryOnRowScanNamed(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.row.ScanNamed"),
		)
	)
	defer func() {
		onDone(err)
	}()

	return r.namedScanner.ScanNamed(dst...)
}

func (r row) ScanStruct(dst interface{}, opts ...scanner.ScanStructOption) (err error) {
	var (
		ctx    = r.ctx
		onDone = trace.QueryOnRowScanStruct(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.row.ScanStruct"),
		)
	)
	defer func() {
		onDone(err)
	}()

	return r.structScanner.ScanStruct(dst, opts...)
}
