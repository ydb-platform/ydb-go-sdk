package query

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Part = (*resultPart)(nil)

type (
	resultPart struct {
		resultSetIndex int64
		columns        []*Ydb.Column
		rows           []*Ydb.Value
		columnNames    []string
		columnTypes    []types.Type
		rowIndex       int
	}
)

func (p *resultPart) ResultSetIndex() int64 {
	return p.resultSetIndex
}

func (p *resultPart) ColumnNames() []string {
	if len(p.columnNames) != 0 {
		return p.columnNames
	}
	names := make([]string, len(p.columns))
	for i, col := range p.columns {
		names[i] = col.GetName()
	}
	p.columnNames = names

	return names
}

func (p *resultPart) ColumnTypes() []types.Type {
	if len(p.columnTypes) != 0 {
		return p.columnTypes
	}
	colTypes := make([]types.Type, len(p.columns))
	for i, col := range p.columns {
		colTypes[i] = types.TypeFromYDB(col.GetType())
	}
	p.columnTypes = colTypes

	return colTypes
}

func (p *resultPart) NextRow(ctx context.Context) (query.Row, error) {
	if p.rowIndex == len(p.rows) {
		return nil, xerrors.WithStackTrace(io.EOF)
	}

	defer func() {
		p.rowIndex++
	}()

	return NewRow(p.columns, p.rows[p.rowIndex]), nil
}

func newResultPart(part *Ydb_Query.ExecuteQueryResponsePart) *resultPart {
	return &resultPart{
		resultSetIndex: part.GetResultSetIndex(),
		columns:        part.GetResultSet().GetColumns(),
		rows:           part.GetResultSet().GetRows(),
		rowIndex:       0,
	}
}
