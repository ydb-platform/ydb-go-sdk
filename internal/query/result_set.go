package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var (
	_ query.ResultSet = (*resultSet)(nil)
	_ query.ResultSet = (*materializedResultSet)(nil)
)

type (
	materializedResultSet struct {
		index       int
		columnNames []string
		columnTypes []types.Type
		rows        []query.Row
		rowIndex    int
		closeArrow  func()
	}
	resultSet struct {
		index               int64
		recv                func() (*Ydb_Query.ExecuteQueryResponsePart, error)
		columns             []*Ydb.Column
		currentPart         *Ydb_Query.ExecuteQueryResponsePart
		wirePart            *wirePart
		nextWirePart        func() *wirePart
		rowIndex            int
		ended               atomic.Bool
		mustBeLastResultSet bool
		notifyError         func(context.Context, error) error
		decodeArrow         func(context.Context, []*Ydb.Column, *Ydb.ResultSet) ([]*arrowRowData, error)
		arrowBatches        []*arrowRowData
		arrowDecoded        bool
		arrowRowCount       int
		arrowBatchIndex     int
		arrowBatchOffset    int
	}
	resultSetWithClose struct {
		*resultSet

		close func(ctx context.Context) error
	}
)

func rangeRows(ctx context.Context, rs result.Set) xiter.Seq2[result.Row, error] {
	return func(yield func(result.Row, error) bool) {
		for {
			rs, err := rs.NextRow(ctx)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					return
				}
			}
			cont := yield(rs, err)
			if !cont || err != nil {
				return
			}
		}
	}
}

func (rs *materializedResultSet) Close(context.Context) error {
	if rs.closeArrow != nil {
		rs.closeArrow()
	}

	return nil
}

func (rs *resultSetWithClose) Close(ctx context.Context) error {
	return rs.close(ctx)
}

func (rs *materializedResultSet) Rows(ctx context.Context) xiter.Seq2[result.Row, error] {
	return rangeRows(ctx, rs)
}

func (rs *resultSet) Rows(ctx context.Context) xiter.Seq2[result.Row, error] {
	return rangeRows(ctx, rs)
}

func (rs *materializedResultSet) Columns() (columnNames []string) {
	return rs.columnNames
}

func (rs *materializedResultSet) ColumnTypes() []types.Type {
	return rs.columnTypes
}

func (rs *resultSet) ColumnTypes() (columnTypes []types.Type) {
	columnTypes = make([]types.Type, len(rs.columns))
	for i := range rs.columns {
		columnTypes[i] = types.TypeFromYDB(rs.columns[i].GetType())
	}

	return columnTypes
}

func (rs *resultSet) Columns() (columnNames []string) {
	columnNames = make([]string, len(rs.columns))
	for i := range rs.columns {
		columnNames[i] = rs.columns[i].GetName()
	}

	return columnNames
}

func (rs *materializedResultSet) NextRow(ctx context.Context) (query.Row, error) {
	if rs.rowIndex == len(rs.rows) {
		return nil, io.EOF
	}

	defer func() {
		rs.rowIndex++
	}()

	return rs.rows[rs.rowIndex], nil
}

func (rs *materializedResultSet) Index() int {
	if rs == nil {
		return -1
	}

	return rs.index
}

func MaterializedResultSet(
	index int,
	columnNames []string,
	columnTypes []types.Type,
	rows []query.Row,
) *materializedResultSet {
	return &materializedResultSet{
		index:       index,
		columnNames: columnNames,
		columnTypes: columnTypes,
		rows:        rows,
	}
}

func newResultSet(
	recv func() (*Ydb_Query.ExecuteQueryResponsePart, error),
	part *Ydb_Query.ExecuteQueryResponsePart,
) *resultSet {
	return &resultSet{
		index:       part.GetResultSetIndex(),
		recv:        recv,
		currentPart: part,
		rowIndex:    -1,
		columns:     part.GetResultSet().GetColumns(),
	}
}

//nolint:funlen // Keep row transitions and response-part lifetime together.
func (rs *resultSet) nextRow(ctx context.Context) (query.Row, error) {
	rs.rowIndex++
	for {
		if rs.ended.Load() {
			return nil, io.EOF
		}

		if err := ctx.Err(); err != nil {
			if rs.notifyError != nil {
				err = rs.notifyError(ctx, err)
			}

			return nil, xerrors.WithStackTrace(err)
		}

		rowCount, err := rs.partRowCount(ctx)
		if err != nil {
			return nil, err
		}
		//nolint:nestif
		if rs.rowIndex == rowCount {
			part, err := rs.recv()
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					rs.ended.Store(true)
				}

				if rs.mustBeLastResultSet && errors.Is(err, errReadNextResultSet) {
					// prevent detect io.EOF in the error
					return nil, xerrors.WithStackTrace(xerrors.Wrap(errors.New(err.Error())))
				}

				if xerrors.Is(err, io.EOF) {
					return nil, io.EOF
				}

				return nil, xerrors.WithStackTrace(err)
			}
			rs.rowIndex = 0
			rs.currentPart = part
			if rs.nextWirePart != nil {
				rs.wirePart = rs.nextWirePart()
			}
			rs.arrowBatches = nil
			rs.arrowDecoded = false
			if part == nil {
				rs.ended.Store(true)

				return nil, io.EOF
			}
		}
		if rs.currentPart.GetResultSet() != nil && rs.index != rs.currentPart.GetResultSetIndex() {
			rs.ended.Store(true)

			return nil, xerrors.WithStackTrace(fmt.Errorf(
				"received part with result set index = %d, current result set index = %d: %w",
				rs.index, rs.currentPart.GetResultSetIndex(), errWrongResultSetIndex,
			))
		}

		if row := rs.partRow(); row != nil {
			return row, nil
		}
	}
}

func (rs *resultSet) partRowCount(ctx context.Context) (int, error) {
	if rs.currentPart.GetResultSet().GetFormat() != Ydb.ResultSet_FORMAT_ARROW {
		if rs.wirePart != nil {
			return rs.wirePart.RowCount(), nil
		}

		return len(rs.currentPart.GetResultSet().GetRows()), nil
	}
	if !rs.arrowDecoded {
		var err error
		rs.arrowBatches, err = rs.decodeArrow(ctx, rs.columns, rs.currentPart.GetResultSet())
		if err != nil {
			rs.ended.Store(true)
			if rs.notifyError != nil {
				err = rs.notifyError(ctx, err)
			}

			return 0, xerrors.WithStackTrace(err)
		}
		rs.arrowRowCount = 0
		rs.arrowBatchIndex = 0
		rs.arrowBatchOffset = 0
		for _, data := range rs.arrowBatches {
			rs.arrowRowCount += len(data.rows)
		}
		rs.arrowDecoded = true
	}

	return rs.arrowRowCount, nil
}

func (rs *resultSet) partRow() query.Row {
	if rs.currentPart.GetResultSet().GetFormat() == Ydb.ResultSet_FORMAT_ARROW {
		for rs.arrowBatchIndex < len(rs.arrowBatches) {
			data := rs.arrowBatches[rs.arrowBatchIndex]
			index := rs.rowIndex - rs.arrowBatchOffset
			if index < len(data.rows) {
				return &data.rows[index]
			}
			rs.arrowBatchOffset += len(data.rows)
			rs.arrowBatchIndex++
		}
	} else if rs.wirePart != nil && rs.rowIndex < rs.wirePart.RowCount() {
		return rs.wirePart.row(rs.rowIndex, rs.columns)
	} else if rs.rowIndex < len(rs.currentPart.GetResultSet().GetRows()) {
		return NewRow(rs.columns, rs.currentPart.GetResultSet().GetRows()[rs.rowIndex])
	}

	return nil
}

func (rs *resultSet) NextRow(ctx context.Context) (_ query.Row, err error) {
	return rs.nextRow(ctx)
}

func (rs *resultSet) Index() int {
	if rs == nil {
		return -1
	}

	return int(rs.index)
}
