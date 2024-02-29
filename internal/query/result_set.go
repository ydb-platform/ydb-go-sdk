package query

import (
	"context"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.ResultSet = (*resultSet)(nil)

type resultSet struct {
	index       int64
	recv        func() (*Ydb_Query.ExecuteQueryResponsePart, error)
	columns     []*Ydb.Column
	currentPart *Ydb_Query.ExecuteQueryResponsePart
	rowIndex    int
	done        chan struct{}
}

func newResultSet(
	recv func() (
		*Ydb_Query.ExecuteQueryResponsePart, error,
	),
	part *Ydb_Query.ExecuteQueryResponsePart,
) *resultSet {
	return &resultSet{
		index:       part.GetResultSetIndex(),
		recv:        recv,
		currentPart: part,
		rowIndex:    -1,
		columns:     part.GetResultSet().GetColumns(),
		done:        make(chan struct{}),
	}
}

func (rs *resultSet) next(ctx context.Context) (*row, error) {
	rs.rowIndex++
	select {
	case <-rs.done:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, xerrors.WithStackTrace(ctx.Err())
	default:
		if rs.rowIndex == len(rs.currentPart.GetResultSet().GetRows()) {
			part, err := rs.recv()
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					close(rs.done)
				}

				return nil, xerrors.WithStackTrace(err)
			}
			rs.rowIndex = 0
			rs.currentPart = part
		}
		if rs.index != rs.currentPart.GetResultSetIndex() {
			close(rs.done)

			return nil, xerrors.WithStackTrace(fmt.Errorf(
				"received part with result set index = %d, current result set index = %d: %w",
				rs.index, rs.currentPart.GetResultSetIndex(), errWrongResultSetIndex,
			))
		}

		return newRow(rs.columns, rs.currentPart.GetResultSet().GetRows()[rs.rowIndex])
	}
}

func (rs *resultSet) NextRow(ctx context.Context) (query.Row, error) {
	return rs.next(ctx)
}
