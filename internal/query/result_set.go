package query

import (
	"context"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ query.ResultSet = (*resultSet)(nil)

type resultSet struct {
	index       int64
	recv        func() (*Ydb_Query.ExecuteQueryResponsePart, error)
	columns     []*Ydb.Column
	currentPart *Ydb_Query.ExecuteQueryResponsePart
	rowIndex    int
	trace       *trace.Query
	done        chan struct{}
}

func newResultSet(
	recv func() (*Ydb_Query.ExecuteQueryResponsePart, error),
	part *Ydb_Query.ExecuteQueryResponsePart,
	t *trace.Query,
) *resultSet {
	if t == nil {
		t = &trace.Query{}
	}

	return &resultSet{
		index:       part.GetResultSetIndex(),
		recv:        recv,
		currentPart: part,
		rowIndex:    -1,
		columns:     part.GetResultSet().GetColumns(),
		trace:       t,
		done:        make(chan struct{}),
	}
}

func (rs *resultSet) nextRow(ctx context.Context) (*row, error) {
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
			if part == nil {
				close(rs.done)

				return nil, xerrors.WithStackTrace(io.EOF)
			}
		}
		if rs.index != rs.currentPart.GetResultSetIndex() {
			close(rs.done)

			return nil, xerrors.WithStackTrace(fmt.Errorf(
				"received part with result set index = %d, current result set index = %d: %w",
				rs.index, rs.currentPart.GetResultSetIndex(), errWrongResultSetIndex,
			))
		}

		return newRow(ctx, rs.columns, rs.currentPart.GetResultSet().GetRows()[rs.rowIndex], rs.trace)
	}
}

func (rs *resultSet) NextRow(ctx context.Context) (_ query.Row, err error) {
	onDone := trace.QueryOnResultSetNextRow(rs.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*resultSet).NextRow"),
	)
	defer func() {
		onDone(err)
	}()

	return rs.nextRow(ctx)
}
