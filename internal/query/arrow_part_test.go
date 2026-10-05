package query

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestArrowPartOwnership(t *testing.T) {
	readErr := errors.New("read next part")
	for _, boundary := range []string{"part", "skip result set", "EOF", "error", "Close"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(arrowTestPart(0, arrowTestColumns(), "first"), nil)
			first := []*arrowTestBatch{
				{rows: [][]types.Value{
					{types.Int32Value(1), types.OptionalValue(types.TextValue("one"))},
					{types.Int32Value(2), types.OptionalValue(types.TextValue("two"))},
				}},
				{rows: [][]types.Value{{types.Int32Value(3), types.OptionalValue(types.TextValue("three"))}}},
			}
			second := &arrowTestBatch{rows: [][]types.Value{{types.Int32Value(4), types.OptionalValue(types.TextValue("four"))}}}
			decoderCalls := 0
			decoder := func(context.Context, []arrow.Column, io.Reader) ([]arrow.Batch, error) {
				decoderCalls++
				if decoderCalls == 1 {
					return []arrow.Batch{first[0], first[1]}, nil
				}

				return []arrow.Batch{second}, nil
			}
			stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
				for _, batch := range first {
					require.Equal(t, 1, batch.releases)
				}
				switch boundary {
				case "part", "Close":
					return arrowTestPart(0, nil, "second"), nil
				case "skip result set":
					return arrowTestPart(1, arrowTestColumns(), "second"), nil
				case "error":
					return nil, readErr
				default:
					return nil, io.EOF
				}
			})
			stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			r, err := newResult(ctx, stream, withArrowDecoder(decoder))
			require.NoError(t, err)
			rs, err := r.NextResultSet(ctx)
			require.NoError(t, err)
			var saved *string
			var values []types.Value
			lastRow := int32(3)
			if boundary == "skip result set" || boundary == "Close" {
				lastRow = 1
			}
			for i := int32(1); i <= lastRow; i++ {
				row, err := rs.NextRow(ctx)
				require.NoError(t, err)
				var id int32
				require.NoError(t, row.Scan(&id, &saved))
				require.Equal(t, i, id)
				values = row.Values()
				for _, batch := range first {
					require.Zero(t, batch.releases)
				}
			}
			switch boundary {
			case "skip result set":
				rs, err = r.NextResultSet(ctx)
				require.NoError(t, err)

				fallthrough
			case "part":
				row, err := rs.NextRow(ctx)
				require.NoError(t, err)
				var id int32
				require.NoError(t, row.ScanNamed(query.Named("id", &id)))
				require.Equal(t, int32(4), id)
				require.Zero(t, second.releases)
			case "EOF":
				_, err = rs.NextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			case "error":
				_, err = rs.NextRow(ctx)
				require.ErrorIs(t, err, readErr)
			case "Close":
				require.NoError(t, r.Close(ctx))
			}
			require.Equal(t, map[int32]string{1: "one", 3: "three"}[lastRow], *saved)
			var detachedID int32
			require.NoError(t, types.CastTo(values[0], &detachedID))
			require.Equal(t, lastRow, detachedID)
			_ = r.Close(ctx)
			_ = r.Close(ctx)
			for _, batch := range first {
				require.Equal(t, 1, batch.releases)
			}
			if decoderCalls == 2 {
				require.Equal(t, 1, second.releases)
			}
		})
	}
}

func TestArrowPartCloseCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().Return(arrowTestPart(0, arrowTestColumns(), "first"), nil)
	stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
	closed := make(chan struct{})
	var releases atomic.Int32
	batch := &cancelArrowBatch{
		Batch: arrowTestBatches([][]types.Value{{types.Int32Value(1), types.NullValue(types.TypeText)}})[0],
		onRelease: func() {
			if releases.Add(1) == 1 {
				cancel()
				<-closed
			}
		},
	}
	decoder := func(context.Context, []arrow.Column, io.Reader) ([]arrow.Batch, error) {
		return []arrow.Batch{batch}, nil
	}
	r, err := newResult(ctx, stream, withArrowDecoder(decoder), withStreamResultOnClose(func() { close(closed) }))
	require.NoError(t, err)
	rs, err := r.NextResultSet(ctx)
	require.NoError(t, err)
	_, err = rs.NextRow(ctx)
	require.NoError(t, err)
	_ = r.Close(ctx)
	require.Equal(t, int32(1), releases.Load())
}

type cancelArrowBatch struct {
	arrow.Batch

	onRelease func()
}

func (b *cancelArrowBatch) Release() {
	b.onRelease()
	b.Batch.Release()
}
