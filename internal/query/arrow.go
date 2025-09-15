package query

import (
	"bytes"
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
)

type (
	arrowResult struct {
		stream         Ydb_Query_V1.QueryService_ExecuteQueryClient
		resultSetIndex int64
		close          context.CancelFunc
	}

	arrowPart struct {
		resultSetIndex int64
		reader         io.Reader
		data           []byte
	}
)

func (r *arrowResult) Parts(ctx context.Context) xiter.Seq2[arrow.Part, error] {
	return func(yield func(arrow.Part, error) bool) {
		for {
			part, err := r.nextPart(ctx)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					return
				}
			}

			cont := yield(part, err)
			if !cont || err != nil {
				return
			}
		}
	}
}

func (r *arrowResult) nextPart(ctx context.Context) (arrow.Part, error) {
	if ctx.Err() != nil {
		return nil, xerrors.WithStackTrace(ctx.Err())
	}

	part, err := r.stream.Recv()
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if part.GetResultSetIndex() <= 0 && r.resultSetIndex > 0 {
		return nil, xerrors.WithStackTrace(io.EOF)
	}
	r.resultSetIndex = part.GetResultSetIndex()

	schema := part.GetResultSet().GetArrowFormatMeta().GetSchema()

	// Apache Arrow ipc.Reader expects schema and data to be concatenated
	data := append(schema, part.GetResultSet().GetData()...)

	rdr := bytes.NewReader(data)

	return &arrowPart{reader: rdr, data: data, resultSetIndex: part.GetResultSetIndex()}, nil
}

func (r *arrowResult) Close(ctx context.Context) error {
	r.close()

	return nil
}

func (p *arrowPart) Bytes() []byte {
	return p.data
}

func (p *arrowPart) Read(buf []byte) (n int, err error) {
	return p.reader.Read(buf)
}

func (p *arrowPart) GetResultSetIndex() int64 {
	return p.resultSetIndex
}
