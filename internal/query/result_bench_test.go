package query

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc/metadata"
)

// fakeStream is a lightweight, allocation-free stream stub for benchmarks.
// It serves a fixed slice of pre-built response parts and then returns io.EOF.
type fakeStream struct {
	parts []*Ydb_Query.ExecuteQueryResponsePart
	idx   int
}

func (f *fakeStream) reset() { f.idx = 0 }

func (f *fakeStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	if f.idx >= len(f.parts) {
		return nil, io.EOF
	}

	p := f.parts[f.idx]
	f.idx++

	return p, nil
}

func (f *fakeStream) CloseSend() error             { return nil }
func (f *fakeStream) Context() context.Context     { return context.Background() }
func (f *fakeStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (f *fakeStream) RecvMsg(any) error            { return nil }
func (f *fakeStream) SendMsg(any) error            { return nil }
func (f *fakeStream) Trailer() metadata.MD         { return nil }

// newFakeParts builds a slice of N minimal response parts with a single uint64 column.
func newFakeParts(n int) []*Ydb_Query.ExecuteQueryResponsePart {
	col := &Ydb.Column{
		Name: "a",
		Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
	}
	parts := make([]*Ydb_Query.ExecuteQueryResponsePart, n)
	for i := range parts {
		parts[i] = &Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet: &Ydb.ResultSet{
				Columns: []*Ydb.Column{col},
				Rows: []*Ydb.Value{{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{Uint64Value: uint64(i)},
					}},
				}},
			},
		}
	}

	return parts
}

// BenchmarkResultNextPart measures the cost of creating a stream result,
// consuming all N parts, and closing it.
func BenchmarkResultNextPart(b *testing.B) {
	const numParts = 100

	parts := newFakeParts(numParts)
	ctx := context.Background()

	b.ReportAllocs()
	stream := &fakeStream{parts: parts}

	for i := 0; i < b.N; i++ {
		stream.reset()

		r, err := newResult(ctx, stream)
		require.NoError(b, err)

		for {
			_, err := r.nextPart(ctx)
			if err != nil {
				break
			}
		}

		// Close takes the fast path since the stream is already exhausted.
		if err := r.Close(ctx); err != nil {
			b.Fatal(err)
		}
	}

}
