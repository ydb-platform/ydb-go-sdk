package query

import (
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// executeQueryPartRecv is one logical item from ExecuteQuery stream (typed Recv).
type executeQueryPartRecv struct {
	part *Ydb_Query.ExecuteQueryResponsePart
	err  error
}

// prefetchExecuteQueryStream reads the inner stream in a background goroutine and
// buffers up to cap(chan) parts so that network I/O can overlap with application
// work between consumer Recv calls.
type prefetchExecuteQueryStream struct {
	Ydb_Query_V1.QueryService_ExecuteQueryClient
	ch       chan executeQueryPartRecv
	pumpOnce sync.Once
}

func newPrefetchExecuteQueryStream(
	inner Ydb_Query_V1.QueryService_ExecuteQueryClient,
	prefetch int,
) *prefetchExecuteQueryStream {
	return &prefetchExecuteQueryStream{
		QueryService_ExecuteQueryClient: inner,
		ch:                              make(chan executeQueryPartRecv, prefetch),
	}
}

func wrapExecuteQueryStreamWithPrefetch(
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	prefetch int,
) Ydb_Query_V1.QueryService_ExecuteQueryClient {
	if prefetch <= 0 {
		return stream
	}
	return newPrefetchExecuteQueryStream(stream, prefetch)
}

func (p *prefetchExecuteQueryStream) startPump() {
	p.pumpOnce.Do(func() {
		go p.pump()
	})
}

func (p *prefetchExecuteQueryStream) pump() {
	defer close(p.ch)
	for {
		part, err := p.QueryService_ExecuteQueryClient.Recv()
		p.ch <- executeQueryPartRecv{part: part, err: err}
		if err != nil {
			return
		}
	}
}

func (p *prefetchExecuteQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	p.startPump()
	item, ok := <-p.ch
	if !ok {
		return nil, io.EOF
	}

	return item.part, item.err
}

func (p *prefetchExecuteQueryStream) RecvMsg(m any) error {
	part, err := p.Recv()
	if err != nil {
		return err
	}
	dst, ok := m.(*Ydb_Query.ExecuteQueryResponsePart)
	if !ok {
		return xerrors.WithStackTrace(fmt.Errorf(
			"ydb/query: prefetch stream RecvMsg expects *Ydb_Query.ExecuteQueryResponsePart, got %T", m,
		))
	}
	proto.Reset(dst)
	if part != nil {
		proto.Merge(dst, part)
	}

	return nil
}
