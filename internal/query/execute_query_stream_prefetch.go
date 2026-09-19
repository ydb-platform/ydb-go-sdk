package query

import (
	"fmt"
	"io"

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

type asyncPrefetchExecuteQueryStream struct {
	Ydb_Query_V1.QueryService_ExecuteQueryClient

	ch       chan executeQueryPartRecv
	terminal chan executeQueryPartRecv
}

func wrapExecuteQueryStreamWithAsyncPrefetch(
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	prefetch int,
) Ydb_Query_V1.QueryService_ExecuteQueryClient {
	if prefetch <= 0 {
		return stream
	}
	s := &asyncPrefetchExecuteQueryStream{
		QueryService_ExecuteQueryClient: stream,
		ch:                              make(chan executeQueryPartRecv, prefetch),
		terminal:                        make(chan executeQueryPartRecv, 1),
	}

	go s.pump()

	return s
}

func (p *asyncPrefetchExecuteQueryStream) pump() {
	defer close(p.ch)
	defer close(p.terminal)
	ctx := p.QueryService_ExecuteQueryClient.Context()
	for {
		part, err := p.QueryService_ExecuteQueryClient.Recv()
		item := executeQueryPartRecv{part: part, err: err}
		if err != nil {
			// Preserve the terminal receive result even when Recv cancels the
			// stream context itself. The consumer must observe the operation or
			// transport error instead of an indistinguishable io.EOF.
			p.terminal <- item

			return
		}

		select {
		case p.ch <- item:
		case <-ctx.Done():
			return
		}
	}
}

func (p *asyncPrefetchExecuteQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	item, ok := <-p.ch
	if ok {
		return item.part, item.err
	}
	item, ok = <-p.terminal
	if ok {
		return item.part, item.err
	}

	return nil, io.EOF
}

func (p *asyncPrefetchExecuteQueryStream) RecvMsg(m any) error {
	part, err := p.Recv()
	if err != nil {
		return err
	}
	dst, ok := m.(*Ydb_Query.ExecuteQueryResponsePart)
	if !ok {
		return xerrors.WithStackTrace(fmt.Errorf(
			"%T is not '*Ydb_Query.ExecuteQueryResponsePart'", m,
		))
	}
	proto.Reset(dst)
	if part != nil {
		proto.Merge(dst, part)
	}

	return nil
}
