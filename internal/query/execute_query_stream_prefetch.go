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

	ch chan executeQueryPartRecv
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
	}

	go s.pump()

	return s
}

func (p *asyncPrefetchExecuteQueryStream) pump() {
	defer close(p.ch)
	ctx := p.QueryService_ExecuteQueryClient.Context()
	for {
		part, err := p.QueryService_ExecuteQueryClient.Recv()
		item := executeQueryPartRecv{part: part, err: err}

		select {
		case p.ch <- item:
		case <-ctx.Done():
			return
		}

		if err != nil {
			return
		}
	}
}

func (p *asyncPrefetchExecuteQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	item, ok := <-p.ch
	if !ok {
		return nil, io.EOF
	}

	return item.part, item.err
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
