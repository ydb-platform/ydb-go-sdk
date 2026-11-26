package query

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type asyncPrefetchExecuteQueryStream struct {
	Ydb_Query_V1.QueryService_ExecuteQueryClient

	ch          chan *Ydb_Query.ExecuteQueryResponsePart
	terminalErr error // Written by pump before closing ch.
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
		ch:                              make(chan *Ydb_Query.ExecuteQueryResponsePart, prefetch),
	}

	go s.pump()

	return s
}

func (p *asyncPrefetchExecuteQueryStream) pump() {
	defer close(p.ch)
	ctx := p.QueryService_ExecuteQueryClient.Context()
	for {
		part, err := p.QueryService_ExecuteQueryClient.Recv()
		if err != nil {
			p.terminalErr = err

			return
		}

		select {
		case p.ch <- part:
		case <-ctx.Done():
			p.terminalErr = ctx.Err()

			return
		}
	}
}

func (p *asyncPrefetchExecuteQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	part, ok := <-p.ch
	if !ok {
		return nil, p.terminalErr
	}

	return part, nil
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
