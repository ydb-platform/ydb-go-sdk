package query

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// executeQueryPartRecv is one logical item from ExecuteQuery stream (typed Recv).
type executeQueryPartRecv struct {
	part *Ydb_Query.ExecuteQueryResponsePart
	wire *wirePart
	err  error
}

type asyncPrefetchExecuteQueryStream struct {
	Ydb_Query_V1.QueryService_ExecuteQueryClient

	executeCtx   context.Context //nolint:containedctx
	ch           chan executeQueryPartRecv
	cancelErr    error // Published by closing ch.
	terminalRead atomic.Bool
}

func wrapExecuteQueryStreamWithAsyncPrefetch(
	ctx context.Context,
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	prefetch int,
) Ydb_Query_V1.QueryService_ExecuteQueryClient {
	if prefetch <= 0 {
		return stream
	}
	s := &asyncPrefetchExecuteQueryStream{
		QueryService_ExecuteQueryClient: stream,
		executeCtx:                      ctx,
		ch:                              make(chan executeQueryPartRecv, prefetch),
	}

	go s.pump()

	return s
}

func (p *asyncPrefetchExecuteQueryStream) pump() {
	defer close(p.ch)
	for {
		part, wire, err := recvQueryPart(p.QueryService_ExecuteQueryClient)
		item := executeQueryPartRecv{part: part, wire: wire, err: err}

		// A read-ahead interceptor can finish the transport before returning its last part.
		// Only cancellation of the query execution may abandon a pending delivery.
		select {
		case p.ch <- item:
		case <-p.executeCtx.Done():
			p.cancelErr = p.executeCtx.Err()

			return
		}

		if err != nil {
			return
		}
	}
}

// Recv materializes the ordinary protobuf response, including its rows.
// Wire consumers use RecvPart to retain the wire-decoding allocation savings.
func (p *asyncPrefetchExecuteQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	part, wire, err := p.RecvPart()
	if err != nil || wire == nil {
		return part, err
	}
	materialized := new(Ydb_Query.ExecuteQueryResponsePart)
	proto.Merge(materialized, part)
	if resultSet := materialized.GetResultSet(); resultSet != nil {
		rows := make([]*Ydb.Value, wire.RowCount())
		for i := range rows {
			rows[i] = new(Ydb.Value)
			if err := proto.Unmarshal(wire.rowBytes(i), rows[i]); err != nil {
				return nil, err
			}
		}
		resultSet.Rows = rows
	}

	return materialized, nil
}

func (p *asyncPrefetchExecuteQueryStream) RecvPart() (
	*Ydb_Query.ExecuteQueryResponsePart, *wirePart, error,
) {
	item, ok := <-p.ch
	if !ok {
		return nil, nil, cmp.Or(p.cancelErr, io.EOF)
	}

	if item.err != nil {
		p.terminalRead.Store(true)
	}

	return item.part, item.wire, item.err
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
