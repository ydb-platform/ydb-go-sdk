package query

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
)

func rangeResultSets(ctx context.Context, r result.Result) xiter.Seq2[result.Set, error] {
	return func(yield func(result.Set, error) bool) {
		for {
			rs, err := r.NextResultSet(ctx)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					return
				}
			}
			cont := yield(rs, err)
			if !cont || err != nil {
				return
			}
		}
	}
}

func rangeRows(ctx context.Context, rs result.Set) xiter.Seq2[result.Set, error] {
	return func(yield func(result.Row, error) bool) {
		for {
			rs, err := rs.NextRow(ctx)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					return
				}
			}
			cont := yield(rs, err)
			if !cont || err != nil {
				return
			}
		}
	}
}
