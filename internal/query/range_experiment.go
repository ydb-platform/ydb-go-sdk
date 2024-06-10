package query

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func rangeResultSets(ctx context.Context, r query.Result) xiter.Seq2[query.ResultSet, error] {
	return func(yield func(query.ResultSet, error) bool) {
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

func rangeRows(ctx context.Context, rs query.ResultSet) xiter.Seq2[query.Row, error] {
	return func(yield func(query.Row, error) bool) {
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
