//go:build go1.22 && goexperiment.rangefunc

package query

import (
	"context"
	"io"

	"iter"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type (
	resultRange struct {
		r query.Result
	}
	resultSetRange struct {
		rs query.ResultSet
	}
)

func (r *resultRange) Range(ctx context.Context) iter.Seq2[query.ResultSet, error] {
	return func(yield func(query.ResultSet, error) bool) {
		for {
			rs, err := r.r.NextResultSet(ctx)
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

func (r *resultSetRange) Range(ctx context.Context) iter.Seq2[query.Row, error] {
	return func(yield func(query.Row, error) bool) {
		for {
			rs, err := r.rs.NextRow(ctx)
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
