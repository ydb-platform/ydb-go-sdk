//go:build go1.22 && goexperiment.rangefunc

package query

import (
	"context"
	"iter"
)

type (
	resultSetsRanger interface {
		Range(ctx context.Context) iter.Seq2[ResultSet, error]
	}
	rowsRanger interface {
		Range(ctx context.Context) iter.Seq2[Row, error]
	}
)
