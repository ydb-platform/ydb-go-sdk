package arrow

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type Column struct {
	Name string
	Type types.Type
}

// Batch owns column data until Release. Scan writes one cell directly to dst.
// Value returns an owned SDK value on demand.
type Batch interface {
	NumRows() int
	NumCols() int
	Scan(row, column int, dst any) error
	Value(row, column int) value.Value
	Release()
}

type Decoder func(ctx context.Context, columns []Column, ipc io.Reader) ([]Batch, error)
