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

// Array is the version-independent column interface used by NewDecoder.
type Array interface {
	Len() int
	IsNull(row int) bool
	NullN() int
}

// Record is the version-independent Arrow record interface used by NewDecoder.
type Record[A Array] interface {
	Retain()
	Release()
	NumRows() int64
	NumCols() int64
	ColumnName(column int) string
	Column(column int) A
}

// IPCReader is the version-independent reader interface used by NewDecoder.
// Read returns a borrowed record valid until the next Read or Release, or io.EOF.
type IPCReader[B any] interface {
	Read() (B, error)
	Release()
}
