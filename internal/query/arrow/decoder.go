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

type Decoder func(ctx context.Context, columns []Column, ipc io.Reader) ([][]value.Value, error)
