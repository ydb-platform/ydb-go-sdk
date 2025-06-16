package arrow

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
)

type (
	Result interface {
		closer.Closer

		// Parts is the range iterator for result parts (parts implement io.Reader)
		Parts(ctx context.Context) xiter.Seq2[Part, error]
	}
	Part interface {
		io.Reader

		GetResultSetIndex() int64
	}
)
