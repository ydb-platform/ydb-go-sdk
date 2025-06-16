package arrow

import (
	"context"
	"io"
)

type (
	Result interface {
		//closer.Closer

		// NextPart returns next part of result
		NextPart(ctx context.Context) (Part, error)
	}
	Part interface {
		Schema() io.Reader
		Data() io.Reader
		GetResultSetIndex() int64
	}
)
