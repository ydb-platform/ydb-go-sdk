package metrics

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func errorBrief(err error) string {
	if err == nil {
		return "OK"
	}
	var (
		netErr *net.OpError
		ydbErr ydb.Error
	)
	switch {
	case errors.As(err, &netErr):
		return "network/" + netErr.Op + " -> " + netErr.Err.Error()
	case errors.Is(err, io.EOF):
		return "io/EOF"
	case errors.Is(err, context.DeadlineExceeded):
		return "context/DeadlineExceeded"
	case errors.Is(err, context.Canceled):
		return "context/Canceled"
	case errors.As(err, &ydbErr):
		return ydbErr.Name()
	default:
		return "unknown"
	}
}
