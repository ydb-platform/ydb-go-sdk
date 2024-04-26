package metrics

import (
	"context"
	"io"
	"net"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

func errorBrief(err error) string {
	if err == nil {
		return "OK"
	}
	if xerrors.Is(err, io.EOF) {
		return "io/EOF"
	}
	if netErr := (*net.OpError)(nil); xerrors.As(err, &netErr) {
		buffer := xstring.Buffer()
		defer buffer.Free()
		buffer.WriteString("network")
		if netErr.Op != "" {
			buffer.WriteByte('/')
			buffer.WriteString(netErr.Op)
		}
		if netErr.Addr != nil {
			buffer.WriteByte('[')
			buffer.WriteString(netErr.Addr.String())
			buffer.WriteByte(']')
		}
		if netErr.Err != nil {
			buffer.WriteByte('(')
			buffer.WriteString(errorBrief(netErr.Err))
			buffer.WriteByte(')')
		}

		return buffer.String()
	}
	if xerrors.Is(err, context.DeadlineExceeded) {
		return "context/DeadlineExceeded"
	}
	if xerrors.Is(err, context.Canceled) {
		return "context/Canceled"
	}
	if xerrors.IsTransportError(err) {
		return xerrors.TransportError(err).Name()
	}
	if xerrors.IsOperationErrorTransactionLocksInvalidated(err) {
		return "operation/ABORTED/TLI"
	}
	if xerrors.IsOperationError(err) {
		return xerrors.OperationError(err).Name()
	}
	if ydbErr := xerrors.Error(nil); xerrors.As(err, &ydbErr) {
		return ydbErr.Name()
	}

	return "unknown"
}
