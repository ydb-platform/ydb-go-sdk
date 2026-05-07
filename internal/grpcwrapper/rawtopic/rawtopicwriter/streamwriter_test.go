package rawtopicwriter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func TestStreamWriterRecvErr(t *testing.T) {
	wrapErr := func(err error) error {
		if xerrors.IsContextError(err) {
			return xerrors.WithStackTrace(err)
		}

		return err
	}

	t.Run("grpcStatus.Error", func(t *testing.T) {
		sendErr := wrapErr(xerrors.WithStackTrace(fmt.Errorf("stream context is done: %w", xerrors.Join(
			grpcStatus.Error(grpcCodes.Canceled, "Cancelled on the server side"),
			context.Canceled,
		))))

		if !xerrors.IsErrorFromServer(sendErr) {
			sendErr = xerrors.Transport(sendErr)
		}

		code, errType, backoffType := xerrors.Check(sendErr)
		assert.EqualValues(t, -1, code)
		assert.EqualValues(t, xerrors.TypeNonRetryable, errType)
		assert.EqualValues(t, backoff.TypeNoBackoff, backoffType)
	})
	t.Run("xerrors.Transport", func(t *testing.T) {
		sendErr := wrapErr(xerrors.WithStackTrace(fmt.Errorf("stream context is done: %w", xerrors.Join(
			xerrors.Transport(grpcStatus.Error(grpcCodes.Canceled, "Cancelled on the server side")),
			context.Canceled,
		))))

		if !xerrors.IsErrorFromServer(sendErr) {
			sendErr = xerrors.Transport(sendErr)
		}

		code, errType, backoffType := xerrors.Check(sendErr)
		assert.EqualValues(t, grpcCodes.Canceled, code)
		assert.EqualValues(t, xerrors.TypeConditionallyRetryable, errType)
		assert.EqualValues(t, backoff.TypeFast, backoffType)
	})
}
