package retry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
)

func unwrapErrBadConn(err error) error {
	var e *badconn.Error
	if xerrors.As(err, &e) {
		origin := e.Origin()
		if xerrors.IsRetryableError(err) {
			return xerrors.Retryable(origin)
		}

		return origin
	}

	return err
}
