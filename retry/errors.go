package retry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
)

func unwrapErrBadConn(err error) error {
	var e *badconn.Error
	if xerrors.As(err, &e) {
		return e.Origin()
	}

	return err
}
