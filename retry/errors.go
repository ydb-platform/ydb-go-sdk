package retry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func unwrapErrBadConn(err error) error {
	var e *badconn.Error
	if xerrors.As(err, &e) {
		return e.Origin()
	}

	return err
}
