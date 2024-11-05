package ydb

import (
	"database/sql"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/connector"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func Unwrap[T *sql.DB | *sql.Conn](v T) (*Driver, error) {
	c, err := connector.Unwrap(v)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return c.Parent().(*Driver), nil //nolint:forcetypeassert
}
