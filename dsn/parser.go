package dsn

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/dsn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func RegisterParser(param string, parser func(value string) ([]config.Option, error)) (err error) {
	err = dsn.Register(param, parser)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("%w: %s", err, param))
	}
	return nil
}
