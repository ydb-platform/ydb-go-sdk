package value

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func CastTo(v Value, dst interface{}) error {
	if dst == nil {
		return errNilDestination
	}
	if ptr, has := dst.(*Value); has {
		*ptr = v

		return nil
	}

	if err := v.castTo(dst); err == nil {
		return nil
	}

	if ptr, has := dst.(*driver.Value); has {
		*ptr = v

		return nil
	}

	return xerrors.WithStackTrace(err)
}
