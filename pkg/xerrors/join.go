package xerrors

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"

func Join(errs ...error) error {
	return xerrors.Join(errs...)
}
