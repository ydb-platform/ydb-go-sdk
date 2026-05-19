package spans

import (
	"errors"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errNestedCall = errors.New("")

func skipEOF(err error) error {
	if err == nil || xerrors.Is(err, io.EOF) {
		return nil
	}

	return err
}
