package spans

import (
	"errors"
	"io"
)

var errNestedCall = errors.New("")

func skipEOF(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}

	return err
}
