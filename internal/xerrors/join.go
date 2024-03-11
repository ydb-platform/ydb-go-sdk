package xerrors

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

func Join(errs ...error) joinErrors {
	return errs
}

type joinErrors []error

func (errs joinErrors) Error() string {
	b := xstring.Buffer()
	defer b.Free()
	b.WriteByte('[')
	for i, err := range errs {
		if i > 0 {
			_ = b.WriteByte(',')
		}
		_, _ = fmt.Fprintf(b, "%q", err.Error())
	}
	b.WriteByte(']')

	return b.String()
}

func (errs joinErrors) As(target interface{}) bool {
	for _, err := range errs {
		if As(err, target) {
			return true
		}
	}

	return false
}

func (errs joinErrors) Is(target error) bool {
	for _, err := range errs {
		if Is(err, target) {
			return true
		}
	}

	return false
}

func (errs joinErrors) Unwrap() []error {
	return errs
}
