package xerrors

import (
	"fmt"
	"strings"
)

var _ error = (*multiErrorf)(nil)

type multiErrorf struct {
	format string
	errs   []error
	args   []interface{}
}

func MultiErrorf(format string, args ...interface{}) *multiErrorf {
	e := &multiErrorf{
		format: format,
		args:   args,
	}
	for _, arg := range args {
		if err, has := arg.(error); has {
			e.errs = append(e.errs, err)
		}
	}
	e.format = strings.ReplaceAll(e.format, "%w", "%v")
	return e
}

func (e *multiErrorf) Error() string {
	return fmt.Sprintf(e.format, e.args...)
}

func (e *multiErrorf) As(target interface{}) bool {
	for _, err := range e.errs {
		if As(err, target) {
			return true
		}
	}
	return false
}

func (e *multiErrorf) Is(target error) bool {
	for _, err := range e.errs {
		if Is(err, target) {
			return true
		}
	}
	return false
}
