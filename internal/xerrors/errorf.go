package xerrors

import (
	"fmt"
	"strings"
)

var _ error = (*multiErrorf)(nil)

type multiErrorf struct {
	msg  string
	errs []error
}

func Errorf(format string, args ...interface{}) *multiErrorf {
	e := &multiErrorf{
		msg: fmt.Sprintf(strings.ReplaceAll(format, "%w", "%v"), args...),
	}
	for _, arg := range args {
		if err, has := arg.(error); has {
			e.errs = append(e.errs, err)
		}
	}
	return e
}

func (e *multiErrorf) Error() string {
	return e.msg
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
