package sugar

import "regexp"

var re = regexp.MustCompile("\\s+at\\s+`[^`]+`")

func removeStackRecords(s string) string {
	// Some error constructors (such as fmt.Errorf, grpcStatus.Error) are serializing the error string at the
	// construction time. Thats why the "true way" with casting the wrapped error into *xerrors.stackError
	// has no effect
	return re.ReplaceAllString(s, "")
}

// PrintErrorWithoutStack removed stacktrace records from error string
func PrintErrorWithoutStack(err error) string {
	return removeStackRecords(err.Error())
}

// UnwrapError unwrapps source error to root errors
func UnwrapError(err error) (errs []error) {
	if x, has := err.(interface {
		Unwrap() error
	}); has {
		return UnwrapError(x.Unwrap())
	} else if x, has := err.(interface {
		Unwrap() []error
	}); has {
		for _, xx := range x.Unwrap() {
			errs = append(errs, UnwrapError(xx)...)
		}

		return errs
	} else if len(errs) == 0 {
		return []error{err}
	}

	return errs
}
