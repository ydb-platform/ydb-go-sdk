package sugar

import "regexp"

var re = regexp.MustCompile("\\s+at\\s+`[^`]+`")

func removeStackRecords(s string) string {
	// some error constructors (such as fmt.Errorf, grpcStatus.Error) serialized error string at construct time
	// thats why true way with cast wrapped error to *xerrors.stackError has no effect
	return re.ReplaceAllString(s, "")
}

// PrintErrorWithoutStack removed stacktrace records from error string
func PrintErrorWithoutStack(err error) string {
	return removeStackRecords(err.Error())
}

// UnwrapError unwrapped source error to root errors
func UnwrapError(err error) (errs []error) {
	for {
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
		} else {
			return errs
		}
	}
}
