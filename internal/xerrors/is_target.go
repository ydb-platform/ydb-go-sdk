package xerrors

type isTargetError struct {
	err    error
	target error
}

func IsTarget(err error, target error) error {
	return isTargetError{
		err:    err,
		target: target,
	}
}

func (e isTargetError) Unwrap() error {
	return e.err
}

func (e isTargetError) Error() string {
	return e.err.Error()
}

func (e isTargetError) Is(err error) bool {
	if err == e.target {
		return true
	}

	return Is(e.err, err)
}
