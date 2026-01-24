package xerrors

type ErrorWithCastToTargetError struct {
	Err    error
	Target error
}

func (e ErrorWithCastToTargetError) Origin() error {
	return e.Err
}

func (e ErrorWithCastToTargetError) Error() string {
	return e.Err.Error()
}

func (e ErrorWithCastToTargetError) Is(err error) bool {
	//nolint:nolintlint
	if err == e.Target { //nolint:errorlint
		return true
	}

	return Is(e.Err, err)
}
