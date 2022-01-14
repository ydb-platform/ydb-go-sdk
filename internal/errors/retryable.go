package errors

type RetryableError struct {
	Err               error
	BackoffType       BackoffType
	MustDeleteSession bool
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}
