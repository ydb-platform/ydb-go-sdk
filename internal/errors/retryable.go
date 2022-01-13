package errors

type RetryableError struct {
	Msg               string
	BackoffType       BackoffType
	MustDeleteSession bool
}

func (e *RetryableError) Error() string {
	return e.Msg
}
