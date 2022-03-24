package errors

type retryableError struct {
	err               error
	backoffType       BackoffType
	mustDeleteSession bool
}

func (e *retryableError) Error() string {
	return e.err.Error()
}

type RetryableErrorOption func(e *retryableError)

func WithBackoff(t BackoffType) RetryableErrorOption {
	return func(e *retryableError) {
		e.backoffType = t
	}
}

func WithDeleteSession() RetryableErrorOption {
	return func(e *retryableError) {
		e.mustDeleteSession = true
	}
}

func RetryableError(err error, opts ...RetryableErrorOption) error {
	re := &retryableError{
		err: err,
	}
	for _, o := range opts {
		o(re)
	}
	return re
}
