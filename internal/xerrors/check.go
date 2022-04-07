package xerrors

// Check returns retry mode for err.
func Check(err error) (
	statusCode int64,
	operationStatus OperationStatus,
	backoff BackoffType,
	deleteSession bool,
) {
	var te *transportError
	var oe *operationError
	var re *retryableError
	switch {
	case As(err, &te):
		return int64(te.code),
			te.OperationStatus(),
			te.BackoffType(),
			te.MustDeleteSession()
	case As(err, &oe):
		return int64(oe.code),
			oe.OperationStatus(),
			oe.BackoffType(),
			oe.MustDeleteSession()
	case As(err, &re):
		return -1,
			OperationNotFinished,
			re.backoffType,
			re.mustDeleteSession
	default:
		return -1,
			OperationFinished, // it's finished, not need any retry attempts
			BackoffTypeNoBackoff,
			false
	}
}
