package errors

type ydbError interface {
	isYdbError()
}

func IsYdb(err error) (ok bool) {
	if err == nil {
		return false
	}
	// nolint:errorlint
	_, ok = err.(ydbError)
	return ok
}
