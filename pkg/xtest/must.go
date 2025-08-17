package xtest

func Must[R any](res R, err error) R {
	if err != nil {
		panic(err)
	}

	return res
}
