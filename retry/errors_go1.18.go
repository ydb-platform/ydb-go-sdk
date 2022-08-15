//go:build go1.18
// +build go1.18

package retry

func unwrapErrBadConn(err error) error {
	return err
}
