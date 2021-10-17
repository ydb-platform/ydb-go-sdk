// +build integration

package test

import "log"

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type quet struct {
}

func (q quet) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func Quet() {
	log.SetOutput(&quet{})
}
