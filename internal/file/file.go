//go:build !go1.16
// +build !go1.16

package file

import (
	"io/ioutil"
)

func Read(name string) ([]byte, error) {
	return ioutil.ReadFile(name)
}
