//go:build go1.16
// +build go1.16

package file

import (
	"os"
)

// Read reads the named file and returns the contents.
// A successful call returns err == nil, not err == EOF.
// Because Read reads the whole file, it does not treat an EOF from Read
// as an error to be reported.
// Read is a copy-paste of os.ReadFile for support go under 1.15
func Read(name string) ([]byte, error) {
	return os.ReadFile(name)
}
