package testutil

import (
	"path/filepath"
	"runtime"
	"strconv"
)

func FileLine(skip int) string {
	_, file, line, _ := runtime.Caller(skip)

	return filepath.Base(file) + ":" + strconv.Itoa(line)
}
