package stack

import (
	"runtime"
	"strconv"
	"strings"
)

func Record(depth int) string {
	function, file, line, _ := runtime.Caller(depth + 1)
	name := runtime.FuncForPC(function).Name()
	return name + "(" + fileName(file) + ":" + strconv.Itoa(line) + ")"
}

func fileName(original string) string {
	i := strings.LastIndex(original, "/")
	if i == -1 {
		return original
	}
	return original[i+1:]
}
