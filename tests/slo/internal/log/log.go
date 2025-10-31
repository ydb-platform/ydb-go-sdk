package log

import (
	"fmt"
	"time"
)

func timestampPrefix() string {
	return "[" + time.Now().Format(time.RFC3339) + "] "
}

func Printf(format string, args ...any) {
	fmt.Printf(timestampPrefix()+format+"\n", args...)
}

func Println(args ...any) {
	fmt.Println(append([]any{timestampPrefix()}, args...)...)
}

func Panicf(format string, args ...any) {
	panic(fmt.Sprintf(timestampPrefix()+format, args...))
}
