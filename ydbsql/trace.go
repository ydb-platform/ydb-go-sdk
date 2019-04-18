package ydbsql

import "context"

type Trace struct {
	DialStart func(DialStartInfo)
	DialDone  func(DialDoneInfo)
}

type (
	DialStartInfo struct {
		Context context.Context
	}
	DialDoneInfo struct {
		Context context.Context
		Error   error
	}
)
