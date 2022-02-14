package trace

// tool gtrace used from repository github.com/asmyasnikov/cmd/gtrace

//go:generate gtrace

import (
	"context"
)

type (
	//gtrace:gen
	//gtrace:set Shortcut
	Retry struct {
		OnRetry func(RetryLoopStartInfo) func(RetryLoopIntermediateInfo) func(RetryLoopDoneInfo)
	}
	RetryLoopStartInfo struct {
		Context context.Context
		ID      string
	}
	RetryLoopIntermediateInfo struct {
		Error error
	}
	RetryLoopDoneInfo struct {
		Attempts int
		Error    error
	}
)
