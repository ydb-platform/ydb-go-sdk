package topic

import "time"

type RetrySettings struct {
	Timeout    time.Duration // Full retry timeout
	CheckError PublicCheckRetryFunc
}

type PublicCheckRetryFunc func(errInfo PublicCheckRetryArgs) PublicCheckRetryResult

type PublicCheckRetryArgs struct {
	Attempt int
	Error   error
}

type PublicCheckRetryResult int

const (
	PublicRetryDecisionDefault PublicCheckRetryResult = iota // Apply default behavior for the error
	PublicRetryDecisionRetry                                 // Do once more retry
	PublicRetryDecisionStop                                  // Do not retry
)
