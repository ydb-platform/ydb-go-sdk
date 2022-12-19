package topic

import "time"

type RetrySettings struct {
	Timeout    time.Duration // Full retry timeout
	CheckError IsRetriableErrorFunc
}

type IsRetriableErrorFunc func(errInfo CheckErrorArgs) RetryDecision

type CheckErrorArgs struct {
	Attempt int
	Error   error
}

type RetryDecision int

const (
	RetryDecisionDefault RetryDecision = iota // Apply default behavior for the error
	RetryDecisionRetry                        // Do once more retry
	RetryDecisionStop                         // Do not retry
)
