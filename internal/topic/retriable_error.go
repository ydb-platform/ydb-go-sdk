package topic

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

const (
	DefaultStartTimeout = time.Minute
)

type RetrySettings struct {
	StartTimeout time.Duration // Full retry timeout
	CheckError   PublicCheckErrorRetryFunction
}

type PublicCheckErrorRetryFunction func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult

type PublicCheckErrorRetryArgs struct {
	Error error
}

func NewCheckRetryArgs(err error) PublicCheckErrorRetryArgs {
	return PublicCheckErrorRetryArgs{
		Error: err,
	}
}

type PublicCheckRetryResult struct {
	val int
}

var (
	PublicRetryDecisionDefault = PublicCheckRetryResult{val: 0}
	PublicRetryDecisionRetry   = PublicCheckRetryResult{val: 1}
	PublicRetryDecisionStop    = PublicCheckRetryResult{val: 2}
)

func CheckResetReconnectionCounters(lastTry, now time.Time, connectionTimeout time.Duration) bool {
	const resetAttemptEmpiricalCoefficient = 10
	return now.Sub(lastTry) > connectionTimeout*resetAttemptEmpiricalCoefficient
}

func CheckRetryMode(err error, settings RetrySettings, retriesDuration time.Duration) (
	_ backoff.Backoff,
	isRetriable bool,
) {
	// nil is not error and doesn't need retry it.
	if err == nil {
		return nil, false
	}

	// eof is retriable for topic
	if errors.Is(err, io.EOF) && xerrors.RetryableError(err) == nil {
		err = xerrors.Retryable(err, xerrors.WithName("TopicEOF"))
	}

	if retriesDuration > settings.StartTimeout {
		return nil, false
	}

	mode := retry.Check(err)

	decision := PublicRetryDecisionDefault
	if settings.CheckError != nil {
		decision = settings.CheckError(NewCheckRetryArgs(err))
	}

	switch decision {
	case PublicRetryDecisionDefault:
		isRetriable = mode.MustRetry(true)
	case PublicRetryDecisionRetry:
		isRetriable = true
	case PublicRetryDecisionStop:
		isRetriable = false
	default:
		panic(fmt.Errorf("unexpected retry decision: %v", decision))
	}

	if !isRetriable {
		return nil, false
	}

	if mode.BackoffType() == backoff.TypeFast {
		return backoff.Fast, true
	}

	return backoff.Slow, true
}
