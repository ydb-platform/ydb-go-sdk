package testutil

import "time"

// BackoffFunc is an adapter to allow the use of ordinary functions as Backoff.
type BackoffFunc func(n int) <-chan time.Time

// Wait implements Backoff interface.
func (f BackoffFunc) Wait(n int) <-chan time.Time {
	return f(n)
}
