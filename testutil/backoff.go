package testutil

import "time"

// BackoffFunc is an adapter to allow the use of ordinary functions as Backoff.
type BackoffFunc func(n int) <-chan time.Time

func (f BackoffFunc) Delay(i int) time.Duration {
	return time.Until(<-f(i))
}

// Wait implements Backoff interface.
func (f BackoffFunc) Wait(n int) <-chan time.Time {
	return f(n)
}
