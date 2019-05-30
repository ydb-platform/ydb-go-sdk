package timetest

import (
	"sync"
	"testing"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

type SingleTimer struct {
	once    sync.Once
	C       chan time.Time
	Created chan struct{}
	Reset   chan time.Duration
	Cleanup func()
}

func StubSingleTimer(t *testing.T) *SingleTimer {
	s := &SingleTimer{
		C:       make(chan time.Time),
		Created: make(chan struct{}),
		Reset:   make(chan time.Duration),
	}
	s.Cleanup = timeutil.StubTestHookNewTimer(func(d time.Duration) (r timeutil.Timer) {
		var success bool
		s.once.Do(func() {
			success = true
		})
		if !success {
			t.Fatalf("timeutil.NewTimer() is called more than once")
			return nil
		}
		close(s.Created)
		return Timer{
			Ch: s.C,
			OnReset: func(d time.Duration) bool {
				s.Reset <- d
				return true
			},
		}
	})
	return s
}

type Timer struct {
	Ch      <-chan time.Time
	OnReset func(time.Duration) bool
	OnStop  func() bool
}

func (t Timer) Reset(d time.Duration) bool {
	if t.OnReset != nil {
		return t.OnReset(d)
	}
	return true
}

func (t Timer) Stop() bool {
	if t.OnStop != nil {
		return t.OnStop()
	}
	return true
}

func (t Timer) C() <-chan time.Time {
	return t.Ch
}
