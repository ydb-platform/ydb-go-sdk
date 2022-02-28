package test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil/timetest"
)

func TestRepeater(t *testing.T) {
	timerC := make(chan time.Time, 1)
	timer := timetest.Timer{
		Ch: timerC,
	}
	cleanup := timeutil.StubTestHookNewTimer(func(time.Duration) timeutil.Timer {
		return timer
	})
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec := make(chan struct{})
	r := repeater.NewRepeater(ctx, 42*time.Second,
		func(_ context.Context) error {
			exec <- struct{}{}
			return nil
		})

	timerC <- time.Now()
	assertRecv(t, 500*time.Millisecond, exec)
	assertRecv(t, 50*time.Millisecond, exec)

	r.Stop()
	timerC <- time.Now()
	assertNoRecv(t, 50*time.Millisecond, exec)
}

func TestRepeaterCancellation(t *testing.T) {
	var (
		timerC = make(chan time.Time)
		enter  = make(chan struct{}, 2)
		exit   = make(chan struct{}, 2)
	)

	timer := timetest.Timer{
		Ch: timerC,
	}

	cleanup := timeutil.StubTestHookNewTimer(func(time.Duration) timeutil.Timer {
		return timer
	})
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := repeater.NewRepeater(
		ctx,
		42*time.Second,
		func(ctx context.Context) error {
			enter <- struct{}{}
			<-ctx.Done()
			exit <- struct{}{}
			return nil
		},
	)

	// Run callback in a separate goroutine to avoid deadlock.
	// That is, StubTimer run its function in the same goroutine as Emit
	// called.
	go func() { timerC <- time.Now() }()

	const timeout = 100 * time.Millisecond

	assertRecv(t, timeout, enter)
	assertNoRecv(t, timeout, enter)
	assertNoRecv(t, timeout, exit)

	r.Stop()

	assertRecv(t, timeout, exit)
}

func recv(ch interface{}, timeout time.Duration) error {
	i, _, _ := reflect.Select([]reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(timeout))},
	})
	if i == 0 {
		return nil
	}
	return fmt.Errorf("timed out: %s", timeout)
}

func noRecv(ch interface{}, timeout time.Duration) error {
	i, _, _ := reflect.Select([]reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(timeout))},
	})
	if i == 1 {
		return nil
	}
	return fmt.Errorf("unexepcted filling")
}

func assertRecv(t *testing.T, timeout time.Duration, ch interface{}) {
	t.Helper()
	if err := recv(ch, timeout); err != nil {
		t.Fatalf("%s: %v", testutil.FileLine(2), err)
	}
}

func assertNoRecv(t *testing.T, timeout time.Duration, ch interface{}) {
	t.Helper()
	if err := noRecv(ch, timeout); err != nil {
		t.Fatalf("%s: %v", testutil.FileLine(2), err)
	}
}
