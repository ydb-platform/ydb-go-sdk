package gogroup

import (
	"context"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// Group is group is a collection of goroutines working on subtasks that are part of
// the same overall task with limit of parallel goroutines work
type Group struct {
	ctx context.Context

	maxGoroutinesGuard chan struct{}
	goroutinesInFlight sync.WaitGroup

	onFirstError sync.Once
	firstError   error
}

func WithContext(ctx context.Context, maxGoroutines int) *Group {
	if maxGoroutines <= 0 {
		panic("maxGoroutines must be more then zero")
	}

	return &Group{
		ctx:                ctx,
		maxGoroutinesGuard: make(chan struct{}, maxGoroutines),
	}
}

// Go calls the given function in a new goroutine.
// Wait to finish previous goroutines work if maxGoroutines reached.
// func f will not start if group context cancelled
func (group *Group) Go(f func() error) {
	group.goroutinesInFlight.Add(1)

	go func() {
		defer func() {
			group.goroutinesInFlight.Done()
		}()

		if group.ctx.Err() != nil {
			// deny run not run on cancelled context
			return
		}

		select {
		case <-group.ctx.Done():
			// Kill this goroutine on cancellation
			return
		case group.maxGoroutinesGuard <- struct{}{}:
		}

		defer func() {
			<-group.maxGoroutinesGuard
		}()

		var err error
		defer func() {
			if r := recover(); r != nil {
				err = xerrors.WithStackTrace(fmt.Errorf("handled panic: %v", r))
			}

			if err != nil {
				group.onFirstError.Do(func() {
					group.firstError = err
				})
			}
		}()

		err = f()
	}()
}

// Wait blocks until all function calls from the Go method have returned or group context cancelled, then
// returns the first non-nil error (if any) from them.
func (group *Group) Wait() error {
	if err := group.ctx.Err(); err != nil {
		return err
	}

	waitCh := make(chan struct{})
	go func() {
		group.goroutinesInFlight.Wait()
		waitCh <- struct{}{}
	}()

	select {
	case <-group.ctx.Done():
		// Release this goroutine from Wait on cancellation
		return group.ctx.Err()
	case <-waitCh:
	}

	return group.firstError
}
