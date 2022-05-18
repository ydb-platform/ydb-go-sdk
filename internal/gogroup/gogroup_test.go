package gogroup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGoGroupZero(t *testing.T) {
	require.Panics(t, func() {
		_ = WithContext(context.Background(), 0)
	})
}

func TestGoGroupOk(t *testing.T) {
	sg := WithContext(context.Background(), 1)
	sg.Go(func() error {
		return nil
	})
	err := sg.Wait()
	require.NoError(t, err)
}

func TestGoGroupWithErrors(t *testing.T) {
	firstErr := errors.New("first")
	sg := WithContext(context.Background(), 1)

	sg.Go(func() error {
		return firstErr
	})
	err := sg.Wait()
	require.ErrorIs(t, err, firstErr)

	secondCalled := false
	sg.Go(func() error {
		secondCalled = true
		return errors.New("second")
	})
	require.ErrorIs(t, err, firstErr)
	err = sg.Wait()
	require.ErrorIs(t, err, firstErr)
	require.True(t, secondCalled)
}

func TestGoGroupHandlePanic(t *testing.T) {
	sg := WithContext(context.Background(), 1)
	require.NotPanics(t, func() {
		sg.Go(func() error {
			panic("test")
		})
		err := sg.Wait()
		require.Error(t, err)
	})
}

func TestGoGroupDoesNotBlock(t *testing.T) {
	sg := WithContext(context.Background(), 1)
	blocker := make(chan struct{})

	firstStarted := make(chan struct{})
	sg.Go(func() error {
		close(firstStarted)
		// This call blocks in waiting for struct
		<-blocker
		return nil
	})

	// wait to start first goroutine
	<-firstStarted

	// First goroutine wait channel and second goroutine can't start now
	// but sg.Go must exit without blocking

	// This call should not block
	secondCalled := int64(0)
	sg.Go(func() error {
		atomic.AddInt64(&secondCalled, 1)
		<-blocker
		return nil
	})

	// second goroutine doesn't start before first finished
	require.Equal(t, int64(0), atomic.LoadInt64(&secondCalled))

	// Release first goroutine
	blocker <- struct{}{}

	// Release second goroutine
	blocker <- struct{}{}

	require.NoError(t, sg.Wait())
	require.Equal(t, int64(1), secondCalled)
}

func TestGoGroupRespectsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sg := WithContext(ctx, 1)
	blocker := make(chan struct{})

	fistStarted := make(chan struct{})
	sg.Go(func() error {
		fistStarted <- struct{}{}

		// This call blocks in waiting for struct
		<-blocker
		return nil
	})

	// wait to first goroutine started
	<-fistStarted

	// allow to finish first goroutine
	blocker <- struct{}{}

	cancel()

	sg.Go(func() error {
		t.Fatal("should not start after cancel context")
		return nil
	})

	require.Error(t, sg.Wait())
}
