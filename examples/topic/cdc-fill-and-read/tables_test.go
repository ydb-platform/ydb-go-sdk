package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunPeriodicallyStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := runPeriodically(ctx, time.Hour, func() error {
		calls++
		cancel()

		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runPeriodically() error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("operation called %d times, want 1", calls)
	}
}

func TestRunPeriodicallyDoesNotRunAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := runPeriodically(ctx, time.Hour, func() error {
		calls++

		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runPeriodically() error = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("operation called %d times, want 0", calls)
	}
}

func TestRunPeriodicallyReturnsOperationError(t *testing.T) {
	want := errors.New("write failed")
	err := runPeriodically(context.Background(), time.Hour, func() error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("runPeriodically() error = %v, want %v", err, want)
	}
}
