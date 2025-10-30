//go:build go1.25

package xcontext_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

func TestWithStoppableTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, _ := xcontext.WithStoppableTimeout(context.Background(), 10*time.Second)
		select {
		case <-time.After(100500 * time.Second):
			t.Fatal("context should be done")
		case <-ctx.Done():
		}
	})

	synctest.Test(t, func(t *testing.T) {
		ctx, stop := xcontext.WithStoppableTimeout(context.Background(), 10*time.Second)

		stop()

		select {
		case <-time.After(100500 * time.Second):
		case <-ctx.Done():
			t.Fatal("context shouldn't be canceled")
		}
	})
}
