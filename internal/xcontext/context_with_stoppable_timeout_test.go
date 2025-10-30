package xcontext_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

func TestWithStoppableTimeoutCause(t *testing.T) {
	wantErr := errors.New("some error")

	synctest.Test(t, func(t *testing.T) {
		ctx, _ := xcontext.WithStoppableTimeoutCause(context.Background(), 10*time.Second, wantErr)
		select {
		case <-time.After(100500 * time.Second):
			t.Fatal("context should be done")
		case <-ctx.Done():
			assert.ErrorIs(t, context.Cause(ctx), wantErr)
		}
	})

	synctest.Test(t, func(t *testing.T) {
		ctx, stop := xcontext.WithStoppableTimeoutCause(context.Background(), 10*time.Second, wantErr)

		stop()

		select {
		case <-time.After(100500 * time.Second):
		case <-ctx.Done():
			t.Fatal("context shouldn't be canceled")
		}
	})
}
