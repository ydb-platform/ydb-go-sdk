package ydb

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

func TestOperationParams(t *testing.T) {
	for _, test := range []struct {
		name string

		ctxTimeout time.Duration
		ctxMapping ContextDeadlineMapping

		opTimeout time.Duration
		opCancel  time.Duration
		opMode    OperationMode

		exp OperationParams
	}{
		{
			name: "nothing",
		},
		{
			name:       "mode: unknown, mapping timeout",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			exp: OperationParams{
				Timeout: time.Second,
			},
		},
		{
			name:       "mode: sync, mapping timeout",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			opMode:     OperationModeSync,
			exp: OperationParams{
				Timeout: time.Second,
				Mode:    OperationModeSync,
			},
		},
		{
			name:       "mode: async, mapping timeout",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			opMode:     OperationModeAsync,
			exp: OperationParams{
				Timeout: time.Second,
				Mode:    OperationModeAsync,
			},
		},
		{
			name:       "mode: unknown, mapping deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			ctxTimeout: time.Second,
			exp: OperationParams{
				CancelAfter: time.Second,
			},
		},
		{
			name:       "mode: sync, mapping deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			ctxTimeout: time.Second,
			opMode:     OperationModeSync,
			exp: OperationParams{
				CancelAfter: time.Second,
				Mode:        OperationModeSync,
			},
		},
		{
			name:       "mode: async, mapping deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			opMode:     OperationModeAsync,
			ctxTimeout: time.Second,
			exp: OperationParams{
				CancelAfter: time.Second,
				Mode:        OperationModeAsync,
			},
		},
		{
			name:       "mode: unknown, override op timeout",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			opTimeout:  time.Hour,
			exp: OperationParams{
				Timeout: time.Hour,
			},
		},
		{
			name:       "mode: sync, override op timeout",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			opMode:     OperationModeSync,
			opTimeout:  time.Hour,
			exp: OperationParams{
				Timeout: time.Second,
				Mode:    OperationModeSync,
			},
		},
		{
			name:       "mode: async, override op timeout",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			opMode:     OperationModeAsync,
			opTimeout:  time.Hour,
			exp: OperationParams{
				Timeout: time.Hour,
				Mode:    OperationModeAsync,
			},
		},
		{
			name:       "mode: unknown, override op deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			ctxTimeout: time.Second,
			opCancel:   time.Hour,
			exp: OperationParams{
				CancelAfter: time.Hour,
			},
		},
		{
			name:       "mode: sync, override op deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			ctxTimeout: time.Second,
			opMode:     OperationModeSync,
			opCancel:   time.Hour,
			exp: OperationParams{
				CancelAfter: time.Second,
				Mode:        OperationModeSync,
			},
		},
		{
			name:       "mode: async, override op deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			ctxTimeout: time.Second,
			opMode:     OperationModeAsync,
			opCancel:   time.Hour,
			exp: OperationParams{
				CancelAfter: time.Hour,
				Mode:        OperationModeAsync,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
			defer cleanupNow()

			ctx := context.Background()
			if t := test.opTimeout; t > 0 {
				ctx = WithOperationTimeout(ctx, t)
			}
			if t := test.opCancel; t > 0 {
				ctx = WithOperationCancelAfter(ctx, t)
			}
			if m := test.opMode; m != 0 {
				ctx = WithOperationMode(ctx, m)
			}
			if t := test.ctxTimeout; t > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, timeutil.Now().Add(t))
				defer cancel()
			}

			act, _ := operationParams(ctx, test.ctxMapping)

			if exp := test.exp; !reflect.DeepEqual(act, exp) {
				t.Fatalf(
					"unexpected operation parameters: %v; want %v",
					act, exp,
				)
			}
		})
	}
}
