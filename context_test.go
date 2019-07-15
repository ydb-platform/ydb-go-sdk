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
		name       string
		ctxTimeout time.Duration
		opTimeout  time.Duration
		opCancel   time.Duration
		opMode     OperationMode
		ctxMapping ContextDeadlineMapping

		exp OperationParams
	}{
		{
			name: "nothing",
		},
		{
			name:       "mapping timeout",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			exp: OperationParams{
				Timeout: time.Second,
			},
		},
		{
			name:       "mapping deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			ctxTimeout: time.Second,
			exp: OperationParams{
				CancelAfter: time.Second,
			},
		},
		{
			name:       "override context deadline",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			opTimeout:  time.Hour,
			exp: OperationParams{
				Timeout: time.Hour,
			},
		},
		{
			name:       "override context deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			ctxTimeout: time.Second,
			opCancel:   time.Hour,
			exp: OperationParams{
				CancelAfter: time.Hour,
			},
		},
		{
			name:   "mode",
			opMode: OperationModeAsync,
			exp: OperationParams{
				Mode: OperationModeAsync,
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
