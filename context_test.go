package ydb

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Operations"
	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

type operation struct {
	params *Ydb_Operations.OperationParams
}

func (op *operation) SetOperationParams(p *Ydb_Operations.OperationParams) {
	op.params = p
}

func TestSetOperationParams(t *testing.T) {
	for _, test := range []struct {
		name       string
		ctxTimeout time.Duration
		opTimeout  time.Duration
		opCancel   time.Duration
		opMode     OperationMode
		ctxMapping ContextDeadlineMapping

		exp *Ydb_Operations.OperationParams
	}{
		{
			name: "nothing",
			exp:  nil,
		},
		{
			name:       "mapping timeout",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: timeoutParam(time.Second),
			},
		},
		{
			name:       "mapping deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			ctxTimeout: time.Second,
			exp: &Ydb_Operations.OperationParams{
				CancelAfter: timeoutParam(time.Second),
			},
		},
		{
			name:       "override context deadline",
			ctxMapping: ContextDeadlineOperationTimeout,
			ctxTimeout: time.Second,
			opTimeout:  time.Hour,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: timeoutParam(time.Hour),
			},
		},
		{
			name:       "override context deadline",
			ctxMapping: ContextDeadlineOperationCancelAfter,
			ctxTimeout: time.Second,
			opCancel:   time.Hour,
			exp: &Ydb_Operations.OperationParams{
				CancelAfter: timeoutParam(time.Hour),
			},
		},
		{
			name:   "mode",
			opMode: OperationModeAsync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode: (OperationModeAsync).toYDB(),
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
				ctx, _ = context.WithDeadline(ctx, timeutil.Now().Add(t))
			}

			op := new(operation)
			setOperationParams(ctx, test.ctxMapping, op)

			if act, exp := op.params, test.exp; !reflect.DeepEqual(act, exp) {
				t.Fatalf(
					"unexpected operation parameters: %v; want %v",
					act, exp,
				)
			}
		})
	}
}
