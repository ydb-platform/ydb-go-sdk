package operation

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

func TestParams(t *testing.T) {
	for _, tt := range []struct {
		ctx                  context.Context //nolint:containedctx
		preferContextTimeout bool
		timeout              time.Duration
		cancelAfter          time.Duration
		mode                 Mode
		exp                  *Ydb_Operations.OperationParams
	}{
		{
			ctx:         context.Background(),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp:         nil,
		},
		{
			ctx: WithTimeout(
				context.Background(),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithTimeout(
				WithTimeout(
					WithTimeout(
						WithTimeout(
							WithTimeout(
								context.Background(),
								time.Second*1,
							),
							time.Second*2,
						),
						time.Second*3,
					),
					time.Second*4,
				),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second * 1),
			},
		},
		{
			ctx: WithTimeout(
				WithTimeout(
					WithTimeout(
						WithTimeout(
							WithTimeout(
								context.Background(),
								time.Second*5,
							),
							time.Second*4,
						),
						time.Second*3,
					),
					time.Second*2,
				),
				time.Second*1,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second * 1),
			},
		},
		{
			ctx: WithCancelAfter(
				context.Background(),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				CancelAfter: durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithCancelAfter(
				WithCancelAfter(
					WithCancelAfter(
						WithCancelAfter(
							WithCancelAfter(
								context.Background(),
								time.Second*1,
							),
							time.Second*2,
						),
						time.Second*3,
					),
					time.Second*4,
				),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				CancelAfter: durationpb.New(time.Second * 1),
			},
		},
		{
			ctx: WithCancelAfter(
				WithCancelAfter(
					WithCancelAfter(
						WithCancelAfter(
							WithCancelAfter(
								context.Background(),
								time.Second*5,
							),
							time.Second*4,
						),
						time.Second*3,
					),
					time.Second*2,
				),
				time.Second*1,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				CancelAfter: durationpb.New(time.Second * 1),
			},
		},
		{
			ctx: WithCancelAfter(
				WithTimeout(
					context.Background(),
					time.Second*5,
				),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithCancelAfter(
				WithTimeout(
					context.Background(),
					time.Second*5,
				),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        ModeSync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithCancelAfter(
				WithTimeout(
					context.Background(),
					time.Second*5,
				),
				time.Second*5,
			),
			timeout:     time.Second * 2,
			cancelAfter: 0,
			mode:        ModeSync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithCancelAfter(
				WithTimeout(
					context.Background(),
					time.Second*5,
				),
				time.Second*5,
			),
			timeout:     time.Second * 2,
			cancelAfter: 0,
			mode:        ModeAsync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_ASYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: func() context.Context {
				ctx, _ := xcontext.WithTimeout(WithCancelAfter(
					WithTimeout(
						context.Background(),
						time.Second*5,
					),
					time.Second*5,
				), time.Second*10)

				return ctx
			}(),
			timeout:     time.Second * 2,
			cancelAfter: 0,
			mode:        ModeAsync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_ASYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: func() context.Context {
				ctx, _ := xcontext.WithTimeout(WithCancelAfter(
					WithTimeout(
						context.Background(),
						time.Second*5,
					),
					time.Second*5,
				), time.Second*1)

				return ctx
			}(),
			timeout:     time.Second * 2,
			cancelAfter: 0,
			mode:        ModeAsync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_ASYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: func() context.Context {
				ctx, _ := xcontext.WithTimeout(WithCancelAfter(
					WithTimeout(
						context.Background(),
						time.Second*5,
					),
					time.Second*5,
				), time.Second*1)

				return ctx
			}(),
			preferContextTimeout: true,
			timeout:              time.Second * 2,
			cancelAfter:          0,
			mode:                 ModeSync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second * 1),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: func() context.Context {
				ctx, _ := xcontext.WithTimeout(context.Background(), time.Second*1)

				return ctx
			}(),
			preferContextTimeout: true,
			timeout:              time.Second * 2,
			cancelAfter:          0,
			mode:                 ModeSync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second * 1),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := Params(tt.ctx, tt.timeout, tt.cancelAfter, tt.mode)
			t.Logf(
				"Params(): {%v}, compare to: {%v}",
				got,
				tt.exp,
			)
			if !tt.preferContextTimeout {
				if !reflect.DeepEqual(got, tt.exp) {
					t.Errorf(
						"Params(): {%v}, want: {%v}",
						got,
						tt.exp,
					)
				}

				return
			}
			if !reflect.DeepEqual(got.GetOperationMode(), tt.exp.GetOperationMode()) {
				t.Errorf(
					"Params().OperationMode: %v, want: %v",
					got.GetOperationMode(),
					tt.exp.GetOperationMode(),
				)
			}
			if !reflect.DeepEqual(got.GetCancelAfter(), tt.exp.GetCancelAfter()) {
				t.Errorf(
					"Params().CancelAfter: %v, want: %v",
					got.GetCancelAfter().AsDuration(),
					tt.exp.GetCancelAfter().AsDuration(),
				)
			}
			if got.GetOperationTimeout().AsDuration() > tt.exp.GetOperationTimeout().AsDuration() {
				t.Errorf(
					"Params().OperationTimeout: %v, want: <= %v",
					got.GetOperationTimeout().AsDuration(),
					tt.exp.GetOperationTimeout().AsDuration(),
				)
			}
		})
	}
}
