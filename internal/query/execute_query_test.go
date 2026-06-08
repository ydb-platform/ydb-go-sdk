package query

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"google.golang.org/protobuf/proto"
)

func TestExecute(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		stream := happyWayStream(ctrl)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
		var txID string
		r, err := execute(ctx, "123", client, "", options.ExecuteSettings(),
			onTxMeta(func(txMeta *Ydb_Query.TransactionMeta) {
				txID = txMeta.GetId()
			}),
		)
		require.NoError(t, err)
		defer r.Close(ctx)
		require.EqualValues(t, "456", txID)
		require.EqualValues(t, -1, r.resultSetIndex)
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 0, rs.index)
			{
				t.Log("next (row=1)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=2)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=3)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				t.Log("next (row=4)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=5)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=6)")
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
		}
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 1, rs.index)
		}
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 2, rs.index)
			{
				t.Log("next (row=1)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=2)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=3)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=4)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=5)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				t.Log("next (row=6)")
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
		}
		{
			t.Log("close result")
			r.Close(t.Context())
		}
		{
			t.Log("nextResultSet")
			_, err := r.nextResultSet(t.Context())
			require.ErrorIs(t, err, io.EOF)
		}
	})
	t.Run("TransportError", func(t *testing.T) {
		t.Run("OnCall", func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
			t.Log("execute")
			_, err := execute(ctx, "123", client, "", options.ExecuteSettings())
			require.Error(t, err)
			require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
		})
		t.Run("OnStream", func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status: Ydb.StatusIds_SUCCESS,
				TxMeta: Ydb_Query.TransactionMeta_builder{
					Id: "456",
				}.Build(),
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
			t.Log("execute")
			var txID string
			r, err := execute(ctx, "123", client, "", options.ExecuteSettings(),
				onTxMeta(func(txMeta *Ydb_Query.TransactionMeta) {
					txID = txMeta.GetId()
				}),
			)
			require.NoError(t, err)
			defer r.Close(ctx)
			require.EqualValues(t, "456", txID)
			require.EqualValues(t, -1, r.resultSetIndex)
			{
				t.Log("nextResultSet")
				rs, err := r.nextResultSet(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.index)
				{
					t.Log("next (row=1)")
					_, err := rs.nextRow(ctx)
					require.NoError(t, err)
					require.EqualValues(t, 0, rs.rowIndex)
				}
				{
					t.Log("next (row=2)")
					_, err := rs.nextRow(ctx)
					require.NoError(t, err)
					require.EqualValues(t, 1, rs.rowIndex)
				}
				{
					t.Log("next (row=3)")
					_, err := rs.nextRow(ctx)
					require.NoError(t, err)
					require.EqualValues(t, 2, rs.rowIndex)
				}
				{
					t.Log("next (row=4)")
					_, err := rs.nextRow(ctx)
					require.NoError(t, err)
					require.EqualValues(t, 0, rs.rowIndex)
				}
				{
					t.Log("next (row=5)")
					_, err := rs.nextRow(ctx)
					require.NoError(t, err)
					require.EqualValues(t, 1, rs.rowIndex)
				}
				{
					t.Log("next (row=6)")
					_, err := rs.nextRow(ctx)
					require.Error(t, err)
					require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
				}
			}
		})
	})
	t.Run("OperationError", func(t *testing.T) {
		t.Run("OnCall", func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(nil, xerrors.Operation(xerrors.WithStatusCode(
				Ydb.StatusIds_UNAVAILABLE,
			)))
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
			t.Log("execute")
			_, err := execute(ctx, "123", client, "", options.ExecuteSettings())
			require.Error(t, err)
			require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
		})
		t.Run("OnStream", func(t *testing.T) {
			ctx := t.Context()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status: Ydb.StatusIds_SUCCESS,
				TxMeta: Ydb_Query.TransactionMeta_builder{
					Id: "456",
				}.Build(),
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(nil, xerrors.Operation(xerrors.WithStatusCode(
				Ydb.StatusIds_UNAVAILABLE,
			)))
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
			t.Log("execute")
			var txID string
			r, err := execute(ctx, "123", client, "", options.ExecuteSettings(),
				onTxMeta(func(txMeta *Ydb_Query.TransactionMeta) {
					txID = txMeta.GetId()
				}),
			)
			require.NoError(t, err)
			defer r.Close(ctx)
			require.EqualValues(t, "456", txID)
			require.EqualValues(t, -1, r.resultSetIndex)
			{
				t.Log("nextResultSet")
				rs, err := r.nextResultSet(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.index)
				{
					t.Log("next (row=1)")
					_, err := rs.nextRow(ctx)
					require.NoError(t, err)
					require.EqualValues(t, 0, rs.rowIndex)
				}
				{
					t.Log("next (row=2)")
					_, err := rs.nextRow(ctx)
					require.NoError(t, err)
					require.EqualValues(t, 1, rs.rowIndex)
				}
				{
					t.Log("next (row=3)")
					_, err := rs.nextRow(ctx)
					require.NoError(t, err)
					require.EqualValues(t, 2, rs.rowIndex)
				}
				{
					t.Log("next (row=4)")
					_, err := rs.nextRow(ctx)
					require.Error(t, err)
					require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
				}
			}
		})
	})
	t.Run("ContextCancellation", func(t *testing.T) {
		t.Run("CancelWhileExecute", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctx, cancel := context.WithCancel(t.Context())
			var executeCtx context.Context

			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
				cancel() // canceling happen in the beginning of the Recv() call

				<-executeCtx.Done()

				return nil, executeCtx.Err()
			})

			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
					Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
				) {
					executeCtx = ctx

					return stream, nil
				})

			// When execute() with context, cancelled in progress
			_, err := execute(ctx, "123", client, "", options.ExecuteSettings())

			// Then context cancellation error is returned
			require.ErrorIs(t, err, context.Canceled)
		})

		t.Run("CancelAfterExecute", func(t *testing.T) {
			ctrl := gomock.NewController(t)

			stream := happyWayStream(ctrl)

			var streamCtx context.Context
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
					Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
				) {
					streamCtx = ctx

					return stream, nil
				})

			executeCtx, cancelExecuteCtx := context.WithCancel(t.Context())
			r, err := execute(executeCtx, "123", client, "", options.ExecuteSettings())
			require.NoError(t, err)

			cancelExecuteCtx()

			_, err = readResultSet(t.Context(), r)
			require.NoError(t, err)
			_, err = readResultSet(t.Context(), r)
			require.NoError(t, err)
			_, err = readResultSet(t.Context(), r)
			require.NoError(t, err)

			// check here because the last `readResultSet()` closes stream with stream cancellation
			require.NoError(t, streamCtx.Err())

			_, err = readResultSet(t.Context(), r)
			require.ErrorIs(t, err, io.EOF)
		})

		// Regression test for https://github.com/ydb-platform/ydb-go-sdk/issues/2081.
		//
		// When the parent ctx is cancelled inside ExecuteQuery (simulating session
		// expiry while the gRPC stream is still open), execute() must surface that
		// cancellation as context.Canceled to the caller, regardless of whether the
		// stream's Recv() is reached.
		//
		// Two paths can lead to context.Canceled, depending on how the
		// context.AfterFunc that forwards the parent ctx to executeCtx races with
		// execute() proceeding into newResult:
		//   - the AfterFunc has already cancelled executeCtx by the time
		//     nextPart() runs, so the ctx.Err() check at the top of nextPart()
		//     returns context.Canceled without invoking Recv();
		//   - the AfterFunc has not fired yet, nextPart() proceeds to Recv() and
		//     the mock returns ctx.Err() (parent ctx is already cancelled).
		// Either way the final error must be context.Canceled. The mock therefore
		// allows Recv() any number of times - including zero - so the test stays
		// deterministic across both paths.
		t.Run("CancelParentContextAfterStreamOpen", func(t *testing.T) {
			t.Run("idempotent=true", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				ctx, cancel := context.WithCancel(t.Context())

				// Recv() may or may not be reached depending on the AfterFunc race
				// described above; on either path it returns the parent ctx's
				// cancellation error.
				stream := newExecuteQueryStreamMock(ctrl)
				stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
					return nil, ctx.Err()
				}).AnyTimes()

				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, _ *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
						Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
					) {
						// Simulate session expiry: canceling ctx closes ctx.Done() so that
						// the non-blocking check in execute() fires after ExecuteQuery returns.
						cancel()

						return stream, nil
					})

				_, err := execute(xcontext.WithIdempotent(ctx, true),
					"123", client, "", options.ExecuteSettings(),
				)
				require.Error(t, err)
				require.ErrorIs(t, err, context.Canceled)
			})
			t.Run("idempotent=false", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				ctx, cancel := context.WithCancel(t.Context())

				// Same race-tolerant expectation as the idempotent=true case:
				// Recv() may be invoked zero or more times depending on whether
				// the AfterFunc forwarding parent ctx → executeCtx fires before
				// nextPart()'s ctx.Err() check.
				stream := newExecuteQueryStreamMock(ctrl)
				stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
					return nil, ctx.Err()
				}).AnyTimes()

				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, _ *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
						Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
					) {
						// Simulate session expiry: canceling ctx closes ctx.Done() so that
						// the non-blocking check in execute() fires after ExecuteQuery returns.
						cancel()

						return stream, nil
					})

				_, err := execute(ctx, // xcontext.WithIdempotent(ctx, false),
					"123", client, "", options.ExecuteSettings(),
				)
				require.Error(t, err)
				require.ErrorIs(t, err, context.Canceled)
			})
		})

		// Per-call ctx cancellation is checked before stream.Recv() only. See
		// per_call_ctx_recv_test.go for blocked-Recv behavior and Close unblocking.
		t.Run("CancelCallCtxReturnsWithoutCancelingExecuteStream", func(t *testing.T) {
			ctrl := gomock.NewController(t)

			stream := newExecuteQueryStreamMock(ctrl)
			gomock.InOrder(
				stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
					Status:         Ydb.StatusIds_SUCCESS,
					TxMeta:         Ydb_Query.TransactionMeta_builder{Id: "456"}.Build(),
					ResultSetIndex: 0,
					ResultSet:      Ydb.ResultSet_builder{}.Build(),
				}.Build(), nil),
				// Close(background) must still drain the stream after per-call ctx cancel.
				stream.EXPECT().Recv().Return(nil, io.EOF),
			)

			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
					Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
				) {
					return stream, nil
				})

			r, err := execute(t.Context(), "123", client, "", options.ExecuteSettings())
			require.NoError(t, err)

			callCtx, callCancel := context.WithCancel(t.Context())
			callCancel()

			_, err = r.nextPart(callCtx)
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, r.lastErr)

			require.NoError(t, r.Close(t.Context()))
		})

		t.Run("CancelCallCtxWhileRecvBlockedViaExecute", func(t *testing.T) {
			ctrl := gomock.NewController(t)

			var executeCtx context.Context
			recvEntered := make(chan struct{})

			stream := NewMockQueryService_ExecuteQueryClient(ctrl)
			gomock.InOrder(
				stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
					Status:         Ydb.StatusIds_SUCCESS,
					TxMeta:         Ydb_Query.TransactionMeta_builder{Id: "456"}.Build(),
					ResultSetIndex: 0,
					ResultSet:      Ydb.ResultSet_builder{}.Build(),
				}.Build(), nil),
				stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
					close(recvEntered)

					<-executeCtx.Done()

					return nil, executeCtx.Err()
				}),
			)
			// Drain Recv during Close is optional: if callCtx cancel already
			// propagated to executeCtx via withStreamCancel, Close returns early.
			stream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()

			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ *Ydb_Query.ExecuteQueryRequest, _ ...grpc.CallOption) (
					Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
				) {
					executeCtx = ctx
					stubExecuteQueryStreamContext(ctx, stream)

					return stream, nil
				})

			r, err := execute(t.Context(), "123", client, "", options.ExecuteSettings(),
				withStreamResultCloseTimeout(50*time.Millisecond),
			)
			require.NoError(t, err)

			callCtx, callCancel := context.WithCancel(t.Context())

			iterDone := make(chan error, 1)
			go func() {
				_, err := r.nextPart(callCtx)
				iterDone <- err
			}()

			<-recvEntered
			callCancel()

			// execute() wires withStreamCancel(executeCancel), so per-call ctx
			// cancel forwards to the gRPC stream context asynchronously via
			// context.AfterFunc — not synchronously at callCancel() time.
			require.Eventually(t, func() bool {
				return executeCtx.Err() != nil
			}, time.Second, time.Millisecond,
				"callCtx cancel must propagate to execute stream")
			require.ErrorIs(t, executeCtx.Err(), context.Canceled)

			start := time.Now()
			closeErr := r.Close(t.Context())
			require.Less(t, time.Since(start), time.Second)

			select {
			case err := <-iterDone:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("nextPart still blocked after Close")
			}

			// Close may return nil if the stream was already torn down by
			// streamCancel, or DeadlineExceeded if drain hit closeTimeout first.
			if closeErr != nil {
				require.ErrorIs(t, closeErr, context.DeadlineExceeded)
			}
		})
	})
}

// TestNewResult_DecoupledExecuteCtx verifies the semantic contract that
// newResult succeeds when passed executeCtx (derived from a cancelled parent
// via xcontext.ValueOnly), which is exactly what execute() does after the fix
// for https://github.com/ydb-platform/ydb-go-sdk/issues/2081.
func TestNewResult_DecoupledExecuteCtx(t *testing.T) {
	t.Run("CancelledParentCtxCausesImmediateError", func(t *testing.T) {
		// With the parent ctx passed directly, a cancelled ctx makes newResult
		// fail before it ever calls Recv(). This is the old (buggy) behavior
		// that the fix addresses at the execute() call-site.
		parentCtx, parentCancel := context.WithCancel(t.Context())
		parentCancel()

		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		// Recv must NOT be called — the cancelled ctx short-circuits newResult.

		_, err := newResult(parentCtx, stream)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("DecoupledExecuteCtxSucceedsWhenParentCancelled", func(t *testing.T) {
		// Create executeCtx exactly as execute() does: strip cancellation from
		// parent via xcontext.ValueOnly, then add an independent cancel.
		// Even though parentCtx is already cancelled, executeCtx is not — so
		// newResult can proceed to Recv() and return the first response part.
		parentCtx, parentCancel := context.WithCancel(t.Context())
		parentCancel()

		executeCtx, executeCancel := xcontext.WithCancel(xcontext.ValueOnly(parentCtx))
		defer executeCancel()

		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 0,
			ResultSet:      &Ydb.ResultSet{},
		}.Build(), nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)

		r, err := newResult(executeCtx, stream)
		require.NoError(t, err)
		if r != nil {
			r.Close(t.Context())
		}
	})
}

func TestExecuteQueryRequest(t *testing.T) {
	for _, tt := range []struct {
		name        string
		opts        []options.Execute
		request     *Ydb_Query.ExecuteQueryRequest
		callOptions []grpc.CallOption
	}{
		{
			name: "WithoutOptions",
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithoutOptions",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithoutOptions",
				}.Build(),
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			}.Build(),
		},
		{
			name: "WithTxControl",
			opts: []options.Execute{
				options.WithTxControl(query.SerializableReadWriteTxControl(query.CommitTx())),
			},
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithTxControl",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				TxControl: Ydb_Query.TransactionControl_builder{
					BeginTx: Ydb_Query.TransactionSettings_builder{
						SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
					}.Build(),
					CommitTx: true,
				}.Build(),
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithTxControl",
				}.Build(),
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			}.Build(),
		},
		{
			name: "WithParams",
			opts: []options.Execute{
				options.WithParameters(
					params.Builder{}.
						Param("$a").Text("A").
						Param("$b").Text("B").
						Param("$c").Text("C").
						Build(),
				),
			},
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithParams",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithParams",
				}.Build(),
				Parameters: map[string]*Ydb.TypedValue{
					"$a": Ydb.TypedValue_builder{
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
						Value: Ydb.Value_builder{
							TextValue: proto.String("A"),
						}.Build(),
					}.Build(),
					"$b": Ydb.TypedValue_builder{
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
						Value: Ydb.Value_builder{
							TextValue: proto.String("B"),
						}.Build(),
					}.Build(),
					"$c": Ydb.TypedValue_builder{
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
						Value: Ydb.Value_builder{
							TextValue: proto.String("C"),
						}.Build(),
					}.Build(),
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			}.Build(),
		},
		{
			name: "WithExplain",
			opts: []options.Execute{
				options.WithExecMode(options.ExecModeExplain),
			},
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithExplain",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXPLAIN,
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithExplain",
				}.Build(),
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			}.Build(),
		},
		{
			name: "WithValidate",
			opts: []options.Execute{
				options.WithExecMode(options.ExecModeValidate),
			},
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithValidate",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_VALIDATE,
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithValidate",
				}.Build(),
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			}.Build(),
		},
		{
			name: "WithValidate",
			opts: []options.Execute{
				options.WithExecMode(options.ExecModeParse),
			},
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithValidate",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_PARSE,
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithValidate",
				}.Build(),
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			}.Build(),
		},
		{
			name: "WithStatsFull",
			opts: []options.Execute{
				options.WithStatsMode(options.StatsModeFull, nil),
			},
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithStatsFull",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithStatsFull",
				}.Build(),
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_FULL,
				ConcurrentResultSets: false,
			}.Build(),
		},
		{
			name: "WithStatsBasic",
			opts: []options.Execute{
				options.WithStatsMode(options.StatsModeBasic, nil),
			},
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithStatsBasic",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithStatsBasic",
				}.Build(),
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_BASIC,
				ConcurrentResultSets: false,
			}.Build(),
		},
		{
			name: "WithStatsProfile",
			opts: []options.Execute{
				options.WithStatsMode(options.StatsModeProfile, nil),
			},
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithStatsProfile",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithStatsProfile",
				}.Build(),
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_PROFILE,
				ConcurrentResultSets: false,
			}.Build(),
		},
		{
			name: "WithGrpcCallOptions",
			opts: []options.Execute{
				options.WithCallOptions(grpc.Header(&metadata.MD{
					"ext-header": []string{"test"},
				})),
			},
			request: Ydb_Query.ExecuteQueryRequest_builder{
				SessionId: "WithGrpcCallOptions",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				QueryContent: Ydb_Query.QueryContent_builder{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "WithGrpcCallOptions",
				}.Build(),
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			}.Build(),
			callOptions: []grpc.CallOption{
				grpc.Header(&metadata.MD{
					"ext-header": []string{"test"},
				}),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request, callOptions, err := executeQueryRequest(tt.name, tt.name, options.ExecuteSettings(tt.opts...))
			require.NoError(t, err)
			require.Equal(t, request.String(), tt.request.String())
			require.Equal(t, tt.callOptions, callOptions)
		})
	}
}

func happyWayStream(ctrl *gomock.Controller) Ydb_Query_V1.QueryService_ExecuteQueryClient {
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status: Ydb.StatusIds_SUCCESS,
		TxMeta: Ydb_Query.TransactionMeta_builder{
			Id: "456",
		}.Build(),
		ResultSetIndex: 0,
		ResultSet: Ydb.ResultSet_builder{
			Columns: []*Ydb.Column{
				Ydb.Column_builder{
					Name: "a",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UINT64.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "b",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UTF8.Enum(),
					}.Build(),
				}.Build(),
			},
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(1),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("1"),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(2),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("2"),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(3),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("3"),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet: Ydb.ResultSet_builder{
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(4),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("4"),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(5),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("5"),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 1,
		ResultSet: Ydb.ResultSet_builder{
			Columns: []*Ydb.Column{
				Ydb.Column_builder{
					Name: "c",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UINT64.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "d",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UTF8.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "e",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_BOOL.Enum(),
					}.Build(),
				}.Build(),
			},
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(1),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("1"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(true),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(2),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("2"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 1,
		ResultSet: Ydb.ResultSet_builder{
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(3),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("3"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(true),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(4),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("4"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(5),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("5"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 2,
		ResultSet: Ydb.ResultSet_builder{
			Columns: []*Ydb.Column{
				Ydb.Column_builder{
					Name: "c",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UINT64.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "d",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UTF8.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "e",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_BOOL.Enum(),
					}.Build(),
				}.Build(),
			},
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(1),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("1"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(true),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(2),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("2"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 2,
		ResultSet: Ydb.ResultSet_builder{
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(3),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("3"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(true),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(4),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("4"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(5),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("5"),
					}.Build(), Ydb.Value_builder{
						BoolValue: proto.Bool(false),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)

	return stream
}
