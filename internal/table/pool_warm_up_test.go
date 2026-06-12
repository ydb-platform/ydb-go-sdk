package table

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestSessionPoolWarmUp(t *testing.T) {
	t.Run("DisabledByDefault", func(t *testing.T) {
		var createCalls atomic.Int32

		cc := testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			testutil.TableCreateSession: func(any) (proto.Message, error) {
				createCalls.Add(1)

				return Ydb_Table.CreateSessionResult_builder{
					SessionId: testutil.SessionID(),
				}.Build(), nil
			},
		}))

		c, err := New(t.Context(), cc, config.New(config.WithSizeLimit(10)))
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close(t.Context()) })

		require.Equal(t, 0, c.pool.Stats().Idle)
		require.Equal(t, int32(0), createCalls.Load())
	})

	t.Run("PreCreatesSessions", func(t *testing.T) {
		const warmUpSize = 3

		var createCalls atomic.Int32

		cc := testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			testutil.TableCreateSession: func(any) (proto.Message, error) {
				createCalls.Add(1)

				return Ydb_Table.CreateSessionResult_builder{
					SessionId: testutil.SessionID(),
				}.Build(), nil
			},
			testutil.TableDeleteSession: func(any) (proto.Message, error) {
				return &Ydb_Table.DeleteSessionResponse{}, nil
			},
		}))

		c, err := New(t.Context(), cc, config.New(
			config.WithSizeLimit(10),
			config.WithSessionPoolWarmUpSessions(warmUpSize),
		))
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close(t.Context()) })

		require.Equal(t, warmUpSize, c.pool.Stats().Idle)
		require.Equal(t, int32(warmUpSize), createCalls.Load())
	})

	t.Run("CappedBySizeLimit", func(t *testing.T) {
		const (
			limit      = 2
			warmUpSize = 5
		)

		var createCalls atomic.Int32

		cc := testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			testutil.TableCreateSession: func(any) (proto.Message, error) {
				createCalls.Add(1)

				return Ydb_Table.CreateSessionResult_builder{
					SessionId: testutil.SessionID(),
				}.Build(), nil
			},
			testutil.TableDeleteSession: func(any) (proto.Message, error) {
				return &Ydb_Table.DeleteSessionResponse{}, nil
			},
		}))

		c, err := New(t.Context(), cc, config.New(
			config.WithSizeLimit(limit),
			config.WithSessionPoolWarmUpSessions(warmUpSize),
		))
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close(t.Context()) })

		require.Equal(t, limit, c.pool.Stats().Idle)
		require.Equal(t, int32(limit), createCalls.Load())
	})

	t.Run("CreateSessionErrorFailsNew", func(t *testing.T) {
		cc := testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			testutil.TableCreateSession: func(any) (proto.Message, error) {
				return nil, context.DeadlineExceeded
			},
		}))

		_, err := New(t.Context(), cc, config.New(
			config.WithSessionPoolWarmUpSessions(1),
		))
		require.Error(t, err)
	})
}
