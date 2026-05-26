package query

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
)

func TestSessionPoolWarmUp(t *testing.T) {
	ctx := t.Context()

	t.Run("DisabledByDefault", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := newMockQueryServiceForWarmUp(ctrl, 0)

		c, err := newWithQueryServiceClient(ctx, client, nil, config.New(config.WithPoolLimit(10)))
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close(ctx) })

		require.Equal(t, 0, c.explicitSessionPool.Stats().Idle)
		require.Equal(t, 0, c.implicitSessionPool.Stats().Idle)
	})

	t.Run("ExplicitPoolOnly", func(t *testing.T) {
		const warmUpSize = 3

		ctrl := gomock.NewController(t)
		client := newMockQueryServiceForWarmUp(ctrl, warmUpSize)

		c, err := newWithQueryServiceClient(ctx, client, nil, config.New(
			config.WithPoolLimit(10),
			config.WithSessionPoolWarmUpSessions(warmUpSize),
		))
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close(ctx) })

		require.Equal(t, warmUpSize, c.explicitSessionPool.Stats().Idle)
		require.Equal(t, 0, c.implicitSessionPool.Stats().Idle)
	})

	t.Run("ImplicitPoolNeverWarmsUp", func(t *testing.T) {
		const warmUpSize = 3

		ctrl := gomock.NewController(t)
		client := newMockQueryServiceForWarmUp(ctrl, warmUpSize)

		c, err := newWithQueryServiceClient(ctx, client, nil, config.New(
			config.AllowImplicitSessions(),
			config.WithPoolLimit(10),
			config.WithSessionPoolWarmUpSessions(warmUpSize),
		))
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close(ctx) })

		require.Equal(t, warmUpSize, c.explicitSessionPool.Stats().Idle)
		require.Equal(t, 0, c.implicitSessionPool.Stats().Idle)
	})

	t.Run("CappedByPoolLimit", func(t *testing.T) {
		const (
			limit      = 2
			warmUpSize = 5
		)

		ctrl := gomock.NewController(t)
		client := newMockQueryServiceForWarmUp(ctrl, limit)

		c, err := newWithQueryServiceClient(ctx, client, nil, config.New(
			config.WithPoolLimit(limit),
			config.WithSessionPoolWarmUpSessions(warmUpSize),
		))
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close(ctx) })

		require.Equal(t, limit, c.explicitSessionPool.Stats().Idle)
	})

	t.Run("CreateSessionErrorFailsNew", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil, context.DeadlineExceeded)

		_, err := newWithQueryServiceClient(ctx, client, nil, config.New(
			config.WithSessionPoolWarmUpSessions(1),
		))
		require.Error(t, err)
	})
}

func newMockQueryServiceForWarmUp(ctrl *gomock.Controller, createSessions int) Ydb_Query_V1.QueryServiceClient {
	client := NewMockQueryServiceClient(ctrl)

	if createSessions == 0 {
		return client
	}

	attachStream := NewMockQueryService_AttachSessionClient(ctrl)
	stubAttachStreamContext(attachStream, nil)
	attachStream.EXPECT().Recv().Return(&Ydb_Query.SessionState{
		Status: Ydb.StatusIds_SUCCESS,
	}, nil).AnyTimes()

	var sessionSeq atomic.Int32

	client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *Ydb_Query.CreateSessionRequest, ...grpc.CallOption) (
			*Ydb_Query.CreateSessionResponse, error,
		) {
			id := sessionSeq.Add(1)

			return &Ydb_Query.CreateSessionResponse{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: fmt.Sprintf("session-%d", id),
			}, nil
		},
	).Times(createSessions)
	client.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil).Times(createSessions)
	client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
		Status: Ydb.StatusIds_SUCCESS,
	}, nil).AnyTimes()

	return client
}
