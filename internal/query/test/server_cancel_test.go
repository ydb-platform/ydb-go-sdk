package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestQueryClientServerCanceledIsNotContextError(t *testing.T) {
	mockSrv := mock.Server(t, mock.WithExecuteQuery(func(context.Context) error {
		return status.Error(grpcCodes.Canceled, "server canceled")
	}))

	ctx := t.Context()
	db, err := ydb.Open(ctx, mockSrv.ConnString(), ydb.WithAnonymousCredentials())
	require.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	_, err = db.Query().Query(ctx, "SELECT 1")

	require.Error(t, err)
	require.NoError(t, ctx.Err())
	require.True(t, xerrors.IsTransportError(err, grpcCodes.Canceled))
	require.False(t, xerrors.IsContextError(err))
}

func TestQueryClientCanceledContextIsContextError(t *testing.T) {
	control := make(chan struct{})
	mockSrv := mock.Server(t, mock.WithExecuteQuery(func(context.Context) error {
		control <- struct{}{}
		<-control

		return nil
	}))

	ctx := t.Context()
	db, err := ydb.Open(ctx, mockSrv.ConnString(), ydb.WithAnonymousCredentials())
	require.NoError(t, err)
	defer func() { _ = db.Close(ctx) }()

	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-control:
		case <-ctx.Done():
			return
		}
		cancel()
		select {
		case control <- struct{}{}:
		case <-ctx.Done():
		}
	}()

	_, err = db.Query().Query(queryCtx, "SELECT 1")
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, xerrors.IsContextError(err))
}
