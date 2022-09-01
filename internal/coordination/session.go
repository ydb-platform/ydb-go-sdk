package coordination

import (
	"context"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"math/rand"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Session struct {
	sessionID uint64
	path      string
	client    *Client
}

func (s *Session) Stop() error {
	return nil
}

func (s *Session) CreateSemaphore(
	ctx context.Context,
	name, path string,
	limit uint64,
) error {
	if s.client == nil {
		return xerrors.WithStackTrace(errNilClient)
	}

	return xerrors.WithStackTrace(s.createSemaphore(ctx, name, path, limit))
}

func (s *Session) createSemaphore(
	ctx context.Context,
	name, path string,
	limit uint64,
) error {
	serviceClient, err := s.client.service.Session(ctx)
	if err != nil {
		return err
	}
	defer serviceClient.CloseSend()

	_, err = s.client.sessionStart(serviceClient, path,
		config.WithSessionID(s.sessionID),
	)
	if err != nil {
		return err
	}

	err = serviceClient.Send(&Ydb_Coordination.SessionRequest{
		Request: &Ydb_Coordination.SessionRequest_CreateSemaphore_{
			CreateSemaphore: &Ydb_Coordination.SessionRequest_CreateSemaphore{
				ReqId: rand.Uint64(),
				Name:  name,
				Limit: limit,
			},
		},
	})
	if err != nil {
		return err
	}

	var (
		response *Ydb_Coordination.SessionResponse
		result   *Ydb_Coordination.SessionResponse_CreateSemaphoreResult
	)
	for result == nil {
		response, err = serviceClient.Recv()
		if err != nil {
			return err
		}
		result = response.GetCreateSemaphoreResult()

	}

	return nil
}

func (s *Session) DeleteSemaphore(
	ctx context.Context,
	name, path string,
) error {
	if s.client == nil {
		return xerrors.WithStackTrace(errNilClient)
	}

	return xerrors.WithStackTrace(s.deleteSemaphore(ctx, name, path))
}

func (s *Session) deleteSemaphore(
	ctx context.Context,
	name, path string,
) error {
	serviceClient, err := s.client.service.Session(ctx)
	if err != nil {
		return err
	}
	defer serviceClient.CloseSend()

	_, err = s.client.sessionStart(serviceClient, path,
		config.WithSessionID(s.sessionID),
	)
	if err != nil {
		return err
	}

	return serviceClient.Send(&Ydb_Coordination.SessionRequest{
		Request: &Ydb_Coordination.SessionRequest_DeleteSemaphore_{
			DeleteSemaphore: &Ydb_Coordination.SessionRequest_DeleteSemaphore{
				ReqId: rand.Uint64(),
				Name:  name,
			},
		},
	})
}

func (s *Session) DescribeSemaphore(
	ctx context.Context,
	name, path string,
	id uint64,
) (
	coordination.SemaphoreData,
	error,
) {
	if s.client == nil {
		return coordination.SemaphoreData{}, xerrors.WithStackTrace(errNilClient)
	}

	result, err := s.describeSemaphore(ctx, name, path, id)
	if err != nil {
		return coordination.SemaphoreData{}, xerrors.WithStackTrace(err)
	}

	return result, nil
}

func (s *Session) describeSemaphore(
	ctx context.Context,
	name, path string,
	id uint64,
) (
	coordination.SemaphoreData,
	error,
) {
	serviceClient, err := s.client.service.Session(ctx)
	if err != nil {
		return coordination.SemaphoreData{}, err
	}
	defer serviceClient.CloseSend()

	_, err = s.client.sessionStart(serviceClient, path,
		config.WithSessionID(s.sessionID),
	)
	if err != nil {
		return coordination.SemaphoreData{}, err
	}

	return s.client.loadData(serviceClient, id, name)
}

func (s *Session) CreateLocker(name string) coordination.Locker {
	return &Locker{
		name:      name,
		path:      s.path,
		sessionID: s.sessionID,
		client:    s.client,
	}
}

func (s *Session) KeepAlive(ctx context.Context) error {
	return nil
}

func (s *Session) KeepAliveOnce(ctx context.Context) error {
	if s.client == nil {
		return xerrors.WithStackTrace(errNilClient)
	}

	return xerrors.WithStackTrace(s.keepAliveOnce(ctx))
}

func (s *Session) keepAliveOnce(ctx context.Context) error {
	serviceClient, err := s.client.service.Session(ctx)
	if err != nil {
		return err
	}
	defer serviceClient.CloseSend()

	_, err = s.client.sessionStart(serviceClient, s.path,
		config.WithSessionID(s.sessionID),
	)

	return err
}
