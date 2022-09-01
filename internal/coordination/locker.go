package coordination

import (
	"context"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// Locker - distributed semaphore
type Locker struct {
	name      string
	path      string
	sessionID uint64
	client    *Client
}

func (l *Locker) Lock(ctx context.Context, opts ...config.LockOption) error {
	cfg := config.NewLockConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if l.client == nil {
		return xerrors.WithStackTrace(errNilClient)
	}

	return xerrors.WithStackTrace(l.lock(ctx, cfg))
}

func (l *Locker) lock(ctx context.Context, cfg *config.LockConfig) error {
	serviceClient, err := l.client.service.Session(ctx)
	if err != nil {
		return err
	}
	defer serviceClient.CloseSend()

	_, err = l.client.sessionStart(serviceClient, l.path, config.WithSessionID(l.sessionID))
	if err != nil {
		return err
	}

	err = serviceClient.Send(&Ydb_Coordination.SessionRequest{
		Request: &Ydb_Coordination.SessionRequest_AcquireSemaphore_{
			AcquireSemaphore: &Ydb_Coordination.SessionRequest_AcquireSemaphore{
				ReqId:         l.sessionID,
				Name:          l.name,
				TimeoutMillis: cfg.TimeoutMillis(),
				Count:         cfg.Count(),
				Data:          cfg.Data(),
			},
		},
	})
	if err != nil {
		return err
	}

	var (
		response *Ydb_Coordination.SessionResponse
		result   *Ydb_Coordination.SessionResponse_AcquireSemaphoreResult
	)
	for result.GetReqId() != l.sessionID {
		response, err = serviceClient.Recv()
		if err != nil {
			return err
		}

		result = response.GetAcquireSemaphoreResult()
	}

	return nil
}

func (l *Locker) Unlock(ctx context.Context) error {
	if l.client == nil {
		return xerrors.WithStackTrace(errNilClient)
	}

	err := l.unlock(ctx)
	if err != nil {
		return err
	}

	l.sessionID = 0
	return nil
}

func (l *Locker) unlock(
	ctx context.Context,
) error {
	serviceClient, err := l.client.service.Session(ctx)
	if err != nil {
		return err
	}
	defer serviceClient.CloseSend()

	_, err = l.client.sessionStart(serviceClient, l.path, config.WithSessionID(l.sessionID))
	if err != nil {
		return err
	}

	err = serviceClient.Send(&Ydb_Coordination.SessionRequest{
		Request: &Ydb_Coordination.SessionRequest_ReleaseSemaphore_{
			ReleaseSemaphore: &Ydb_Coordination.SessionRequest_ReleaseSemaphore{
				ReqId: l.sessionID,
				Name:  l.name,
			},
		},
	})

	if err != nil {
		return err
	}

	var (
		response *Ydb_Coordination.SessionResponse
		result   *Ydb_Coordination.SessionResponse_ReleaseSemaphoreResult
	)
	for result.GetReqId() != l.sessionID {
		response, err = serviceClient.Recv()
		if err != nil {
			return err
		}

		result = response.GetReleaseSemaphoreResult()
	}

	return nil
}

func (l *Locker) StoreData(ctx context.Context, date []byte) error {
	return nil
}

func (l *Locker) LoadData(ctx context.Context) ([]byte, error) {
	if l.client == nil {
		return nil, xerrors.WithStackTrace(errNilClient)
	}

	serviceClient, err := l.client.service.Session(ctx)
	if err != nil {
		return nil, err
	}
	defer serviceClient.CloseSend()

	_, err = l.client.sessionStart(serviceClient, l.path, config.WithSessionID(l.sessionID))
	if err != nil {
		return nil, err
	}

	res, err := l.client.loadData(serviceClient, l.sessionID, l.name)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return res.GetSemaphoreDescription().GetData(), nil
}
