package coordination

import (
	"context"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type Locker interface {
	Lock(ctx context.Context, opts ...config.LockOption) error
	Unlock(ctx context.Context) error

	//LoadData(ctx context.Context) ([]byte, error)
	//StoreData(ctx context.Context, data []byte) error
}

type Session interface {
	CreateLocker(name, path string) Locker
	Stop() error

	//KeepAlive(ctx context.Context) error
	//KeepAliveOnce(ctx context.Context) error

	CreateSemaphore(ctx context.Context, name, path string, limit uint64) error
	DeleteSemaphore(ctx context.Context, name, path string) error
	DescribeSemaphore(ctx context.Context, name, path string, id uint64) (*Ydb_Coordination.SessionResponse_DescribeSemaphoreResult, error)
}

type Client interface {
	closer.Closer

	CreateNode(ctx context.Context, path string, config NodeConfig) (err error)
	AlterNode(ctx context.Context, path string, config NodeConfig) (err error)
	DropNode(ctx context.Context, path string) (err error)
	DescribeNode(ctx context.Context, path string) (_ *scheme.Entry, _ *NodeConfig, err error)

	SessionStart(ctx context.Context, path string, opts ...config.SessionStartOption) (Session, error)
}
