package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

type (
	SessionInfo interface {
		ID() string
		NodeID() uint32
		Status() string
	}
	Session interface {
		SessionInfo
		Executor

		Begin(ctx context.Context, txSettings TransactionSettings) (Transaction, error)
	}
	Stats = *stats.QueryStats
)
