package pool

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errClosedPool     = errors.New("closed pool")
	errPoolOverflow   = xerrors.Retryable(errors.New("pool overflow"))
	errItemIsNotAlive = errors.New("item is not alive")
)
