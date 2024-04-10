package balancer

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errBalancerClosed = xerrors.Wrap(errors.New("balancer closed"))
