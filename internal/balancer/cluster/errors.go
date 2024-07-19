package cluster

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrNoEndpoints = xerrors.Wrap(fmt.Errorf("no endpoints"))
	ErrNilPtr      = xerrors.Wrap(fmt.Errorf("nil pointer"))
)
