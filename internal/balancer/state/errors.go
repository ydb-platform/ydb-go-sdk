package state

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrNoEndpoints = xerrors.Wrap(fmt.Errorf("no endpoints"))
	ErrNilState    = xerrors.Wrap(fmt.Errorf("nil state"))
)
