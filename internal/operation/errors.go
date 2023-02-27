package operation

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// ErrNotReady specified error when operation is not ready
var ErrNotReady = xerrors.Wrap(fmt.Errorf("operation is not ready yet"))
