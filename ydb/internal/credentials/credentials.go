package credentials

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// errNoCredentials may be returned by Credentials implementations to
// make driver act as if there are no Credentials at all. That is, driver will
// not send any token meta information during request.
var errNoCredentials = xerrors.Wrap(fmt.Errorf("ydb: credentials: no credentials"))

// Credentials is an interface of YDB credentials required for connect with YDB
type Credentials interface {
	// Token must return actual token or error
	Token(context.Context) (string, error)
}
