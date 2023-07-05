package credentials

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/secret"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
)

var (
	_ Credentials  = (*AccessToken)(nil)
	_ fmt.Stringer = (*AccessToken)(nil)
)

// AccessToken implements Credentials interface with static
// authorization parameters.
type AccessToken struct {
	token      string
	sourceInfo string
}

func NewAccessTokenCredentials(token string, opts ...Option) *AccessToken {
	options := optionsHolder{
		sourceInfo: stack.Record(1),
	}
	for _, opt := range opts {
		opt(&options)
	}
	return &AccessToken{
		token:      token,
		sourceInfo: options.sourceInfo,
	}
}

// Token implements Credentials.
func (c AccessToken) Token(_ context.Context) (string, error) {
	return c.token, nil
}

// Token implements Credentials.
func (c AccessToken) String() string {
	return fmt.Sprintf("AccessToken(token:%q,from:%q)", secret.Token(c.token), c.sourceInfo)
}
