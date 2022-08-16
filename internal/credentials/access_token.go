package credentials

import (
	"context"
)

// accessTokenCredentials implements Credentials interface with static
// authorization parameters.
type accessTokenCredentials struct {
	token      string
	sourceInfo string
}

func NewAccessTokenCredentials(token string, sourceInfo string) Credentials {
	return &accessTokenCredentials{
		token:      token,
		sourceInfo: sourceInfo,
	}
}

// Token implements Credentials.
func (a accessTokenCredentials) Token(_ context.Context) (string, error) {
	return a.token, nil
}
