package credentials

import (
	"context"
)

// anonymousCredentials implements Credentials interface with anonymousCredentials access
type anonymousCredentials struct {
	sourceInfo string
}

func NewAnonymousCredentials(sourceInfo string) Credentials {
	return &anonymousCredentials{
		sourceInfo: sourceInfo,
	}
}

// Token implements Credentials.
func (a anonymousCredentials) Token(_ context.Context) (string, error) {
	return "", nil
}
