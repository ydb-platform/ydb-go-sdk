package credentials

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

// ErrNoCredentials may be returned by Credentials implementations to
// make driver act as if there are no Credentials at all. That is, driver will
// not send any token meta information during request.
var ErrNoCredentials = fmt.Errorf("ydb: credentials: no credentials")

// Credentials is an interface that contains options used to authorize a
// client.
type Credentials interface {
	Token(context.Context) (string, error)
}

// accessToken implements Credentials interface with static
// authorization parameters.
type accessToken struct {
	token      string
	sourceInfo string
}

func NewAccessTokenCredentials(token string, sourceInfo string) Credentials {
	return &accessToken{
		token:      token,
		sourceInfo: sourceInfo,
	}
}

// Token implements Credentials.
func (a accessToken) Token(_ context.Context) (string, error) {
	return a.token, nil
}

// anonymous implements Credentials interface with anonymous access
type anonymous struct {
	sourceInfo string
}

func NewAnonymousCredentials(sourceInfo string) Credentials {
	return &anonymous{
		sourceInfo: sourceInfo,
	}
}

// Token implements Credentials.
func (a anonymous) Token(_ context.Context) (string, error) {
	return "", nil
}

type multiCredentials struct {
	cs []Credentials
}

func (m *multiCredentials) Token(ctx context.Context) (token string, err error) {
	for _, c := range m.cs {
		token, err = c.Token(ctx)
		if err == nil {
			return
		}
	}
	if err == nil {
		err = errors.Errorf("multiCredentials.Token(): %w", ErrNoCredentials)
	}
	return
}

// MultiCredentials creates Credentials which represents multiple ways of
// obtaining token.
// Its Token() method proxies call to the underlying credentials in order.
// When first successful call met, it returns. If there are no successful
// calls, it returns last error.
func MultiCredentials(cs ...Credentials) Credentials {
	all := make([]Credentials, 0, len(cs))
	for _, c := range cs {
		if m, ok := c.(*multiCredentials); ok {
			all = append(all, m.cs...)
		} else {
			all = append(all, c)
		}
	}
	return &multiCredentials{
		cs: all,
	}
}
