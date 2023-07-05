package credentials

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
)

var (
	_ Credentials  = (*Anonymous)(nil)
	_ fmt.Stringer = (*Anonymous)(nil)
)

// Anonymous implements Credentials interface with Anonymous access
type Anonymous struct {
	sourceInfo string
}

func NewAnonymousCredentials(opts ...Option) *Anonymous {
	options := optionsHolder{
		sourceInfo: stack.Record(1),
	}
	for _, opt := range opts {
		opt(&options)
	}
	return &Anonymous{
		sourceInfo: options.sourceInfo,
	}
}

// Token implements Credentials.
func (c Anonymous) Token(_ context.Context) (string, error) {
	return "", nil
}

// Token implements Credentials.
func (c Anonymous) String() string {
	return fmt.Sprintf("Anonymous(from:%q)", c.sourceInfo)
}
