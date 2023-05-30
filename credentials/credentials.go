package credentials

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
)

// Credentials is an interface of YDB credentials required for connect with YDB
type Credentials interface {
	// Token must return actual token or error
	Token(context.Context) (string, error)
}

type optionsHolder struct {
	sourceInfo string
}

type option func(h *optionsHolder)

// WithSourceInfo option append to credentials object the source info for reporting source info details on error case
func WithSourceInfo(sourceInfo string) option {
	return func(h *optionsHolder) {
		h.sourceInfo = sourceInfo
	}
}

// NewAccessTokenCredentials makes access token credentials object
// Passed options redefines default values of credentials object internal fields
func NewAccessTokenCredentials(accessToken string, opts ...option) Credentials {
	h := &optionsHolder{
		sourceInfo: "credentials.NewAccessTokenCredentials(token)",
	}
	for _, o := range opts {
		if o != nil {
			o(h)
		}
	}
	return credentials.NewAccessTokenCredentials(accessToken, h.sourceInfo)
}

// NewAnonymousCredentials makes anonymous credentials object
// Passed options redefines default values of credentials object internal fields
func NewAnonymousCredentials(opts ...option) Credentials {
	h := &optionsHolder{
		sourceInfo: "credentials.NewAnonymousCredentials()",
	}
	for _, o := range opts {
		if o != nil {
			o(h)
		}
	}
	return credentials.NewAnonymousCredentials(h.sourceInfo)
}

// NewStaticCredentials makes static credentials object
func NewStaticCredentials(user, password, authEndpoint string, opts ...grpc.DialOption) Credentials {
	return credentials.NewStaticCredentials(user, password, authEndpoint, opts...)
}
