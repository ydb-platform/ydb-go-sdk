// nolint:revive
package ydb_credentials

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
)

type Credentials = credentials.Credentials

type optionsHolder struct {
	sourceInfo string
}

type option func(h *optionsHolder)

func WithSourceInfo(sourceInfo string) option {
	return func(h *optionsHolder) {
		h.sourceInfo = sourceInfo
	}
}

func NewAccessTokenCredentials(accessToken string, opts ...option) Credentials {
	h := &optionsHolder{
		sourceInfo: "credentials.NewAccessTokenCredentials(token)",
	}
	for _, o := range opts {
		o(h)
	}
	return credentials.NewAccessTokenCredentials(accessToken, h.sourceInfo)
}

func NewAnonymousCredentials(opts ...option) Credentials {
	h := &optionsHolder{
		sourceInfo: "credentials.NewAnonymousCredentials()",
	}
	for _, o := range opts {
		o(h)
	}
	return credentials.NewAnonymousCredentials(h.sourceInfo)
}
