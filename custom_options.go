package ydb

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
)

type customOptions struct {
	database    string
	credentials credentials.Credentials
}

type CustomOption func(opts *customOptions)

func WithCustomToken(accessToken string) CustomOption {
	return func(opts *customOptions) {
		opts.credentials = credentials.NewAccessTokenCredentials(
			accessToken,
			fmt.Sprintf(`WithCustomToken("%s")`, accessToken),
		)
	}
}

func WithCustomDatabase(database string) CustomOption {
	return func(opts *customOptions) {
		opts.database = database
	}
}
