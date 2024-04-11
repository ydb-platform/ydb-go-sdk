package credentials

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
)

// Credentials is an interface of YDB credentials required for connect with YDB
type Credentials interface {
	// Token must return actual token or error
	Token(ctx context.Context) (string, error)
}

// NewAccessTokenCredentials makes access token credentials object
// Passed options redefines default values of credentials object internal fields
func NewAccessTokenCredentials(
	accessToken string, opts ...credentials.AccessTokenCredentialsOption,
) *credentials.AccessToken {
	return credentials.NewAccessTokenCredentials(accessToken, opts...)
}

// NewAnonymousCredentials makes anonymous credentials object
// Passed options redefines default values of credentials object internal fields
func NewAnonymousCredentials(
	opts ...credentials.AnonymousCredentialsOption,
) *credentials.Anonymous {
	return credentials.NewAnonymousCredentials(opts...)
}

// NewStaticCredentials makes static credentials object
func NewStaticCredentials(
	user, password, authEndpoint string, opts ...credentials.StaticCredentialsOption,
) *credentials.Static {
	return credentials.NewStaticCredentials(user, password, authEndpoint, opts...)
}

// NewOauth2TokenExchangeCredentials makes OAuth 2.0 token exchange protocol credentials object
func NewOauth2TokenExchangeCredentials(
	opts ...credentials.Oauth2TokenExchangeCredentialsOption,
) (Credentials, error) {
	return credentials.NewOauth2TokenExchangeCredentials(opts...)
}

// NewJWTTokenSource makes JWT token source for OAuth 2.0 token exchange credentials
func NewJWTTokenSource(opts ...credentials.JWTTokenSourceOption) (credentials.TokenSource, error) {
	return credentials.NewJWTTokenSource(opts...)
}

// NewFixedTokenSource makes fixed token source for OAuth 2.0 token exchange credentials
func NewFixedTokenSource(token, tokenType string) credentials.TokenSource {
	return credentials.NewFixedTokenSource(token, tokenType)
}
