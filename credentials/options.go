package credentials

import (
	"time"

	"github.com/golang-jwt/jwt/v4"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
)

// WithSourceInfo option append to credentials object the source info for reporting source info details on error case
func WithSourceInfo(sourceInfo string) credentials.SourceInfoOption {
	return credentials.WithSourceInfo(sourceInfo)
}

// WithGrpcDialOptions option append to static credentials object GRPC dial options
func WithGrpcDialOptions(opts ...grpc.DialOption) credentials.StaticCredentialsOption {
	return credentials.WithGrpcDialOptions(opts...)
}

// TokenEndpoint
func WithTokenEndpoint(endpoint string) credentials.Oauth2TokenExchangeCredentialsOption {
	return credentials.WithTokenEndpoint(endpoint)
}

// GrantType
func WithGrantType(grantType string) credentials.Oauth2TokenExchangeCredentialsOption {
	return credentials.WithGrantType(grantType)
}

// Resource
func WithResource(resource string) credentials.Oauth2TokenExchangeCredentialsOption {
	return credentials.WithResource(resource)
}

// RequestedTokenType
func WithRequestedTokenType(requestedTokenType string) credentials.Oauth2TokenExchangeCredentialsOption {
	return credentials.WithRequestedTokenType(requestedTokenType)
}

// Scope
func WithScope(scope ...string) credentials.Oauth2TokenExchangeCredentialsOption {
	return credentials.WithScope(scope...)
}

// RequestTimeout
func WithRequestTimeout(timeout time.Duration) credentials.Oauth2TokenExchangeCredentialsOption {
	return credentials.WithRequestTimeout(timeout)
}

// SubjectTokenSource
func WithSubjectToken(subjectToken credentials.TokenSource) credentials.Oauth2TokenExchangeCredentialsOption {
	return credentials.WithSubjectToken(subjectToken)
}

// ActorTokenSource
func WithActorToken(actorToken credentials.TokenSource) credentials.Oauth2TokenExchangeCredentialsOption {
	return credentials.WithActorToken(actorToken)
}

// Audience
type oauthCredentialsAndJWTCredentialsOption interface {
	credentials.Oauth2TokenExchangeCredentialsOption
	credentials.JWTTokenSourceOption
}

func WithAudience(audience ...string) oauthCredentialsAndJWTCredentialsOption {
	return credentials.WithAudience(audience...)
}

// Issuer
func WithIssuer(issuer string) credentials.JWTTokenSourceOption {
	return credentials.WithIssuer(issuer)
}

// Subject
func WithSubject(subject string) credentials.JWTTokenSourceOption {
	return credentials.WithSubject(subject)
}

// ID
func WithID(id string) credentials.JWTTokenSourceOption {
	return credentials.WithID(id)
}

// TokenTTL
func WithTokenTTL(ttl time.Duration) credentials.JWTTokenSourceOption {
	return credentials.WithTokenTTL(ttl)
}

// SigningMethod
func WithSigningMethod(method jwt.SigningMethod) credentials.JWTTokenSourceOption {
	return credentials.WithSigningMethod(method)
}

// KeyID
func WithKeyID(id string) credentials.JWTTokenSourceOption {
	return credentials.WithKeyID(id)
}

// PrivateKey
func WithPrivateKey(key interface{}) credentials.JWTTokenSourceOption {
	return credentials.WithPrivateKey(key)
}
