package credentials

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v4"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Oauth2TokenExchangeCredentialsOption interface {
	ApplyOauth2CredentialsOption(c *Oauth2TokenExchange)
}

// TokenEndpoint
type tokenEndpointOption string

func (endpoint tokenEndpointOption) ApplyOauth2CredentialsOption(c *Oauth2TokenExchange) {
	c.tokenEndpoint = string(endpoint)
}

func WithTokenEndpoint(endpoint string) tokenEndpointOption {
	return tokenEndpointOption(endpoint)
}

// GrantType
type grantTypeOption string

func (grantType grantTypeOption) ApplyOauth2CredentialsOption(c *Oauth2TokenExchange) {
	c.grantType = string(grantType)
}

func WithGrantType(grantType string) grantTypeOption {
	return grantTypeOption(grantType)
}

// Resource
type resourceOption string

func (resource resourceOption) ApplyOauth2CredentialsOption(c *Oauth2TokenExchange) {
	c.resource = string(resource)
}

func WithResource(resource string) resourceOption {
	return resourceOption(resource)
}

// RequestedTokenType
type requestedTokenTypeOption string

func (requestedTokenType requestedTokenTypeOption) ApplyOauth2CredentialsOption(c *Oauth2TokenExchange) {
	c.requestedTokenType = string(requestedTokenType)
}

func WithRequestedTokenType(requestedTokenType string) requestedTokenTypeOption {
	return requestedTokenTypeOption(requestedTokenType)
}

// Audience
type audienceOption []string

func (audience audienceOption) ApplyOauth2CredentialsOption(c *Oauth2TokenExchange) {
	c.audience = audience
}

func WithAudience(audience ...string) audienceOption {
	return audience
}

// Scope
type scopeOption []string

func (scope scopeOption) ApplyOauth2CredentialsOption(c *Oauth2TokenExchange) {
	c.scope = scope
}

func WithScope(scope ...string) scopeOption {
	return scope
}

// RequestTimeout
type requestTimeoutOption time.Duration

func (timeout requestTimeoutOption) ApplyOauth2CredentialsOption(c *Oauth2TokenExchange) {
	c.requestTimeout = time.Duration(timeout)
}

func WithRequestTimeout(timeout time.Duration) requestTimeoutOption {
	return requestTimeoutOption(timeout)
}

// SubjectTokenSource
type subjectTokenSourceOption struct {
	source TokenSource
}

func (subjectToken *subjectTokenSourceOption) ApplyOauth2CredentialsOption(c *Oauth2TokenExchange) {
	c.subjectTokenSource = subjectToken.source
}

func WithSubjectToken(subjectToken TokenSource) *subjectTokenSourceOption {
	return &subjectTokenSourceOption{subjectToken}
}

// ActorTokenSource
type actorTokenSourceOption struct {
	source TokenSource
}

func (actorToken *actorTokenSourceOption) ApplyOauth2CredentialsOption(c *Oauth2TokenExchange) {
	c.actorTokenSource = actorToken.source
}

func WithActorToken(actorToken TokenSource) *actorTokenSourceOption {
	return &actorTokenSourceOption{actorToken}
}

const defaultRequestTimeout = time.Second * 10

func NewOauth2TokenExchangeCredentials(
	opts ...Oauth2TokenExchangeCredentialsOption,
) (*Oauth2TokenExchange, error) {
	c := &Oauth2TokenExchange{
		grantType:          "urn:ietf:params:oauth:grant-type:token-exchange",
		requestedTokenType: "urn:ietf:params:oauth:token-type:access_token",
		requestTimeout:     defaultRequestTimeout,
	}

	for _, opt := range opts {
		if opt != nil {
			opt.ApplyOauth2CredentialsOption(c)
		}
	}

	if c.tokenEndpoint == "" {
		return nil, xerrors.WithStackTrace(errors.New("OAuth2 token exchange: empty token endpoint"))
	}

	return c, nil
}

func (provider *Oauth2TokenExchange) getScopeParam() string {
	var scope string
	if len(provider.scope) != 0 {
		for _, s := range provider.scope {
			if len(s) != 0 {
				if len(scope) != 0 {
					scope += " "
				}
				scope += s
			}
		}
	}

	return scope
}

func (provider *Oauth2TokenExchange) getRequestParams() (string, error) {
	params := url.Values{}
	params.Set("grant_type", provider.grantType)
	if provider.resource != "" {
		params.Set("resource", provider.resource)
	}
	for _, aud := range provider.audience {
		if aud != "" {
			params.Add("audience", aud)
		}
	}
	scope := provider.getScopeParam()
	if len(scope) != 0 {
		params.Set("scope", scope)
	}

	params.Set("requested_token_type", provider.requestedTokenType)
	if provider.subjectTokenSource != nil {
		token, err := provider.subjectTokenSource.Token()
		if err != nil {
			return "", xerrors.WithStackTrace(err)
		}
		params.Set("subject_token", token.Token)
		params.Set("subject_token_type", token.TokenType)
	}
	if provider.actorTokenSource != nil {
		token, err := provider.actorTokenSource.Token()
		if err != nil {
			return "", xerrors.WithStackTrace(err)
		}
		params.Set("actor_token", token.Token)
		params.Set("actor_token_type", token.TokenType)
	}

	return params.Encode(), nil
}

func (provider *Oauth2TokenExchange) processTokenExchangeResponse(result *http.Response, now time.Time) error {
	var (
		data []byte
		err  error
	)
	if result.Body != nil {
		data, err = io.ReadAll(result.Body)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	} else {
		data = make([]byte, 0)
	}

	if result.StatusCode != http.StatusOK {
		description := fmt.Sprintf("OAuth2 token exchange: could not exchange token: %s", result.Status)

		//nolint:tagliatelle
		type errorResponse struct {
			Error       string `json:"error"`
			Description string `json:"error_description"`
			ErrorURI    string `json:"error_uri"`
		}
		var parsedErrorResponse errorResponse
		if err := json.Unmarshal(data, &parsedErrorResponse); err != nil {
			description += fmt.Sprintf(", could not parse response: %s", err.Error())

			return xerrors.WithStackTrace(errors.New(description))
		}

		if parsedErrorResponse.Error != "" {
			description += fmt.Sprintf(", error: %s", parsedErrorResponse.Error)
		}

		if parsedErrorResponse.Description != "" {
			description += fmt.Sprintf(", description: \"%s\"", parsedErrorResponse.Description)
		}

		if parsedErrorResponse.ErrorURI != "" {
			description += fmt.Sprintf(", error_uri: %s", parsedErrorResponse.ErrorURI)
		}

		return xerrors.WithStackTrace(errors.New(description))
	}

	//nolint:tagliatelle
	type response struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int64  `json:"expires_in"`
		Scope       string `json:"scope"`
	}
	var parsedResponse response
	if err := json.Unmarshal(data, &parsedResponse); err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("OAuth2 token exchange: could not parse response: %w", err))
	}

	if !strings.EqualFold(parsedResponse.TokenType, "bearer") {
		return xerrors.WithStackTrace(fmt.Errorf("OAuth2 token exchange: unsupported token type: \"%s\"", parsedResponse.TokenType))
	}

	if parsedResponse.ExpiresIn <= 0 {
		return xerrors.WithStackTrace(fmt.Errorf("OAuth2 token exchange: incorrect expiration time: %d", parsedResponse.ExpiresIn))
	}

	if parsedResponse.Scope != "" {
		scope := provider.getScopeParam()
		if parsedResponse.Scope != scope {
			return xerrors.WithStackTrace(fmt.Errorf("OAuth2 token exchange: got different scope. Expected \"%s\", but got \"%s\"", scope, parsedResponse.Scope))
		}
	}

	provider.receivedToken = "Bearer " + parsedResponse.AccessToken

	// Expire time
	expireDelta := time.Duration(parsedResponse.ExpiresIn)
	expireDelta *= time.Second
	provider.receivedTokenExpireTime = now.Add(expireDelta)

	updateDelta := time.Duration(parsedResponse.ExpiresIn / 2) //nolint:gomnd
	updateDelta *= time.Second
	provider.updateTokenTime = now.Add(updateDelta)

	return nil
}

func (provider *Oauth2TokenExchange) exchangeToken(ctx context.Context, now time.Time) error {
	body, err := provider.getRequestParams()
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("OAuth2 token exchange: could not make http request: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.tokenEndpoint, strings.NewReader(body))
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("OAuth2 token exchange: could not make http request: %w", err))
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(body)))
	req.Close = true

	client := http.Client{
		Transport: http.DefaultTransport,
		Timeout:   provider.requestTimeout,
	}

	result, err := client.Do(req)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("iam: could not exchange token: %w", err))
	}

	defer result.Body.Close()

	return provider.processTokenExchangeResponse(result, now)
}

func (provider *Oauth2TokenExchange) exchangeTokenInbackground() {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()

	now := time.Now()
	if !provider.needUpdate(now) {
		return
	}

	ctx := context.Background()
	_ = provider.exchangeToken(ctx, now)

	provider.updating.Store(false)
}

func (provider *Oauth2TokenExchange) checkbackgroundUpdate(now time.Time) {
	if provider.needUpdate(now) && !provider.updating.Load() {
		if provider.updating.CompareAndSwap(false, true) {
			go provider.exchangeTokenInbackground()
		}
	}
}

func (provider *Oauth2TokenExchange) expired(now time.Time) bool {
	return now.Compare(provider.receivedTokenExpireTime) > 0
}

func (provider *Oauth2TokenExchange) needUpdate(now time.Time) bool {
	return now.Compare(provider.updateTokenTime) > 0
}

func (provider *Oauth2TokenExchange) fastCheck(now time.Time) string {
	provider.mutex.RLock()
	defer provider.mutex.RUnlock()

	if !provider.expired(now) {
		provider.checkbackgroundUpdate(now)

		return provider.receivedToken
	}

	return ""
}

func (provider *Oauth2TokenExchange) Token(ctx context.Context) (string, error) {
	now := time.Now()

	token := provider.fastCheck(now)
	if token != "" {
		return token, nil
	}

	provider.mutex.Lock()
	defer provider.mutex.Unlock()

	if !provider.expired(now) {
		return provider.receivedToken, nil
	}

	if err := provider.exchangeToken(ctx, now); err != nil {
		return "", err
	}

	return provider.receivedToken, nil
}

type Oauth2TokenExchange struct {
	tokenEndpoint string

	// grant_type parameter
	// urn:ietf:params:oauth:grant-type:token-exchange by default
	grantType string

	resource string
	audience []string
	scope    []string

	// requested_token_type parameter
	// urn:ietf:params:oauth:token-type:access_token by default
	requestedTokenType string

	subjectTokenSource TokenSource

	actorTokenSource TokenSource

	// Http request timeout
	// 10 by default
	requestTimeout time.Duration

	// Received data
	receivedToken           string
	updateTokenTime         time.Time
	receivedTokenExpireTime time.Time

	mutex    sync.RWMutex
	updating atomic.Bool // true if separate goroutine is run and updates token in background
}

type Token struct {
	Token string

	// token type according to OAuth 2.0 token exchange protocol
	// https://www.rfc-editor.org/rfc/rfc8693#TokenTypeIdentifiers
	// for example urn:ietf:params:oauth:token-type:jwt
	TokenType string
}

type TokenSource interface {
	Token() (Token, error)
}

type FixedTokenSource struct {
	fixedToken Token
}

func (s *FixedTokenSource) Token() (Token, error) {
	return s.fixedToken, nil
}

func NewFixedTokenSource(token, tokenType string) *FixedTokenSource {
	return &FixedTokenSource{
		fixedToken: Token{
			Token:     token,
			TokenType: tokenType,
		},
	}
}

type JWTTokenSourceOption interface {
	ApplyJWTTokenSourceOption(s *JWTTokenSource)
}

// Issuer
type issuerOption string

func (issuer issuerOption) ApplyJWTTokenSourceOption(s *JWTTokenSource) {
	s.issuer = string(issuer)
}

func WithIssuer(issuer string) issuerOption {
	return issuerOption(issuer)
}

// Subject
type subjectOption string

func (subject subjectOption) ApplyJWTTokenSourceOption(s *JWTTokenSource) {
	s.subject = string(subject)
}

func WithSubject(subject string) subjectOption {
	return subjectOption(subject)
}

// Audience
func (audience audienceOption) ApplyJWTTokenSourceOption(s *JWTTokenSource) {
	s.audience = []string(audience)
}

// ID
type idOption string

func (id idOption) ApplyJWTTokenSourceOption(s *JWTTokenSource) {
	s.id = string(id)
}

func WithID(id string) idOption {
	return idOption(id)
}

// TokenTTL
type tokenTTLOption time.Duration

func (ttl tokenTTLOption) ApplyJWTTokenSourceOption(s *JWTTokenSource) {
	s.tokenTTL = time.Duration(ttl)
}

func WithTokenTTL(ttl time.Duration) tokenTTLOption {
	return tokenTTLOption(ttl)
}

// SigningMethod
type signingMethodOption struct {
	method jwt.SigningMethod
}

func (method *signingMethodOption) ApplyJWTTokenSourceOption(s *JWTTokenSource) {
	s.signingMethod = method.method
}

func WithSigningMethod(method jwt.SigningMethod) *signingMethodOption {
	return &signingMethodOption{method}
}

// KeyID
type keyIDOption string

func (id keyIDOption) ApplyJWTTokenSourceOption(s *JWTTokenSource) {
	s.keyID = string(id)
}

func WithKeyID(id string) keyIDOption {
	return keyIDOption(id)
}

// PrivateKey
type privateKeyOption struct {
	key interface{}
}

func (key *privateKeyOption) ApplyJWTTokenSourceOption(s *JWTTokenSource) {
	s.privateKey = key.key
}

func WithPrivateKey(key interface{}) *privateKeyOption {
	return &privateKeyOption{key}
}

func NewJWTTokenSource(opts ...JWTTokenSourceOption) (*JWTTokenSource, error) {
	s := &JWTTokenSource{
		tokenTTL: time.Hour,
	}

	for _, opt := range opts {
		if opt != nil {
			opt.ApplyJWTTokenSourceOption(s)
		}
	}

	if s.signingMethod == nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("JWT token source: no signing method"))
	}

	if s.privateKey == nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("JWT token source: no private key"))
	}

	return s, nil
}

type JWTTokenSource struct {
	signingMethod jwt.SigningMethod
	keyID         string
	privateKey    interface{} // symmetric key in case of symmetric algorithm

	// JWT claims
	issuer   string
	subject  string
	audience []string
	id       string
	tokenTTL time.Duration
}

func (s *JWTTokenSource) Token() (Token, error) {
	var (
		now    = time.Now()
		issued = jwt.NewNumericDate(now.UTC())
		expire = jwt.NewNumericDate(now.Add(s.tokenTTL).UTC())
		err    error
	)
	t := jwt.Token{
		Header: map[string]interface{}{
			"typ": "JWT",
			"alg": s.signingMethod.Alg(),
			"kid": s.keyID,
		},
		Claims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   s.subject,
			IssuedAt:  issued,
			Audience:  s.audience,
			ExpiresAt: expire,
			ID:        s.id,
		},
		Method: s.signingMethod,
	}

	var token Token
	token.Token, err = t.SignedString(s.privateKey)
	if err != nil {
		return token, xerrors.WithStackTrace(fmt.Errorf("JWTTokenSource: could not sign jwt token: %w", err))
	}
	token.TokenType = "urn:ietf:params:oauth:token-type:jwt"

	return token, nil
}
