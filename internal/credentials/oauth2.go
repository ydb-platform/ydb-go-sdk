package credentials

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v4"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/secret"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

const (
	defaultRequestTimeout = time.Second * 10
	defaultJWTTokenTTL    = 3600 * time.Second
	updateTimeDivider     = 2
)

var (
	errEmptyTokenEndpointError    = errors.New("OAuth2 token exchange: empty token endpoint")
	errCouldNotParseResponse      = errors.New("OAuth2 token exchange: could not parse response")
	errCouldNotExchangeToken      = errors.New("OAuth2 token exchange: could not exchange token")
	errUnsupportedTokenType       = errors.New("OAuth2 token exchange: unsupported token type")
	errIncorrectExpirationTime    = errors.New("OAuth2 token exchange: incorrect expiration time")
	errDifferentScope             = errors.New("OAuth2 token exchange: got different scope")
	errCouldNotMakeHTTPRequest    = errors.New("OAuth2 token exchange: could not make http request")
	errCouldNotApplyOption        = errors.New("OAuth2 token exchange: could not apply option")
	errCouldNotCreateTokenSource  = errors.New("OAuth2 token exchange: could not createTokenSource")
	errNoSigningMethodError       = errors.New("JWT token source: no signing method")
	errNoPrivateKeyError          = errors.New("JWT token source: no private key")
	errCouldNotSignJWTToken       = errors.New("JWT token source: could not sign jwt token")
	errCouldNotApplyJWTOption     = errors.New("JWT token source: could not apply option")
	errCouldNotparseRSAPrivateKey = errors.New("JWT token source: could not parse RSA private key from PEM")
	errCouldNotParseHomeDir       = errors.New("JWT token source: could not parse home dir for private key")
	errCouldNotReadPrivateKeyFile = errors.New("JWT token source: could not read from private key file")
)

type Oauth2TokenExchangeCredentialsOption interface {
	ApplyOauth2CredentialsOption(c *oauth2TokenExchange) error
}

// TokenEndpoint
type tokenEndpointOption string

func (endpoint tokenEndpointOption) ApplyOauth2CredentialsOption(c *oauth2TokenExchange) error {
	c.tokenEndpoint = string(endpoint)

	return nil
}

func WithTokenEndpoint(endpoint string) tokenEndpointOption {
	return tokenEndpointOption(endpoint)
}

// GrantType
type grantTypeOption string

func (grantType grantTypeOption) ApplyOauth2CredentialsOption(c *oauth2TokenExchange) error {
	c.grantType = string(grantType)

	return nil
}

func WithGrantType(grantType string) grantTypeOption {
	return grantTypeOption(grantType)
}

// Resource
type resourceOption string

func (resource resourceOption) ApplyOauth2CredentialsOption(c *oauth2TokenExchange) error {
	c.resource = string(resource)

	return nil
}

func WithResource(resource string) resourceOption {
	return resourceOption(resource)
}

// RequestedTokenType
type requestedTokenTypeOption string

func (requestedTokenType requestedTokenTypeOption) ApplyOauth2CredentialsOption(c *oauth2TokenExchange) error {
	c.requestedTokenType = string(requestedTokenType)

	return nil
}

func WithRequestedTokenType(requestedTokenType string) requestedTokenTypeOption {
	return requestedTokenTypeOption(requestedTokenType)
}

// Audience
type audienceOption []string

func (audience audienceOption) ApplyOauth2CredentialsOption(c *oauth2TokenExchange) error {
	c.audience = audience

	return nil
}

func WithAudience(audience ...string) audienceOption {
	return audience
}

// Scope
type scopeOption []string

func (scope scopeOption) ApplyOauth2CredentialsOption(c *oauth2TokenExchange) error {
	c.scope = scope

	return nil
}

func WithScope(scope ...string) scopeOption {
	return scope
}

// RequestTimeout
type requestTimeoutOption time.Duration

func (timeout requestTimeoutOption) ApplyOauth2CredentialsOption(c *oauth2TokenExchange) error {
	c.requestTimeout = time.Duration(timeout)

	return nil
}

func WithRequestTimeout(timeout time.Duration) requestTimeoutOption {
	return requestTimeoutOption(timeout)
}

const (
	SubjectTokenSourceType = 1
	ActorTokenSourceType   = 2
)

// SubjectTokenSource/ActorTokenSource
type tokenSourceOption struct {
	source          TokenSource
	createFunc      func() (TokenSource, error)
	tokenSourceType int
}

func (tokenSource *tokenSourceOption) ApplyOauth2CredentialsOption(c *oauth2TokenExchange) error {
	src := tokenSource.source
	var err error
	if src == nil {
		src, err = tokenSource.createFunc()
		if err != nil {
			return xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotCreateTokenSource, err))
		}
	}
	switch tokenSource.tokenSourceType {
	case SubjectTokenSourceType:
		c.subjectTokenSource = src
	case ActorTokenSourceType:
		c.actorTokenSource = src
	}

	return nil
}

func WithSubjectToken(subjectToken TokenSource) *tokenSourceOption {
	return &tokenSourceOption{
		source:          subjectToken,
		createFunc:      nil,
		tokenSourceType: SubjectTokenSourceType,
	}
}

func WithFixedSubjectToken(token, tokenType string) *tokenSourceOption {
	return &tokenSourceOption{
		source: nil,
		createFunc: func() (TokenSource, error) {
			return NewFixedTokenSource(token, tokenType), nil
		},
		tokenSourceType: SubjectTokenSourceType,
	}
}

func WithJWTSubjectToken(opts ...JWTTokenSourceOption) *tokenSourceOption {
	return &tokenSourceOption{
		source: nil,
		createFunc: func() (TokenSource, error) {
			return NewJWTTokenSource(opts...)
		},
		tokenSourceType: SubjectTokenSourceType,
	}
}

// ActorTokenSource
func WithActorToken(actorToken TokenSource) *tokenSourceOption {
	return &tokenSourceOption{
		source:          actorToken,
		createFunc:      nil,
		tokenSourceType: ActorTokenSourceType,
	}
}

func WithFixedActorToken(token, tokenType string) *tokenSourceOption {
	return &tokenSourceOption{
		source: nil,
		createFunc: func() (TokenSource, error) {
			return NewFixedTokenSource(token, tokenType), nil
		},
		tokenSourceType: ActorTokenSourceType,
	}
}

func WithJWTActorToken(opts ...JWTTokenSourceOption) *tokenSourceOption {
	return &tokenSourceOption{
		source: nil,
		createFunc: func() (TokenSource, error) {
			return NewJWTTokenSource(opts...)
		},
		tokenSourceType: ActorTokenSourceType,
	}
}

type oauth2TokenExchange struct {
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

	sourceInfo string
}

func NewOauth2TokenExchangeCredentials(
	opts ...Oauth2TokenExchangeCredentialsOption,
) (*oauth2TokenExchange, error) {
	c := &oauth2TokenExchange{
		tokenEndpoint:           "",
		grantType:               "urn:ietf:params:oauth:grant-type:token-exchange",
		resource:                "",
		audience:                nil,
		scope:                   nil,
		requestedTokenType:      "urn:ietf:params:oauth:token-type:access_token",
		subjectTokenSource:      nil,
		actorTokenSource:        nil,
		requestTimeout:          defaultRequestTimeout,
		receivedToken:           "",
		updateTokenTime:         time.Time{},
		receivedTokenExpireTime: time.Time{},
		mutex:                   sync.RWMutex{},
		updating:                atomic.Bool{},
		sourceInfo:              stack.Record(1),
	}

	var err error
	for _, opt := range opts {
		if opt != nil {
			err = opt.ApplyOauth2CredentialsOption(c)
			if err != nil {
				return nil, xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotApplyOption, err))
			}
		}
	}

	if c.tokenEndpoint == "" {
		return nil, xerrors.WithStackTrace(errEmptyTokenEndpointError)
	}

	return c, nil
}

func (provider *oauth2TokenExchange) getScopeParam() string {
	var scope string
	if len(provider.scope) != 0 {
		for _, s := range provider.scope {
			if s != "" {
				if scope != "" {
					scope += " "
				}
				scope += s
			}
		}
	}

	return scope
}

func (provider *oauth2TokenExchange) addTokenSrc(params *url.Values, src TokenSource, tName, tTypeName string) error {
	if src != nil {
		token, err := src.Token()
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		params.Set(tName, token.Token)
		params.Set(tTypeName, token.TokenType)
	}

	return nil
}

func (provider *oauth2TokenExchange) getRequestParams() (string, error) {
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
	if scope != "" {
		params.Set("scope", scope)
	}

	params.Set("requested_token_type", provider.requestedTokenType)

	err := provider.addTokenSrc(&params, provider.subjectTokenSource, "subject_token", "subject_token_type")
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	err = provider.addTokenSrc(&params, provider.actorTokenSource, "actor_token", "actor_token_type")
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	return params.Encode(), nil
}

func (provider *oauth2TokenExchange) processTokenExchangeResponse(result *http.Response, now time.Time) error {
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
		description := result.Status

		//nolint:tagliatelle
		type errorResponse struct {
			ErrorName        string `json:"error"`
			ErrorDescription string `json:"error_description"`
			ErrorURI         string `json:"error_uri"`
		}
		var parsedErrorResponse errorResponse
		if err := json.Unmarshal(data, &parsedErrorResponse); err != nil {
			description += ", could not parse response: " + err.Error()

			return xerrors.WithStackTrace(fmt.Errorf("%w: %s", errCouldNotExchangeToken, description))
		}

		if parsedErrorResponse.ErrorName != "" {
			description += ", error: " + parsedErrorResponse.ErrorName
		}

		if parsedErrorResponse.ErrorDescription != "" {
			description += fmt.Sprintf(", description: %q", parsedErrorResponse.ErrorDescription)
		}

		if parsedErrorResponse.ErrorURI != "" {
			description += ", error_uri: " + parsedErrorResponse.ErrorURI
		}

		return xerrors.WithStackTrace(fmt.Errorf("%w: %s", errCouldNotExchangeToken, description))
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
		return xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotParseResponse, err))
	}

	if !strings.EqualFold(parsedResponse.TokenType, "bearer") {
		return xerrors.WithStackTrace(
			fmt.Errorf("%w: %q", errUnsupportedTokenType, parsedResponse.TokenType))
	}

	if parsedResponse.ExpiresIn <= 0 {
		return xerrors.WithStackTrace(
			fmt.Errorf("%w: %d", errIncorrectExpirationTime, parsedResponse.ExpiresIn))
	}

	if parsedResponse.Scope != "" {
		scope := provider.getScopeParam()
		if parsedResponse.Scope != scope {
			return xerrors.WithStackTrace(
				fmt.Errorf("%w. Expected %q, but got %q", errDifferentScope, scope, parsedResponse.Scope))
		}
	}

	provider.receivedToken = "Bearer " + parsedResponse.AccessToken

	// Expire time
	expireDelta := time.Duration(parsedResponse.ExpiresIn)
	expireDelta *= time.Second
	provider.receivedTokenExpireTime = now.Add(expireDelta)

	updateDelta := time.Duration(parsedResponse.ExpiresIn / updateTimeDivider)
	updateDelta *= time.Second
	provider.updateTokenTime = now.Add(updateDelta)

	return nil
}

func (provider *oauth2TokenExchange) exchangeToken(ctx context.Context, now time.Time) error {
	body, err := provider.getRequestParams()
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotMakeHTTPRequest, err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.tokenEndpoint, strings.NewReader(body))
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotMakeHTTPRequest, err))
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(body)))
	req.Close = true

	client := http.Client{
		Transport:     http.DefaultTransport,
		CheckRedirect: nil,
		Jar:           nil,
		Timeout:       provider.requestTimeout,
	}

	result, err := client.Do(req)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotExchangeToken, err))
	}

	defer result.Body.Close()

	return provider.processTokenExchangeResponse(result, now)
}

func (provider *oauth2TokenExchange) exchangeTokenInBackground() {
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

func (provider *oauth2TokenExchange) checkBackgroundUpdate(now time.Time) {
	if provider.needUpdate(now) && !provider.updating.Load() {
		if provider.updating.CompareAndSwap(false, true) {
			go provider.exchangeTokenInBackground()
		}
	}
}

func (provider *oauth2TokenExchange) expired(now time.Time) bool {
	return now.Compare(provider.receivedTokenExpireTime) > 0
}

func (provider *oauth2TokenExchange) needUpdate(now time.Time) bool {
	return now.Compare(provider.updateTokenTime) > 0
}

func (provider *oauth2TokenExchange) fastCheck(now time.Time) string {
	provider.mutex.RLock()
	defer provider.mutex.RUnlock()

	if !provider.expired(now) {
		provider.checkBackgroundUpdate(now)

		return provider.receivedToken
	}

	return ""
}

func (provider *oauth2TokenExchange) Token(ctx context.Context) (string, error) {
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

func (provider *oauth2TokenExchange) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	fmt.Fprintf(
		buffer,
		"OAuth2TokenExchange{Endpoint:%q,GrantType:%s,Resource:%s,Audience:%v,Scope:%v,RequestedTokenType:%s",
		provider.tokenEndpoint,
		provider.grantType,
		provider.resource,
		provider.audience,
		provider.scope,
		provider.requestedTokenType,
	)
	if provider.subjectTokenSource != nil {
		fmt.Fprintf(buffer, ",SubjectToken:%s", provider.subjectTokenSource)
	}
	if provider.actorTokenSource != nil {
		fmt.Fprintf(buffer, ",ActorToken:%s", provider.actorTokenSource)
	}
	if provider.sourceInfo != "" {
		fmt.Fprintf(buffer, ",From:%q", provider.sourceInfo)
	}
	buffer.WriteByte('}')

	return buffer.String()
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

type fixedTokenSource struct {
	fixedToken Token
}

func (s *fixedTokenSource) Token() (Token, error) {
	return s.fixedToken, nil
}

func (s *fixedTokenSource) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	fmt.Fprintf(
		buffer,
		"FixedTokenSource{Token:%q,Type:%s}",
		secret.Token(s.fixedToken.Token),
		s.fixedToken.TokenType,
	)

	return buffer.String()
}

func NewFixedTokenSource(token, tokenType string) *fixedTokenSource {
	return &fixedTokenSource{
		fixedToken: Token{
			Token:     token,
			TokenType: tokenType,
		},
	}
}

type JWTTokenSourceOption interface {
	ApplyJWTTokenSourceOption(s *jwtTokenSource) error
}

// Issuer
type issuerOption string

func (issuer issuerOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	s.issuer = string(issuer)

	return nil
}

func WithIssuer(issuer string) issuerOption {
	return issuerOption(issuer)
}

// Subject
type subjectOption string

func (subject subjectOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	s.subject = string(subject)

	return nil
}

func WithSubject(subject string) subjectOption {
	return subjectOption(subject)
}

// Audience
func (audience audienceOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	s.audience = audience

	return nil
}

// ID
type idOption string

func (id idOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	s.id = string(id)

	return nil
}

func WithID(id string) idOption {
	return idOption(id)
}

// TokenTTL
type tokenTTLOption time.Duration

func (ttl tokenTTLOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	s.tokenTTL = time.Duration(ttl)

	return nil
}

func WithTokenTTL(ttl time.Duration) tokenTTLOption {
	return tokenTTLOption(ttl)
}

// SigningMethod
type signingMethodOption struct {
	method jwt.SigningMethod
}

func (method *signingMethodOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	s.signingMethod = method.method

	return nil
}

func WithSigningMethod(method jwt.SigningMethod) *signingMethodOption {
	return &signingMethodOption{method}
}

// KeyID
type keyIDOption string

func (id keyIDOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	s.keyID = string(id)

	return nil
}

func WithKeyID(id string) keyIDOption {
	return keyIDOption(id)
}

// PrivateKey
type privateKeyOption struct {
	key interface{}
}

func (key *privateKeyOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	s.privateKey = key.key

	return nil
}

func WithPrivateKey(key interface{}) *privateKeyOption {
	return &privateKeyOption{key}
}

// PrivateKey
type rsaPrivateKeyPemContentOption struct {
	keyContent []byte
}

func (key *rsaPrivateKeyPemContentOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(key.keyContent)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotparseRSAPrivateKey, err))
	}
	s.privateKey = privateKey

	return nil
}

func WithRSAPrivateKeyPEMContent(key []byte) *rsaPrivateKeyPemContentOption {
	return &rsaPrivateKeyPemContentOption{key}
}

// PrivateKey
type rsaPrivateKeyPemFileOption struct {
	path string
}

func (key *rsaPrivateKeyPemFileOption) ApplyJWTTokenSourceOption(s *jwtTokenSource) error {
	if len(key.path) > 0 && key.path[0] == '~' {
		usr, err := user.Current()
		if err != nil {
			return xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotParseHomeDir, err))
		}
		key.path = filepath.Join(usr.HomeDir, key.path[1:])
	}
	bytes, err := os.ReadFile(key.path)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotReadPrivateKeyFile, err))
	}

	o := rsaPrivateKeyPemContentOption{bytes}

	return o.ApplyJWTTokenSourceOption(s)
}

func WithRSAPrivateKeyPEMFile(path string) *rsaPrivateKeyPemFileOption {
	return &rsaPrivateKeyPemFileOption{path}
}

func NewJWTTokenSource(opts ...JWTTokenSourceOption) (*jwtTokenSource, error) {
	s := &jwtTokenSource{
		signingMethod: nil,
		keyID:         "",
		privateKey:    nil,
		issuer:        "",
		subject:       "",
		audience:      nil,
		id:            "",
		tokenTTL:      defaultJWTTokenTTL,
	}

	var err error
	for _, opt := range opts {
		if opt != nil {
			err = opt.ApplyJWTTokenSourceOption(s)
			if err != nil {
				return nil, xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotApplyJWTOption, err))
			}
		}
	}

	if s.signingMethod == nil {
		return nil, xerrors.WithStackTrace(errNoSigningMethodError)
	}

	if s.privateKey == nil {
		return nil, xerrors.WithStackTrace(errNoPrivateKeyError)
	}

	return s, nil
}

type jwtTokenSource struct {
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

func (s *jwtTokenSource) Token() (Token, error) {
	var (
		now    = time.Now()
		issued = jwt.NewNumericDate(now.UTC())
		expire = jwt.NewNumericDate(now.Add(s.tokenTTL).UTC())
		err    error
	)
	t := jwt.Token{
		Raw:    "",
		Method: s.signingMethod,
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
			NotBefore: nil,
			ID:        s.id,
		},
		Signature: "",
		Valid:     false,
	}

	var token Token
	token.Token, err = t.SignedString(s.privateKey)
	if err != nil {
		return token, xerrors.WithStackTrace(fmt.Errorf("%w: %w", errCouldNotSignJWTToken, err))
	}
	token.TokenType = "urn:ietf:params:oauth:token-type:jwt"

	return token, nil
}

func (s *jwtTokenSource) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	fmt.Fprintf(
		buffer,
		"JWTTokenSource{Method:%s,KeyID:%s,Issuer:%q,Subject:%q,Audience:%v,ID:%s,TokenTTL:%s}",
		s.signingMethod.Alg(),
		s.keyID,
		s.issuer,
		s.subject,
		s.audience,
		s.id,
		s.tokenTTL,
	)

	return buffer.String()
}
