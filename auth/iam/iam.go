/*
Package iam provides interface for retreiving and caching iam tokens.
*/
package iam

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"sync"
	"time"

	jwt "github.com/dgrijalva/jwt-go"

	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

// Default Client parameters.
const (
	DefaultAudience = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
	DefaultTokenTTL = time.Hour
)

type transport interface {
	CreateToken(ctx context.Context, jwt string) (token string, expires time.Time, err error)
}

// Client contains options for interaction with the iam.
type Client struct {
	Endpoint string
	CertPool *x509.CertPool

	Key    *rsa.PrivateKey
	KeyID  string
	Issuer string

	TokenTTL time.Duration
	Audience string

	once    sync.Once
	mu      sync.RWMutex
	err     error
	token   string
	expires time.Time

	// transport is a stub used for tests.
	transport transport
}

func (c *Client) init() (err error) {
	c.once.Do(func() {
		if c.Endpoint == "" {
			c.err = fmt.Errorf("iam: endpoint required")
			return
		}
		if c.Audience == "" {
			c.Audience = DefaultAudience
		}
		if c.TokenTTL == 0 {
			c.TokenTTL = DefaultTokenTTL
		}
		if c.transport == nil {
			c.transport = &grpcTransport{
				endpoint: c.Endpoint,
				certPool: c.CertPool,
			}
		}
	})
	return c.err
}

// Token returns cached token if no c.TokenTTL time has passed or no token
// expiratation deadline from the last request exceeded. In other way, it makes
// request for a new one token.
func (c *Client) Token(ctx context.Context) (token string, err error) {
	if err = c.init(); err != nil {
		return
	}
	c.mu.RLock()
	if !c.expired() {
		token = c.token
	}
	c.mu.RUnlock()
	if token != "" {
		return token, nil
	}
	now := timeutil.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.expired() {
		return c.token, nil
	}
	var expires time.Time
	token, expires, err = c.transport.CreateToken(ctx, c.jwt(now))
	if err != nil {
		return "", err
	}
	c.token = token
	c.expires = now.Add(c.TokenTTL)
	if expires.Before(c.expires) {
		c.expires = expires
	}
	return token, nil
}

func (c *Client) expired() bool {
	return c.expires.Sub(timeutil.Now()) <= 0
}

// By default Go RSA PSS uses PSSSaltLengthAuto, but RFC states that salt size
// must be equal to hash size.
//
// See https://tools.ietf.org/html/rfc7518#section-3.5
var ps256WithSaltLengthEqualsHash = &jwt.SigningMethodRSAPSS{
	SigningMethodRSA: jwt.SigningMethodPS256.SigningMethodRSA,
	Options: &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
	},
}

func (c *Client) jwt(now time.Time) string {
	var (
		issued = now.UTC().Unix()
		expire = now.Add(c.TokenTTL).UTC().Unix()
		method = ps256WithSaltLengthEqualsHash
	)
	t := jwt.Token{
		Header: map[string]interface{}{
			"typ": "JWT",
			"alg": method.Alg(),
			"kid": c.KeyID,
		},
		Claims: jwt.StandardClaims{
			Issuer:    c.Issuer,
			IssuedAt:  issued,
			Audience:  c.Audience,
			ExpiresAt: expire,
		},
		Method: method,
	}
	s, err := t.SignedString(c.Key)
	if err != nil {
		panic(fmt.Sprintf("iam: could not sign jwt token: %v", err))
	}
	return s
}
