package credentials

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
)

var (
	testPrivateKeyContent = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC75/JS3rMcLJxv\nFgpOzF5+2gH+Yig3RE2MTl9uwC0BZKAv6foYr7xywQyWIK+W1cBhz8R4LfFmZo2j\nM0aCvdRmNBdW0EDSTnHLxCsFhoQWLVq+bI5f5jzkcoiioUtaEpADPqwgVULVtN/n\nnPJiZ6/dU30C3jmR6+LUgEntUtWt3eq3xQIn5lG3zC1klBY/HxtfH5Hu8xBvwRQT\nJnh3UpPLj8XwSmriDgdrhR7o6umWyVuGrMKlLHmeivlfzjYtfzO1MOIMG8t2/zxG\nR+xb4Vwks73sH1KruH/0/JMXU97npwpe+Um+uXhpldPygGErEia7abyZB2gMpXqr\nWYKMo02NAgMBAAECggEAO0BpC5OYw/4XN/optu4/r91bupTGHKNHlsIR2rDzoBhU\nYLd1evpTQJY6O07EP5pYZx9mUwUdtU4KRJeDGO/1/WJYp7HUdtxwirHpZP0lQn77\nuccuX/QQaHLrPekBgz4ONk+5ZBqukAfQgM7fKYOLk41jgpeDbM2Ggb6QUSsJISEp\nzrwpI/nNT/wn+Hvx4DxrzWU6wF+P8kl77UwPYlTA7GsT+T7eKGVH8xsxmK8pt6lg\nsvlBA5XosWBWUCGLgcBkAY5e4ZWbkdd183o+oMo78id6C+PQPE66PLDtHWfpRRmN\nm6XC03x6NVhnfvfozoWnmS4+e4qj4F/emCHvn0GMywKBgQDLXlj7YPFVXxZpUvg/\nrheVcCTGbNmQJ+4cZXx87huqwqKgkmtOyeWsRc7zYInYgraDrtCuDBCfP//ZzOh0\nLxepYLTPk5eNn/GT+VVrqsy35Ccr60g7Lp/bzb1WxyhcLbo0KX7/6jl0lP+VKtdv\nmto+4mbSBXSM1Y5BVVoVgJ3T/wKBgQDsiSvPRzVi5TTj13x67PFymTMx3HCe2WzH\nJUyepCmVhTm482zW95pv6raDr5CTO6OYpHtc5sTTRhVYEZoEYFTM9Vw8faBtluWG\nBjkRh4cIpoIARMn74YZKj0C/0vdX7SHdyBOU3bgRPHg08Hwu3xReqT1kEPSI/B2V\n4pe5fVrucwKBgQCNFgUxUA3dJjyMES18MDDYUZaRug4tfiYouRdmLGIxUxozv6CG\nZnbZzwxFt+GpvPUV4f+P33rgoCvFU+yoPctyjE6j+0aW0DFucPmb2kBwCu5J/856\nkFwCx3blbwFHAco+SdN7g2kcwgmV2MTg/lMOcU7XwUUcN0Obe7UlWbckzQKBgQDQ\nnXaXHL24GGFaZe4y2JFmujmNy1dEsoye44W9ERpf9h1fwsoGmmCKPp90az5+rIXw\nFXl8CUgk8lXW08db/r4r+ma8Lyx0GzcZyplAnaB5/6j+pazjSxfO4KOBy4Y89Tb+\nTP0AOcCi6ws13bgY+sUTa/5qKA4UVw+c5zlb7nRpgwKBgGXAXhenFw1666482iiN\ncHSgwc4ZHa1oL6aNJR1XWH+aboBSwR+feKHUPeT4jHgzRGo/aCNHD2FE5I8eBv33\nof1kWYjAO0YdzeKrW0rTwfvt9gGg+CS397aWu4cy+mTI+MNfBgeDAIVBeJOJXLlX\nhL8bFAuNNVrCOp79TNnNIsh7\n-----END PRIVATE KEY-----\n"
	testPublicKeyContent  = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu+fyUt6zHCycbxYKTsxe\nftoB/mIoN0RNjE5fbsAtAWSgL+n6GK+8csEMliCvltXAYc/EeC3xZmaNozNGgr3U\nZjQXVtBA0k5xy8QrBYaEFi1avmyOX+Y85HKIoqFLWhKQAz6sIFVC1bTf55zyYmev\n3VN9At45kevi1IBJ7VLVrd3qt8UCJ+ZRt8wtZJQWPx8bXx+R7vMQb8EUEyZ4d1KT\ny4/F8Epq4g4Ha4Ue6OrplslbhqzCpSx5nor5X842LX8ztTDiDBvLdv88RkfsW+Fc\nJLO97B9Sq7h/9PyTF1Pe56cKXvlJvrl4aZXT8oBhKxImu2m8mQdoDKV6q1mCjKNN\njQIDAQAB\n-----END PUBLIC KEY-----\n"
)

const keyServerAddr = "test_server"

func WriteErr(w http.ResponseWriter, err error) {
	w.WriteHeader(500)
	w.Write([]byte(err.Error()))
}

func runTokenExchangeServer(t *testing.T, ctx context.Context, cancel context.CancelFunc, port int, currentTestParams *Oauth2TokenExchangeTestParams) {
	defer cancel()
	mux := http.NewServeMux()
	mux.HandleFunc("/exchange", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			WriteErr(w, err)
		}

		fmt.Printf("got token exchange request: %s\n", body)

		params, err := url.ParseQuery(string(body))
		if err != nil {
			WriteErr(w, err)
		}
		expectedParams := url.Values{}
		expectedParams.Set("scope", "test_scope1 test_scope2")
		expectedParams.Set("audience", "test_audiience")
		expectedParams.Set("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
		expectedParams.Set("requested_token_type", "urn:ietf:params:oauth:token-type:access_token")
		expectedParams.Set("subject_token", "test_source_token")
		expectedParams.Set("subject_token_type", "urn:ietf:params:oauth:token-type:test_jwt")
		assert.Equal(t, expectedParams, params)

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(currentTestParams.HttpStatus)
		w.Write([]byte(currentTestParams.HttpResponse))
	})
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
		BaseContext: func(l net.Listener) context.Context {
			ctx = context.WithValue(ctx, keyServerAddr, l.Addr().String())
			return ctx
		},
	}
	err := server.ListenAndServe()
	assert.NoError(t, err)
}

type Oauth2TokenExchangeTestParams struct {
	HttpResponse      string
	HttpStatus        int
	ExpectedToken     string
	ExpectedErrorPart string
}

func TestOauth2TokenExchange(t *testing.T) {
	var currentTestParams Oauth2TokenExchangeTestParams
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runCtx, runCancel := context.WithCancel(ctx)
	go runTokenExchangeServer(t, runCtx, runCancel, 14321, &currentTestParams)

	testsParams := []Oauth2TokenExchangeTestParams{
		{
			HttpResponse:      `{"access_token": "test_token", "token_type": "BEARER", "expires_in": 42, "some_other_field": "x"}`,
			HttpStatus:        200,
			ExpectedToken:     "Bearer test_token",
			ExpectedErrorPart: "",
		},
		{
			HttpResponse:      `aaa`,
			HttpStatus:        200,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: could not parse response:",
		},
		{
			HttpResponse:      `{}`,
			HttpStatus:        400,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: could not exchange token: 400 Bad Request",
		},
		{
			HttpResponse:      `not json`,
			HttpStatus:        404,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: could not exchange token: 404 Not Found",
		},
		{
			HttpResponse:      `{"error": "invalid_request"}`,
			HttpStatus:        400,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: could not exchange token: 400 Bad Request, error: invalid_request",
		},
		{
			HttpResponse:      `{"error": "unauthorized_client", "error_description": "somehting went bad"}`,
			HttpStatus:        500,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: could not exchange token: 500 Internal Server Error, error: unauthorized_client, description: \"somehting went bad\"",
		},
		{
			HttpResponse:      `{"error_description": "somehting went bad", "error_uri": "my_error_uri"}`,
			HttpStatus:        403,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: could not exchange token: 403 Forbidden, description: \"somehting went bad\", error_uri: my_error_uri",
		},
		{
			HttpResponse:      `{"access_token": "test_token", "token_type": "", "expires_in": 42, "some_other_field": "x"}`,
			HttpStatus:        200,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: unsupported token type: \"\"",
		},
		{
			HttpResponse:      `{"access_token": "test_token", "token_type": "basic", "expires_in": 42, "some_other_field": "x"}`,
			HttpStatus:        200,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: unsupported token type: \"basic\"",
		},
		{
			HttpResponse:      `{"access_token": "test_token", "token_type": "Bearer", "expires_in": -42, "some_other_field": "x"}`,
			HttpStatus:        200,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: incorrect expiration time: -42",
		},
		{
			HttpResponse:      `{"access_token": "test_token", "token_type": "Bearer", "expires_in": 42, "scope": "s"}`,
			HttpStatus:        200,
			ExpectedToken:     "",
			ExpectedErrorPart: "OAuth2 token exchange: got different scope. Expected \"test_scope1 test_scope2\", but got \"s\"",
		},
	}

	for _, params := range testsParams {
		currentTestParams = params

		client, err := NewOauth2TokenExchangeCredentials(
			WithTokenEndpoint("http://localhost:14321/exchange"),
			WithAudience("test_audiience"),
			WithScope("test_scope1", "test_scope2"),
			WithSubjectToken(NewFixedTokenSource("test_source_token", "urn:ietf:params:oauth:token-type:test_jwt")),
		)
		assert.NoError(t, err)

		token, err := client.Token(ctx)
		if params.ExpectedErrorPart == "" {
			assert.NoError(t, err)
		} else {
			assert.ErrorContains(t, err, params.ExpectedErrorPart)
		}
		assert.Equal(t, params.ExpectedToken, token)
	}
}

func TestOauth2TokenUpdate(t *testing.T) {
	var currentTestParams Oauth2TokenExchangeTestParams
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runCtx, runCancel := context.WithCancel(ctx)
	go runTokenExchangeServer(t, runCtx, runCancel, 14322, &currentTestParams)

	// First exchange
	currentTestParams = Oauth2TokenExchangeTestParams{
		HttpResponse: `{"access_token": "test_token_1", "token_type": "Bearer", "expires_in": 2}`,
		HttpStatus:   200,
	}

	client, err := NewOauth2TokenExchangeCredentials(
		WithTokenEndpoint("http://localhost:14322/exchange"),
		WithAudience("test_audiience"),
		WithScope("test_scope1", "test_scope2"),
		WithSubjectToken(NewFixedTokenSource("test_source_token", "urn:ietf:params:oauth:token-type:test_jwt")),
	)
	assert.NoError(t, err)

	token, err := client.Token(ctx)
	t1 := time.Now()
	assert.NoError(t, err)
	assert.Equal(t, "Bearer test_token_1", token)

	// Second exchange
	currentTestParams = Oauth2TokenExchangeTestParams{
		HttpResponse: `{"access_token": "test_token_2", "token_type": "Bearer", "expires_in": 10000}`,
		HttpStatus:   200,
	}

	token, err = client.Token(ctx)
	t2 := time.Now()
	assert.NoError(t, err)
	if t2.Sub(t1) <= time.Second { // half expire period => no attempts to update
		assert.Equal(t, "Bearer test_token_1", token)
	}

	time.Sleep(time.Second) // wait half expire period
	for i := 1; i <= 100; i++ {
		t3 := time.Now()
		token, err = client.Token(ctx)
		assert.NoError(t, err)
		if t3.Sub(t1) >= 2*time.Second {
			assert.Equal(t, "Bearer test_token_2", token) // Must update at least sync
			break
		} else {
			if token == "Bearer test_token_2" { // already updated
				break
			}
			assert.Equal(t, "Bearer test_token_1", token)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Third exchange (never got, because token will be expired later)
	currentTestParams = Oauth2TokenExchangeTestParams{
		HttpResponse: `{}`,
		HttpStatus:   500,
	}

	for i := 1; i <= 5; i++ {
		token, err = client.Token(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "Bearer test_token_2", token)
	}
}

func TestJWTTokenSource(t *testing.T) {
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(testPrivateKeyContent))
	assert.NoError(t, err)

	publicKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(testPublicKeyContent))
	assert.NoError(t, err)
	getPublicKey := func(*jwt.Token) (interface{}, error) {
		return publicKey, nil
	}

	var src TokenSource
	src, err = NewJWTTokenSource(
		WithPrivateKey(privateKey),
		WithKeyID("key_id"),
		WithSigningMethod(jwt.SigningMethodRS256),
		WithIssuer("test_issuer"),
		WithAudience("test_audience"),
	)
	assert.NoError(t, err)

	token, err := src.Token()
	assert.Equal(t, "urn:ietf:params:oauth:token-type:jwt", token.TokenType)

	claims := jwt.RegisteredClaims{}
	parsedToken, err := jwt.ParseWithClaims(token.Token, &claims, getPublicKey)
	assert.NoError(t, err)

	assert.True(t, parsedToken.Valid)
	assert.NoError(t, parsedToken.Claims.Valid())
	assert.Equal(t, "test_issuer", claims.Issuer)
	assert.Equal(t, "test_audience", claims.Audience[0])
	assert.Equal(t, "key_id", parsedToken.Header["kid"].(string))
	assert.Equal(t, "RS256", parsedToken.Header["alg"].(string))
}
