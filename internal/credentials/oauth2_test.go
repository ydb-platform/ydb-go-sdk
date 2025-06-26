package credentials

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

// #nosec G101
var (
	testRSAPrivateKeyContent       = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC75/JS3rMcLJxv\nFgpOzF5+2gH+Yig3RE2MTl9uwC0BZKAv6foYr7xywQyWIK+W1cBhz8R4LfFmZo2j\nM0aCvdRmNBdW0EDSTnHLxCsFhoQWLVq+bI5f5jzkcoiioUtaEpADPqwgVULVtN/n\nnPJiZ6/dU30C3jmR6+LUgEntUtWt3eq3xQIn5lG3zC1klBY/HxtfH5Hu8xBvwRQT\nJnh3UpPLj8XwSmriDgdrhR7o6umWyVuGrMKlLHmeivlfzjYtfzO1MOIMG8t2/zxG\nR+xb4Vwks73sH1KruH/0/JMXU97npwpe+Um+uXhpldPygGErEia7abyZB2gMpXqr\nWYKMo02NAgMBAAECggEAO0BpC5OYw/4XN/optu4/r91bupTGHKNHlsIR2rDzoBhU\nYLd1evpTQJY6O07EP5pYZx9mUwUdtU4KRJeDGO/1/WJYp7HUdtxwirHpZP0lQn77\nuccuX/QQaHLrPekBgz4ONk+5ZBqukAfQgM7fKYOLk41jgpeDbM2Ggb6QUSsJISEp\nzrwpI/nNT/wn+Hvx4DxrzWU6wF+P8kl77UwPYlTA7GsT+T7eKGVH8xsxmK8pt6lg\nsvlBA5XosWBWUCGLgcBkAY5e4ZWbkdd183o+oMo78id6C+PQPE66PLDtHWfpRRmN\nm6XC03x6NVhnfvfozoWnmS4+e4qj4F/emCHvn0GMywKBgQDLXlj7YPFVXxZpUvg/\nrheVcCTGbNmQJ+4cZXx87huqwqKgkmtOyeWsRc7zYInYgraDrtCuDBCfP//ZzOh0\nLxepYLTPk5eNn/GT+VVrqsy35Ccr60g7Lp/bzb1WxyhcLbo0KX7/6jl0lP+VKtdv\nmto+4mbSBXSM1Y5BVVoVgJ3T/wKBgQDsiSvPRzVi5TTj13x67PFymTMx3HCe2WzH\nJUyepCmVhTm482zW95pv6raDr5CTO6OYpHtc5sTTRhVYEZoEYFTM9Vw8faBtluWG\nBjkRh4cIpoIARMn74YZKj0C/0vdX7SHdyBOU3bgRPHg08Hwu3xReqT1kEPSI/B2V\n4pe5fVrucwKBgQCNFgUxUA3dJjyMES18MDDYUZaRug4tfiYouRdmLGIxUxozv6CG\nZnbZzwxFt+GpvPUV4f+P33rgoCvFU+yoPctyjE6j+0aW0DFucPmb2kBwCu5J/856\nkFwCx3blbwFHAco+SdN7g2kcwgmV2MTg/lMOcU7XwUUcN0Obe7UlWbckzQKBgQDQ\nnXaXHL24GGFaZe4y2JFmujmNy1dEsoye44W9ERpf9h1fwsoGmmCKPp90az5+rIXw\nFXl8CUgk8lXW08db/r4r+ma8Lyx0GzcZyplAnaB5/6j+pazjSxfO4KOBy4Y89Tb+\nTP0AOcCi6ws13bgY+sUTa/5qKA4UVw+c5zlb7nRpgwKBgGXAXhenFw1666482iiN\ncHSgwc4ZHa1oL6aNJR1XWH+aboBSwR+feKHUPeT4jHgzRGo/aCNHD2FE5I8eBv33\nof1kWYjAO0YdzeKrW0rTwfvt9gGg+CS397aWu4cy+mTI+MNfBgeDAIVBeJOJXLlX\nhL8bFAuNNVrCOp79TNnNIsh7\n-----END PRIVATE KEY-----\n"                             //nolint:lll
	testRSAPrivateKeyJSONContent   = "-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC75/JS3rMcLJxv\\nFgpOzF5+2gH+Yig3RE2MTl9uwC0BZKAv6foYr7xywQyWIK+W1cBhz8R4LfFmZo2j\\nM0aCvdRmNBdW0EDSTnHLxCsFhoQWLVq+bI5f5jzkcoiioUtaEpADPqwgVULVtN/n\\nnPJiZ6/dU30C3jmR6+LUgEntUtWt3eq3xQIn5lG3zC1klBY/HxtfH5Hu8xBvwRQT\\nJnh3UpPLj8XwSmriDgdrhR7o6umWyVuGrMKlLHmeivlfzjYtfzO1MOIMG8t2/zxG\\nR+xb4Vwks73sH1KruH/0/JMXU97npwpe+Um+uXhpldPygGErEia7abyZB2gMpXqr\\nWYKMo02NAgMBAAECggEAO0BpC5OYw/4XN/optu4/r91bupTGHKNHlsIR2rDzoBhU\\nYLd1evpTQJY6O07EP5pYZx9mUwUdtU4KRJeDGO/1/WJYp7HUdtxwirHpZP0lQn77\\nuccuX/QQaHLrPekBgz4ONk+5ZBqukAfQgM7fKYOLk41jgpeDbM2Ggb6QUSsJISEp\\nzrwpI/nNT/wn+Hvx4DxrzWU6wF+P8kl77UwPYlTA7GsT+T7eKGVH8xsxmK8pt6lg\\nsvlBA5XosWBWUCGLgcBkAY5e4ZWbkdd183o+oMo78id6C+PQPE66PLDtHWfpRRmN\\nm6XC03x6NVhnfvfozoWnmS4+e4qj4F/emCHvn0GMywKBgQDLXlj7YPFVXxZpUvg/\\nrheVcCTGbNmQJ+4cZXx87huqwqKgkmtOyeWsRc7zYInYgraDrtCuDBCfP//ZzOh0\\nLxepYLTPk5eNn/GT+VVrqsy35Ccr60g7Lp/bzb1WxyhcLbo0KX7/6jl0lP+VKtdv\\nmto+4mbSBXSM1Y5BVVoVgJ3T/wKBgQDsiSvPRzVi5TTj13x67PFymTMx3HCe2WzH\\nJUyepCmVhTm482zW95pv6raDr5CTO6OYpHtc5sTTRhVYEZoEYFTM9Vw8faBtluWG\\nBjkRh4cIpoIARMn74YZKj0C/0vdX7SHdyBOU3bgRPHg08Hwu3xReqT1kEPSI/B2V\\n4pe5fVrucwKBgQCNFgUxUA3dJjyMES18MDDYUZaRug4tfiYouRdmLGIxUxozv6CG\\nZnbZzwxFt+GpvPUV4f+P33rgoCvFU+yoPctyjE6j+0aW0DFucPmb2kBwCu5J/856\\nkFwCx3blbwFHAco+SdN7g2kcwgmV2MTg/lMOcU7XwUUcN0Obe7UlWbckzQKBgQDQ\\nnXaXHL24GGFaZe4y2JFmujmNy1dEsoye44W9ERpf9h1fwsoGmmCKPp90az5+rIXw\\nFXl8CUgk8lXW08db/r4r+ma8Lyx0GzcZyplAnaB5/6j+pazjSxfO4KOBy4Y89Tb+\\nTP0AOcCi6ws13bgY+sUTa/5qKA4UVw+c5zlb7nRpgwKBgGXAXhenFw1666482iiN\\ncHSgwc4ZHa1oL6aNJR1XWH+aboBSwR+feKHUPeT4jHgzRGo/aCNHD2FE5I8eBv33\\nof1kWYjAO0YdzeKrW0rTwfvt9gGg+CS397aWu4cy+mTI+MNfBgeDAIVBeJOJXLlX\\nhL8bFAuNNVrCOp79TNnNIsh7\\n-----END PRIVATE KEY-----\\n" //nolint:lll
	testRSAPublicKeyContent        = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu+fyUt6zHCycbxYKTsxe\nftoB/mIoN0RNjE5fbsAtAWSgL+n6GK+8csEMliCvltXAYc/EeC3xZmaNozNGgr3U\nZjQXVtBA0k5xy8QrBYaEFi1avmyOX+Y85HKIoqFLWhKQAz6sIFVC1bTf55zyYmev\n3VN9At45kevi1IBJ7VLVrd3qt8UCJ+ZRt8wtZJQWPx8bXx+R7vMQb8EUEyZ4d1KT\ny4/F8Epq4g4Ha4Ue6OrplslbhqzCpSx5nor5X842LX8ztTDiDBvLdv88RkfsW+Fc\nJLO97B9Sq7h/9PyTF1Pe56cKXvlJvrl4aZXT8oBhKxImu2m8mQdoDKV6q1mCjKNN\njQIDAQAB\n-----END PUBLIC KEY-----\n"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     //nolint:lll
	testECPrivateKeyContent        = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIB6fv25gf7P/7fkjW/2kcKICUhHeOygkFeUJ/ylyU3hloAoGCCqGSM49\nAwEHoUQDQgAEvkKy92hpLiT0GEpzFkYBEWWnkAGTTA6141H0oInA9X30eS0RObAa\nmVY8yD39NI7Nj03hBxEa4Z0tOhrq9cW8eg==\n-----END EC PRIVATE KEY-----\n"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         //nolint:lll
	testECPrivateKeyJSONContent    = "-----BEGIN EC PRIVATE KEY-----\\nMHcCAQEEIB6fv25gf7P/7fkjW/2kcKICUhHeOygkFeUJ/ylyU3hloAoGCCqGSM49\\nAwEHoUQDQgAEvkKy92hpLiT0GEpzFkYBEWWnkAGTTA6141H0oInA9X30eS0RObAa\\nmVY8yD39NI7Nj03hBxEa4Z0tOhrq9cW8eg==\\n-----END EC PRIVATE KEY-----\\n"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    //nolint:lll
	testECPublicKeyContent         = "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEvkKy92hpLiT0GEpzFkYBEWWnkAGT\nTA6141H0oInA9X30eS0RObAamVY8yD39NI7Nj03hBxEa4Z0tOhrq9cW8eg==\n-----END PUBLIC KEY-----\n"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           //nolint:lll
	testHMACSecretKeyBase64Content = "VGhlIHdvcmxkIGhhcyBjaGFuZ2VkLgpJIHNlZSBpdCBpbiB0aGUgd2F0ZXIuCkkgZmVlbCBpdCBpbiB0aGUgRWFydGguCkkgc21lbGwgaXQgaW4gdGhlIGFpci4KTXVjaCB0aGF0IG9uY2Ugd2FzIGlzIGxvc3QsCkZvciBub25lIG5vdyBsaXZlIHdobyByZW1lbWJlciBpdC4K"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 //nolint:lll
)

func WriteErr(w http.ResponseWriter, err error) {
	WriteResponse(w, http.StatusInternalServerError, err.Error(), "text/html")
}

func WriteResponse(w http.ResponseWriter, code int, body string, bodyType string) {
	w.Header().Add("Content-Type", bodyType)
	w.Header().Add("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body))
}

func runTokenExchangeServer(
	currentTestParams *Oauth2TokenExchangeTestParams,
	firstReplyIsError bool,
	serverRequests *atomic.Int64,
) *httptest.Server {
	mux := http.NewServeMux()
	returnedErr := !firstReplyIsError
	returnedErrPtr := &returnedErr

	mux.HandleFunc("/exchange", func(w http.ResponseWriter, r *http.Request) {
		if serverRequests != nil {
			serverRequests.Add(1)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			WriteErr(w, err)
		}

		params, err := url.ParseQuery(string(body))
		if err != nil {
			WriteErr(w, err)
		}
		expectedParams := url.Values{}
		expectedParams.Set("scope", "test_scope1 test_scope2")
		expectedParams.Set("audience", "test_audience")
		expectedParams.Set("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
		expectedParams.Set("requested_token_type", "urn:ietf:params:oauth:token-type:access_token")
		expectedParams.Set("subject_token", "test_source_token")
		expectedParams.Set("subject_token_type", "urn:ietf:params:oauth:token-type:test_jwt")

		if !reflect.DeepEqual(expectedParams, params) {
			WriteResponse(w, 555, fmt.Sprintf("Params are not as expected: \"%s\" != \"%s\"",
				expectedParams.Encode(), body), "text/html") // error will be checked in test thread
		} else {
			if !*returnedErrPtr {
				WriteResponse(w, http.StatusInternalServerError, "test error", "text/html")
				*returnedErrPtr = true
			} else {
				WriteResponse(w, currentTestParams.Status, currentTestParams.Response, "application/json")
			}
		}
	})

	return httptest.NewServer(mux)
}

type Oauth2TokenExchangeTestParams struct {
	Response          string
	Status            int
	ExpectedToken     string
	ExpectedError     error
	ExpectedErrorPart string
}

func TestOauth2TokenExchange(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	testsParams := []Oauth2TokenExchangeTestParams{
		{
			Response:      `{"access_token":"test_token","token_type":"BEARER","expires_in":42,"some_other_field":"x"}`,
			Status:        http.StatusOK,
			ExpectedToken: "Bearer test_token",
		},
		{
			Response:      `aaa`,
			Status:        http.StatusOK,
			ExpectedToken: "",
			ExpectedError: errCouldNotParseResponse,
		},
		{
			Response:      `{}`,
			Status:        http.StatusBadRequest,
			ExpectedToken: "",
			ExpectedError: errCouldNotExchangeToken,
		},
		{
			Response:      `not json`,
			Status:        http.StatusNotFound,
			ExpectedToken: "",
			ExpectedError: errCouldNotExchangeToken,
		},
		{
			Response:          `{"error": "invalid_request"}`,
			Status:            http.StatusBadRequest,
			ExpectedToken:     "",
			ExpectedError:     errCouldNotExchangeToken,
			ExpectedErrorPart: "400 Bad Request, error: invalid_request",
		},
		{
			Response:          `{"error":"unauthorized_client","error_description":"something went bad"}`,
			Status:            http.StatusInternalServerError,
			ExpectedToken:     "",
			ExpectedError:     errCouldNotExchangeToken,
			ExpectedErrorPart: "500 Internal Server Error, error: unauthorized_client, description: \\\\\\\"something went bad\\\\\\\"", //nolint:lll
		},
		{
			Response:          `{"error_description":"something went bad","error_uri":"my_error_uri"}`,
			Status:            http.StatusForbidden,
			ExpectedToken:     "",
			ExpectedError:     errCouldNotExchangeToken,
			ExpectedErrorPart: "403 Forbidden, description: \\\"something went bad\\\", error_uri: my_error_uri",
		},
		{
			Response:      `{"access_token":"test_token","token_type":"","expires_in":42,"some_other_field":"x"}`,
			Status:        http.StatusOK,
			ExpectedToken: "",
			ExpectedError: errUnsupportedTokenType,
		},
		{
			Response:      `{"access_token":"test_token","token_type":"basic","expires_in":42,"some_other_field":"x"}`,
			Status:        http.StatusOK,
			ExpectedToken: "",
			ExpectedError: errUnsupportedTokenType,
		},
		{
			Response:      `{"access_token":"test_token","token_type":"Bearer","expires_in":-42,"some_other_field":"x"}`,
			Status:        http.StatusOK,
			ExpectedToken: "",
			ExpectedError: errIncorrectExpirationTime,
		},
		{
			Response:          `{"access_token":"test_token","token_type":"Bearer","expires_in":42,"scope":"s"}`,
			Status:            http.StatusOK,
			ExpectedToken:     "",
			ExpectedError:     errDifferentScope,
			ExpectedErrorPart: "Expected \\\"test_scope1 test_scope2\\\", but got \\\"s\\\"",
		},
		{
			Response:      `{"access_token":"","token_type":"Bearer","expires_in":42}`,
			Status:        http.StatusOK,
			ExpectedToken: "",
			ExpectedError: errEmptyAccessToken,
		},
		{
			Response:      `{"token_type":"Bearer","expires_in":42}`,
			Status:        http.StatusOK,
			ExpectedToken: "",
			ExpectedError: errEmptyAccessToken,
		},
	}

	for _, params := range testsParams {
		t.Run("", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				var currentTestParams Oauth2TokenExchangeTestParams
				server := runTokenExchangeServer(&currentTestParams, true, nil)
				defer server.Close()

				currentTestParams = params

				client, err := NewOauth2TokenExchangeCredentials(
					WithTokenEndpoint(server.URL+"/exchange"),
					WithAudience("test_audience"),
					WithScope("test_scope1", "test_scope2"),
					WithSubjectToken(NewFixedTokenSource("test_source_token", "urn:ietf:params:oauth:token-type:test_jwt")),
					WithSyncExchangeTimeout(time.Second*3),
				)
				require.NoError(t, err)

				token, err := client.Token(ctx)
				if params.ExpectedErrorPart == "" && params.ExpectedError == nil { //nolint:nestif
					require.NoError(t, err)
				} else {
					if !errors.Is(err, context.DeadlineExceeded) {
						if params.ExpectedErrorPart != "" {
							require.ErrorContains(t, err, params.ExpectedErrorPart)
						}
						if params.ExpectedError != nil {
							require.ErrorIs(t, err, params.ExpectedError)
						}
					}
				}
				require.Equal(t, params.ExpectedToken, token)
			}, xtest.StopAfter(5+time.Second))
		})
	}
}

func TestOauth2TokenUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	xtest.TestManyTimes(t, func(t testing.TB) {
		var currentTestParams Oauth2TokenExchangeTestParams
		server := runTokenExchangeServer(&currentTestParams, true, nil)
		defer server.Close()

		// First exchange
		currentTestParams = Oauth2TokenExchangeTestParams{
			Response: `{"access_token":"test_token_1", "token_type":"Bearer","expires_in":2}`,
			Status:   http.StatusOK,
		}

		client, err := NewOauth2TokenExchangeCredentials(
			WithTokenEndpoint(server.URL+"/exchange"),
			WithAudience("test_audience"),
			WithScope("test_scope1", "test_scope2"),
			WithFixedSubjectToken("test_source_token", "urn:ietf:params:oauth:token-type:test_jwt"),
		)
		require.NoError(t, err)

		token, err := client.Token(ctx)
		t1 := time.Now()
		require.NoError(t, err)
		require.Equal(t, "Bearer test_token_1", token)

		// Second exchange
		currentTestParams = Oauth2TokenExchangeTestParams{
			Response: `{"access_token":"test_token_2", "token_type":"Bearer","expires_in":10000}`,
			Status:   http.StatusOK,
		}

		token, err = client.Token(ctx)
		t2 := time.Now()
		require.NoError(t, err)
		if t2.Sub(t1) <= time.Second { // half expire period => no attempts to update
			require.Equal(t, "Bearer test_token_1", token)
		}

		time.Sleep(time.Second) // wait half expire period
		for i := 1; i <= 100; i++ {
			t3 := time.Now()
			token, err = client.Token(ctx)
			require.NoError(t, err)
			if t3.Sub(t1) >= 2*time.Second {
				require.Equal(t, "Bearer test_token_2", token) // Must update at least sync
			}
			if token == "Bearer test_token_2" { // already updated
				break
			}
			require.Equal(t, "Bearer test_token_1", token)

			time.Sleep(10 * time.Millisecond)
		}

		// Third exchange (never got, because token will be expired later)
		currentTestParams = Oauth2TokenExchangeTestParams{
			Response: `{}`,
			Status:   http.StatusInternalServerError,
		}

		for i := 1; i <= 5; i++ {
			token, err = client.Token(ctx)
			require.NoError(t, err)
			require.Equal(t, "Bearer test_token_2", token)
		}
	}, xtest.StopAfter(14*time.Second))
}

func TestReturnsOldTokenWhileUpdating(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	var currentTestParams Oauth2TokenExchangeTestParams
	var serverRequests atomic.Int64
	server := runTokenExchangeServer(&currentTestParams, true, &serverRequests)
	defer server.Close()

	// First exchange
	currentTestParams = Oauth2TokenExchangeTestParams{
		Response: `{"access_token":"test_token_1", "token_type":"Bearer","expires_in":6}`,
		Status:   http.StatusOK,
	}

	client, err := NewOauth2TokenExchangeCredentials(
		WithTokenEndpoint(server.URL+"/exchange"),
		WithAudience("test_audience"),
		WithScope("test_scope1", "test_scope2"),
		WithFixedSubjectToken("test_source_token", "urn:ietf:params:oauth:token-type:test_jwt"),
	)
	require.NoError(t, err)

	token, err := client.Token(ctx)
	t1 := time.Now()
	require.NoError(t, err)
	require.Equal(t, "Bearer test_token_1", token)

	// Second exchange
	currentTestParams = Oauth2TokenExchangeTestParams{
		Response: `{"error":"unauthorized_client","error_description":"something went bad"}`,
		Status:   http.StatusInternalServerError,
	}

	token, err = client.Token(ctx)
	t2 := time.Now()
	require.NoError(t, err)
	if t2.Sub(t1) <= time.Second*3 {
		require.Equal(t, "Bearer test_token_1", token)
		require.Equal(t, int64(2), serverRequests.Load())
	}

	time.Sleep(time.Second * 3) // wait half expire period
	for i := 1; i <= 100; i++ {
		token, err = client.Token(ctx)
		t3 := time.Now()
		if t3.Sub(t1) < 6*time.Second {
			require.NoError(t, err)
			require.Equal(t, "Bearer test_token_1", token)
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	require.Greater(t, serverRequests.Load(), int64(3)) // at least one retry
}

func TestWrongParameters(t *testing.T) {
	_, err := NewOauth2TokenExchangeCredentials(
		// No endpoint
		WithFixedActorToken("test_source_token", "urn:ietf:params:oauth:token-type:test_jwt"),
		WithRequestedTokenType("access_token"),
	)
	require.ErrorIs(t, err, errEmptyTokenEndpointError)
}

type errorTokenSource struct{}

var errTokenSource = errors.New("test error")

func (s *errorTokenSource) Token() (Token, error) {
	return Token{"", ""}, errTokenSource
}

func TestErrorInSourceToken(t *testing.T) {
	// Create
	_, err := NewOauth2TokenExchangeCredentials(
		WithTokenEndpoint("http:trololo"),
		WithJWTSubjectToken(
			WithRSAPrivateKeyPEMContent([]byte("invalid")),
			WithKeyID("key_id"),
			WithSigningMethod(jwt.SigningMethodRS256),
			WithIssuer("test_issuer"),
			WithAudience("test_audience"),
		),
	)
	require.ErrorIs(t, err, errCouldNotCreateTokenSource)

	_, err = NewOauth2TokenExchangeCredentials(
		WithTokenEndpoint("http:trololo"),
		WithJWTSubjectToken(
			WithECPrivateKeyPEMContent([]byte("invalid")),
			WithKeyID("key_id"),
			WithSigningMethod(jwt.SigningMethodES512),
			WithIssuer("test_issuer"),
			WithAudience("test_audience"),
		),
	)
	require.ErrorIs(t, err, errCouldNotCreateTokenSource)

	_, err = NewOauth2TokenExchangeCredentials(
		WithTokenEndpoint("http:trololo"),
		WithJWTSubjectToken(
			WithHMACSecretKeyBase64Content("<not base64>"),
			WithKeyID("key_id"),
			WithSigningMethod(jwt.SigningMethodHS384),
			WithIssuer("test_issuer"),
			WithAudience("test_audience"),
		),
	)
	require.ErrorIs(t, err, errCouldNotCreateTokenSource)

	_, err = NewOauth2TokenExchangeCredentials(
		WithTokenEndpoint("http:trololo"),
		WithJWTSubjectToken(
			WithHMACSecretKeyBase64Content(testHMACSecretKeyBase64Content),
			WithKeyID("key_id"),
			WithSigningMethodName("unknown"),
			WithIssuer("test_issuer"),
			WithAudience("test_audience"),
		),
	)
	require.ErrorIs(t, err, errUnsupportedSigningMethod)

	// Use
	client, err := NewOauth2TokenExchangeCredentials(
		WithTokenEndpoint("http:trololo"),
		WithGrantType("grant_type"),
		WithRequestTimeout(time.Second),
		WithResource("res", "res2"),
		WithFixedSubjectToken("t", "tt"),
		WithActorToken(&errorTokenSource{}),
		WithSourceInfo("TestErrorInSourceToken"),
	)
	require.NoError(t, err)

	// Check that token prints well
	formatted := fmt.Sprint(client)
	require.Equal(t, `OAuth2TokenExchange{Endpoint:"http:trololo",GrantType:grant_type,Resource:[res res2],Audience:[],Scope:[],RequestedTokenType:urn:ietf:params:oauth:token-type:access_token,SubjectToken:FixedTokenSource{Token:"****(CRC-32c: 856A5AA8)",Type:tt},ActorToken:&{},From:"TestErrorInSourceToken"}`, formatted) //nolint:lll

	token, err := client.Token(context.Background())
	require.ErrorIs(t, err, errTokenSource)
	require.Equal(t, "", token)

	client, err = NewOauth2TokenExchangeCredentials(
		WithTokenEndpoint("http:trololo"),
		WithGrantType("grant_type"),
		WithRequestTimeout(time.Second),
		WithResource("res", "res2"),
		WithSubjectToken(&errorTokenSource{}),
	)
	require.NoError(t, err)

	token, err = client.Token(context.Background())
	require.ErrorIs(t, err, errTokenSource)
	require.Equal(t, "", token)
}

func TestErrorInHTTPRequest(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		client, err := NewOauth2TokenExchangeCredentials(
			WithTokenEndpoint("http://invalid_host:42/exchange"),
			WithJWTSubjectToken(
				WithRSAPrivateKeyPEMContent([]byte(testRSAPrivateKeyContent)),
				WithKeyID("key_id"),
				WithSigningMethod(jwt.SigningMethodRS256),
				WithIssuer("test_issuer"),
				WithAudience("test_audience"),
			),
			WithJWTActorToken(
				WithRSAPrivateKeyPEMContent([]byte(testRSAPrivateKeyContent)),
				WithKeyID("key_id"),
				WithSigningMethod(jwt.SigningMethodRS256),
				WithIssuer("test_issuer"),
			),
			WithScope("1", "2", "3"),
			WithSourceInfo("TestErrorInHTTPRequest"),
			WithSyncExchangeTimeout(time.Second*3),
		)
		require.NoError(t, err)

		token, err := client.Token(context.Background())
		if !errors.Is(err, context.DeadlineExceeded) {
			require.ErrorIs(t, err, errCouldNotExchangeToken)
		}
		require.Equal(t, "", token)

		// check format:
		formatted := fmt.Sprint(client)
		require.Equal(t, `OAuth2TokenExchange{Endpoint:"http://invalid_host:42/exchange",GrantType:urn:ietf:params:oauth:grant-type:token-exchange,Resource:[],Audience:[],Scope:[1 2 3],RequestedTokenType:urn:ietf:params:oauth:token-type:access_token,SubjectToken:JWTTokenSource{Method:RS256,KeyID:key_id,Issuer:"test_issuer",Subject:"",Audience:[test_audience],ID:,TokenTTL:1h0m0s},ActorToken:JWTTokenSource{Method:RS256,KeyID:key_id,Issuer:"test_issuer",Subject:"",Audience:[],ID:,TokenTTL:1h0m0s},From:"TestErrorInHTTPRequest"}`, formatted) //nolint:lll
	}, xtest.StopAfter(15*time.Second))
}

func TestJWTTokenSource(t *testing.T) {
	methods := []string{
		"RS384",
		"ES256",
		"HS512",
		"PS512",
	}
	binaryOpts := []bool{
		false,
		true,
	}

	for _, method := range methods {
		for _, binary := range binaryOpts {
			var publicKey interface{}
			var src TokenSource
			var err error

			//nolint:nestif
			if method[0:2] == "HS" {
				publicKey, err = base64.StdEncoding.DecodeString(testHMACSecretKeyBase64Content)
				require.NoError(t, err)

				if binary {
					src, err = NewJWTTokenSource(
						WithHMACSecretKey(publicKey.([]byte)),
						WithKeyID("key_id"),
						WithSigningMethodName(method),
						WithIssuer("test_issuer"),
						WithAudience("test_audience"),
					)
					require.NoError(t, err)
				} else {
					src, err = NewJWTTokenSource(
						WithHMACSecretKeyBase64Content(testHMACSecretKeyBase64Content),
						WithKeyID("key_id"),
						WithSigningMethodName(method),
						WithIssuer("test_issuer"),
						WithAudience("test_audience"),
					)
					require.NoError(t, err)
				}
			} else if method[0:2] == "ES" {
				if binary {
					continue
				}

				publicKey, err = jwt.ParseECPublicKeyFromPEM([]byte(testECPublicKeyContent))
				require.NoError(t, err)

				src, err = NewJWTTokenSource(
					WithECPrivateKeyPEMContent([]byte(testECPrivateKeyContent)),
					WithKeyID("key_id"),
					WithSigningMethodName(method),
					WithIssuer("test_issuer"),
					WithAudience("test_audience"),
				)
				require.NoError(t, err)
			} else {
				if binary {
					continue
				}

				publicKey, err = jwt.ParseRSAPublicKeyFromPEM([]byte(testRSAPublicKeyContent))
				require.NoError(t, err)

				src, err = NewJWTTokenSource(
					WithRSAPrivateKeyPEMContent([]byte(testRSAPrivateKeyContent)),
					WithKeyID("key_id"),
					WithSigningMethodName(method),
					WithIssuer("test_issuer"),
					WithAudience("test_audience"),
				)
				require.NoError(t, err)
			}

			getPublicKey := func(*jwt.Token) (interface{}, error) {
				return publicKey, nil
			}

			token, err := src.Token()
			require.NoError(t, err)
			require.Equal(t, "urn:ietf:params:oauth:token-type:jwt", token.TokenType)

			claims := jwt.RegisteredClaims{}
			parsedToken, err := jwt.ParseWithClaims(token.Token, &claims, getPublicKey)
			require.NoError(t, err)

			require.True(t, parsedToken.Valid)
			require.NoError(t, parsedToken.Claims.Valid())
			require.Equal(t, "test_issuer", claims.Issuer)
			require.Equal(t, "test_audience", claims.Audience[0])
			require.Equal(t, "key_id", parsedToken.Header["kid"].(string))
			require.Equal(t, method, parsedToken.Header["alg"].(string))
		}
	}
}

func TestJWTTokenBadParams(t *testing.T) {
	_, err := NewJWTTokenSource(
		// no private key
		WithKeyID("key_id"),
		WithSigningMethod(jwt.SigningMethodRS256),
		WithIssuer("test_issuer"),
		WithAudience("test_audience"),
		WithID("id"),
	)
	require.ErrorIs(t, err, errNoPrivateKeyError)

	_, err = NewJWTTokenSource(
		WithPrivateKey([]byte{1, 2, 3}),
		WithKeyID("key_id"),
		// no signing method
		WithSubject("s"),
		WithTokenTTL(time.Minute),
		WithAudience("test_audience"),
	)
	require.ErrorIs(t, err, errNoSigningMethodError)
}

func TestJWTTokenSourceReadPrivateKeyFromFile(t *testing.T) {
	methods := []string{
		"ES256",
		"PS512",
		"RS384",
		"HS256",
	}
	binaryOpts := []bool{
		false,
		true,
	}

	for _, method := range methods {
		for _, binary := range binaryOpts {
			f, err := os.CreateTemp(t.TempDir(), "tmpfile-")
			require.NoError(t, err)
			defer os.Remove(f.Name())
			defer f.Close()

			var publicKey interface{}
			var src TokenSource

			//nolint:nestif
			if method[0:2] == "HS" {
				publicKey, err = base64.StdEncoding.DecodeString(testHMACSecretKeyBase64Content)
				require.NoError(t, err)

				if binary {
					_, err = f.Write(publicKey.([]byte))
					require.NoError(t, err)
					f.Close()

					_, err = NewJWTTokenSource(
						WithHMACSecretKeyFile("~/unknown_file"),
						WithKeyID("key_id"),
						WithSigningMethodName(method),
						WithIssuer("test_issuer"),
						WithAudience("test_audience"),
					)
					require.ErrorIs(t, err, errCouldNotReadPrivateKeyFile)

					src, err = NewJWTTokenSource(
						WithHMACSecretKeyFile(f.Name()),
						WithKeyID("key_id"),
						WithSigningMethodName(method),
						WithIssuer("test_issuer"),
						WithAudience("test_audience"),
					)
					require.NoError(t, err)
				} else {
					_, err = f.WriteString(testHMACSecretKeyBase64Content)
					require.NoError(t, err)
					f.Close()

					_, err = NewJWTTokenSource(
						WithHMACSecretKeyBase64File("~/unknown_file"),
						WithKeyID("key_id"),
						WithSigningMethodName(method),
						WithIssuer("test_issuer"),
						WithAudience("test_audience"),
					)
					require.ErrorIs(t, err, errCouldNotReadPrivateKeyFile)

					src, err = NewJWTTokenSource(
						WithHMACSecretKeyBase64File(f.Name()),
						WithKeyID("key_id"),
						WithSigningMethodName(method),
						WithIssuer("test_issuer"),
						WithAudience("test_audience"),
					)
					require.NoError(t, err)
				}
			} else if method[0:2] == "ES" {
				if binary {
					continue
				}

				publicKey, err = jwt.ParseECPublicKeyFromPEM([]byte(testECPublicKeyContent))
				require.NoError(t, err)

				_, err = f.WriteString(testECPrivateKeyContent)
				require.NoError(t, err)
				f.Close()

				_, err = NewJWTTokenSource(
					WithECPrivateKeyPEMFile("~/unknown_file"),
					WithKeyID("key_id"),
					WithSigningMethodName(method),
					WithIssuer("test_issuer"),
					WithAudience("test_audience"),
				)
				require.ErrorIs(t, err, errCouldNotReadPrivateKeyFile)

				src, err = NewJWTTokenSource(
					WithECPrivateKeyPEMFile(f.Name()),
					WithKeyID("key_id"),
					WithSigningMethodName(method),
					WithIssuer("test_issuer"),
					WithAudience("test_audience"),
				)
				require.NoError(t, err)
			} else {
				if binary {
					continue
				}

				publicKey, err = jwt.ParseRSAPublicKeyFromPEM([]byte(testRSAPublicKeyContent))
				require.NoError(t, err)

				_, err = f.WriteString(testRSAPrivateKeyContent)
				require.NoError(t, err)
				f.Close()

				_, err = NewJWTTokenSource(
					WithRSAPrivateKeyPEMFile("~/unknown_file"),
					WithKeyID("key_id"),
					WithSigningMethodName(method),
					WithIssuer("test_issuer"),
					WithAudience("test_audience"),
				)
				require.ErrorIs(t, err, errCouldNotReadPrivateKeyFile)

				src, err = NewJWTTokenSource(
					WithRSAPrivateKeyPEMFile(f.Name()),
					WithKeyID("key_id"),
					WithSigningMethodName(method),
					WithIssuer("test_issuer"),
					WithAudience("test_audience"),
				)
				require.NoError(t, err)
			}

			token, err := src.Token()
			require.NoError(t, err)

			// parse token
			getPublicKey := func(*jwt.Token) (interface{}, error) {
				return publicKey, nil
			}

			claims := jwt.RegisteredClaims{}
			parsedToken, err := jwt.ParseWithClaims(token.Token, &claims, getPublicKey)
			require.NoError(t, err)

			require.True(t, parsedToken.Valid)
			require.NoError(t, parsedToken.Claims.Valid())
			require.Equal(t, "test_issuer", claims.Issuer)
			require.Equal(t, "test_audience", claims.Audience[0])
			require.Equal(t, "key_id", parsedToken.Header["kid"].(string))
			require.Equal(t, method, parsedToken.Header["alg"].(string))
		}
	}
}

type parseSettingsFromFileTestParams struct {
	Cfg                          string
	CfgFile                      string
	ExpectedError                error
	ExpectedFormattedCredentials string
}

func TestParseSettingsFromFile(t *testing.T) {
	testsParams := []parseSettingsFromFileTestParams{
		{
			Cfg: `{
				"token-endpoint": "http://localhost:123",
				"res": "tEst",
				"grant-type": "grant",
				"subject-credentials": {
					"type": "fixed",
					"token": "test-token",
					"token-type": "test-token-type"
				}
			}`,
			ExpectedFormattedCredentials: `OAuth2TokenExchange{Endpoint:"http://localhost:123",GrantType:grant,Resource:[tEst],Audience:[],Scope:[],RequestedTokenType:urn:ietf:params:oauth:token-type:access_token,SubjectToken:FixedTokenSource{Token:"****(CRC-32c: 1203ABFA)",Type:test-token-type},From:"TestParseSettingsFromFile"}`, //nolint:lll
		},
		{
			Cfg: `{
				"token-endpoint": "http://localhost:123",
				"aud": "test-aud",
				"res": [
					"r1",
					"r2"
				],
				"scope": [
					"s1",
					"s2"
				],
				"unknown-field": [123],
				"actor-credentials": {
					"type": "fixed",
					"token": "test-token",
					"token-type": "test-token-type"
				}
			}`,
			ExpectedFormattedCredentials: `OAuth2TokenExchange{Endpoint:"http://localhost:123",GrantType:urn:ietf:params:oauth:grant-type:token-exchange,Resource:[r1 r2],Audience:[test-aud],Scope:[s1 s2],RequestedTokenType:urn:ietf:params:oauth:token-type:access_token,ActorToken:FixedTokenSource{Token:"****(CRC-32c: 1203ABFA)",Type:test-token-type},From:"TestParseSettingsFromFile"}`, //nolint:lll
		},
		{
			Cfg: `{
				"token-endpoint": "http://localhost:123",
				"requested-token-type": "access_token",
				"subject-credentials": {
					"type": "JWT",
					"alg": "ps256",
					"private-key": "` + testRSAPrivateKeyJSONContent + `",
					"aud": ["a1", "a2"],
					"jti": "123",
					"sub": "test_subject",
					"iss": "test_issuer",
					"kid": "test_key_id",
					"ttl": "24h",
					"unknown_field": [123]
				}
			}`,
			ExpectedFormattedCredentials: `OAuth2TokenExchange{Endpoint:"http://localhost:123",GrantType:urn:ietf:params:oauth:grant-type:token-exchange,Resource:[],Audience:[],Scope:[],RequestedTokenType:access_token,SubjectToken:JWTTokenSource{Method:PS256,KeyID:test_key_id,Issuer:"test_issuer",Subject:"test_subject",Audience:[a1 a2],ID:123,TokenTTL:24h0m0s},From:"TestParseSettingsFromFile"}`, //nolint:lll
		},
		{
			Cfg: `{
				"token-endpoint": "http://localhost:123",
				"subject-credentials": {
					"type": "JWT",
					"alg": "es256",
					"private-key": "` + testECPrivateKeyJSONContent + `",
					"ttl": "3m"
				}
			}`,
			ExpectedFormattedCredentials: `OAuth2TokenExchange{Endpoint:"http://localhost:123",GrantType:urn:ietf:params:oauth:grant-type:token-exchange,Resource:[],Audience:[],Scope:[],RequestedTokenType:urn:ietf:params:oauth:token-type:access_token,SubjectToken:JWTTokenSource{Method:ES256,KeyID:,Issuer:"",Subject:"",Audience:[],ID:,TokenTTL:3m0s},From:"TestParseSettingsFromFile"}`, //nolint:lll
		},
		{
			Cfg: `{
				"token-endpoint": "http://localhost:123",
				"subject-credentials": {
					"type": "JWT",
					"alg": "hs512",
					"private-key": "` + testHMACSecretKeyBase64Content + `"
				}
			}`,
			ExpectedFormattedCredentials: `OAuth2TokenExchange{Endpoint:"http://localhost:123",GrantType:urn:ietf:params:oauth:grant-type:token-exchange,Resource:[],Audience:[],Scope:[],RequestedTokenType:urn:ietf:params:oauth:token-type:access_token,SubjectToken:JWTTokenSource{Method:HS512,KeyID:,Issuer:"",Subject:"",Audience:[],ID:,TokenTTL:1h0m0s},From:"TestParseSettingsFromFile"}`, //nolint:lll
		},
		{
			Cfg: `{
				"token-endpoint": "http://localhost:123",
				"subject-credentials": {
					"type": "JWT",
					"alg": "rs512",
					"private-key": "` + testHMACSecretKeyBase64Content + `"
				}
			}`,
			ExpectedError: errCouldNotparsePrivateKey, // wrong private key format
		},
		{
			Cfg: `{
				"token-endpoint": "http://localhost:123",
				"subject-credentials": {
					"type": "JWT",
					"alg": "es512",
					"private-key": "` + testHMACSecretKeyBase64Content + `"
				}
			}`,
			ExpectedError: errCouldNotparsePrivateKey, // wrong private key format
		},
		{
			Cfg: `{
				"token-endpoint": "http://localhost:123",
				"subject-credentials": {
					"type": "JWT",
					"alg": "es512",
					"private-key": "` + testRSAPrivateKeyJSONContent + `"
				}
			}`,
			ExpectedError: errCouldNotparsePrivateKey, // wrong private key format
		},
		{
			Cfg: `{
				"token-endpoint": "http://localhost:123",
				"subject-credentials": {
					"type": "JWT",
					"alg": "hs512",
					"private-key": "<not base64>"
				}
			}`,
			ExpectedError: errCouldNotParseBase64Secret, // wrong private key format
		},
		{
			CfgFile:       "~/unknown-file.cfg",
			ExpectedError: errCouldNotReadConfigFile,
		},
		{
			Cfg:           "{not json",
			ExpectedError: errCouldNotUnmarshalJSON,
		},
		{
			Cfg: `{
				"actor-credentials": ""
			}`,
			ExpectedError: errCouldNotUnmarshalJSON,
		},
		{
			Cfg: `{
				"subject-credentials": {
					"type": "JWT",
					"ttl": 123
				}
			}`,
			ExpectedError: errCouldNotUnmarshalJSON,
		},
		{
			Cfg: `{
				"subject-credentials": {
					"type": "JWT",
					"ttl": "123"
				}
			}`,
			ExpectedError: errCouldNotUnmarshalJSON,
		},
		{
			Cfg: `{
				"subject-credentials": {
					"type": "JWT",
					"ttl": "-3h"
				}
			}`,
			ExpectedError: errTTLMustBePositive,
		},
		{
			Cfg: `{
				"actor-credentials": {
					"type": "JWT",
					"alg": "HS384"
				}
			}`,
			ExpectedError: errAlgAndKeyRequired,
		},
		{
			Cfg: `{
				"actor-credentials": {
					"type": "JWT",
					"private-key": "1234"
				}
			}`,
			ExpectedError: errAlgAndKeyRequired,
		},
		{
			Cfg: `{
				"actor-credentials": {
					"type": "JWT",
					"alg": "unknown",
					"private-key": "1234"
				}
			}`,
			ExpectedError: errUnsupportedSigningMethod,
		},
		{
			Cfg: `{
				"actor-credentials": {
					"type": "JWT",
					"ttl": "3h"
				}
			}`,
			ExpectedError: errAlgAndKeyRequired,
		},
		{
			Cfg: `{
				"aud": {
					"value": "wrong_format of aud: not string and not list"
				},
				"actor-credentials": {
					"type": "fixed",
					"token": "test-token",
					"token-type": "test-token-type"
				}
			}`,
			ExpectedError: errCouldNotUnmarshalJSON,
		},
		{
			Cfg: `{
				"actor-credentials": {
					"type": "unknown"
				}
			}`,
			ExpectedError: errUnknownTokenSourceType,
		},
		{
			Cfg: `{
				"subject-credentials": {
					"token": "123"
				}
			}`,
			ExpectedError: errUnknownTokenSourceType,
		},
		{
			Cfg: `{
				"subject-credentials": {
					"type": "FIXED",
					"token": "123"
				}
			}`,
			ExpectedError: errTokenAndTokenTypeRequired,
		},
		{
			Cfg: `{
				"actor-credentials": {
					"type": "Fixed",
					"token-type": "t"
				}
			}`,
			ExpectedError: errTokenAndTokenTypeRequired,
		},
	}
	for _, params := range testsParams {
		var fileName string
		if params.Cfg != "" {
			f, err := os.CreateTemp(t.TempDir(), "cfg-")
			require.NoError(t, err)
			defer os.Remove(f.Name())
			defer f.Close()
			_, err = f.WriteString(params.Cfg)
			require.NoError(t, err)
			fileName = f.Name()
		} else {
			fileName = params.CfgFile
		}

		client, err := NewOauth2TokenExchangeCredentialsFile(
			fileName,
			WithSourceInfo("TestParseSettingsFromFile"),
		)
		t.Logf("Cfg:\n%s\n", params.Cfg)
		if params.ExpectedError != nil {
			require.ErrorIs(t, err, params.ExpectedError)
		} else {
			require.NoError(t, err)
			formatted := fmt.Sprint(client)
			require.Equal(t, params.ExpectedFormattedCredentials, formatted)
		}
	}
}
