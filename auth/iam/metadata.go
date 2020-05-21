package iam

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
)

var _ ydb.Credentials = &instanceServiceAccountCredentials{}

type instanceServiceAccountCredentials struct {
	token     string
	expiresAt time.Time

	mu *sync.RWMutex

	httpClient http.Client
}

// Returns cached token if it is valid. Otherwise, will try to renew.
func (m *instanceServiceAccountCredentials) Token(ctx context.Context) (string, error) {
	m.mu.RLock()

	const renewOverhead = 15 * time.Second
	if m.token != "" {
		if time.Until(m.expiresAt) > renewOverhead {
			defer m.mu.RUnlock()
			return m.token, nil
		}
	}

	//renew
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	const metadataURL = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token" // from YC go-sdk
	req, err := http.NewRequest("GET", metadataURL, nil)
	if err != nil {
		return "", &CreateTokenError{
			Reason: fmt.Errorf("failed to create HTTP request"),
		}
	}

	req.Header.Set("Metadata-Flavor", "Google") // from YC go-sdk

	reqTime := time.Now()
	resp, err := m.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", &CreateTokenError{
			Reason: fmt.Errorf("failed medatata request (outside of compute instance?): %s",
				err,
			),
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", &CreateTokenError{
			Reason: fmt.Errorf("%s: possibly missing service_account_id in instance spec",
				resp.Status,
			),
		}
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", &CreateTokenError{
			Reason: fmt.Errorf("response body read failed: %s", err.Error()),
		}
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s", resp.Status)
	}

	var tokenResponse struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}

	err = json.Unmarshal(body, &tokenResponse)
	if err != nil {
		return "", &CreateTokenError{
			Reason: fmt.Errorf("failed response body unmarshal: %s", err.Error()),
		}
	}
	m.token = tokenResponse.AccessToken
	m.expiresAt = reqTime.Add(time.Duration(tokenResponse.ExpiresIn) * time.Second)

	return m.token, nil
}

// Credentials provider that uses instance metadata to obtain token for service account attached to instance.
func InstanceServiceAccount() (ydb.Credentials, error) {
	return &instanceServiceAccountCredentials{
		mu: &sync.RWMutex{},
		httpClient: http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   time.Second, // One second should be enough for localhost connection.
					KeepAlive: -1,          // No keep alive. Near token per hour requested.
				}).DialContext,
			},
		},
	}, nil
}
