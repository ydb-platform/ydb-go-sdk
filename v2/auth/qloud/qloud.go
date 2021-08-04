package qloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"a.yandex-team.ru/library/go/core/log"
)

// TODO(prime@): replace this with real TVM client.
type QloudCredentials struct {
	authorization string
	ticket        string
	client        *http.Client
	src           string
	alias         string
	port          int
	m             sync.Mutex
	logger        log.Logger
	ctx           context.Context
}

var _ ydb.Credentials = (*QloudCredentials)(nil)

func (p *QloudCredentials) Token(ctx context.Context) (string, error) {
	p.m.Lock()
	defer p.m.Unlock()
	return p.ticket, nil
}

func (p *QloudCredentials) init(ctx context.Context) error {
	err := p.updateToken()
	if err != nil {
		p.logger.Error("TVM update token fail", log.Error(err))
		return err
	}
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := p.updateToken()
				if err != nil {
					p.logger.Error("Unable to update token", log.Error(err))
				}
			case <-ctx.Done():
				return
			}

		}
	}()
	return nil
}

func (p *QloudCredentials) updateToken() error {
	request, err := http.NewRequest(
		"GET",
		fmt.Sprintf(
			"http://127.0.0.1:%v/tvm/tickets",
			p.port,
		),
		nil)
	if err != nil {
		return err
	}
	params := url.Values{
		"src":  {p.src},
		"dsts": {p.alias},
	}
	request.URL.RawQuery = params.Encode()
	request.Header.Add("Authorization", p.authorization)
	resp, err := p.client.Do(request.WithContext(p.ctx))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("TVM http error %d: %s", resp.StatusCode, resp.Status)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = resp.Body.Close()
	if err != nil {
		return err
	}

	var response map[string]*ResponseItem
	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}
	if response[p.alias] == nil {
		return fmt.Errorf("TVM error: Alias not found in response %s", p.alias)
	}

	if response[p.alias].Error != "" {
		return fmt.Errorf("TVM error: %s", response[p.alias].Error)
	}

	p.m.Lock()
	defer p.m.Unlock()
	p.ticket = response[p.alias].Ticket
	return nil
}

type ResponseItem struct {
	Ticket string `json:"ticket"`
	TVMId  int    `json:"tvm_id"`
	Error  string `json:"error"`
}

func NewQloudCredentials(ctx context.Context, token, alias string, port int, logger log.Logger) (*QloudCredentials, error) {
	return NewQloudCredentialsFor(ctx, token, "", alias, port, logger)
}

// Same as NewQloudCredentialsFor but allows usage with multi-source configuration
func NewQloudCredentialsFor(ctx context.Context, token, src, alias string, port int, logger log.Logger) (*QloudCredentials, error) {
	localTCPAddr := net.TCPAddr{}
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				LocalAddr: &localTCPAddr,
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	provider := &QloudCredentials{
		port:          port,
		src:           src,
		alias:         alias,
		authorization: token,
		client:        client,
		ctx:           ctx,
		logger:        logger,
	}

	err := provider.init(ctx)
	if err != nil {
		return nil, err
	}

	return provider, err
}
