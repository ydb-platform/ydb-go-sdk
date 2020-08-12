package tvm2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap/zaptest"

	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool"
)

type MockTvmTool struct {
	server *httptest.Server
	t      *testing.T
	alias  string
	ticket string
	tvmID  tvm.ClientID
}

func (m *MockTvmTool) Start() {
	m.alias = "alias"
	m.ticket = "3:serv:abcdefABCDEF"
	m.tvmID = 2001000
	m.server = httptest.NewServer(http.HandlerFunc(m.Handle))
}

func (m *MockTvmTool) Stop() {
	m.server.Close()
}

func (m *MockTvmTool) Handle(w http.ResponseWriter, r *http.Request) {
	type ticketsResponse map[string]struct {
		Error  string       `json:"error"`
		Ticket string       `json:"ticket"`
		TvmID  tvm.ClientID `json:"tvm_id"`
	}
	data, err := json.Marshal(ticketsResponse{
		m.alias: {
			Ticket: m.ticket,
			TvmID:  m.tvmID,
		},
	})
	if err != nil {
		m.t.Errorf("failed to marshal response json: %s", err)
	}
	if _, err := w.Write(data); err != nil {
		m.t.Errorf("failed to write response: %s", err)
	}
}

func TestTvmCredentials(t *testing.T) {
	mock := MockTvmTool{t: t}
	mock.Start()
	defer mock.Stop()

	var ctx = context.Background()
	var logger = &zap.Logger{L: zaptest.NewLogger(t)}

	client, err := tvmtool.NewClient(mock.server.URL)

	if err != nil {
		t.Fatalf("failed to create TVM client: %s", err)
	}

	t.Run("Alias", func(t *testing.T) {
		var provider, err = NewTvmCredentialsForAlias(ctx, client, mock.alias, logger)

		if err != nil {
			t.Fatalf("failed to create credentials: %s", err)
		}

		token, err := provider.Token(ctx)
		if err != nil {
			t.Fatalf("failed to create get token from credentials: %s", err)
		}

		if token != mock.ticket {
			t.Errorf("wrong ticket received: %s", token)
		}
	})

	t.Run("ClientID", func(t *testing.T) {
		var provider, err = NewTvmCredentialsForID(ctx, client, uint32(mock.tvmID), logger)

		if err != nil {
			t.Fatalf("failed to create credentials: %s", err)
		}

		token, err := provider.Token(ctx)
		if err != nil {
			t.Fatalf("failed to create get token from credentials: %s", err)
		}

		if token != "3:serv:abcdefABCDEF" {
			t.Errorf("wrong ticket received: %s", token)
		}
	})
}
