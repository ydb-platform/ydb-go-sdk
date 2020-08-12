package qloud

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap/zaptest"

	"a.yandex-team.ru/library/go/core/log/zap"
)

func TestQloudCredentials_Token(t *testing.T) {
	srv := startTvmTool(t)
	defer srv.Close()

	ctx := context.Background()
	provider, err := NewQloudCredentials(
		ctx,
		"random_string_random_string_rand",
		"lb",
		srv.Listener.Addr().(*net.TCPAddr).Port,
		&zap.Logger{L: zaptest.NewLogger(t)},
	)
	if err != nil {
		t.Error(err)
		return
	}

	token, err := provider.Token(ctx)
	if err != nil {
		t.Error(err)
		return
	}

	if len(token) == 0 {
		t.Error("Unable to get token")
	}
}

func TestQloudCredentialsFor_Token(t *testing.T) {
	srv := startTvmTool(t)
	defer srv.Close()

	ctx := context.Background()
	provider, err := NewQloudCredentialsFor(
		ctx,
		"random_string_random_string_rand",
		"",
		"lb",
		srv.Listener.Addr().(*net.TCPAddr).Port,
		&zap.Logger{L: zaptest.NewLogger(t)},
	)
	if err != nil {
		t.Error(err)
		return
	}

	token, err := provider.Token(ctx)
	if err != nil {
		t.Error(err)
		return
	}

	if len(token) == 0 {
		t.Error("Unable to get token")
	}
}

func startTvmTool(tb testing.TB) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]*ResponseItem{
			"lb": {
				Ticket: "qwerty123",
				TVMId:  12345,
			},
		}
		data, err := json.Marshal(response)
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			tb.Fatal(err)
		}
	}))
}
