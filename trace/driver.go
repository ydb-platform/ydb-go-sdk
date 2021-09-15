package trace

//go:generate gtrace

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/operation"
	"strings"
)

type (
	//gtrace:gen
	//gtrace:set shortcut
	DriverTrace struct {
		OnDial func(DialStartInfo) func(DialDoneInfo)

		OnGetConn func(GetConnStartInfo) func(GetConnDoneInfo)

		OnPessimization func(PessimizationStartInfo) func(PessimizationDoneInfo)

		// Only for background.
		OnTrackConn func(TrackConnStartInfo) func(TrackConnDoneInfo)

		OnGetCredentials func(GetCredentialsStartInfo) func(GetCredentialsDoneInfo)

		OnDiscovery func(DiscoveryStartInfo) func(DiscoveryDoneInfo)

		OnOperation func(OperationStartInfo) func(OperationDoneInfo)

		OnStream func(StreamStartInfo) func(StreamRecvDoneInfo) func(StreamDoneInfo)
	}
)

// Method represents rpc method.
type Method string

// Name returns the rpc method name.
func (m Method) Name() (s string) {
	_, s = m.Split()
	return
}

// Service returns the rpc service name.
func (m Method) Service() (s string) {
	s, _ = m.Split()
	return
}

// Split returns service name and method.
func (m Method) Split() (service, method string) {
	i := strings.LastIndex(string(m), "/")
	if i == -1 {
		return string(m), string(m)
	}
	return strings.TrimPrefix(string(m[:i]), "/"), string(m[i+1:])
}

type (
	DialStartInfo struct {
		Context context.Context
		Address string
	}
	DialDoneInfo struct {
		Error error
	}
	GetConnStartInfo struct {
		Context context.Context
	}
	GetConnDoneInfo struct {
		Address string
		Error   error
	}
	PessimizationStartInfo struct {
		Context context.Context
		Address string
		Cause   error
	}
	PessimizationDoneInfo struct {
		Error error
	}
	TrackConnStartInfo struct {
		Address string
	}
	TrackConnDoneInfo struct {
		Address string
	}
	GetCredentialsStartInfo struct {
		Context context.Context
	}
	GetCredentialsDoneInfo struct {
		TokenOk bool
		Error   error
	}
	DiscoveryStartInfo struct {
		Context context.Context
	}
	DiscoveryDoneInfo struct {
		Context   context.Context
		Endpoints []endpoint.Endpoint
		Error     error
	}
	OperationStartInfo struct {
		Context context.Context
		Address string
		Method  Method
		Params  operation.Params
	}
	OperationDoneInfo struct {
		OpID   string
		Issues errors.IssueIterator
		Error  error
	}
	StreamStartInfo struct {
		Context context.Context
		Address string
		Method  Method
	}
	StreamRecvDoneInfo struct {
		Error error
	}
	StreamDoneInfo struct {
		Error error
	}
)
