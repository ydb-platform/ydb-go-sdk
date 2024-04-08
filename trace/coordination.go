package trace

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Coordination specified trace of coordination client activity.
	// gtrace:gen
	Coordination struct {
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnNew func(CoordinationNewStartInfo) func(CoordinationNewDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnCreateNode func(CoordinationCreateNodeStartInfo) func(CoordinationCreateNodeDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnAlterNode func(CoordinationAlterNodeStartInfo) func(CoordinationAlterNodeDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnDropNode func(CoordinationDropNodeStartInfo) func(CoordinationDropNodeDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnDescribeNode func(CoordinationDescribeNodeStartInfo) func(CoordinationDescribeNodeDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSession func(CoordinationSessionStartInfo) func(CoordinationSessionDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnClose func(CoordinationCloseStartInfo) func(CoordinationCloseDoneInfo)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnStreamNew func(CoordinationStreamNewStartInfo) func(CoordinationStreamNewDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionStarted func(CoordinationSessionStartedInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionStartTimeout func(CoordinationSessionStartTimeoutInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionKeepAliveTimeout func(CoordinationSessionKeepAliveTimeoutInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionStopped func(CoordinationSessionStoppedInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionStopTimeout func(CoordinationSessionStopTimeoutInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionClientTimeout func(CoordinationSessionClientTimeoutInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionServerExpire func(CoordinationSessionServerExpireInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionServerError func(CoordinationSessionServerErrorInfo)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionReceive func(CoordinationSessionReceiveStartInfo) func(CoordinationSessionReceiveDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionReceiveUnexpected func(CoordinationSessionReceiveUnexpectedInfo)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionStop func(CoordinationSessionStopInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionStart func(CoordinationSessionStartStartInfo) func(CoordinationSessionStartDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnSessionSend func(CoordinationSessionSendStartInfo) func(CoordinationSessionSendDoneInfo)
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationNewDoneInfo struct{}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationCloseDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationCreateNodeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Path string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationCreateNodeDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationAlterNodeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Path string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationAlterNodeDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationDropNodeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Path string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationDropNodeDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationDescribeNodeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Path string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationDescribeNodeDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Path string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationStreamNewStartInfo struct{}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationStreamNewDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionStartedInfo struct {
		SessionID         uint64
		ExpectedSessionID uint64
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionStartTimeoutInfo struct {
		Timeout time.Duration
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionKeepAliveTimeoutInfo struct {
		LastGoodResponseTime time.Time
		Timeout              time.Duration
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionStoppedInfo struct {
		SessionID         uint64
		ExpectedSessionID uint64
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionStopTimeoutInfo struct {
		Timeout time.Duration
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionClientTimeoutInfo struct {
		LastGoodResponseTime time.Time
		Timeout              time.Duration
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionServerExpireInfo struct {
		Failure *Ydb_Coordination.SessionResponse_Failure
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionServerErrorInfo struct {
		Failure *Ydb_Coordination.SessionResponse_Failure
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionReceiveStartInfo struct{}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionReceiveDoneInfo struct {
		Response *Ydb_Coordination.SessionResponse
		Error    error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionReceiveUnexpectedInfo struct {
		Response *Ydb_Coordination.SessionResponse
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionStartStartInfo struct{}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionStartDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionStopInfo struct {
		SessionID uint64
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionSendStartInfo struct {
		Request *Ydb_Coordination.SessionRequest
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	CoordinationSessionSendDoneInfo struct {
		Error error
	}
)
