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
		OnNew func(CoordinationNewStartInfo) func(CoordinationNewDoneInfo)

		OnStreamNew               func(CoordinationStreamNewStartInfo) func(CoordinationStreamNewDoneInfo)
		OnSessionStarted          func(CoordinationSessionStartedInfo)
		OnSessionStartTimeout     func(CoordinationSessionStartTimeoutInfo)
		OnSessionKeepAliveTimeout func(CoordinationSessionKeepAliveTimeoutInfo)
		OnSessionStopped          func(CoordinationSessionStoppedInfo)
		OnSessionStopTimeout      func(CoordinationSessionStopTimeoutInfo)
		OnSessionClientTimeout    func(CoordinationSessionClientTimeoutInfo)
		OnSessionServerExpire     func(CoordinationSessionServerExpireInfo)
		OnSessionServerError      func(CoordinationSessionServerErrorInfo)

		OnSessionReceive           func(CoordinationSessionReceiveStartInfo) func(CoordinationSessionReceiveDoneInfo)
		OnSessionReceiveUnexpected func(CoordinationSessionReceiveUnexpectedInfo)

		OnSessionStop  func(CoordinationSessionStopInfo)
		OnSessionStart func(CoordinationSessionStartStartInfo) func(CoordinationSessionStartDoneInfo)
		OnSessionSend  func(CoordinationSessionSendStartInfo) func(CoordinationSessionSendDoneInfo)
	}
	CoordinationNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	CoordinationNewDoneInfo        struct{}
	CoordinationStreamNewStartInfo struct{}
	CoordinationStreamNewDoneInfo  struct {
		Error error
	}
	CoordinationSessionStartedInfo struct {
		SessionID         uint64
		ExpectedSessionID uint64
	}
	CoordinationSessionStartTimeoutInfo struct {
		Timeout time.Duration
	}
	CoordinationSessionKeepAliveTimeoutInfo struct {
		LastGoodResponseTime time.Time
		Timeout              time.Duration
	}
	CoordinationSessionStoppedInfo struct {
		SessionID         uint64
		ExpectedSessionID uint64
	}
	CoordinationSessionStopTimeoutInfo struct {
		Timeout time.Duration
	}
	CoordinationSessionClientTimeoutInfo struct {
		LastGoodResponseTime time.Time
		Timeout              time.Duration
	}
	CoordinationSessionServerExpireInfo struct {
		Failure *Ydb_Coordination.SessionResponse_Failure
	}
	CoordinationSessionServerErrorInfo struct {
		Failure *Ydb_Coordination.SessionResponse_Failure
	}
	CoordinationSessionReceiveStartInfo struct{}
	CoordinationSessionReceiveDoneInfo  struct {
		Response *Ydb_Coordination.SessionResponse
		Error    error
	}
	CoordinationSessionReceiveUnexpectedInfo struct {
		Response *Ydb_Coordination.SessionResponse
	}
	CoordinationSessionStartStartInfo struct{}
	CoordinationSessionStartDoneInfo  struct {
		Error error
	}
	CoordinationSessionStopInfo struct {
		SessionID uint64
	}
	CoordinationSessionSendStartInfo struct {
		Request *Ydb_Coordination.SessionRequest
	}
	CoordinationSessionSendDoneInfo struct {
		Error error
	}
)
