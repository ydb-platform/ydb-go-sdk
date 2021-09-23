package models

import (
	"context"
	"time"
)

type (
	QueryParameters interface {
	}
	DataQuery interface {
	}
	SessionInfo interface {
		Status() string
	}
	Result interface {
	}
	CreateSessionStartInfo struct {
		Context context.Context
	}
	CreateSessionDoneInfo struct {
		Context   context.Context
		SessionID string
		Endpoint  string
		Latency   time.Duration
		Error     error
	}
	KeepAliveStartInfo struct {
		Context   context.Context
		SessionID string
	}
	KeepAliveDoneInfo struct {
		Context     context.Context
		SessionID   string
		SessionInfo SessionInfo
		Error       error
	}
	DeleteSessionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	DeleteSessionDoneInfo struct {
		Context   context.Context
		SessionID string
		Latency   time.Duration
		Error     error
	}
	PrepareDataQueryStartInfo struct {
		Context   context.Context
		SessionID string
		Query     string
	}
	PrepareDataQueryDoneInfo struct {
		Context   context.Context
		SessionID string
		Query     string
		Result    DataQuery
		Cached    bool
		Error     error
	}
	ExecuteDataQueryStartInfo struct {
		Context    context.Context
		SessionID  string
		TxID       string
		Query      DataQuery
		Parameters QueryParameters
	}
	ExecuteDataQueryDoneInfo struct {
		Context    context.Context
		SessionID  string
		TxID       string
		Query      DataQuery
		Parameters QueryParameters
		Prepared   bool
		Result     Result
		Error      error
	}
	StreamReadTableStartInfo struct {
		Context   context.Context
		SessionID string
	}
	StreamReadTableDoneInfo struct {
		Context   context.Context
		SessionID string
		Result    Result
		Error     error
	}
	StreamExecuteScanQueryStartInfo struct {
		Context    context.Context
		SessionID  string
		Query      DataQuery
		Parameters QueryParameters
	}
	StreamExecuteScanQueryDoneInfo struct {
		Context    context.Context
		SessionID  string
		Query      DataQuery
		Parameters QueryParameters
		Result     Result
		Error      error
	}
	BeginTransactionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	BeginTransactionDoneInfo struct {
		Context   context.Context
		SessionID string
		TxID      string
		Error     error
	}
	CommitTransactionStartInfo struct {
		Context   context.Context
		SessionID string
		TxID      string
	}
	CommitTransactionDoneInfo struct {
		Context   context.Context
		SessionID string
		TxID      string
		Error     error
	}
	RollbackTransactionStartInfo struct {
		Context   context.Context
		SessionID string
		TxID      string
	}
	RollbackTransactionDoneInfo struct {
		Context   context.Context
		SessionID string
		TxID      string
		Error     error
	}
	RetryLoopStartInfo struct {
		Context context.Context
	}
	RetryLoopDoneInfo struct {
		Context  context.Context
		Latency  time.Duration
		Attempts int
	}
)

type (
	SessionPoolCreateStartInfo struct {
		Context context.Context
	}
	SessionPoolCreateDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	SessionPoolGetStartInfo struct {
		Context context.Context
	}
	SessionPoolGetDoneInfo struct {
		Context       context.Context
		SessionID     string
		Latency       time.Duration
		RetryAttempts int
		Error         error
	}
	SessionPoolWaitStartInfo struct {
		Context context.Context
	}
	SessionPoolWaitDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	SessionPoolTakeStartInfo struct {
		Context   context.Context
		SessionID string
	}
	SessionPoolTakeWaitInfo struct {
		Context   context.Context
		SessionID string
	}
	SessionPoolTakeDoneInfo struct {
		Context   context.Context
		SessionID string
		Took      bool
		Error     error
	}
	SessionPoolPutStartInfo struct {
		Context   context.Context
		SessionID string
	}
	SessionPoolPutDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	SessionPoolCloseSessionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	SessionPoolCloseSessionDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	SessionPoolCloseStartInfo struct {
		Context context.Context
	}
	SessionPoolCloseDoneInfo struct {
		Context context.Context
		Error   error
	}
)
