package table

import "context"

// ClientTrace contains options for tracing table client activity.
type ClientTrace struct {
	CreateSessionStart func(CreateSessionStartInfo)
	CreateSessionDone  func(CreateSessionDoneInfo)

	KeepAliveStart func(KeepAliveStartInfo)
	KeepAliveDone  func(KeepAliveDoneInfo)

	DeleteSessionStart func(DeleteSessionStartInfo)
	DeleteSessionDone  func(DeleteSessionDoneInfo)

	PrepareDataQueryStart func(PrepareDataQueryStartInfo)
	PrepareDataQueryDone  func(PrepareDataQueryDoneInfo)

	ExecuteDataQueryStart func(ExecuteDataQueryStartInfo)
	ExecuteDataQueryDone  func(ExecuteDataQueryDoneInfo)

	BeginTransactionStart func(BeginTransactionStartInfo)
	BeginTransactionDone  func(BeginTransactionDoneInfo)

	CommitTransactionStart func(CommitTransactionStartInfo)
	CommitTransactionDone  func(CommitTransactionDoneInfo)

	RollbackTransactionStart func(RollbackTransactionStartInfo)
	RollbackTransactionDone  func(RollbackTransactionDoneInfo)
}

type (
	CreateSessionStartInfo struct {
		Context context.Context
	}
	CreateSessionDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	KeepAliveStartInfo struct {
		Context context.Context
		Session *Session
	}
	KeepAliveDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	DeleteSessionStartInfo struct {
		Context context.Context
		Session *Session
	}
	DeleteSessionDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	PrepareDataQueryStartInfo struct {
		Context context.Context
		Session *Session
		Query   string
	}
	PrepareDataQueryDoneInfo struct {
		Context context.Context
		Session *Session
		Query   string
		Result  *DataQuery
		Cached  bool
		Error   error
	}
	ExecuteDataQueryStartInfo struct {
		Context    context.Context
		Session    *Session
		TxID       string
		Query      *DataQuery
		Parameters *QueryParameters
	}
	ExecuteDataQueryDoneInfo struct {
		Context    context.Context
		Session    *Session
		TxID       string
		Query      *DataQuery
		Parameters *QueryParameters
		Result     *Result
		Error      error
	}
	BeginTransactionStartInfo struct {
		Context context.Context
		Session *Session
	}
	BeginTransactionDoneInfo struct {
		Context context.Context
		Session *Session
		TxID    string
		Error   error
	}
	CommitTransactionStartInfo struct {
		Context context.Context
		Session *Session
		TxID    string
	}
	CommitTransactionDoneInfo struct {
		Context context.Context
		Session *Session
		TxID    string
		Error   error
	}
	RollbackTransactionStartInfo struct {
		Context context.Context
		Session *Session
		TxID    string
	}
	RollbackTransactionDoneInfo struct {
		Context context.Context
		Session *Session
		TxID    string
		Error   error
	}
)

type clientTraceContextKey struct{}

func WithClientTrace(ctx context.Context, trace ClientTrace) context.Context {
	return context.WithValue(ctx,
		clientTraceContextKey{},
		composeClientTrace(
			ContextClientTrace(ctx), trace,
		),
	)
}

func ContextClientTrace(ctx context.Context) ClientTrace {
	trace, _ := ctx.Value(clientTraceContextKey{}).(ClientTrace)
	return trace
}

func composeClientTrace(a, b ClientTrace) (c ClientTrace) {
	switch {
	case a.CreateSessionStart == nil:
		c.CreateSessionStart = b.CreateSessionStart
	case b.CreateSessionStart == nil:
		c.CreateSessionStart = a.CreateSessionStart
	default:
		c.CreateSessionStart = func(info CreateSessionStartInfo) {
			a.CreateSessionStart(info)
			b.CreateSessionStart(info)
		}
	}
	switch {
	case a.CreateSessionDone == nil:
		c.CreateSessionDone = b.CreateSessionDone
	case b.CreateSessionDone == nil:
		c.CreateSessionDone = a.CreateSessionDone
	default:
		c.CreateSessionDone = func(info CreateSessionDoneInfo) {
			a.CreateSessionDone(info)
			b.CreateSessionDone(info)
		}
	}
	switch {
	case a.KeepAliveStart == nil:
		c.KeepAliveStart = b.KeepAliveStart
	case b.KeepAliveStart == nil:
		c.KeepAliveStart = a.KeepAliveStart
	default:
		c.KeepAliveStart = func(info KeepAliveStartInfo) {
			a.KeepAliveStart(info)
			b.KeepAliveStart(info)
		}
	}
	switch {
	case a.KeepAliveDone == nil:
		c.KeepAliveDone = b.KeepAliveDone
	case b.KeepAliveDone == nil:
		c.KeepAliveDone = a.KeepAliveDone
	default:
		c.KeepAliveDone = func(info KeepAliveDoneInfo) {
			a.KeepAliveDone(info)
			b.KeepAliveDone(info)
		}
	}
	switch {
	case a.DeleteSessionStart == nil:
		c.DeleteSessionStart = b.DeleteSessionStart
	case b.DeleteSessionStart == nil:
		c.DeleteSessionStart = a.DeleteSessionStart
	default:
		c.DeleteSessionStart = func(info DeleteSessionStartInfo) {
			a.DeleteSessionStart(info)
			b.DeleteSessionStart(info)
		}
	}
	switch {
	case a.DeleteSessionDone == nil:
		c.DeleteSessionDone = b.DeleteSessionDone
	case b.DeleteSessionDone == nil:
		c.DeleteSessionDone = a.DeleteSessionDone
	default:
		c.DeleteSessionDone = func(info DeleteSessionDoneInfo) {
			a.DeleteSessionDone(info)
			b.DeleteSessionDone(info)
		}
	}
	switch {
	case a.PrepareDataQueryStart == nil:
		c.PrepareDataQueryStart = b.PrepareDataQueryStart
	case b.PrepareDataQueryStart == nil:
		c.PrepareDataQueryStart = a.PrepareDataQueryStart
	default:
		c.PrepareDataQueryStart = func(info PrepareDataQueryStartInfo) {
			a.PrepareDataQueryStart(info)
			b.PrepareDataQueryStart(info)
		}
	}
	switch {
	case a.PrepareDataQueryDone == nil:
		c.PrepareDataQueryDone = b.PrepareDataQueryDone
	case b.PrepareDataQueryDone == nil:
		c.PrepareDataQueryDone = a.PrepareDataQueryDone
	default:
		c.PrepareDataQueryDone = func(info PrepareDataQueryDoneInfo) {
			a.PrepareDataQueryDone(info)
			b.PrepareDataQueryDone(info)
		}
	}
	switch {
	case a.ExecuteDataQueryStart == nil:
		c.ExecuteDataQueryStart = b.ExecuteDataQueryStart
	case b.ExecuteDataQueryStart == nil:
		c.ExecuteDataQueryStart = a.ExecuteDataQueryStart
	default:
		c.ExecuteDataQueryStart = func(info ExecuteDataQueryStartInfo) {
			a.ExecuteDataQueryStart(info)
			b.ExecuteDataQueryStart(info)
		}
	}
	switch {
	case a.ExecuteDataQueryDone == nil:
		c.ExecuteDataQueryDone = b.ExecuteDataQueryDone
	case b.ExecuteDataQueryDone == nil:
		c.ExecuteDataQueryDone = a.ExecuteDataQueryDone
	default:
		c.ExecuteDataQueryDone = func(info ExecuteDataQueryDoneInfo) {
			a.ExecuteDataQueryDone(info)
			b.ExecuteDataQueryDone(info)
		}
	}
	switch {
	case a.BeginTransactionStart == nil:
		c.BeginTransactionStart = b.BeginTransactionStart
	case b.BeginTransactionStart == nil:
		c.BeginTransactionStart = a.BeginTransactionStart
	default:
		c.BeginTransactionStart = func(info BeginTransactionStartInfo) {
			a.BeginTransactionStart(info)
			b.BeginTransactionStart(info)
		}
	}
	switch {
	case a.BeginTransactionDone == nil:
		c.BeginTransactionDone = b.BeginTransactionDone
	case b.BeginTransactionDone == nil:
		c.BeginTransactionDone = a.BeginTransactionDone
	default:
		c.BeginTransactionDone = func(info BeginTransactionDoneInfo) {
			a.BeginTransactionDone(info)
			b.BeginTransactionDone(info)
		}
	}
	switch {
	case a.CommitTransactionStart == nil:
		c.CommitTransactionStart = b.CommitTransactionStart
	case b.CommitTransactionStart == nil:
		c.CommitTransactionStart = a.CommitTransactionStart
	default:
		c.CommitTransactionStart = func(info CommitTransactionStartInfo) {
			a.CommitTransactionStart(info)
			b.CommitTransactionStart(info)
		}
	}
	switch {
	case a.CommitTransactionDone == nil:
		c.CommitTransactionDone = b.CommitTransactionDone
	case b.CommitTransactionDone == nil:
		c.CommitTransactionDone = a.CommitTransactionDone
	default:
		c.CommitTransactionDone = func(info CommitTransactionDoneInfo) {
			a.CommitTransactionDone(info)
			b.CommitTransactionDone(info)
		}
	}
	switch {
	case a.RollbackTransactionStart == nil:
		c.RollbackTransactionStart = b.RollbackTransactionStart
	case b.RollbackTransactionStart == nil:
		c.RollbackTransactionStart = a.RollbackTransactionStart
	default:
		c.RollbackTransactionStart = func(info RollbackTransactionStartInfo) {
			a.RollbackTransactionStart(info)
			b.RollbackTransactionStart(info)
		}
	}
	switch {
	case a.RollbackTransactionDone == nil:
		c.RollbackTransactionDone = b.RollbackTransactionDone
	case b.RollbackTransactionDone == nil:
		c.RollbackTransactionDone = a.RollbackTransactionDone
	default:
		c.RollbackTransactionDone = func(info RollbackTransactionDoneInfo) {
			a.RollbackTransactionDone(info)
			b.RollbackTransactionDone(info)
		}
	}
	return
}

// SessionPoolTrace contains options for tracing session pool activity.
type SessionPoolTrace struct {
	GetStart   func(SessionPoolGetStartInfo)
	GetDone    func(SessionPoolGetDoneInfo)
	WaitStart  func(SessionPoolWaitStartInfo)
	WaitDone   func(SessionPoolWaitDoneInfo)
	TakeStart  func(SessionPoolTakeStartInfo)
	TakeDone   func(SessionPoolTakeDoneInfo)
	PutStart   func(SessionPoolPutStartInfo)
	PutDone    func(SessionPoolPutDoneInfo)
	CloseStart func(SessionPoolCloseStartInfo)
	CloseDone  func(SessionPoolCloseDoneInfo)
}

type (
	SessionPoolGetStartInfo struct {
		Context context.Context
	}
	SessionPoolGetDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	SessionPoolWaitStartInfo struct {
		Context context.Context
	}
	SessionPoolWaitDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	SessionPoolTakeStartInfo struct {
		Context context.Context
		Session *Session
	}
	SessionPoolTakeDoneInfo struct {
		Context context.Context
		Session *Session
		Took    bool
	}
	SessionPoolPutStartInfo struct {
		Context context.Context
		Session *Session
	}
	SessionPoolPutDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	SessionPoolCloseStartInfo struct {
		Context context.Context
	}
	SessionPoolCloseDoneInfo struct {
		Context context.Context
		Error   error
	}
)

type sessionPoolTraceContextKey struct{}

func WithSessionPoolTrace(ctx context.Context, trace SessionPoolTrace) context.Context {
	return context.WithValue(ctx,
		sessionPoolTraceContextKey{},
		composeSessionPoolTrace(
			ContextSessionPoolTrace(ctx), trace,
		),
	)
}

func ContextSessionPoolTrace(ctx context.Context) SessionPoolTrace {
	trace, _ := ctx.Value(sessionPoolTraceContextKey{}).(SessionPoolTrace)
	return trace
}

func composeSessionPoolTrace(a, b SessionPoolTrace) (c SessionPoolTrace) {
	switch {
	case a.GetStart == nil:
		c.GetStart = b.GetStart
	case b.GetStart == nil:
		c.GetStart = a.GetStart
	default:
		c.GetStart = func(info SessionPoolGetStartInfo) {
			a.GetStart(info)
			b.GetStart(info)
		}
	}
	switch {
	case a.GetDone == nil:
		c.GetDone = b.GetDone
	case b.GetDone == nil:
		c.GetDone = a.GetDone
	default:
		c.GetDone = func(info SessionPoolGetDoneInfo) {
			a.GetDone(info)
			b.GetDone(info)
		}
	}
	switch {
	case a.WaitStart == nil:
		c.WaitStart = b.WaitStart
	case b.WaitStart == nil:
		c.WaitStart = a.WaitStart
	default:
		c.WaitStart = func(info SessionPoolWaitStartInfo) {
			a.WaitStart(info)
			b.WaitStart(info)
		}
	}
	switch {
	case a.WaitDone == nil:
		c.WaitDone = b.WaitDone
	case b.WaitDone == nil:
		c.WaitDone = a.WaitDone
	default:
		c.WaitDone = func(info SessionPoolWaitDoneInfo) {
			a.WaitDone(info)
			b.WaitDone(info)
		}
	}
	switch {
	case a.TakeStart == nil:
		c.TakeStart = b.TakeStart
	case b.TakeStart == nil:
		c.TakeStart = a.TakeStart
	default:
		c.TakeStart = func(info SessionPoolTakeStartInfo) {
			a.TakeStart(info)
			b.TakeStart(info)
		}
	}
	switch {
	case a.TakeDone == nil:
		c.TakeDone = b.TakeDone
	case b.TakeDone == nil:
		c.TakeDone = a.TakeDone
	default:
		c.TakeDone = func(info SessionPoolTakeDoneInfo) {
			a.TakeDone(info)
			b.TakeDone(info)
		}
	}
	switch {
	case a.PutStart == nil:
		c.PutStart = b.PutStart
	case b.PutStart == nil:
		c.PutStart = a.PutStart
	default:
		c.PutStart = func(info SessionPoolPutStartInfo) {
			a.PutStart(info)
			b.PutStart(info)
		}
	}
	switch {
	case a.PutDone == nil:
		c.PutDone = b.PutDone
	case b.PutDone == nil:
		c.PutDone = a.PutDone
	default:
		c.PutDone = func(info SessionPoolPutDoneInfo) {
			a.PutDone(info)
			b.PutDone(info)
		}
	}
	switch {
	case a.CloseStart == nil:
		c.CloseStart = b.CloseStart
	case b.CloseStart == nil:
		c.CloseStart = a.CloseStart
	default:
		c.CloseStart = func(info SessionPoolCloseStartInfo) {
			a.CloseStart(info)
			b.CloseStart(info)
		}
	}
	switch {
	case a.CloseDone == nil:
		c.CloseDone = b.CloseDone
	case b.CloseDone == nil:
		c.CloseDone = a.CloseDone
	default:
		c.CloseDone = func(info SessionPoolCloseDoneInfo) {
			a.CloseDone(info)
			b.CloseDone(info)
		}
	}
	return
}
