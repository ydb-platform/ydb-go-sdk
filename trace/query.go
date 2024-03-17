package trace

import (
	"context"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	querySessionInfo interface {
		ID() string
		NodeID() int64
		Status() string
	}

	// Query specified trace of retry call activity.
	// gtrace:gen
	Query struct {
		OnNew   func(QueryNewStartInfo) func(info QueryNewDoneInfo)
		OnClose func(QueryCloseStartInfo) func(info QueryCloseDoneInfo)

		OnPoolNew     func(QueryPoolNewStartInfo) func(QueryPoolNewDoneInfo)
		OnPoolClose   func(QueryPoolCloseStartInfo) func(QueryPoolCloseDoneInfo)
		OnPoolProduce func(QueryPoolProduceStartInfo) func(QueryPoolProduceDoneInfo)
		OnPoolTry     func(QueryPoolTryStartInfo) func(QueryPoolTryDoneInfo)
		OnPoolWith    func(QueryPoolWithStartInfo) func(QueryPoolWithDoneInfo)
		OnPoolPut     func(QueryPoolPutStartInfo) func(QueryPoolPutDoneInfo)
		OnPoolGet     func(QueryPoolGetStartInfo) func(QueryPoolGetDoneInfo)
		OnPoolSpawn   func(QueryPoolSpawnStartInfo) func(QueryPoolSpawnDoneInfo)
		OnPoolWant    func(QueryPoolWantStartInfo) func(QueryPoolWantDoneInfo)

		OnDo   func(QueryDoStartInfo) func(QueryDoDoneInfo)
		OnDoTx func(QueryDoTxStartInfo) func(QueryDoTxDoneInfo)

		OnSessionCreate  func(QuerySessionCreateStartInfo) func(info QuerySessionCreateDoneInfo)
		OnSessionAttach  func(QuerySessionAttachStartInfo) func(info QuerySessionAttachDoneInfo)
		OnSessionDelete  func(QuerySessionDeleteStartInfo) func(info QuerySessionDeleteDoneInfo)
		OnSessionExecute func(QuerySessionExecuteStartInfo) func(info QuerySessionExecuteDoneInfo)
	}

	QueryDoStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryDoDoneInfo struct {
		Attempts int
		Error    error
	}
	QueryDoTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
	QuerySessionCreateStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QuerySessionCreateDoneInfo struct {
		Session querySessionInfo
		Error   error
	}
	QuerySessionExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Session querySessionInfo
		Query   string
	}
	QuerySessionExecuteDoneInfo struct {
		Error error
	}
	QuerySessionAttachStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session querySessionInfo
	}
	QuerySessionAttachDoneInfo struct {
		Error error
	}
	QuerySessionDeleteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session querySessionInfo
	}
	QuerySessionDeleteDoneInfo struct {
		Error error
	}
	QueryNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryNewDoneInfo struct {
		Error error
	}
	QueryCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryCloseDoneInfo struct {
		Error error
	}
	QueryPoolNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		// input settings
		MinSize        int
		MaxSize        int
		ProducersCount int
	}
	QueryPoolNewDoneInfo struct {
		Error error

		// actual settings
		MinSize        int
		MaxSize        int
		ProducersCount int
	}
	QueryPoolCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolCloseDoneInfo struct {
		Error error
	}
	QueryPoolProduceStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Concurrency int
	}
	QueryPoolProduceDoneInfo struct{}
	QueryPoolTryStartInfo    struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolTryDoneInfo struct {
		Error error
	}
	QueryPoolWithStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolWithDoneInfo struct {
		Error error

		Attempts int
	}
	QueryPoolPutStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolPutDoneInfo struct {
		Error error
	}
	QueryPoolGetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolGetDoneInfo struct {
		Error error
	}
	QueryPoolSpawnStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolSpawnDoneInfo struct {
		Error error
	}
	QueryPoolWantStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolWantDoneInfo struct {
		Error error
	}
)
