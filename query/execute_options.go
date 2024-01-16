package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
)

type (
	executeOptions struct {
		txControl   *TransactionControl
		syntax      syntax
		params      *Parameters
		callOptions []grpc.CallOption
	}
	ExecuteOption interface {
		applyExecuteOption(o *executeOptions)
	}

	TxExecuteOptions struct {
		params *Parameters
		syntax syntax
	}

	TxExecuteOption interface {
		applyTxExecuteOption(o *TxExecuteOptions)
	}
)

func NewExecuteOptions(opts ...ExecuteOption) (executeOptions executeOptions) {
	executeOptions.syntax = Ydb_Query.Syntax_SYNTAX_YQL_V1
	executeOptions.txControl = DefaultTxControl()
	for _, opt := range opts {
		opt.applyExecuteOption(&executeOptions)
	}
	return executeOptions
}

func (opts executeOptions) TxControl() *TransactionControl {
	return opts.txControl
}

func (opts executeOptions) CallOptions() []grpc.CallOption {
	return opts.callOptions
}

type syntax = Ydb_Query.Syntax

func (opts executeOptions) Syntax() syntax {
	return opts.syntax
}

func (opts executeOptions) Params() queryParams {
	return opts.params.Params()
}

func NewTxExecuteOptions(opts ...TxExecuteOption) (txExecuteOptions TxExecuteOptions) {
	txExecuteOptions.syntax = Ydb_Query.Syntax_SYNTAX_YQL_V1
	for _, opt := range opts {
		opt.applyTxExecuteOption(&txExecuteOptions)
	}
	return txExecuteOptions
}

var _ ExecuteOption = (*Parameters)(nil)

func WithParameters(params ...Parameter) *Parameters {
	q := &Parameters{
		m: make(queryParams, len(params)),
	}
	q.Add(params...)
	return q
}
