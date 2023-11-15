package table

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errTxAlreadyCommitted = xerrors.Wrap(fmt.Errorf("transaction already committed"))
	errTxRollbackedEarly  = xerrors.Wrap(fmt.Errorf("transaction rollbacked early"))
)

type txState struct {
	rawVal xatomic.Uint32
}

func (s *txState) Load() txStateEnum {
	return txStateEnum(s.rawVal.Load())
}

func (s *txState) Store(val txStateEnum) {
	s.rawVal.Store(uint32(val))
}

type txStateEnum uint32

const (
	txStateInitialized txStateEnum = iota
	txStateCommitted
	txStateRollbacked
)

type transaction struct {
	id      string
	s       *session
	control *table.TransactionControl
	state   txState
}

func (tx *transaction) ID() string {
	return tx.id
}

// Execute executes query represented by text within transaction tx.
func (tx *transaction) Execute(
	ctx context.Context,
	query string, params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (r result.Result, err error) {
	onDone := trace.TableOnSessionTransactionExecute(
		tx.s.config.Trace(), &ctx,
		stack.FunctionID(""),
		tx.s, tx, queryFromText(query), params,
	)
	defer func() {
		onDone(r, err)
	}()

	switch tx.state.Load() {
	case txStateCommitted:
		return nil, xerrors.WithStackTrace(errTxAlreadyCommitted)
	case txStateRollbacked:
		return nil, xerrors.WithStackTrace(errTxRollbackedEarly)
	default:
		_, r, err = tx.s.Execute(ctx, tx.control, query, params, opts...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		if tx.control.Desc().CommitTx {
			tx.state.Store(txStateCommitted)
		}

		return r, nil
	}
}

// ExecuteStatement executes prepared statement stmt within transaction tx.
func (tx *transaction) ExecuteStatement(
	ctx context.Context,
	stmt table.Statement, params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (r result.Result, err error) {
	if params == nil {
		params = table.NewQueryParameters()
	}
	a := allocator.New()
	defer a.Free()

	onDone := trace.TableOnSessionTransactionExecuteStatement(
		tx.s.config.Trace(), &ctx,
		stack.FunctionID(""),
		tx.s, tx, stmt.(*statement).query, params,
	)
	defer func() {
		onDone(r, err)
	}()

	switch tx.state.Load() {
	case txStateCommitted:
		return nil, xerrors.WithStackTrace(errTxAlreadyCommitted)
	case txStateRollbacked:
		return nil, xerrors.WithStackTrace(errTxRollbackedEarly)
	default:
		_, r, err = stmt.Execute(ctx, tx.control, params, opts...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		if tx.control.Desc().CommitTx {
			tx.state.Store(txStateCommitted)
		}

		return r, nil
	}
}

// CommitTx commits specified active transaction.
func (tx *transaction) CommitTx(
	ctx context.Context,
	opts ...options.CommitTransactionOption,
) (r result.Result, err error) {
	onDone := trace.TableOnSessionTransactionCommit(
		tx.s.config.Trace(), &ctx,
		stack.FunctionID(""),
		tx.s, tx,
	)
	defer func() {
		onDone(err)
	}()

	switch tx.state.Load() {
	case txStateCommitted:
		return nil, xerrors.WithStackTrace(errTxAlreadyCommitted)
	case txStateRollbacked:
		return nil, xerrors.WithStackTrace(errTxRollbackedEarly)
	default:
		var (
			request = &Ydb_Table.CommitTransactionRequest{
				SessionId: tx.s.id,
				TxId:      tx.id,
				OperationParams: operation.Params(
					ctx,
					tx.s.config.OperationTimeout(),
					tx.s.config.OperationCancelAfter(),
					operation.ModeSync,
				),
			}
			response *Ydb_Table.CommitTransactionResponse
			result   = new(Ydb_Table.CommitTransactionResult)
		)

		for _, opt := range opts {
			if opt != nil {
				opt((*options.CommitTransactionDesc)(request))
			}
		}

		response, err = tx.s.tableService.CommitTransaction(ctx, request)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		err = response.GetOperation().GetResult().UnmarshalTo(result)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		tx.state.Store(txStateCommitted)

		return scanner.NewUnary(
			nil,
			result.GetQueryStats(),
			scanner.WithIgnoreTruncated(tx.s.config.IgnoreTruncated()),
		), nil
	}
}

// Rollback performs a rollback of the specified active transaction.
func (tx *transaction) Rollback(ctx context.Context) (err error) {
	onDone := trace.TableOnSessionTransactionRollback(
		tx.s.config.Trace(), &ctx,
		stack.FunctionID(""),
		tx.s, tx,
	)
	defer func() {
		onDone(err)
	}()

	switch tx.state.Load() {
	case txStateCommitted:
		return nil // nop for committed tx
	case txStateRollbacked:
		return xerrors.WithStackTrace(errTxRollbackedEarly)
	default:
		_, err = tx.s.tableService.RollbackTransaction(ctx,
			&Ydb_Table.RollbackTransactionRequest{
				SessionId: tx.s.id,
				TxId:      tx.id,
				OperationParams: operation.Params(
					ctx,
					tx.s.config.OperationTimeout(),
					tx.s.config.OperationCancelAfter(),
					operation.ModeSync,
				),
			},
		)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		tx.state.Store(txStateRollbacked)

		return nil
	}
}
