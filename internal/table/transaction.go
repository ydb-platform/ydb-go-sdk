package table

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errTxInvalidatedWithCommit = xerrors.Wrap(fmt.Errorf("transaction invalidated from WithCommit() call"))
	errTxAlreadyCommitted      = xerrors.Wrap(fmt.Errorf("transaction already committed"))
	errTxRollbackedEarly       = xerrors.Wrap(fmt.Errorf("transaction rollbacked early"))
)

type txState int32

const (
	txStateInitialized = iota
	txStateInvalidatedWithCommit
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
	switch txState(atomic.LoadInt32((*int32)(&tx.state))) {
	case txStateInvalidatedWithCommit:
		return nil, xerrors.WithStackTrace(errTxInvalidatedWithCommit)
	case txStateCommitted:
		return nil, xerrors.WithStackTrace(errTxAlreadyCommitted)
	case txStateRollbacked:
		return nil, xerrors.WithStackTrace(errTxRollbackedEarly)
	default:
	}
	var (
		a          = allocator.New()
		q          = queryFromText(query)
		optsResult options.ExecuteDataQueryDesc
	)
	defer a.Free()

	if params == nil {
		params = table.NewQueryParameters()
	}

	for _, f := range opts {
		f(&optsResult, a)
	}

	onDone := trace.TableOnSessionTransactionExecute(
		tx.s.config.Trace(),
		&ctx,
		tx.s,
		tx,
		q,
		params,
		optsResult.QueryCachePolicy.GetKeepInCache(),
	)
	defer func() {
		onDone(r, err)
	}()
	_, r, err = tx.s.Execute(ctx, tx.control, query, params, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

// ExecuteStatement executes prepared statement stmt within transaction tx.
func (tx *transaction) ExecuteStatement(
	ctx context.Context,
	stmt table.Statement, params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (r result.Result, err error) {
	switch txState(atomic.LoadInt32((*int32)(&tx.state))) {
	case txStateInvalidatedWithCommit:
		return nil, xerrors.WithStackTrace(errTxInvalidatedWithCommit)
	case txStateCommitted:
		return nil, xerrors.WithStackTrace(errTxAlreadyCommitted)
	case txStateRollbacked:
		return nil, xerrors.WithStackTrace(errTxRollbackedEarly)
	default:
	}
	if params == nil {
		params = table.NewQueryParameters()
	}
	var (
		a          = allocator.New()
		optsResult options.ExecuteDataQueryDesc
	)
	defer a.Free()

	for _, f := range opts {
		f(&optsResult, a)
	}

	onDone := trace.TableOnSessionTransactionExecuteStatement(
		tx.s.config.Trace(),
		&ctx,
		tx.s,
		tx,
		params,
	)
	defer func() {
		onDone(r, err)
	}()

	_, r, err = stmt.Execute(ctx, tx.control, params, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (tx *transaction) WithCommit() table.TransactionActor {
	defer func() {
		atomic.StoreInt32((*int32)(&tx.state), txStateInvalidatedWithCommit)
	}()
	return &transaction{
		id:      tx.id,
		s:       tx.s,
		control: table.TxControl(table.WithTxID(tx.id), table.CommitTx()),
		state:   txState(atomic.LoadInt32((*int32)(&tx.state))),
	}
}

// CommitTx commits specified active transaction.
func (tx *transaction) CommitTx(
	ctx context.Context,
	opts ...options.CommitTransactionOption,
) (r result.Result, err error) {
	switch txState(atomic.LoadInt32((*int32)(&tx.state))) {
	case txStateInvalidatedWithCommit:
		return nil, xerrors.WithStackTrace(errTxInvalidatedWithCommit)
	case txStateCommitted:
		return nil, xerrors.WithStackTrace(errTxAlreadyCommitted)
	case txStateRollbacked:
		return nil, xerrors.WithStackTrace(errTxRollbackedEarly)
	default:
		defer func() {
			if err == nil {
				atomic.StoreInt32((*int32)(&tx.state), txStateCommitted)
			}
		}()
	}
	onDone := trace.TableOnSessionTransactionCommit(
		tx.s.config.Trace(),
		&ctx,
		tx.s,
		tx,
	)
	defer func() {
		onDone(err)
	}()
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
		opt((*options.CommitTransactionDesc)(request))
	}

	response, err = tx.s.tableService.CommitTransaction(ctx, request)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	err = response.GetOperation().GetResult().UnmarshalTo(result)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return scanner.NewUnary(
		nil,
		result.GetQueryStats(),
		scanner.WithIgnoreTruncated(tx.s.config.IgnoreTruncated()),
	), nil
}

// Rollback performs a rollback of the specified active transaction.
func (tx *transaction) Rollback(ctx context.Context) (err error) {
	switch txState(atomic.LoadInt32((*int32)(&tx.state))) {
	case txStateInvalidatedWithCommit:
		return xerrors.WithStackTrace(errTxInvalidatedWithCommit)
	case txStateCommitted:
		return xerrors.WithStackTrace(errTxAlreadyCommitted)
	case txStateRollbacked:
		return xerrors.WithStackTrace(errTxRollbackedEarly)
	default:
		defer func() {
			if err == nil {
				atomic.StoreInt32((*int32)(&tx.state), txStateRollbacked)
			}
		}()
	}
	onDone := trace.TableOnSessionTransactionRollback(
		tx.s.config.Trace(),
		&ctx,
		tx.s,
		tx,
	)
	defer func() {
		onDone(err)
	}()

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
	return xerrors.WithStackTrace(err)
}
