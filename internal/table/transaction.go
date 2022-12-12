package table

import (
	"context"
	"fmt"

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

//nolint:gofumpt
//nolint:nolintlint
var (
	// errAlreadyCommited returns if transaction Commit called twice
	errAlreadyCommited = xerrors.Wrap(fmt.Errorf("already committed"))
)

type transaction struct {
	id string
	s  *session
	c  *table.TransactionControl

	committed bool
}

func (tx *transaction) ID() string {
	return tx.id
}

func (tx *transaction) IsNil() bool {
	return tx == nil
}

// Execute executes query represented by text within transaction tx.
func (tx *transaction) Execute(
	ctx context.Context,
	query string, params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (r result.Result, err error) {
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
	_, r, err = tx.s.Execute(ctx, tx.txc(), query, params, opts...)
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

	_, r, err = stmt.Execute(ctx, tx.txc(), params, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (tx *transaction) WithCommit() table.TransactionActor {
	tx.c = tx.c.With(table.CommitTx())
	return tx
}

// CommitTx commits specified active transaction.
func (tx *transaction) CommitTx(
	ctx context.Context,
	opts ...options.CommitTransactionOption,
) (r result.Result, err error) {
	if tx.committed {
		return nil, xerrors.WithStackTrace(errAlreadyCommited)
	}
	defer func() {
		if err == nil {
			tx.committed = true
		}
	}()
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
	if tx.committed {
		return nil
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

func (tx *transaction) txc() *table.TransactionControl {
	if tx.c == nil {
		tx.c = table.TxControl(table.WithTx(tx))
	}
	return tx.c
}
