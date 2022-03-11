package table

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// nolint:gofumpt
// nolint:nolintlint
var (
	// ErrAlreadyCommited returns if transaction Commit called twice
	ErrAlreadyCommited = fmt.Errorf("already committed")
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
	_, r, err = tx.s.Execute(ctx, tx.txc(), query, params, opts...)
	return
}

// ExecuteStatement executes prepared statement stmt within transaction tx.
func (tx *transaction) ExecuteStatement(
	ctx context.Context,
	stmt table.Statement, params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (r result.Result, err error) {
	_, r, err = stmt.Execute(ctx, tx.txc(), params, opts...)
	return
}

// CommitTx commits specified active transaction.
func (tx *transaction) CommitTx(
	ctx context.Context,
	opts ...options.CommitTransactionOption,
) (r result.Result, err error) {
	if tx.committed {
		return nil, errors.Error(ErrAlreadyCommited)
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

	t := tx.s.trailer()
	defer t.processHints()

	response, err = tx.s.tableService.CommitTransaction(
		cluster.WithEndpoint(ctx, tx.s),
		request,
		t.Trailer(),
	)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(
		response.GetOperation().GetResult().GetValue(),
		result,
	)
	if err != nil {
		return nil, err
	}
	return scanner.NewUnary(
		nil,
		result.GetQueryStats(),
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

	t := tx.s.trailer()
	defer t.processHints()

	_, err = tx.s.tableService.RollbackTransaction(
		cluster.WithEndpoint(ctx, tx.s),
		&Ydb_Table.RollbackTransactionRequest{
			SessionId: tx.s.id,
			TxId:      tx.id,
			OperationParams: operation.Params(
				tx.s.config.OperationTimeout(),
				tx.s.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
		t.Trailer(),
	)
	return err
}

func (tx *transaction) txc() *table.TransactionControl {
	if tx.c == nil {
		tx.c = table.TxControl(table.WithTx(tx))
	}
	return tx.c
}
