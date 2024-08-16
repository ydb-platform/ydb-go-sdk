package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	queryTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ query.Transaction  = (*Transaction)(nil)
	_ baseTx.Transaction = (*Transaction)(nil)
)

type (
	Transaction struct {
		baseTx.Identifier

		s          *Session
		txSettings query.TransactionSettings

		onCompleted xsync.Set[*baseTx.OnTransactionCompletedFunc]
	}
)

func begin(
	ctx context.Context,
	client Ydb_Query_V1.QueryServiceClient,
	s *Session,
	txSettings query.TransactionSettings,
) (baseTx.Identifier, error) {
	a := allocator.New()
	defer a.Free()
	response, err := client.BeginTransaction(ctx,
		&Ydb_Query.BeginTransactionRequest{
			SessionId:  s.id,
			TxSettings: txSettings.ToYDB(a),
		},
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return baseTx.NewID(response.GetTxMeta().GetId()), nil
}

func (tx *Transaction) UnLazy(ctx context.Context) (err error) {
	if tx.Identifier != nil {
		return nil
	}

	tx.Identifier, err = begin(ctx, tx.s.queryServiceClient, tx.s, tx.txSettings)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) QueryResultSet(
	ctx context.Context, q string, opts ...options.Execute,
) (rs query.ResultSet, finalErr error) {
	onDone := trace.QueryOnTxQueryResultSet(tx.s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).QueryResultSet"), tx, q)
	defer func() {
		onDone(finalErr)
	}()

	settings := executeSettings(tx, opts...)

	resultOpts := []resultOption{
		withTrace(tx.s.cfg.Trace()),
	}
	if settings.TxControl().Commit {
		// notification about complete transaction must be sended for any error or for successfully read all result if
		// it was execution with commit flag
		resultOpts = append(resultOpts,
			onNextPartErr(func(err error) {
				tx.notifyOnCompleted(xerrors.HideEOF(err))
			}),
		)
	}
	txID, r, err := execute(ctx, tx.s.id, tx.s.queryServiceClient, q, settings, resultOpts...)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return nil, xerrors.WithStackTrace(err)
	}

	if tx.Identifier == nil {
		tx.Identifier = txID
	}

	rs, err = readResultSet(ctx, r)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return nil, xerrors.WithStackTrace(err)
	}

	return rs, nil
}

func (tx *Transaction) QueryRow(
	ctx context.Context, q string, opts ...options.Execute,
) (row query.Row, finalErr error) {
	onDone := trace.QueryOnTxQueryRow(tx.s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).QueryRow"), tx, q)
	defer func() {
		onDone(finalErr)
	}()

	settings := executeSettings(tx, opts...)

	resultOpts := []resultOption{
		withTrace(tx.s.cfg.Trace()),
	}
	if settings.TxControl().Commit {
		// notification about complete transaction must be sended for any error or for successfully read all result if
		// it was execution with commit flag
		resultOpts = append(resultOpts,
			onNextPartErr(func(err error) {
				tx.notifyOnCompleted(xerrors.HideEOF(err))
			}),
		)
	}
	txID, r, err := execute(ctx, tx.s.id, tx.s.queryServiceClient, q, settings, resultOpts...)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return nil, xerrors.WithStackTrace(err)
	}

	if tx.Identifier == nil {
		tx.Identifier = txID
	}

	row, err = readRow(ctx, r)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func (tx *Transaction) SessionID() string {
	return tx.s.ID()
}

func executeSettings(
	tx interface {
		txControl() *queryTx.Control
	},
	opts ...options.Execute,
) executeConfig {
	return options.ExecuteSettings(
		append(
			[]options.Execute{options.WithTxControl(tx.txControl())},
			opts...,
		)...,
	)
}

func (tx *Transaction) txControl() *queryTx.Control {
	if tx.Identifier != nil {
		return queryTx.NewControl(queryTx.WithTxID(tx.Identifier.ID()))
	}

	return queryTx.NewControl(
		queryTx.BeginTx(tx.txSettings...),
	)
}

func (tx *Transaction) ID() string {
	if tx.Identifier == nil {
		return "LAZY_TX"
	}

	return tx.Identifier.ID()
}

func (tx *Transaction) Exec(ctx context.Context, q string, opts ...options.Execute) (
	finalErr error,
) {
	onDone := trace.QueryOnTxExec(tx.s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).Exec"), tx.s, tx, q)
	defer func() {
		onDone(finalErr)
	}()

	settings := executeSettings(tx, opts...)

	resultOpts := []resultOption{
		withTrace(tx.s.cfg.Trace()),
	}
	if settings.TxControl().Commit {
		// notification about complete transaction must be sended for any error or for successfully read all result if
		// it was execution with commit flag
		resultOpts = append(resultOpts,
			onNextPartErr(func(err error) {
				tx.notifyOnCompleted(xerrors.HideEOF(err))
			}),
		)
	}

	txID, r, err := execute(ctx, tx.s.id, tx.s.queryServiceClient, q, settings, resultOpts...)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	if tx.Identifier == nil {
		tx.Identifier = txID
	}

	err = readAll(ctx, r)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) Query(ctx context.Context, q string, opts ...options.Execute) (
	_ query.Result, finalErr error,
) {
	onDone := trace.QueryOnTxQuery(tx.s.cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).Query"), tx.s, tx, q)
	defer func() {
		onDone(finalErr)
	}()

	settings := executeSettings(tx, opts...)

	resultOpts := []resultOption{
		withTrace(tx.s.cfg.Trace()),
	}
	if settings.TxControl().Commit {
		// notification about complete transaction must be sended for any error or for successfully read all result if
		// it was execution with commit flag
		resultOpts = append(resultOpts,
			onNextPartErr(func(err error) {
				tx.notifyOnCompleted(xerrors.HideEOF(err))
			}),
		)
	}
	txID, r, err := execute(ctx, tx.s.id, tx.s.queryServiceClient, q, settings, resultOpts...)
	if err != nil {
		if xerrors.IsOperationError(err) {
			tx.s.setStatus(statusClosed)
		}

		return nil, xerrors.WithStackTrace(err)
	}

	if tx.Identifier == nil {
		tx.Identifier = txID
	}

	return r, nil
}

func commitTx(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID, txID string) error {
	_, err := client.CommitTransaction(ctx, &Ydb_Query.CommitTransactionRequest{
		SessionId: sessionID,
		TxId:      txID,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) CommitTx(ctx context.Context) (err error) {
	defer func() {
		tx.notifyOnCompleted(err)
	}()

	if tx.Identifier == nil {
		return nil
	}

	err = commitTx(ctx, tx.s.queryServiceClient, tx.s.id, tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func rollback(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID, txID string) error {
	_, err := client.RollbackTransaction(ctx, &Ydb_Query.RollbackTransactionRequest{
		SessionId: sessionID,
		TxId:      txID,
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) Rollback(ctx context.Context) error {
	if tx.Identifier == nil {
		return nil
	}

	tx.notifyOnCompleted(ErrTransactionRollingBack)

	err := rollback(ctx, tx.s.queryServiceClient, tx.s.id, tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.setStatus(statusClosed)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) OnCompleted(f baseTx.OnTransactionCompletedFunc) {
	tx.onCompleted.Add(&f)
}

func (tx *Transaction) notifyOnCompleted(err error) {
	tx.onCompleted.Range(func(f *baseTx.OnTransactionCompletedFunc) bool {
		(*f)(err)

		return tx.onCompleted.Remove(f)
	})
}
