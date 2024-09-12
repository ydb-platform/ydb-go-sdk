package query

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/session"
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

const (
	LazyTxID = "LAZY_TX"
)

type (
	Transaction struct {
		baseTx.Identifier

		s          *Session
		txSettings query.TransactionSettings

		completed bool

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
			SessionId:  s.ID(),
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

	tx.Identifier, err = begin(ctx, tx.s.client, tx.s, tx.txSettings)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) QueryResultSet(
	ctx context.Context, q string, opts ...options.Execute,
) (rs result.ClosableResultSet, finalErr error) {
	onDone := trace.QueryOnTxQueryResultSet(tx.s.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).QueryResultSet"), tx, q)
	defer func() {
		onDone(finalErr)
	}()

	if tx.completed {
		return nil, xerrors.WithStackTrace(errExecuteOnCompletedTx)
	}

	settings, err := tx.executeSettings(opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	resultOpts := []resultOption{
		withTrace(tx.s.trace),
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
	txID, r, err := execute(ctx, tx.s.ID(), tx.s.client, q, settings, resultOpts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if settings.TxControl().Commit {
		if txID != nil && tx.Identifier != nil {
			return nil, xerrors.WithStackTrace(errUnexpectedTxIDOnCommitFlag)
		}
		tx.completed = true
	} else if tx.Identifier == nil {
		if txID == nil {
			return nil, xerrors.WithStackTrace(errExpectedTxID)
		}
		tx.Identifier = txID
	}

	rs, err = readResultSet(ctx, r)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return rs, nil
}

func (tx *Transaction) QueryRow(
	ctx context.Context, q string, opts ...options.Execute,
) (row query.Row, finalErr error) {
	onDone := trace.QueryOnTxQueryRow(tx.s.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).QueryRow"), tx, q)
	defer func() {
		onDone(finalErr)
	}()

	settings := options.ExecuteSettings(
		append(
			[]options.Execute{options.WithTxControl(tx.txControl())},
			opts...,
		)...,
	)

	resultOpts := []resultOption{
		withTrace(tx.s.trace),
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
	txID, r, err := execute(ctx, tx.s.ID(), tx.s.client, q, settings, resultOpts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if tx.Identifier == nil {
		tx.Identifier = txID
	}

	row, err = readRow(ctx, r)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func (tx *Transaction) SessionID() string {
	return tx.s.ID()
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
		return LazyTxID
	}

	return tx.Identifier.ID()
}

func (tx *Transaction) Exec(ctx context.Context, q string, opts ...options.Execute) (
	finalErr error,
) {
	onDone := trace.QueryOnTxExec(tx.s.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).Exec"), tx.s, tx, q)
	defer func() {
		onDone(finalErr)
	}()

	if tx.completed {
		return xerrors.WithStackTrace(errExecuteOnCompletedTx)
	}

	settings, err := tx.executeSettings(opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	resultOpts := []resultOption{
		withTrace(tx.s.trace),
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

	txID, r, err := execute(ctx, tx.s.ID(), tx.s.client, q, settings, resultOpts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	if settings.TxControl().Commit {
		if txID != nil && tx.Identifier != nil {
			return xerrors.WithStackTrace(errUnexpectedTxIDOnCommitFlag)
		}
		tx.completed = true
	} else if tx.Identifier == nil {
		if txID == nil {
			return xerrors.WithStackTrace(errExpectedTxID)
		}
		tx.Identifier = txID
	}

	err = readAll(ctx, r)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *Transaction) executeSettings(opts ...options.Execute) (_ executeSettings, _ error) {
	for _, opt := range opts {
		if opt == nil {
			return nil, xerrors.WithStackTrace(errExpectedTxID)
		}
		if _, has := opt.(options.ExecuteNoTx); has {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%T: %w", opt, ErrOptionNotForTxExecute),
			)
		}
	}

	if tx.Identifier != nil {
		return options.ExecuteSettings(append([]options.Execute{
			options.WithTxControl(
				queryTx.NewControl(
					queryTx.WithTxID(tx.Identifier.ID()),
				),
			),
		}, opts...)...), nil
	}

	return options.ExecuteSettings(append([]options.Execute{
		options.WithTxControl(
			queryTx.NewControl(
				queryTx.BeginTx(tx.txSettings...),
			),
		),
	}, opts...)...), nil
}

func (tx *Transaction) Query(ctx context.Context, q string, opts ...options.Execute) (
	_ query.Result, finalErr error,
) {
	onDone := trace.QueryOnTxQuery(tx.s.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).Query"), tx.s, tx, q)
	defer func() {
		onDone(finalErr)
	}()

	if tx.completed {
		return nil, xerrors.WithStackTrace(errExecuteOnCompletedTx)
	}

	settings, err := tx.executeSettings(opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	resultOpts := []resultOption{
		withTrace(tx.s.trace),
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
	txID, r, err := execute(ctx, tx.s.ID(), tx.s.client, q, settings, resultOpts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if settings.TxControl().Commit {
		if txID != nil && tx.Identifier != nil {
			return nil, xerrors.WithStackTrace(errUnexpectedTxIDOnCommitFlag)
		}
		tx.completed = true

		return r, nil
	} else if tx.Identifier == nil {
		if txID == nil {
			return nil, xerrors.WithStackTrace(errExpectedTxID)
		}
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

func (tx *Transaction) CommitTx(ctx context.Context) (finalErr error) {
	if tx.Identifier == nil {
		return nil
	}

	if tx.completed {
		return nil
	}

	defer func() {
		tx.notifyOnCompleted(finalErr)
		tx.completed = true
	}()

	err := commitTx(ctx, tx.s.client, tx.s.ID(), tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.SetStatus(session.StatusClosed)
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

func (tx *Transaction) Rollback(ctx context.Context) (finalErr error) {
	if tx.Identifier == nil {
		return nil
	}

	if tx.completed {
		return nil
	}

	tx.completed = true

	tx.notifyOnCompleted(ErrTransactionRollingBack)

	err := rollback(ctx, tx.s.client, tx.s.ID(), tx.ID())
	if err != nil {
		if xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION) {
			tx.s.SetStatus(session.StatusClosed)
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
