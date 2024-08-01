package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
)

type (
	TxActor interface {
		tx.Identifier

		// Execute executes query.
		//
		// Execute used by default:
		// - DefaultTxControl
		// - flag WithKeepInCache(true) if params is not empty.
		Execute(ctx context.Context, query string, opts ...options.TxExecuteOption) (r Result, err error)

		// ReadRow is a helper which read only one row from first result set in result
		//
		// ReadRow returns error if result contains more than one result set or more than one row
		//
		// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
		ReadRow(ctx context.Context, query string, opts ...options.TxExecuteOption) (Row, error)

		// ReadResultSet is a helper which read all rows from first result set in result
		//
		// ReadRow returns error if result contains more than one result set
		//
		// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
		ReadResultSet(ctx context.Context, query string, opts ...options.TxExecuteOption) (ResultSet, error)
	}
	Transaction interface {
		TxActor

		CommitTx(ctx context.Context) (err error)
		Rollback(ctx context.Context) (err error)
	}
	TransactionControl  = internal.Control
	TransactionSettings = internal.Settings
	TransactionOption   = internal.Option
)

// BeginTx returns selector transaction control option
func BeginTx(opts ...TransactionOption) internal.ControlOption {
	return internal.BeginTx(opts...)
}

func WithTx(t tx.Identifier) internal.ControlOption {
	return internal.WithTx(t)
}

func WithTxID(txID string) internal.ControlOption {
	return internal.WithTxID(txID)
}

// CommitTx returns commit transaction control option
func CommitTx() internal.ControlOption {
	return internal.CommitTx()
}

// TxControl makes transaction control from given options
func TxControl(opts ...internal.ControlOption) *TransactionControl {
	return internal.NewControl(opts...)
}

func NoTx() *TransactionControl {
	return nil
}

// DefaultTxControl returns default transaction control for use default tx control on server-side
func DefaultTxControl() *TransactionControl {
	return NoTx()
}

// SerializableReadWriteTxControl returns transaction control with serializable read-write isolation mode
func SerializableReadWriteTxControl(opts ...internal.ControlOption) *TransactionControl {
	return internal.SerializableReadWriteTxControl(opts...)
}

// OnlineReadOnlyTxControl returns online read-only transaction control
func OnlineReadOnlyTxControl(opts ...internal.OnlineReadOnlyOption) *TransactionControl {
	return TxControl(
		BeginTx(WithOnlineReadOnly(opts...)),
		CommitTx(), // open transactions not supported for OnlineReadOnly
	)
}

// StaleReadOnlyTxControl returns stale read-only transaction control
func StaleReadOnlyTxControl() *TransactionControl {
	return TxControl(
		BeginTx(WithStaleReadOnly()),
		CommitTx(), // open transactions not supported for StaleReadOnly
	)
}

// SnapshotReadOnlyTxControl returns snapshot read-only transaction control
func SnapshotReadOnlyTxControl() *TransactionControl {
	return TxControl(
		BeginTx(WithSnapshotReadOnly()),
		CommitTx(), // open transactions not supported for StaleReadOnly
	)
}

// TxSettings returns transaction settings
func TxSettings(opts ...internal.Option) TransactionSettings {
	return opts
}

func WithDefaultTxMode() TransactionOption {
	return internal.WithDefaultTxMode()
}

func WithSerializableReadWrite() TransactionOption {
	return internal.WithSerializableReadWrite()
}

func WithSnapshotReadOnly() TransactionOption {
	return internal.WithSnapshotReadOnly()
}

func WithStaleReadOnly() TransactionOption {
	return internal.WithStaleReadOnly()
}

func WithInconsistentReads() internal.OnlineReadOnlyOption {
	return internal.WithInconsistentReads()
}

func WithOnlineReadOnly(opts ...internal.OnlineReadOnlyOption) TransactionOption {
	return internal.WithOnlineReadOnly(opts...)
}
