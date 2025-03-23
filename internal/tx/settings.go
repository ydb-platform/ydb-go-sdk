package tx

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

var (
	querySerializableReadWrite = &Ydb_Query.TransactionSettings_SerializableReadWrite{
		SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
	}
	queryStaleReadOnly = &Ydb_Query.TransactionSettings_StaleReadOnly{
		StaleReadOnly: &Ydb_Query.StaleModeSettings{},
	}
	querySnapshotReadOnly = &Ydb_Query.TransactionSettings_SnapshotReadOnly{
		SnapshotReadOnly: &Ydb_Query.SnapshotModeSettings{},
	}
	queryOnlineReadOnlyAllowInconsistentReads = &Ydb_Query.TransactionSettings_OnlineReadOnly{
		OnlineReadOnly: &Ydb_Query.OnlineModeSettings{AllowInconsistentReads: true},
	}
	queryOnlineReadOnlyForbidInconsistentReads = &Ydb_Query.TransactionSettings_OnlineReadOnly{
		OnlineReadOnly: &Ydb_Query.OnlineModeSettings{AllowInconsistentReads: false},
	}
	tableSerializableReadWrite = &Ydb_Table.TransactionSettings_SerializableReadWrite{
		SerializableReadWrite: &Ydb_Table.SerializableModeSettings{},
	}
	tableStaleReadOnly = &Ydb_Table.TransactionSettings_StaleReadOnly{
		StaleReadOnly: &Ydb_Table.StaleModeSettings{},
	}
	tableSnapshotReadOnly = &Ydb_Table.TransactionSettings_SnapshotReadOnly{
		SnapshotReadOnly: &Ydb_Table.SnapshotModeSettings{},
	}
	tableOnlineReadOnlyAllowInconsistentReads = &Ydb_Table.TransactionSettings_OnlineReadOnly{
		OnlineReadOnly: &Ydb_Table.OnlineModeSettings{AllowInconsistentReads: true},
	}
	tableOnlineReadOnlyForbidInconsistentReads = &Ydb_Table.TransactionSettings_OnlineReadOnly{
		OnlineReadOnly: &Ydb_Table.OnlineModeSettings{AllowInconsistentReads: false},
	}
)

// Transaction settings options
type (
	Option interface {
		ApplyQueryTxSettingsOption(a *allocator.Allocator, txSettings *Ydb_Query.TransactionSettings)
		ApplyTableTxSettingsOption(a *allocator.Allocator, txSettings *Ydb_Table.TransactionSettings)
	}
	Settings []Option
)

func (opts Settings) applyTableTxSelector(a *allocator.Allocator, txControl *Ydb_Table.TransactionControl) {
	beginTx := a.TableTransactionControlBeginTx()
	beginTx.BeginTx = a.TableTransactionSettings()
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyTableTxSettingsOption(a, beginTx.BeginTx)
		}
	}
	txControl.TxSelector = beginTx
}

func (opts Settings) applyQueryTxSelector(a *allocator.Allocator, txControl *Ydb_Query.TransactionControl) {
	beginTx := a.QueryTransactionControlBeginTx()
	beginTx.BeginTx = a.QueryTransactionSettings()
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyQueryTxSettingsOption(a, beginTx.BeginTx)
		}
	}
	txControl.TxSelector = beginTx
}

func (opts Settings) ToYDB(a *allocator.Allocator) *Ydb_Query.TransactionSettings {
	txSettings := a.QueryTransactionSettings()
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyQueryTxSettingsOption(a, txSettings)
		}
	}

	return txSettings
}

// NewSettings returns transaction settings
func NewSettings(opts ...Option) Settings {
	return opts
}

func WithDefaultTxMode() Option {
	return WithSerializableReadWrite()
}

var _ Option = serializableReadWriteTxSettingsOption{}

type serializableReadWriteTxSettingsOption struct{}

func (serializableReadWriteTxSettingsOption) ApplyTableTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Table.TransactionSettings,
) {
	settings.TxMode = tableSerializableReadWrite
}

func (serializableReadWriteTxSettingsOption) ApplyQueryTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	settings.TxMode = querySerializableReadWrite
}

func WithSerializableReadWrite() Option {
	return serializableReadWriteTxSettingsOption{}
}

var _ Option = snapshotReadOnlyTxSettingsOption{}

type snapshotReadOnlyTxSettingsOption struct{}

func (snapshotReadOnlyTxSettingsOption) ApplyTableTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Table.TransactionSettings,
) {
	settings.TxMode = tableSnapshotReadOnly
}

func (snapshotReadOnlyTxSettingsOption) ApplyQueryTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	settings.TxMode = querySnapshotReadOnly
}

func WithSnapshotReadOnly() Option {
	return snapshotReadOnlyTxSettingsOption{}
}

var _ Option = staleReadOnlySettingsOption{}

type staleReadOnlySettingsOption struct{}

func (staleReadOnlySettingsOption) ApplyTableTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Table.TransactionSettings,
) {
	settings.TxMode = tableStaleReadOnly
}

func (staleReadOnlySettingsOption) ApplyQueryTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	settings.TxMode = queryStaleReadOnly
}

func WithStaleReadOnly() Option {
	return staleReadOnlySettingsOption{}
}

type (
	onlineReadOnly       bool
	OnlineReadOnlyOption interface {
		applyTxOnlineReadOnlyOption(opt *onlineReadOnly)
	}
)

var _ OnlineReadOnlyOption = inconsistentReadsTxOnlineReadOnlyOption{}

type inconsistentReadsTxOnlineReadOnlyOption struct{}

func (i inconsistentReadsTxOnlineReadOnlyOption) applyTxOnlineReadOnlyOption(b *onlineReadOnly) {
	*b = true
}

func WithInconsistentReads() OnlineReadOnlyOption {
	return inconsistentReadsTxOnlineReadOnlyOption{}
}

var _ Option = onlineReadOnlySettingsOption{}

type onlineReadOnlySettingsOption []OnlineReadOnlyOption

func (opts onlineReadOnlySettingsOption) ApplyQueryTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	var ro onlineReadOnly
	for _, opt := range opts {
		if opt != nil {
			opt.applyTxOnlineReadOnlyOption(&ro)
		}
	}
	if ro {
		settings.TxMode = queryOnlineReadOnlyAllowInconsistentReads
	} else {
		settings.TxMode = queryOnlineReadOnlyForbidInconsistentReads
	}
}

func (opts onlineReadOnlySettingsOption) ApplyTableTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Table.TransactionSettings,
) {
	var ro onlineReadOnly
	for _, opt := range opts {
		if opt != nil {
			opt.applyTxOnlineReadOnlyOption(&ro)
		}
	}
	if ro {
		settings.TxMode = tableOnlineReadOnlyAllowInconsistentReads
	} else {
		settings.TxMode = tableOnlineReadOnlyForbidInconsistentReads
	}
}

func WithOnlineReadOnly(opts ...OnlineReadOnlyOption) onlineReadOnlySettingsOption {
	return opts
}
