package query

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

var (
	serializableReadWrite = &Ydb_Query.TransactionSettings_SerializableReadWrite{
		SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
	}
	staleReadOnly = &Ydb_Query.TransactionSettings_StaleReadOnly{
		StaleReadOnly: &Ydb_Query.StaleModeSettings{},
	}
	snapshotReadOnly = &Ydb_Query.TransactionSettings_SnapshotReadOnly{
		SnapshotReadOnly: &Ydb_Query.SnapshotModeSettings{},
	}
	onlineReadOnlyAllowInconsistentReads = &Ydb_Query.TransactionSettings_OnlineReadOnly{
		OnlineReadOnly: &Ydb_Query.OnlineModeSettings{AllowInconsistentReads: true},
	}
	onlineReadOnlyForbidInconsistentReads = &Ydb_Query.TransactionSettings_OnlineReadOnly{
		OnlineReadOnly: &Ydb_Query.OnlineModeSettings{AllowInconsistentReads: false},
	}
)

// Transaction settings options
type (
	txSettingsOption interface {
		applyTxSettingsOption(a *allocator.Allocator, txSettings *Ydb_Query.TransactionSettings)
	}
	TransactionSettings []txSettingsOption
)

func (opts TransactionSettings) applyTxSelector(a *allocator.Allocator, txControl *Ydb_Query.TransactionControl) {
	beginTx := a.QueryTransactionControlBeginTx()
	beginTx.BeginTx = a.QueryTransactionSettings()
	for _, opt := range opts {
		opt.applyTxSettingsOption(a, beginTx.BeginTx)
	}
	txControl.TxSelector = beginTx
}

func (opts TransactionSettings) ToYDB(a *allocator.Allocator) *Ydb_Query.TransactionSettings {
	txSettings := a.QueryTransactionSettings()
	for _, opt := range opts {
		opt.applyTxSettingsOption(a, txSettings)
	}

	return txSettings
}

// TxSettings returns transaction settings
func TxSettings(opts ...txSettingsOption) TransactionSettings {
	return opts
}

func WithDefaultTxMode() txSettingsOption {
	return WithSerializableReadWrite()
}

var _ txSettingsOption = serializableReadWriteTxSettingsOption{}

type serializableReadWriteTxSettingsOption struct{}

func (o serializableReadWriteTxSettingsOption) applyTxSettingsOption(
	a *allocator.Allocator, txSettings *Ydb_Query.TransactionSettings,
) {
	txSettings.TxMode = serializableReadWrite
}

func WithSerializableReadWrite() txSettingsOption {
	return serializableReadWriteTxSettingsOption{}
}

var _ txSettingsOption = snapshotReadOnlyTxSettingsOption{}

type snapshotReadOnlyTxSettingsOption struct{}

func (snapshotReadOnlyTxSettingsOption) applyTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	settings.TxMode = snapshotReadOnly
}

func WithSnapshotReadOnly() txSettingsOption {
	return snapshotReadOnlyTxSettingsOption{}
}

var _ txSettingsOption = staleReadOnlySettingsOption{}

type staleReadOnlySettingsOption struct{}

func (staleReadOnlySettingsOption) applyTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	settings.TxMode = staleReadOnly
}

func WithStaleReadOnly() txSettingsOption {
	return staleReadOnlySettingsOption{}
}

type (
	txOnlineReadOnly       bool
	TxOnlineReadOnlyOption interface {
		applyTxOnlineReadOnlyOption(opt *txOnlineReadOnly)
	}
)

var _ TxOnlineReadOnlyOption = inconsistentReadsTxOnlineReadOnlyOption{}

type inconsistentReadsTxOnlineReadOnlyOption struct{}

func (i inconsistentReadsTxOnlineReadOnlyOption) applyTxOnlineReadOnlyOption(b *txOnlineReadOnly) {
	*b = true
}

func WithInconsistentReads() TxOnlineReadOnlyOption {
	return inconsistentReadsTxOnlineReadOnlyOption{}
}

var _ txSettingsOption = onlineReadOnlySettingsOption{}

type onlineReadOnlySettingsOption []TxOnlineReadOnlyOption

func (opts onlineReadOnlySettingsOption) applyTxSettingsOption(
	a *allocator.Allocator, settings *Ydb_Query.TransactionSettings,
) {
	var ro txOnlineReadOnly
	for _, opt := range opts {
		if opt != nil {
			opt.applyTxOnlineReadOnlyOption(&ro)
		}
	}
	if ro {
		settings.TxMode = onlineReadOnlyAllowInconsistentReads
	} else {
		settings.TxMode = onlineReadOnlyForbidInconsistentReads
	}
}

func WithOnlineReadOnly(opts ...TxOnlineReadOnlyOption) onlineReadOnlySettingsOption {
	return opts
}
