package isolation

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var errUnsupportedReadOnlyMode = errors.New("unsupported read-only mode")

// ToYDB maps driver transaction options to ydb transaction Option or query transaction control.
// This caused by ydb logic that prevents start actual transaction with OnlineReadOnly mode and ReadCommitted
// and ReadUncommitted isolation levels should use tx_control in every query request.
// It returns error on unsupported options.
func ToYDB(opts driver.TxOptions) (txSettings *table.TransactionSettings, err error) {
	level := sql.IsolationLevel(opts.Isolation)
	if !opts.ReadOnly {
		switch level {
		case sql.LevelDefault,
			sql.LevelSerializable:
			return table.TxSettings(table.WithSerializableReadWrite()), nil
		default:
			return nil, xerrors.WithStackTrace(fmt.Errorf(
				"ydb: unsupported transaction options: isolation=%s read_only=%t",
				nameIsolationLevel(level), opts.ReadOnly,
			))
		}
	}
	return nil, xerrors.WithStackTrace(errUnsupportedReadOnlyMode)
}

func nameIsolationLevel(x sql.IsolationLevel) string {
	if int(x) < len(isolationLevelName) {
		return isolationLevelName[x]
	}
	return "unknown_isolation"
}

var isolationLevelName = [...]string{
	sql.LevelDefault:         "default",
	sql.LevelReadUncommitted: "read_uncommitted",
	sql.LevelReadCommitted:   "read_committed",
	sql.LevelWriteCommitted:  "write_committed",
	sql.LevelRepeatableRead:  "repeatable_read",
	sql.LevelSnapshot:        "snapshot",
	sql.LevelSerializable:    "serializable",
	sql.LevelLinearizable:    "linearizable",
}

// FromYDB maps table transaction settings to driver transaction options
func FromYDB(txSettings *table.TransactionSettings) (txOptions *sql.TxOptions, err error) {
	switch txMode := txSettings.Settings().TxMode.(type) {
	case *Ydb_Table.TransactionSettings_SerializableReadWrite:
		return &sql.TxOptions{
			Isolation: sql.LevelSerializable,
			ReadOnly:  false,
		}, nil
	case *Ydb_Table.TransactionSettings_OnlineReadOnly:
		txOptions = &sql.TxOptions{
			Isolation: sql.LevelReadCommitted,
			ReadOnly:  true,
		}
		if txMode.OnlineReadOnly.AllowInconsistentReads {
			txOptions.Isolation = sql.LevelReadUncommitted
		}
		return txOptions, nil
	case *Ydb_Table.TransactionSettings_StaleReadOnly:
		return &sql.TxOptions{
			Isolation: sql.LevelReadCommitted,
			ReadOnly:  true,
		}, nil
	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf(
			"ydb: unsupported transaction options: txMode=%T(%v)",
			txMode, txMode,
		))
	}
}
