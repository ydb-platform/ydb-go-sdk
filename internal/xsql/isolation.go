package xsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// mapIsolation maps driver transaction options to ydb transaction Option or query transaction control.
// This caused by ydb logic that prevents start actual transaction with OnlineReadOnly mode and ReadCommitted
// and ReadUncommitted isolation levels should use tx_control in every query request.
// It returns error on unsupported options.
func mapIsolation(opts driver.TxOptions) (txSettings *table.TransactionSettings, err error) {
	level := sql.IsolationLevel(opts.Isolation)
	if !opts.ReadOnly {
		switch level {
		case sql.LevelDefault,
			sql.LevelSerializable:
			return table.TxSettings(table.WithSerializableReadWrite()), nil
		default:
			// nop
		}
	}
	return nil, fmt.Errorf(
		"ydb: unsupported transaction options: isolation=%s read_only=%t",
		nameIsolationLevel(level), opts.ReadOnly,
	)
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
