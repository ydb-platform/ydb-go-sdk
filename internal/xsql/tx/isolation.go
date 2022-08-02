package tx

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// isolationOrControl maps driver transaction options to ydb transaction Option or query transaction control.
// This caused by ydb logic that prevents start actual transaction with OnlineReadOnly mode and ReadCommitted
// and ReadUncommitted isolation levels should use tx_control in every query request.
// It returns error on unsupported options.
func isolationOrControl(opts driver.TxOptions) (isolation table.TxOption, control []table.TxControlOption, err error) {
	level := sql.IsolationLevel(opts.Isolation)
	switch level {
	case sql.LevelDefault,
		sql.LevelSerializable,
		sql.LevelLinearizable:

		isolation = table.WithSerializableReadWrite()
		return

	case sql.LevelReadUncommitted:
		if opts.ReadOnly {
			control = []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(
						table.WithInconsistentReads(),
					),
				),
				table.CommitTx(),
			}
			return
		}

	case sql.LevelReadCommitted:
		if opts.ReadOnly {
			control = []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(),
				),
				table.CommitTx(),
			}
			return
		}
	}
	return nil, nil, fmt.Errorf(
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
