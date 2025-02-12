package tx

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

func ToQueryTxControl(src *Ydb_Table.TransactionControl) (dst *Ydb_Query.TransactionControl) {
	dst = &Ydb_Query.TransactionControl{
		CommitTx: src.GetCommitTx(),
	}

	switch t := src.GetTxSelector().(type) {
	case *Ydb_Table.TransactionControl_BeginTx:
		switch tt := t.BeginTx.GetTxMode().(type) {
		case *Ydb_Table.TransactionSettings_SerializableReadWrite:
			dst.TxSelector = &Ydb_Query.TransactionControl_BeginTx{
				BeginTx: &Ydb_Query.TransactionSettings{
					TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
						SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
					},
				},
			}
		case *Ydb_Table.TransactionSettings_SnapshotReadOnly:
			dst.TxSelector = &Ydb_Query.TransactionControl_BeginTx{
				BeginTx: &Ydb_Query.TransactionSettings{
					TxMode: &Ydb_Query.TransactionSettings_SnapshotReadOnly{
						SnapshotReadOnly: &Ydb_Query.SnapshotModeSettings{},
					},
				},
			}
		case *Ydb_Table.TransactionSettings_StaleReadOnly:
			dst.TxSelector = &Ydb_Query.TransactionControl_BeginTx{
				BeginTx: &Ydb_Query.TransactionSettings{
					TxMode: &Ydb_Query.TransactionSettings_StaleReadOnly{
						StaleReadOnly: &Ydb_Query.StaleModeSettings{},
					},
				},
			}
		case *Ydb_Table.TransactionSettings_OnlineReadOnly:
			dst.TxSelector = &Ydb_Query.TransactionControl_BeginTx{
				BeginTx: &Ydb_Query.TransactionSettings{
					TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
						OnlineReadOnly: &Ydb_Query.OnlineModeSettings{
							AllowInconsistentReads: tt.OnlineReadOnly.GetAllowInconsistentReads(),
						},
					},
				},
			}
		default:
			panic(fmt.Sprintf("unknown begin tx settings type: %v", tt))
		}
	case *Ydb_Table.TransactionControl_TxId:
		dst.TxSelector = &Ydb_Query.TransactionControl_TxId{
			TxId: t.TxId,
		}
	default:
		panic(fmt.Sprintf("unknown tx selector type: %v", t))
	}

	return dst
}
