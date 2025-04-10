package tx

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

func TestControl(t *testing.T) {
	for _, tt := range []struct {
		ctrl           *Control
		tableTxControl *Ydb_Table.TransactionControl
		queryTxControl *Ydb_Query.TransactionControl
	}{
		{
			ctrl: SerializableReadWriteTxControl(),
			tableTxControl: &Ydb_Table.TransactionControl{
				TxSelector: &Ydb_Table.TransactionControl_BeginTx{
					BeginTx: &Ydb_Table.TransactionSettings{
						TxMode: &Ydb_Table.TransactionSettings_SerializableReadWrite{},
					},
				},
				CommitTx: false,
			},
			queryTxControl: &Ydb_Query.TransactionControl{
				TxSelector: &Ydb_Query.TransactionControl_BeginTx{
					BeginTx: &Ydb_Query.TransactionSettings{
						TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{},
					},
				},
				CommitTx: false,
			},
		},
		{
			ctrl: SerializableReadWriteTxControl(CommitTx()),
			tableTxControl: &Ydb_Table.TransactionControl{
				TxSelector: &Ydb_Table.TransactionControl_BeginTx{
					BeginTx: &Ydb_Table.TransactionSettings{
						TxMode: &Ydb_Table.TransactionSettings_SerializableReadWrite{},
					},
				},
				CommitTx: true,
			},
			queryTxControl: &Ydb_Query.TransactionControl{
				TxSelector: &Ydb_Query.TransactionControl_BeginTx{
					BeginTx: &Ydb_Query.TransactionSettings{
						TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{},
					},
				},
				CommitTx: true,
			},
		},
		{
			ctrl: SnapshotReadOnlyTxControl(),
			tableTxControl: &Ydb_Table.TransactionControl{
				TxSelector: &Ydb_Table.TransactionControl_BeginTx{
					BeginTx: &Ydb_Table.TransactionSettings{
						TxMode: &Ydb_Table.TransactionSettings_SnapshotReadOnly{},
					},
				},
				CommitTx: true,
			},
			queryTxControl: &Ydb_Query.TransactionControl{
				TxSelector: &Ydb_Query.TransactionControl_BeginTx{
					BeginTx: &Ydb_Query.TransactionSettings{
						TxMode: &Ydb_Query.TransactionSettings_SnapshotReadOnly{},
					},
				},
				CommitTx: true,
			},
		},
		{
			ctrl: OnlineReadOnlyTxControl(),
			tableTxControl: &Ydb_Table.TransactionControl{
				TxSelector: &Ydb_Table.TransactionControl_BeginTx{
					BeginTx: &Ydb_Table.TransactionSettings{
						TxMode: &Ydb_Table.TransactionSettings_OnlineReadOnly{},
					},
				},
				CommitTx: true,
			},
			queryTxControl: &Ydb_Query.TransactionControl{
				TxSelector: &Ydb_Query.TransactionControl_BeginTx{
					BeginTx: &Ydb_Query.TransactionSettings{
						TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{},
					},
				},
				CommitTx: true,
			},
		},
		{
			ctrl: OnlineReadOnlyTxControl(WithInconsistentReads()),
			tableTxControl: &Ydb_Table.TransactionControl{
				TxSelector: &Ydb_Table.TransactionControl_BeginTx{
					BeginTx: &Ydb_Table.TransactionSettings{
						TxMode: &Ydb_Table.TransactionSettings_OnlineReadOnly{
							OnlineReadOnly: &Ydb_Table.OnlineModeSettings{AllowInconsistentReads: true},
						},
					},
				},
				CommitTx: true,
			},
			queryTxControl: &Ydb_Query.TransactionControl{
				TxSelector: &Ydb_Query.TransactionControl_BeginTx{
					BeginTx: &Ydb_Query.TransactionSettings{
						TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
							OnlineReadOnly: &Ydb_Query.OnlineModeSettings{AllowInconsistentReads: true},
						},
					},
				},
				CommitTx: true,
			},
		},
		{
			ctrl: StaleReadOnlyTxControl(),
			tableTxControl: &Ydb_Table.TransactionControl{
				TxSelector: &Ydb_Table.TransactionControl_BeginTx{
					BeginTx: &Ydb_Table.TransactionSettings{
						TxMode: &Ydb_Table.TransactionSettings_StaleReadOnly{},
					},
				},
				CommitTx: true,
			},
			queryTxControl: &Ydb_Query.TransactionControl{
				TxSelector: &Ydb_Query.TransactionControl_BeginTx{
					BeginTx: &Ydb_Query.TransactionSettings{
						TxMode: &Ydb_Query.TransactionSettings_StaleReadOnly{},
					},
				},
				CommitTx: true,
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t,
				fmt.Sprintf("%+v", tt.tableTxControl),
				fmt.Sprintf("%+v", tt.ctrl.ToYdbTableTransactionControl()),
			)
			require.Equal(t,
				fmt.Sprintf("%+v", tt.queryTxControl),
				fmt.Sprintf("%+v", tt.ctrl.ToYdbQueryTransactionControl()),
			)
		})
	}
}
