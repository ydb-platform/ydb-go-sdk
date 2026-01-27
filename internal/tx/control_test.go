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
		{
			ctrl: SnapshotReadWriteTxControl(),
			tableTxControl: &Ydb_Table.TransactionControl{
				TxSelector: &Ydb_Table.TransactionControl_BeginTx{
					BeginTx: &Ydb_Table.TransactionSettings{
						TxMode: &Ydb_Table.TransactionSettings_SnapshotReadWrite{},
					},
				},
				CommitTx: false,
			},
			queryTxControl: &Ydb_Query.TransactionControl{
				TxSelector: &Ydb_Query.TransactionControl_BeginTx{
					BeginTx: &Ydb_Query.TransactionSettings{
						TxMode: &Ydb_Query.TransactionSettings_SnapshotReadWrite{},
					},
				},
				CommitTx: false,
			},
		},
		{
			ctrl: SnapshotReadWriteTxControl(CommitTx()),
			tableTxControl: &Ydb_Table.TransactionControl{
				TxSelector: &Ydb_Table.TransactionControl_BeginTx{
					BeginTx: &Ydb_Table.TransactionSettings{
						TxMode: &Ydb_Table.TransactionSettings_SnapshotReadWrite{},
					},
				},
				CommitTx: true,
			},
			queryTxControl: &Ydb_Query.TransactionControl{
				TxSelector: &Ydb_Query.TransactionControl_BeginTx{
					BeginTx: &Ydb_Query.TransactionSettings{
						TxMode: &Ydb_Query.TransactionSettings_SnapshotReadWrite{},
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

func TestControlEqual(t *testing.T) {
	t.Run("SameControls", func(t *testing.T) {
		ctrl1 := SerializableReadWriteTxControl()
		ctrl2 := SerializableReadWriteTxControl()
		require.True(t, ctrl1.Equal(ctrl2))
	})

	t.Run("SameControlsWithCommit", func(t *testing.T) {
		ctrl1 := SerializableReadWriteTxControl(CommitTx())
		ctrl2 := SerializableReadWriteTxControl(CommitTx())
		require.True(t, ctrl1.Equal(ctrl2))
	})

	t.Run("DifferentCommitFlag", func(t *testing.T) {
		ctrl1 := SerializableReadWriteTxControl()
		ctrl2 := SerializableReadWriteTxControl(CommitTx())
		require.False(t, ctrl1.Equal(ctrl2))
	})

	t.Run("DifferentTxModes", func(t *testing.T) {
		ctrl1 := SerializableReadWriteTxControl()
		ctrl2 := SnapshotReadOnlyTxControl()
		require.False(t, ctrl1.Equal(ctrl2))
	})

	t.Run("SnapshotReadOnly", func(t *testing.T) {
		ctrl1 := SnapshotReadOnlyTxControl()
		ctrl2 := SnapshotReadOnlyTxControl()
		require.True(t, ctrl1.Equal(ctrl2))
	})

	t.Run("OnlineReadOnly", func(t *testing.T) {
		ctrl1 := OnlineReadOnlyTxControl()
		ctrl2 := OnlineReadOnlyTxControl()
		require.True(t, ctrl1.Equal(ctrl2))
	})

	t.Run("OnlineReadOnlyWithInconsistentReads", func(t *testing.T) {
		ctrl1 := OnlineReadOnlyTxControl(WithInconsistentReads())
		ctrl2 := OnlineReadOnlyTxControl(WithInconsistentReads())
		require.True(t, ctrl1.Equal(ctrl2))
	})

	t.Run("OnlineReadOnlyDifferentInconsistentReads", func(t *testing.T) {
		ctrl1 := OnlineReadOnlyTxControl()
		ctrl2 := OnlineReadOnlyTxControl(WithInconsistentReads())
		require.False(t, ctrl1.Equal(ctrl2))
	})

	t.Run("StaleReadOnly", func(t *testing.T) {
		ctrl1 := StaleReadOnlyTxControl()
		ctrl2 := StaleReadOnlyTxControl()
		require.True(t, ctrl1.Equal(ctrl2))
	})

	t.Run("NilControls", func(t *testing.T) {
		var ctrl1 *Control
		var ctrl2 *Control
		require.True(t, ctrl1.Equal(ctrl2))
	})

	t.Run("OneNilControl", func(t *testing.T) {
		ctrl1 := SerializableReadWriteTxControl()
		var ctrl2 *Control
		require.False(t, ctrl1.Equal(ctrl2))
		require.False(t, ctrl2.Equal(ctrl1))
	})

	t.Run("WithTxID", func(t *testing.T) {
		ctrl1 := NewControl(WithTxID("tx-123"))
		ctrl2 := NewControl(WithTxID("tx-123"))
		require.True(t, ctrl1.Equal(ctrl2))
	})

	t.Run("DifferentTxID", func(t *testing.T) {
		ctrl1 := NewControl(WithTxID("tx-123"))
		ctrl2 := NewControl(WithTxID("tx-456"))
		require.False(t, ctrl1.Equal(ctrl2))
	})
}
