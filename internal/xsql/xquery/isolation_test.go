package xquery

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestToYDB(t *testing.T) {
	for _, tt := range []struct {
		name      string
		txOptions driver.TxOptions
		txControl tx.SettingsOption
		err       bool
	}{
		// read-write
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  false,
			},
			txControl: query.WithSerializableReadWrite(),
			err:       false,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadUncommitted),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadCommitted),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelWriteCommitted),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelRepeatableRead),
				ReadOnly:  false,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSnapshot),
				ReadOnly:  false,
			},
			txControl: query.WithSnapshotReadWrite(),
			err:       false,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  false,
			},
			txControl: query.WithSerializableReadWrite(),
			err:       false,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  false,
			},
			err: true,
		},

		// read-only
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadUncommitted),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadCommitted),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelWriteCommitted),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelRepeatableRead),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSnapshot),
				ReadOnly:  true,
			},
			txControl: query.WithSnapshotReadOnly(),
			err:       false,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  true,
			},
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  true,
			},
			err: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toYDB(tt.txOptions)
			if !tt.err {
				require.NoError(t, err)
				require.Equal(t, tx.NewSettings(tt.txControl), tx.NewSettings(result))
			} else {
				require.Error(t, err)
			}
		})
	}
}
