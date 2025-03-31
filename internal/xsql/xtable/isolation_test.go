package xtable

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func TestToYDB(t *testing.T) {
	for _, tt := range []struct {
		name      string
		txOptions driver.TxOptions
		txControl table.TxOption
		err       bool
	}{
		// read-write
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  false,
			},
			txControl: table.WithSerializableReadWrite(),
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
			err: true,
		},
		{
			name: xtest.CurrentFileLine(),
			txOptions: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  false,
			},
			txControl: table.WithSerializableReadWrite(),
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
			txControl: table.WithSnapshotReadOnly(),
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
			toYDB, err := toYDB(tt.txOptions)
			if !tt.err {
				require.NoError(t, err)
				require.Equal(t, table.TxSettings(tt.txControl), table.TxSettings(toYDB))
			} else {
				require.Error(t, err)
			}
		})
	}
}
