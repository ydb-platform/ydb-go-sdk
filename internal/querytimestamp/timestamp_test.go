package querytimestamp

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func TestCompare(t *testing.T) {
	identity := NewIdentity("/db")
	makeTimestamp := func(step, id uint64, identity *Identity) VirtualTimestamp {
		return *FromYDB(&Ydb.VirtualTimestamp{PlanStep: step, TxId: id}, identity)
	}

	older := makeTimestamp(1, math.MaxUint64, identity)
	newer := makeTimestamp(2, 0, identity)
	order, err := older.Compare(newer)
	require.NoError(t, err)
	require.Equal(t, -1, order)

	older = makeTimestamp(2, 1<<63, identity)
	newer = makeTimestamp(2, math.MaxUint64, identity)
	order, err = older.Compare(newer)
	require.NoError(t, err)
	require.Equal(t, -1, order)
	order, err = newer.Compare(older)
	require.NoError(t, err)
	require.Equal(t, 1, order)
	order, err = newer.Compare(newer)
	require.NoError(t, err)
	require.Zero(t, order)
	require.Equal(t, uint64(math.MaxUint64), newer.TxID())
	require.Equal(t, uint64(2), newer.PlanStep())
	require.Equal(t, "/db", newer.Database())

	_, err = older.Compare(makeTimestamp(2, 1<<63, NewIdentity("/other")))
	require.ErrorIs(t, err, ErrDifferentDatabaseIdentity)
	_, err = older.Compare(makeTimestamp(2, 1<<63, NewIdentity("/db")))
	require.ErrorIs(t, err, ErrDifferentDatabaseIdentity)
	_, err = older.Compare(makeTimestamp(2, 1<<63, NewIdentity("")))
	require.ErrorIs(t, err, ErrUnknownDatabase)
	_, err = (VirtualTimestamp{}).Compare(older)
	require.ErrorIs(t, err, ErrUnknownDatabase)
	require.Nil(t, FromYDB(nil, identity))
}
