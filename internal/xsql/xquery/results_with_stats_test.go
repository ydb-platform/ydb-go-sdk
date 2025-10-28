package xquery

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

func TestResultWithStats_RowsAffected(t *testing.T) {
	t.Run("NilStats", func(t *testing.T) {
		r := &resultWithStats{
			rowsAffected: nil,
		}

		rows, err := r.RowsAffected()

		require.Equal(t, int64(0), rows)
		require.ErrorIs(t, err, ErrUnsupported)
	})

	t.Run("EmptyStats", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})

	t.Run("SinglePhaseWithDeletes", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 5,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(5), rows)
	})

	t.Run("SinglePhaseWithUpdates", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 3,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(3), rows)
	})

	t.Run("SinglePhaseWithDeletesAndUpdates", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 5,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 3,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(8), rows)
	})

	t.Run("MultipleTableAccesses", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 5,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 3,
							},
						},
						{
							Name: "table2",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 2,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 7,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(17), rows) // 5 + 3 + 2 + 7
	})

	t.Run("MultiplePhases", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 5,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 3,
							},
						},
					},
				},
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table2",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 2,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 7,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(17), rows) // 5 + 3 + 2 + 7
	})

	t.Run("OnlyReadsIgnored", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Reads: &Ydb_TableStats.OperationStats{
								Rows: 100,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})

	t.Run("MixedOperations", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Reads: &Ydb_TableStats.OperationStats{
								Rows: 100,
							},
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 10,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 5,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(15), rows) // Only deletes + updates, reads ignored
	})

	t.Run("ZeroRowsAffected", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 0,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 0,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})

	t.Run("LargeNumberOfRows", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 1000000,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 2000000,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(3000000), rows)
	})

	t.Run("ComplexMultiPhaseMultiTable", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table1",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 10,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 20,
							},
						},
						{
							Name: "table2",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 5,
							},
						},
					},
				},
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table3",
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 15,
							},
						},
					},
				},
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						{
							Name: "table4",
							Deletes: &Ydb_TableStats.OperationStats{
								Rows: 3,
							},
							Updates: &Ydb_TableStats.OperationStats{
								Rows: 7,
							},
						},
					},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(60), rows) // 10 + 20 + 5 + 15 + 3 + 7
	})

	t.Run("EmptyTableAccessInPhase", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				{
					TableAccess: []*Ydb_TableStats.TableAccessStats{},
				},
			},
		}))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})
}

func TestResultWithStats_LastInsertId(t *testing.T) {
	r := &resultWithStats{}

	id, err := r.LastInsertId()

	require.Equal(t, int64(0), id)
	require.ErrorIs(t, err, ErrUnsupported)
}
