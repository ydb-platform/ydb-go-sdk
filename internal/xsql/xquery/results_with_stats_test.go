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
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 5,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(5), rows)
	})

	t.Run("SinglePhaseWithUpdates", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 3,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(3), rows)
	})

	t.Run("SinglePhaseWithDeletesAndUpdates", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 5,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 3,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(8), rows)
	})

	t.Run("MultipleTableAccesses", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 5,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 3,
							}.Build(),
						}.Build(),
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table2",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 2,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 7,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(17), rows) // 5 + 3 + 2 + 7
	})

	t.Run("MultiplePhases", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 5,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 3,
							}.Build(),
						}.Build(),
					},
				}.Build(),
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table2",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 2,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 7,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(17), rows) // 5 + 3 + 2 + 7
	})

	t.Run("OnlyReadsIgnored", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Reads: Ydb_TableStats.OperationStats_builder{
								Rows: 100,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})

	t.Run("MixedOperations", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Reads: Ydb_TableStats.OperationStats_builder{
								Rows: 100,
							}.Build(),
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 10,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 5,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(15), rows) // Only deletes + updates, reads ignored
	})

	t.Run("ZeroRowsAffected", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 0,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 0,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})

	t.Run("LargeNumberOfRows", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 1000000,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 2000000,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(3000000), rows)
	})

	t.Run("ComplexMultiPhaseMultiTable", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table1",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 10,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 20,
							}.Build(),
						}.Build(),
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table2",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 5,
							}.Build(),
						}.Build(),
					},
				}.Build(),
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table3",
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 15,
							}.Build(),
						}.Build(),
					},
				}.Build(),
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{
						Ydb_TableStats.TableAccessStats_builder{
							Name: "table4",
							Deletes: Ydb_TableStats.OperationStats_builder{
								Rows: 3,
							}.Build(),
							Updates: Ydb_TableStats.OperationStats_builder{
								Rows: 7,
							}.Build(),
						}.Build(),
					},
				}.Build(),
			},
		}.Build()))

		rows, err := r.RowsAffected()

		require.NoError(t, err)
		require.Equal(t, int64(60), rows) // 10 + 20 + 5 + 15 + 3 + 7
	})

	t.Run("EmptyTableAccessInPhase", func(t *testing.T) {
		r := &resultWithStats{}
		r.onQueryStats(stats.FromQueryStats(Ydb_TableStats.QueryStats_builder{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
				Ydb_TableStats.QueryPhaseStats_builder{
					TableAccess: []*Ydb_TableStats.TableAccessStats{},
				}.Build(),
			},
		}.Build()))

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
