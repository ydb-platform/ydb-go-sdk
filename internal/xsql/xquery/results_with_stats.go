package xquery

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type (
	resultWithStats struct {
		rowsAffected *uint64
	}
)

var _ driver.Result = &resultWithStats{}

func (r *resultWithStats) onQueryStats(qs query.Stats) {
	var rowsAffected uint64
	for queryPhase := range qs.QueryPhases() {
		for tableAccess := range queryPhase.TableAccess() {
			rowsAffected += tableAccess.Deletes.Rows + tableAccess.Updates.Rows
		}
	}
	// last stats always contains the full stats of query
	r.rowsAffected = &rowsAffected
}

func (r *resultWithStats) RowsAffected() (int64, error) {
	if r.rowsAffected == nil {
		return 0, ErrUnsupported
	}

	return int64(*r.rowsAffected), nil
}

func (r *resultWithStats) LastInsertId() (int64, error) { return 0, ErrUnsupported }
