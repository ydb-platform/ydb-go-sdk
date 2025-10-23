package xquery

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

type (
	resultWithStats struct {
		stats stats.QueryStats
	}
)

var _ driver.Result = &resultWithStats{}

func (r *resultWithStats) RowsAffected() (int64, error) {
	if r.stats == nil {
		return 0, ErrUnsupported
	}

	var rowsAffected uint64
	for {
		phase, ok := r.stats.NextPhase()
		if !ok {
			break
		}

		for {
			tableAccess, ok := phase.NextTableAccess()
			if !ok {
				break
			}

			rowsAffected += tableAccess.Deletes.Rows
			rowsAffected += tableAccess.Updates.Rows
		}
	}

	return int64(rowsAffected), nil
}

func (r *resultWithStats) LastInsertId() (int64, error) { return 0, ErrUnsupported }
