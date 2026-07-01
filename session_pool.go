package ydb

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"

// SessionPoolStats is a snapshot of driver session pool counters.
type SessionPoolStats = pool.Stats

// SessionPoolStats returns counters for the driver session pool shared by table and query clients.
func (d *Driver) SessionPoolStats() (SessionPoolStats, error) {
	if d.sharedSessionPool == nil {
		return pool.Stats{}, nil
	}

	shared, err := d.sharedSessionPool.Get()
	if err != nil {
		return pool.Stats{}, err
	}

	return shared.Stats(), nil
}
