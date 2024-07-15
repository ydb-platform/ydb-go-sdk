package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
)

// Stats returns stats of query client pool
//
// Deprecated: use client.Stats() method instead
// Will be removed after Jan 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func Stats(client Client) (*pool.Stats, error) {
	return client.Stats(), nil
}
