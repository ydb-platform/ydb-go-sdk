package common

import (
	"database/sql/driver"

	"github.com/google/uuid"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

// ToDatabaseSQLValue convert native go values to subset of types for use
// with database/sql.Scanner interface
func ToDatabaseSQLValue(v interface{}) driver.Value {
	// convert types to one of safe database sql types for scan it by scanners
	// https://pkg.go.dev/database/sql@go1.25.5#Scanner
	switch val := v.(type) {
	case uuid.UUID:
		return val.String()
	case value.UUIDIssue1501FixedBytesWrapper:
		return val.PublicRevertReorderForIssue1501().String()
	default:
		return val
	}
}
