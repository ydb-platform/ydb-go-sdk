package sugar

import (
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// GenerateDeclareSection generates DECLARE section text in YQL query by params
//
// Warning: This is an experimental feature and could change at any time
func GenerateDeclareSection(params *table.QueryParameters) string {
	return internal.GenerateDeclareSection(params)
}
