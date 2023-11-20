//go:build !go1.18
// +build !go1.18

package sugar

import (
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/clients/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// GenerateDeclareSection generates DECLARE section text in YQL query by params
//
// Deprecated: use testutil.QueryBind(ydb.WithAutoDeclare()) helper
func GenerateDeclareSection(params *table.QueryParameters) (string, error) {
	return internal.GenerateDeclareSection(params)
}
