package xsql

import (
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func toQueryParams(values []driver.NamedValue) *table.QueryParameters {
	if len(values) == 0 {
		return nil
	}
	opts := make([]table.ParameterOption, len(values))
	for i, arg := range values {
		opts[i] = table.ValueParam(
			arg.Name,
			arg.Value.(types.Value),
		)
	}
	return table.NewQueryParameters(opts...)
}

func toSchemeOptions(values []driver.NamedValue) (opts []options.ExecuteSchemeQueryOption) {
	if len(values) == 0 {
		return nil
	}
	for _, arg := range values {
		opts = append(opts, arg.Value.(options.ExecuteSchemeQueryOption))
	}
	return opts
}
