package sugar

import (
	"database/sql"
	"fmt"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// GenerateDeclareSection generates DECLARE section text in YQL query by params
//
// Warning: This is an experimental feature and could change at any time
func GenerateDeclareSection[T *table.QueryParameters | []table.ParameterOption | []sql.NamedArg](
	params T,
) (string, error) {
	switch v := any(params).(type) {
	case *table.QueryParameters:
		return internal.GenerateDeclareSection(v)
	case []table.ParameterOption:
		return internal.GenerateDeclareSection(table.NewQueryParameters(v...))
	case []sql.NamedArg:
		return xsql.GenerateDeclareSection(v)
	default:
		return "", xerrors.WithStackTrace(fmt.Errorf("unsupported type: %T", v))
	}
}
