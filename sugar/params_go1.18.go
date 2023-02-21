//go:build go1.18
// +build go1.18

package sugar

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/bind"
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
		return bind.GenerateDeclareSection(v)
	default:
		return "", xerrors.WithStackTrace(fmt.Errorf("unsupported type: %T", v))
	}
}

// ToYdbParam converts
//
// Warning: This is an experimental feature and could change at any time
func ToYdbParam(param sql.NamedArg) (table.ParameterOption, error) {
	return bind.ToYdbParam(driver.NamedValue{
		Name:  param.Name,
		Value: param.Value,
	})
}
