package xsql

import (
	"database/sql"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/convert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func GenerateDeclareSection(args []sql.NamedArg) (string, error) {
	values := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		if arg.Name == "" {
			return "", xerrors.WithStackTrace(internal.ErrNameRequired)
		}
		value, err := convert.ToValue(arg.Value)
		if err != nil {
			return "", xerrors.WithStackTrace(err)
		}
		values[i] = table.ValueParam(arg.Name, value)
	}
	return internal.GenerateDeclareSection(table.NewQueryParameters(values...))
}
