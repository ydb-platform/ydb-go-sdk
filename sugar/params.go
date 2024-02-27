package sugar

import (
	"database/sql"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// GenerateDeclareSection generates DECLARE section text in YQL query by params
//
// Deprecated: use testutil.QueryBind(ydb.WithAutoDeclare()) helper
func GenerateDeclareSection[T *table.QueryParameters | []table.ParameterOption | []sql.NamedArg | params.Parameters | []*params.Parameter](
	params T,
) (string, error) {
	switch v := any(params).(type) {
	case *table.QueryParameters:
		return internal.GenerateDeclareSection(v)
	case []table.ParameterOption:
		return internal.GenerateDeclareSection(table.NewQueryParameters(v...))
	case []sql.NamedArg:
		params, err := bind.Params(func() (newArgs []interface{}) {
			for i := range v {
				newArgs = append(newArgs, v[i])
			}

			return newArgs
		}()...)
		if err != nil {
			return "", xerrors.WithStackTrace(err)
		}

		return internal.GenerateDeclareSection(table.NewQueryParameters(func() (opts []table.ParameterOption) {
			opts = make([]table.ParameterOption, 0, len(params))
			for _, p := range params {
				opts = append(opts, p)
			}
			return opts
		}()...))
	default:
		return "", xerrors.WithStackTrace(fmt.Errorf("unsupported type: %T", v))
	}
}

// ToYdbParam converts
//
// Deprecated: use testutil/QueryBind helper
func ToYdbParam(param sql.NamedArg) (*params.Parameter, error) {
	params, err := bind.Params(param)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if len(params) != 1 {
		return nil, xerrors.WithStackTrace(fmt.Errorf("internal error: wrong parameters count: %v", params))
	}

	return params[0], nil
}
