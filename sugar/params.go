package sugar

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type constraint interface {
	params.Params | []*params.Parameter | *table.QueryParameters | []table.ParameterOption | []sql.NamedArg
}

// GenerateDeclareSection generates DECLARE section text in YQL query by params
//
// Deprecated: use testutil.QueryBind(ydb.WithAutoDeclare()) helper.
// In YDB since version 24.1 declare sections not requires.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func GenerateDeclareSection[T constraint](parameters T) (string, error) {
	switch v := any(parameters).(type) {
	case *params.Params:
		return parametersToDeclares(*v), nil
	case []*params.Parameter:
		return parametersToDeclares(v), nil
	case []table.ParameterOption:
		return parameterOptionsToDeclares(v), nil
	case []sql.NamedArg:
		return namedArgsToDeclares(v)
	default:
		return "", xerrors.WithStackTrace(fmt.Errorf("unsupported type: %T", v))
	}
}

// ToYdbParam converts
//
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
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

func parametersToDeclares(v []*params.Parameter) string {
	var (
		buf      = xstring.Buffer()
		names    = make([]string, 0, len(v))
		declares = make(map[string]string, len(v))
	)
	defer buf.Free()

	for _, p := range v {
		name := p.Name()
		names = append(names, name)
		declares[name] = params.Declare(p)
	}

	sort.Strings(names)

	for _, name := range names {
		buf.WriteString(declares[name])
		buf.WriteString(";\n")
	}

	return buf.String()
}

func parameterOptionsToDeclares(v []table.ParameterOption) string {
	parameters := make([]*params.Parameter, len(v))
	for i, p := range v {
		parameters[i] = params.Named(p.Name(), p.Value())
	}

	return parametersToDeclares(parameters)
}

func namedArgsToDeclares(v []sql.NamedArg) (string, error) {
	vv, err := bind.Params(func() (newArgs []interface{}) {
		for i := range v {
			newArgs = append(newArgs, v[i])
		}

		return newArgs
	}()...)
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	return parametersToDeclares(vv), nil
}
