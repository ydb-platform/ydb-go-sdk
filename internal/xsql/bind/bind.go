package bind

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func Bind(query, tablePathPrefix string, args ...driver.NamedValue) (
	_ string, _ *table.QueryParameters, err error,
) {
	if len(args) == 0 {
		if tablePathPrefix == "" {
			return query, nil, nil
		}
		query, err = binder{
			Version:     "v" + meta.Version,
			SourceQuery: query,
			Pragmas:     pragmas(tablePathPrefix),
			FinalQuery:  query,
		}.Render()
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		return query, nil, nil
	}
	switch t := queryType(query); t {
	case numericArgs:
		return bindNumeric(query, tablePathPrefix, args...)
	case positionalArgs:
		return bindPositional(query, tablePathPrefix, args...)
	case ydbArgs:
		return bindYdb(query, tablePathPrefix, args...)
	default:
		return "", nil, xerrors.WithStackTrace(fmt.Errorf("%s (type = %03b): %w", query, t, errUnknownQueryType))
	}
}

func bindYdb(query, tablePathPrefix string, args ...driver.NamedValue) (
	_ string, _ *table.QueryParameters, err error,
) {
	pp := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		if valuer, ok := arg.Value.(driver.Valuer); ok {
			arg.Value, err = valuer.Value()
			if err != nil {
				return "", nil, xerrors.WithStackTrace(err)
			}
		}
		switch v := arg.Value.(type) {
		case *table.QueryParameters:
			if len(args) > 1 {
				return "", nil, xerrors.WithStackTrace(fmt.Errorf("%v: %w", args, ErrMultipleQueryParameters))
			}
			query, err = binder{
				Version:     "v" + meta.Version,
				SourceQuery: query,
				Pragmas:     pragmas(tablePathPrefix),
				Declares:    declares(v),
				FinalQuery:  query,
			}.Render()
			if err != nil {
				return "", nil, xerrors.WithStackTrace(err)
			}
			return query, v, nil
		case table.ParameterOption:
			pp[i] = v
		default:
			if arg.Name == "" {
				return "", nil, xerrors.WithStackTrace(fmt.Errorf("%T: %w", v, errUnknownYdbParam))
			}
			if !strings.HasPrefix(arg.Name, "$") {
				arg.Name = "$" + arg.Name
			}
			var value types.Value
			value, err = ToValue(arg.Value)
			if err != nil {
				return "", nil, xerrors.WithStackTrace(err)
			}
			pp[i] = table.ValueParam(arg.Name, value)
		}
	}
	params := table.NewQueryParameters(pp...)
	query, err = binder{
		Version:     "v" + meta.Version,
		SourceQuery: query,
		Pragmas:     pragmas(tablePathPrefix),
		Declares:    declares(params),
		FinalQuery:  query,
	}.Render()
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}
	return query, params, nil
}

func bindPositional(query, tablePathPrefix string, args ...driver.NamedValue) (
	_ string, _ *table.QueryParameters, err error,
) {
	pp := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		var v types.Value
		v, err = ToValue(arg.Value)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		pp[i] = table.ValueParam("$p"+strconv.Itoa(i), v)
	}
	i := 0
	params := table.NewQueryParameters(pp...)
	query, err = binder{
		Version:     "v" + meta.Version,
		SourceQuery: query,
		Pragmas:     pragmas(tablePathPrefix),
		Declares:    declares(params),
		FinalQuery: positionalArgsRe.ReplaceAllStringFunc(query, func(s string) string {
			defer func() {
				i++
			}()
			return s[:1] + "$p" + strconv.Itoa(i)
		}),
	}.Render()
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}
	return query, params, nil
}

func bindNumeric(query, tablePathPrefix string, args ...driver.NamedValue) (
	_ string, _ *table.QueryParameters, err error,
) {
	pp := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		var v types.Value
		v, err = ToValue(arg.Value)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		pp[i] = table.ValueParam("$p"+strconv.Itoa(i), v)
	}
	params := table.NewQueryParameters(pp...)
	query, err = binder{
		Version:     "v" + meta.Version,
		SourceQuery: query,
		Pragmas:     pragmas(tablePathPrefix),
		Declares:    declares(params),
		FinalQuery: numericArgsRe.ReplaceAllStringFunc(query, func(s string) string {
			n, _ := strconv.Atoi(s[1:])
			return "$p" + strconv.Itoa(n-1)
		}),
	}.Render()
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}
	return query, params, nil
}
