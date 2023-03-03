package bind

import (
	"database/sql/driver"
	"fmt"
	"strconv"

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
	var (
		t  = queryType(query)
		pp = make([]table.ParameterOption, len(args))
	)
	switch t {
	case numericArgs:
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
	case positionalArgs:
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
	case ydbArgs:
		for i, arg := range args {
			v := arg.Value
			if valuer, ok := v.(driver.Valuer); ok {
				v, err = valuer.Value()
				if err != nil {
					return "", nil, xerrors.WithStackTrace(err)
				}
			}
			switch v := v.(type) {
			case *table.QueryParameters:
				if len(args) > 0 {
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
				return "", nil, xerrors.WithStackTrace(fmt.Errorf("%v: %w", v, errUnknownYdbParam))
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
	default:
		return "", nil, xerrors.WithStackTrace(fmt.Errorf("%s (type = %03b): %w", query, t, errUnknownQueryType))
	}
}
