package bind

import (
	"database/sql/driver"
	"fmt"
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func (b Bindings) Bind(q string, args ...driver.NamedValue) (
	query string, _ *table.QueryParameters, _ error,
) {
	if len(args) == 0 {
		return b.wrap(q, nil, nil, nil)
	}
	var (
		t  = queryType(q)
		pp = make([]table.ParameterOption, len(args))
	)
	switch t {
	case numericArgs:
		for i, arg := range args {
			v, err := ToValue(arg.Value)
			if err != nil {
				return "", nil, xerrors.WithStackTrace(err)
			}
			pp[i] = table.ValueParam("$p"+strconv.Itoa(i), v)
		}
		return b.wrap(q, numericArgsRe, func(s string) string {
			n, _ := strconv.Atoi(s[1:])
			return "$p" + strconv.Itoa(n-1)
		}, table.NewQueryParameters(pp...))
	case positionalArgs:
		for i, arg := range args {
			v, err := ToValue(arg.Value)
			if err != nil {
				return "", nil, xerrors.WithStackTrace(err)
			}
			pp[i] = table.ValueParam("$p"+strconv.Itoa(i), v)
		}
		i := 0
		return b.wrap(q, positionalArgsRe, func(s string) string {
			defer func() {
				i++
			}()
			return s[:1] + "$p" + strconv.Itoa(i)
		}, table.NewQueryParameters(pp...))
	case ydbArgs:
		for i, arg := range args {
			v := arg.Value
			if valuer, ok := v.(driver.Valuer); ok {
				var err error
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
				return b.wrap(q, nil, nil, v)
			case table.ParameterOption:
				pp[i] = v
			default:
				return "", nil, xerrors.WithStackTrace(fmt.Errorf("%v: %w", v, errUnknownYdbParam))
			}
		}
		return b.wrap(q, nil, nil, table.NewQueryParameters(pp...))
	default:
		return "", nil, xerrors.WithStackTrace(fmt.Errorf("%s (type = %03b): %w", q, t, errUnknownQueryType))
	}
}
