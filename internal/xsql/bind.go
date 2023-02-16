package xsql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	bindNumericRe      = regexp.MustCompile(`\$\d+`)
	bindPositionalRe   = regexp.MustCompile(`[^\\][?]`)
	bindPositionCharRe = regexp.MustCompile(`[?]`)
	bindNamedRe        = regexp.MustCompile(`@\w+`)
	parameterOptionRe  = regexp.MustCompile(`\$[a-zA-Z\_]+\w+`)

	errBindMixedParamsFormats = errors.New("mixed named, numeric or positional parameters")
	errNoPositionalArg        = errors.New("have no arg for positional param")
	errNoNumericArg           = errors.New("have no arg for numeric param")
	errNoNamedArg             = errors.New("have no arg for named param")
	errNoParameterOptionArg   = errors.New("have no arg for param")
	errFewQueryParametersArg  = errors.New("more then one table.QueryParameters args")
)

type GroupSet struct {
	Value []interface{}
}

type ArraySet []interface{}

//nolint:gocyclo
func bind(query string, args ...driver.NamedValue) (string, *table.QueryParameters, error) {
	if len(args) == 0 {
		return query, nil, nil
	}
	var (
		haveNamed           bool
		haveNumeric         bool
		havePositional      bool
		haveParameterOption bool
		haveQueryParameters bool
	)
	haveNumeric = bindNumericRe.MatchString(query)
	havePositional = bindPositionalRe.MatchString(query)
	if haveNumeric && havePositional {
		return "", nil, xerrors.WithStackTrace(errBindMixedParamsFormats)
	}
	for _, namedValue := range args {
		if namedValue.Name != "" {
			haveNamed = true
		}
		switch namedValue.Value.(type) {
		case driver.NamedValue:
		case table.ParameterOption:
			haveParameterOption = true
		case *table.QueryParameters:
			haveQueryParameters = true
		}
	}
	switch {
	case haveNamed && !(haveNumeric || havePositional || haveParameterOption || haveQueryParameters):
		return bindNamed(query, args...)
	case haveNumeric && !(haveNamed || havePositional || haveParameterOption || haveQueryParameters):
		return bindNumeric(query, args...)
	case havePositional && !(haveNamed || haveNumeric || haveParameterOption || haveQueryParameters):
		return bindPositional(query, args...)
	case haveParameterOption && !(haveNamed || haveNumeric || havePositional || haveQueryParameters):
		return bindParameterOption(query, args...)
	case haveQueryParameters && !(haveNamed || haveNumeric || havePositional || haveParameterOption):
		return bindQueryParameters(query, args...)
	default:
		return "", nil, xerrors.WithStackTrace(errBindMixedParamsFormats)
	}
}

func bindQueryParameters(query string, args ...driver.NamedValue) (_ string, _ *table.QueryParameters, err error) {
	if len(args) != 1 {
		return "", nil, xerrors.WithStackTrace(fmt.Errorf("%v: %w", args, errFewQueryParametersArg))
	}
	params, has := args[0].Value.(*table.QueryParameters)
	if !has {
		return "", nil, xerrors.WithStackTrace(fmt.Errorf("arg %+v is not a *table.ParameterOption", args[0]))
	}
	var (
		queryParams = make(map[string]struct{}, len(args))
		declares    = make([]string, 0, len(args))
	)
	params.Each(func(name string, v types.Value) {
		declares = append(declares, "DECLARE "+name+" AS "+v.Type().Yql()+";")
		queryParams[name] = struct{}{}
	})
	sort.Strings(declares)
	for _, paramName := range parameterOptionRe.FindAllString(query, -1) {
		if _, found := queryParams[paramName]; !found {
			return "", nil, xerrors.WithStackTrace(fmt.Errorf("%s: %w", paramName, errNoParameterOptionArg))
		}
	}
	return strings.Join(declares, "\n") + "\n" + query, params, nil
}

func bindParameterOption(query string, args ...driver.NamedValue) (_ string, _ *table.QueryParameters, err error) {
	var (
		queryParams = make(map[string]struct{}, len(args))
		params      = make([]table.ParameterOption, len(args))
		declares    = make([]string, len(args))
	)
	for i, namedValue := range args {
		param, has := namedValue.Value.(table.ParameterOption)
		if !has {
			return "", nil, xerrors.WithStackTrace(fmt.Errorf("arg %+v is not a table.ParameterOption", namedValue.Value))
		}
		params[i] = param
		declares[i] = "DECLARE " + param.Name() + " AS " + param.Value().Type().Yql() + ";"
		queryParams[param.Name()] = struct{}{}
	}
	sort.Strings(declares)
	for _, paramName := range parameterOptionRe.FindAllString(query, -1) {
		if _, found := queryParams[paramName]; !found {
			return "", nil, xerrors.WithStackTrace(fmt.Errorf("%s: %w", paramName, errNoParameterOptionArg))
		}
	}
	return strings.Join(declares, "\n") + "\n" + query, table.NewQueryParameters(params...), nil
}

func bindPositional(query string, args ...driver.NamedValue) (_ string, _ *table.QueryParameters, err error) {
	var (
		unbind      = make(map[int]struct{}, len(args))
		queryParams = make([]string, len(args))
		params      = make([]table.ParameterOption, len(args))
		declares    = make([]string, len(args))
	)
	for i, namedValue := range args {
		value, err := convertToValue(namedValue.Value)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		paramName := fmt.Sprintf("$p%d", i+1)
		params[i] = table.ValueParam(paramName, value)
		queryParams[i] = paramName
		declares[i] = "DECLARE " + paramName + " AS " + value.Type().Yql() + ";"
	}
	sort.Strings(declares)
	i := 0
	query = bindPositionalRe.ReplaceAllStringFunc(query, func(n string) string {
		if i >= len(queryParams) {
			unbind[i] = struct{}{}
			return ""
		}
		val := queryParams[i]
		i++
		return bindPositionCharRe.ReplaceAllStringFunc(n, func(m string) string {
			return val
		})
	})
	for param := range unbind {
		return "", nil, xerrors.WithStackTrace(fmt.Errorf("position %d: %w", param, errNoPositionalArg))
	}
	// replace \? escape sequence
	query = strings.ReplaceAll(query, "\\?", "?")
	return strings.Join(declares, "\n") + "\n" + query, table.NewQueryParameters(params...), nil
}

func bindNumeric(query string, args ...driver.NamedValue) (_ string, _ *table.QueryParameters, err error) {
	var (
		unbind      = make(map[string]struct{}, len(args))
		queryParams = make(map[string]string, len(args))
		params      = make([]table.ParameterOption, len(args))
		declares    = make([]string, len(args))
	)
	for i, namedValue := range args {
		paramName := fmt.Sprintf("$p%d", i+1)
		value, err := convertToValue(namedValue.Value)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		params[i] = table.ValueParam(paramName, value)
		queryParams[fmt.Sprintf("$%d", i+1)] = paramName
		declares[i] = "DECLARE " + paramName + " AS " + value.Type().Yql() + ";"
	}
	sort.Strings(declares)
	query = bindNumericRe.ReplaceAllStringFunc(query, func(n string) string {
		name, found := queryParams[n]
		if !found {
			unbind[n] = struct{}{}
			return ""
		}
		return name
	})
	for param := range unbind {
		return "", nil, xerrors.WithStackTrace(fmt.Errorf("%s: %w", param, errNoNumericArg))
	}
	return strings.Join(declares, "\n") + "\n" + query, table.NewQueryParameters(params...), nil
}

func bindNamed(query string, args ...driver.NamedValue) (_ string, _ *table.QueryParameters, err error) {
	var (
		unbind      = make(map[string]struct{}, len(args))
		queryParams = make(map[string]string, len(args))
		params      = make([]table.ParameterOption, len(args))
		declares    = make([]string, len(args))
	)
	for i, namedValue := range args {
		value, err := convertToValue(namedValue.Value)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		params[i] = table.ValueParam("$"+namedValue.Name, value)
		queryParams["@"+namedValue.Name] = "$" + namedValue.Name
		declares[i] = "DECLARE $" + namedValue.Name + " AS " + value.Type().Yql() + ";"
	}
	sort.Strings(declares)
	query = bindNamedRe.ReplaceAllStringFunc(query, func(n string) string {
		name, found := queryParams[n]
		if !found {
			unbind[n] = struct{}{}
			return ""
		}
		return name
	})
	for param := range unbind {
		return "", nil, xerrors.WithStackTrace(fmt.Errorf("%s: %w", param, errNoNamedArg))
	}
	return strings.Join(declares, "\n") + "\n" + query, table.NewQueryParameters(params...), nil
}
