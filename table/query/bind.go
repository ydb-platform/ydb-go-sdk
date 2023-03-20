package query

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/convert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var (
	errInconsistentArgs = errors.New("inconsistent args")

	positionalArgsRe = regexp.MustCompile(`[^\\][?]`)
	numericArgsRe    = regexp.MustCompile(`\$\d+`)
)

type Binder interface {
	Bind(query string, args ...interface{}) (transformedQuery string, transformedArgs []interface{}, err error)
}

var (
	_ normalizePath = tablePathPrefixMiddleware{}
	_ normalizePath = Bind(nil)
)

type Bind []Binder

func NewBind(binders ...Binder) Bind {
	return binders
}

func (binders Bind) NormalizePath(folderOrTable string) string {
	for i := range binders {
		if path, has := binders[len(binders)-1-i].(normalizePath); has {
			return path.NormalizePath(folderOrTable)
		}
	}
	return folderOrTable
}

type normalizePath interface {
	NormalizePath(folderOrTable string) string
}

type tablePathPrefixMiddleware struct {
	tablePathPrefix string
}

func (m tablePathPrefixMiddleware) NormalizePath(folderOrTable string) string {
	switch ch := folderOrTable[0]; ch {
	case '/':
		return folderOrTable
	case '.':
		return path.Join(m.tablePathPrefix, strings.TrimLeft(folderOrTable, "."))
	default:
		return path.Join(m.tablePathPrefix, folderOrTable)
	}
}

func (m tablePathPrefixMiddleware) Bind(query string, args ...interface{}) (
	transformedQuery string, transformedArgs []interface{}, err error,
) {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)

	buffer.WriteString("-- bind TablePathPrefix\n")
	buffer.WriteString("PRAGMA TablePathPrefix(\"")
	buffer.WriteString(m.tablePathPrefix)
	buffer.WriteString("\");\n\n")
	buffer.WriteString(query)

	return buffer.String(), args, nil
}

func TablePathPrefix(tablePathPrefix string) Binder {
	return tablePathPrefixMiddleware{tablePathPrefix: tablePathPrefix}
}

type declareMiddleware struct{}

func (m declareMiddleware) Bind(query string, args ...interface{}) (
	transformedQuery string, transformedArgs []interface{}, err error,
) {
	params, err := convert.ArgsToParams(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	if len(params) == 0 {
		return query, args, nil
	}

	var (
		declares = make([]string, 0, len(params))
		buffer   = allocator.Buffers.Get()
	)
	defer allocator.Buffers.Put(buffer)

	buffer.WriteString("-- bind declares\n")

	for _, param := range params {
		declares = append(declares, "DECLARE "+param.Name()+" AS "+param.Value().Type().Yql()+";")
	}

	sort.Strings(declares)

	for _, d := range declares {
		buffer.WriteString(d)
		buffer.WriteByte('\n')
	}

	buffer.WriteByte('\n')

	buffer.WriteString(query)

	for _, param := range params {
		transformedArgs = append(transformedArgs, param)
	}

	return buffer.String(), transformedArgs, nil
}

func Declare() Binder {
	return declareMiddleware{}
}

type positionalArgsMiddleware struct{}

func (m positionalArgsMiddleware) Bind(query string, args ...interface{}) (
	transformedQuery string, transformedArgs []interface{}, err error,
) {
	params, err := convert.ArgsToParams(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	if len(params) == 0 {
		return query, args, nil
	}

	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)

	buffer.WriteString("-- origin query with positional args replacement\n")

	position := 0
	query = strings.TrimSpace(query)
	query = positionalArgsRe.ReplaceAllStringFunc(query, func(s string) string {
		defer func() {
			position++
		}()
		return s[:1] + "$p" + strconv.Itoa(position)
	})

	if position > len(params) {
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: (positional args: %d, query args %d)", errInconsistentArgs, position, len(params)),
		)
	}

	buffer.WriteString(query)

	for _, param := range params {
		transformedArgs = append(transformedArgs, param)
	}

	return buffer.String(), transformedArgs, nil
}

func Positional() Binder {
	return positionalArgsMiddleware{}
}

type numericArgsMiddleware struct{}

func (m numericArgsMiddleware) Bind(query string, args ...interface{}) (
	transformedQuery string, transformedArgs []interface{}, err error,
) {
	params, err := convert.ArgsToParams(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	if len(params) == 0 {
		return query, args, nil
	}

	var (
		buffer = allocator.Buffers.Get()
		hit    = make(map[string]struct{}, len(params))
		miss   = make(map[string]struct{}, len(params))
	)
	defer allocator.Buffers.Put(buffer)

	for _, param := range params {
		miss[param.Name()] = struct{}{}
	}

	buffer.WriteString("-- origin query with numeric args replacement\n")

	query = strings.TrimSpace(query)

	query = numericArgsRe.ReplaceAllStringFunc(query, func(s string) string {
		n, _ := strconv.Atoi(s[1:])
		name := "$p" + strconv.Itoa(n-1)
		hit[name] = struct{}{}
		delete(miss, name)
		return name
	})

	if len(miss) > 0 {
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: %v", errInconsistentArgs, miss),
		)
	}

	if len(hit) != len(params) {
		for k := range miss {
			delete(miss, k)
		}
		for _, p := range params {
			if _, has := hit[p.Name()]; !has {
				miss[p.Name()] = struct{}{}
			}
		}
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: %v", errInconsistentArgs, miss),
		)
	}

	buffer.WriteString(query)

	for _, param := range params {
		transformedArgs = append(transformedArgs, param)
	}

	return buffer.String(), transformedArgs, nil
}

func Numeric() Binder {
	return numericArgsMiddleware{}
}

type originMiddleware struct{}

func (m originMiddleware) Bind(query string, args ...interface{}) (
	transformedQuery string, transformedArgs []interface{}, err error,
) {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	buffer.WriteString("-- modified by ydb-go-sdk@v")
	buffer.WriteString(meta.Version)
	buffer.WriteByte('\n')
	query = strings.TrimSpace(query)
	for _, line := range strings.Split(query, "\n") {
		buffer.WriteString("--   " + line + "\n")
	}
	buffer.WriteString(query)
	return buffer.String(), args, nil
}

func Origin() Binder {
	return originMiddleware{}
}

func (binders Bind) ToYQL(query string, args ...interface{}) (
	transformedQuery string, _ *table.QueryParameters, err error,
) {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)

	for i := range binders {
		query, args, err = binders[len(binders)-1-i].Bind(query, args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
	}

	params, err := convert.ArgsToParams(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	return query, table.NewQueryParameters(params...), nil
}
