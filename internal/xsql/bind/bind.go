package bind

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/convert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	prefix = "-- modified by ydb-go-sdk@v" + meta.Version

	errMultipleQueryParameters = errors.New("only one query arg *table.QueryParameters allowed")
	errUnknownYdbParam         = errors.New("unknown ydb param")
	errUnknownQueryType        = errors.New("unknown query type (mixed args type or nothing args)")
	errArgsCount               = errors.New("count args as query params is not equal of count of matched args in query")

	bindTypesMapping = map[string]bindType{
		"numeric":    bindNumeric,
		"positional": bindPositional,
		"declare":    bindDeclare,
	}
)

func FromString(t string) (bindType, error) {
	if tt, has := bindTypesMapping[t]; has {
		return tt, nil
	}
	return noBind, xerrors.WithStackTrace(
		fmt.Errorf("unknown bind type %q", t),
	)
}

type bindType int

const (
	noBind = bindType(1 << iota >> 1)
	bindTablePathPrefix
	bindNumeric
	bindPositional
	bindDeclare
)

func (t bindType) String() string {
	ss := make([]string, 0, 2)
	if t&bindTablePathPrefix != 0 {
		ss = append(ss, "TablePathPrefix")
	}
	if t&bindNumeric != 0 {
		ss = append(ss, "Numeric")
	}
	if t&bindPositional != 0 {
		ss = append(ss, "Positional")
	}
	if t&bindDeclare != 0 {
		ss = append(ss, "Declare")
	}
	return strings.Join(ss, "|")
}

type Bind struct {
	tablePathPrefix string
	bindType        bindType
}

func NoBind() Bind {
	return Bind{
		bindType: noBind,
	}
}

func TablePathPrefix(tablePathPrefix string) Bind {
	return Bind{
		bindType:        bindTablePathPrefix,
		tablePathPrefix: tablePathPrefix,
	}
}

func GetTablePathPrefix(b Bind) string {
	return b.tablePathPrefix
}

func Positional() Bind {
	return Bind{
		bindType: bindPositional,
	}
}

func Numeric() Bind {
	return Bind{
		bindType: bindNumeric,
	}
}

func Declare() Bind {
	return Bind{
		bindType: bindDeclare,
	}
}

func SwitchType(b Bind, t bindType) Bind {
	b.bindType = t
	return b
}

func (b Bind) WithTablePathPrefix(tablePathPrefix string) Bind {
	b.tablePathPrefix = tablePathPrefix
	b.bindType |= bindTablePathPrefix
	return b
}

func (b Bind) originFragment(query string) string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	buffer.WriteString(prefix)
	buffer.WriteString(" (bind type = " + b.bindType.String() + ")\n")
	for _, line := range strings.Split(query, "\n") {
		buffer.WriteString("--   " + line + "\n")
	}
	buffer.WriteString("\n")
	return buffer.String()
}

func (b Bind) query(query string, params *table.QueryParameters, normalizedArgsQuery string) string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)

	once := sync.Once{}
	writeOrigin := func(buffer *bytes.Buffer) {
		once.Do(func() {
			buffer.WriteString(b.originFragment(query))
		})
	}

	if tablePathPrefixPragma := b.tablePathPrefixPragma(); len(tablePathPrefixPragma) > 0 {
		writeOrigin(buffer)
		buffer.WriteString(tablePathPrefixPragma)
		buffer.WriteString("\n\n")
	}

	if declares := b.declareFragment(params); len(declares) > 0 {
		writeOrigin(buffer)
		buffer.WriteString("-- bind declares\n")
		buffer.WriteString(declares)
	}

	if strings.TrimSpace(query) != normalizedArgsQuery {
		writeOrigin(buffer)
		buffer.WriteString("-- origin query with normalized args\n")
	} else {
		buffer.WriteString("-- origin query\n")
	}

	buffer.WriteString(normalizedArgsQuery)

	return buffer.String()
}

func (b Bind) tablePathPrefixPragma() string {
	if b.bindType&bindTablePathPrefix == 0 {
		return ""
	}
	return "PRAGMA TablePathPrefix(\"" + b.tablePathPrefix + "\");"
}

func (b Bind) declareFragment(params *table.QueryParameters) string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	if declares := b.declares(params); len(declares) > 0 {
		for _, declare := range declares {
			buffer.WriteString(declare.String() + "\n")
		}
		buffer.WriteString("\n")
	}
	return buffer.String()
}

type declare struct {
	Name string
	Type string
}

func (d declare) String() string {
	return "DECLARE " + d.Name + " AS " + d.Type + ";"
}

func (b Bind) declares(params *table.QueryParameters) (declares []declare) {
	params.Each(func(name string, v types.Value) {
		declares = append(declares, declare{
			Name: name,
			Type: v.Type().Yql(),
		})
	})
	sort.Slice(declares, func(i, j int) bool {
		return declares[i].Name < declares[j].Name
	})
	return declares
}

func Normalize(b Bind, query string, args ...driver.NamedValue) (
	_ string, _ *table.QueryParameters, err error,
) {
	return b.bind(query, args...)
}

func NormalizePath(b Bind, folderOrTable string) (absPath string) {
	switch ch := folderOrTable[0]; ch {
	case '/':
		return folderOrTable
	case '.':
		return path.Join(b.tablePathPrefix, strings.TrimLeft(folderOrTable, "."))
	default:
		return path.Join(b.tablePathPrefix, folderOrTable)
	}
}

func (b Bind) bind(query string, args ...driver.NamedValue) (
	_ string, _ *table.QueryParameters, err error,
) {
	switch {
	case b.bindType == noBind:
		params, err := b.argsToParams(args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		return query, params, nil
	case b.bindType == bindTablePathPrefix:
		params, err := b.argsToParams(args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		return b.originFragment(query) + b.tablePathPrefixPragma() + "\n\n-- origin query\n" + query, params, nil
	case b.bindType&bindNumeric != 0:
		return b.bindNumeric(query, args...)
	case b.bindType&bindPositional != 0:
		return b.bindPositional(query, args...)
	case b.bindType&bindDeclare != 0:
		return b.bindYdb(query, args...)
	default:
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("type = %q: %w", b.bindType.String(), errUnknownQueryType),
		)
	}
}

func (b Bind) argsToParams(args ...driver.NamedValue) (_ *table.QueryParameters, err error) {
	pp := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		if valuer, ok := arg.Value.(driver.Valuer); ok {
			arg.Value, err = valuer.Value()
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
		}
		switch v := arg.Value.(type) {
		case *table.QueryParameters:
			if len(args) > 1 {
				return nil, xerrors.WithStackTrace(fmt.Errorf("%v: %w", args, errMultipleQueryParameters))
			}
			return v, nil
		case table.ParameterOption:
			pp[i] = v
		default:
			switch b.bindType {
			case bindNumeric, bindPositional:
			default:
				if arg.Name == "" {
					return nil, xerrors.WithStackTrace(fmt.Errorf("%T: %w", v, errUnknownYdbParam))
				}
				if !strings.HasPrefix(arg.Name, "$") {
					arg.Name = "$" + arg.Name
				}
			}
			var value types.Value
			value, err = convert.ToValue(arg.Value)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
			pp[i] = table.ValueParam(arg.Name, value)
		}
	}
	return table.NewQueryParameters(pp...), nil
}

func (b Bind) bindYdb(query string, args ...driver.NamedValue) (
	_ string, _ *table.QueryParameters, err error,
) {
	params, err := b.argsToParams(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}
	return b.query(query, params, clearQuery(query)), params, nil
}

var positionalArgsRe = regexp.MustCompile(`[^\\][?]`)

func (b Bind) bindPositional(query string, args ...driver.NamedValue) (
	_ string, _ *table.QueryParameters, err error,
) {
	pp := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		var v types.Value
		v, err = convert.ToValue(arg.Value)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		pp[i] = table.ValueParam("$p"+strconv.Itoa(i), v)
	}
	position := 0
	params := table.NewQueryParameters(pp...)
	normalizedArgsQuery := positionalArgsRe.ReplaceAllStringFunc(clearQuery(query), func(s string) string {
		defer func() {
			position++
		}()
		return s[:1] + "$p" + strconv.Itoa(position)
	})
	if position > len(args) {
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: %d != %d", errArgsCount, len(args), position),
		)
	}
	return b.query(query, params, normalizedArgsQuery), params, nil
}

var numericArgsRe = regexp.MustCompile(`\$\d+`)

func (b Bind) bindNumeric(query string, args ...driver.NamedValue) (
	_ string, _ *table.QueryParameters, err error,
) {
	pp := make([]table.ParameterOption, len(args))
	for i, arg := range args {
		var v types.Value
		v, err = convert.ToValue(arg.Value)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		pp[i] = table.ValueParam("$p"+strconv.Itoa(i), v)
	}
	params := table.NewQueryParameters(pp...)
	maxIndex := 0
	normalizedArgsQuery := numericArgsRe.ReplaceAllStringFunc(clearQuery(query), func(s string) string {
		n, _ := strconv.Atoi(s[1:])
		if n > maxIndex {
			maxIndex = n
		}
		return "$p" + strconv.Itoa(n-1)
	})
	if maxIndex > len(args) {
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: %d != %d", errArgsCount, len(args), maxIndex),
		)
	}
	return b.query(query, params, normalizedArgsQuery), params, nil
}
