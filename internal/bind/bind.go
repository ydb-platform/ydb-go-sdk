package bind

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type blockID int

const (
	blockPragma = blockID(iota)
	blockDeclare
	blockYQL
)

type Bind interface {
	RewriteQuery(sql string, args ...interface{}) (
		yql string, newArgs []interface{}, _ error,
	)

	blockID() blockID
}

type Bindings []Bind

func (bindings Bindings) RewriteQuery(query string, args ...interface{}) (
	yql string, _ *table.QueryParameters, err error,
) {
	if len(bindings) == 0 {
		var params []table.ParameterOption
		params, err = Params(args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
		return query, table.NewQueryParameters(params...), nil
	}

	buffer := xstring.Buffer()
	defer buffer.Free()

	for i := range bindings {
		query, args, err = bindings[len(bindings)-1-i].RewriteQuery(query, args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
	}

	params, err := Params(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	return query, table.NewQueryParameters(params...), nil
}

func Sort(bindings []Bind) []Bind {
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].blockID() < bindings[j].blockID()
	})
	return bindings
}
