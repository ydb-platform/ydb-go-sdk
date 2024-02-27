package bind

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
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
	yql string, parameters []*params.Parameter, err error,
) {
	if len(bindings) == 0 {
		parameters, err = Params(args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}

		return query, parameters, nil
	}

	buffer := xstring.Buffer()
	defer buffer.Free()

	for i := range bindings {
		var e error
		query, args, e = bindings[len(bindings)-1-i].RewriteQuery(query, args...)
		if e != nil {
			return "", nil, xerrors.WithStackTrace(e)
		}
	}

	parameters, err = Params(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	return query, parameters, nil
}

func Sort(bindings []Bind) []Bind {
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].blockID() < bindings[j].blockID()
	})

	return bindings
}
