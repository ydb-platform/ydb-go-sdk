package bind

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type blockID int

const (
	blockDefault = blockID(iota)
	blockPragma
	blockDeclare
	blockYQL
	blockCastArgs
)

type Bind interface {
	ToYdb(sql string, args ...any) (
		yql string, newArgs []any, _ error,
	)

	blockID() blockID
}

type Bindings []Bind

func (bindings Bindings) ToYdb(sql string, args ...any) (
	yql string, params params.Params, err error) {
	if len(bindings) == 0 {
		params, err = Params(args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}

		return sql, params, nil
	}

	buffer := xstring.Buffer()
	defer buffer.Free()

	for i := range bindings {
		var err error
		sql, args, err = bindings[len(bindings)-1-i].ToYdb(sql, args...)
		if err != nil {
			return "", nil, xerrors.WithStackTrace(err)
		}
	}

	params, err = Params(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	return sql, params, nil
}

func Sort(bindings []Bind) []Bind {
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].blockID() < bindings[j].blockID()
	})

	return bindings
}
