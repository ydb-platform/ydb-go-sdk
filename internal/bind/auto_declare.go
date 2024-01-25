package bind

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type AutoDeclare struct{}

func (m AutoDeclare) blockID() blockID {
	return blockDeclare
}

func (m AutoDeclare) RewriteQuery(query string, args ...interface{}) (
	yql string, newArgs []interface{}, err error,
) {
	params, err := Params(args...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	if len(params) == 0 {
		return query, args, nil
	}

	var (
		declares = make([]string, 0, len(params))
		buffer   = xstring.Buffer()
	)
	defer buffer.Free()

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
		newArgs = append(newArgs, param)
	}

	return buffer.String(), newArgs, nil
}
