package table

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var ErrNameRequired = xerrors.Wrap(fmt.Errorf("only named parameters are supported"))

// GenerateDeclareSection generates DECLARE section text in YQL query by params
//
// Warning: This is an experimental feature and could change at any time
func GenerateDeclareSection(params *table.QueryParameters) (string, error) {
	var (
		buf      bytes.Buffer
		names    []string
		declares = make(map[string]string, len(params.Params()))
	)
	params.Each(func(name string, v types.Value) {
		names = append(names, name)
		declares[name] = fmt.Sprintf(
			"DECLARE %s AS %s;\n",
			name,
			v.Type().Yql(),
		)
	})
	sort.Strings(names)
	for _, name := range names {
		if name == "" {
			return "", xerrors.WithStackTrace(ErrNameRequired)
		}
		buf.WriteString(declares[name])
	}
	return buf.String(), nil
}
