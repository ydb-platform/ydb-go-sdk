package table

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// GenerateDeclareSection generates DECLARE section text in YQL query by params
//
// Warning: This is an experimental feature and could change at any time
func GenerateDeclareSection(params *table.QueryParameters) string {
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
			v.Type().String(),
		)
	})
	sort.Strings(names)
	for _, name := range names {
		buf.WriteString(declares[name])
	}
	return buf.String()
}
