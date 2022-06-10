package sugar

import (
	"bytes"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// ToDeclare makes DECLARE section text in YQL query by params
func ToDeclare(params *table.QueryParameters) string {
	var buf bytes.Buffer
	params.Each(func(name string, v types.Value) {
		buf.WriteString(fmt.Sprintf(
			"DECLARE %s AS %s;\n",
			name,
			value.TypeFromYDB(v.ToYDB().GetType()).String(),
		))
	})
	return buf.String()
}
