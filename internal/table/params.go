package table

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var ErrNameRequired = xerrors.Wrap(fmt.Errorf("only named parameters are supported"))

// GenerateDeclareSection generates DECLARE section text in YQL query by params
//
// Warning: This is an experimental feature and could change at any time
func GenerateDeclareSection(params *table.QueryParameters) (string, error) {
	var (
		buf      bytes.Buffer
		names    []string
		declares = make(map[string]string, params.Count())
	)
	for _, p := range *params {
		name := p.Name()
		names = append(names, name)
		declares[name] = fmt.Sprintf(
			"DECLARE %s AS %s;\n",
			name,
			p.Value().Type().Yql(),
		)
	}
	sort.Strings(names)
	for _, name := range names {
		if name == "" {
			return "", xerrors.WithStackTrace(ErrNameRequired)
		}
		buf.WriteString(declares[name])
	}

	return buf.String(), nil
}
