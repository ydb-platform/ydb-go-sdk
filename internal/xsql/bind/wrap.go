package bind

import (
	_ "embed"
	"regexp"
	"strings"
	"text/template"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var (
	//go:embed wrap.yql
	wrapQuery    string
	wrapTemplate = template.Must(template.New("").Funcs(template.FuncMap{
		"Lines": func(s string) (ss []string) {
			if s == "" {
				return nil
			}
			return strings.Split(s, "\n")
		},
	}).Parse(wrapQuery))
)

type (
	wrapTemplateParams struct {
		Version          string
		PrintSourceQuery bool
		SourceQuery      string
		Pragmas          []string
		Declares         []Declare
		FinalQuery       string
	}
)

func (b Bindings) wrap(query string, re *regexp.Regexp, replace func(string) string, params *table.QueryParameters) (
	_ string, _ *table.QueryParameters, err error,
) {
	if !b.AllowBindParams && b.TablePathPrefix == "" {
		return query, params, nil
	}

	p := wrapTemplateParams{
		Version:          "v" + meta.Version,
		PrintSourceQuery: b.AllowBindParams || b.TablePathPrefix != "",
		SourceQuery:      query,
		Pragmas:          b.pragmas(),
		Declares:         b.declares(params),
		FinalQuery:       query,
	}

	if b.AllowBindParams && re != nil && replace != nil {
		p.FinalQuery = re.ReplaceAllStringFunc(query, replace)
	}

	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)

	if err = wrapTemplate.Execute(buffer, p); err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	return buffer.String(), params, nil
}
