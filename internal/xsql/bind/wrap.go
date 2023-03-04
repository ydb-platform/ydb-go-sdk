package bind

import (
	_ "embed"
	"strings"
	"text/template"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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

type binder struct {
	Version          string
	PrintSourceQuery bool
	SourceQuery      string
	Pragmas          []string
	Declares         []Declare
	FinalQuery       string
}

func (b binder) Render() (string, error) {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)

	if err := wrapTemplate.Execute(buffer, b); err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	return buffer.String(), nil
}
