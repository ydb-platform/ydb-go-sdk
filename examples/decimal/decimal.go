package main

import (
	"bytes"
	"text/template"
)

type templateConfig struct {
	TablePathPrefix string
}

var writeQuery = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

REPLACE INTO decimals
SELECT
	id,
	value
FROM AS_TABLE($decimals);
`))

var readQuery = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
SELECT value FROM decimals;
`))

func render(t *template.Template, data any) string {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		panic(err)
	}

	return buf.String()
}
