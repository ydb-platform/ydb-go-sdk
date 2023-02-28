{{- .Name}}{{":"}}
{{range .Columns}}
{{- " - "}}{{.Type}}:{{.Name}}
{{end}}