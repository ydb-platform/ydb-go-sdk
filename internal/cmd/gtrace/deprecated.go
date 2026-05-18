package main

import (
	"go/ast"
	"go/token"
	"strings"
)

func isDeprecatedCommentGroup(cg *ast.CommentGroup) bool {
	if cg == nil {
		return false
	}
	for _, c := range cg.List {
		text := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		if strings.Contains(text, "Deprecated") {
			return true
		}
	}

	return false
}

func isDeprecatedField(f *ast.Field) bool {
	return isDeprecatedCommentGroup(f.Doc) || isDeprecatedCommentGroup(f.Comment)
}

func collectDeprecatedFields(files []*ast.File) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{})
	for _, f := range files {
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				typeName := ts.Name.Name
				for _, field := range st.Fields.List {
					if !isDeprecatedField(field) {
						continue
					}
					for _, name := range field.Names {
						if name.Name == "_" {
							continue
						}
						if out[typeName] == nil {
							out[typeName] = make(map[string]struct{})
						}
						out[typeName][name.Name] = struct{}{}
					}
				}
			}
		}
	}

	return out
}
