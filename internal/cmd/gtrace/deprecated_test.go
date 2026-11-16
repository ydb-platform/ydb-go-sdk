package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestIsDeprecatedField(t *testing.T) {
	const src = `package p

type Trace struct {
	OnActive func()
	// Deprecated
	OnLegacy func()
}

type Info struct {
	Active int
	// Deprecated: will be removed
	Index int
}
`

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "p.go", src, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}

	deprecated := collectDeprecatedFields([]*ast.File{f})

	if _, ok := deprecated["Trace"]["OnLegacy"]; !ok {
		t.Fatalf("OnLegacy must be deprecated: %#v", deprecated["Trace"])
	}
	if _, ok := deprecated["Trace"]["OnActive"]; ok {
		t.Fatalf("OnActive must not be deprecated: %#v", deprecated["Trace"])
	}
	if _, ok := deprecated["Info"]["Index"]; !ok {
		t.Fatalf("Index must be deprecated: %#v", deprecated["Info"])
	}
	if _, ok := deprecated["Info"]["Active"]; ok {
		t.Fatalf("Active must not be deprecated: %#v", deprecated["Info"])
	}
}
