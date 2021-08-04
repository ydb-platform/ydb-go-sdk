package main

import (
	"bytes"
	"encoding/hex"
	"hash/fnv"
	"text/template"
)

func Hash(s string) string {
	hasher := fnv.New32a()
	hasher.Write([]byte(s))
	return hex.EncodeToString(hasher.Sum(nil))
}

func isShortCorrect(link string) bool {
	return short.FindStringIndex(link) != nil
}

func isLongCorrect(link string) bool {
	return long.FindStringIndex(link) != nil
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

type templateConfig struct {
	TablePathPrefix string
}


