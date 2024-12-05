package xcontext

import (
	"context"
	"fmt"
)

type QueryMode int

type ctxModeTypeKey struct{}

const (
	UnknownQueryMode = QueryMode(iota)
	DataQueryMode
	ExplainQueryMode
	ScanQueryMode
	SchemeQueryMode
	ScriptingQueryMode

	DefaultQueryMode = DataQueryMode
)

// QueryModeFromContext returns defined QueryMode or DefaultQueryMode
func QueryModeFromContext(ctx context.Context, defaultQueryMode QueryMode) QueryMode {
	if m, ok := ctx.Value(ctxModeTypeKey{}).(QueryMode); ok {
		return m
	}

	return defaultQueryMode
}

// WithQueryMode returns a copy of context with given QueryMode
func WithQueryMode(ctx context.Context, m QueryMode) context.Context {
	return context.WithValue(ctx, ctxModeTypeKey{}, m)
}

var (
	typeToString = map[QueryMode]string{
		DataQueryMode:      "data",
		ScanQueryMode:      "scan",
		ExplainQueryMode:   "explain",
		SchemeQueryMode:    "scheme",
		ScriptingQueryMode: "scripting",
	}
	stringToType = map[string]QueryMode{
		"data":      DataQueryMode,
		"scan":      ScanQueryMode,
		"explain":   ExplainQueryMode,
		"scheme":    SchemeQueryMode,
		"scripting": ScriptingQueryMode,
	}
)

func (t QueryMode) String() string {
	if s, ok := typeToString[t]; ok {
		return s
	}

	return fmt.Sprintf("unknown_mode_%d", t)
}

func QueryModeFromString(s string) QueryMode {
	if t, ok := stringToType[s]; ok {
		return t
	}

	return UnknownQueryMode
}
