package table

import (
	"context"
	"fmt"
)

type (
	QueryMode           int
	ctxQueryModeTypeKey struct{}
)

const (
	UnknownQueryMode = QueryMode(iota)
	DataQueryMode
	ScanQueryMode
	SchemeQueryMode
	ScriptingQueryMode
)

func WithQueryMode(ctx context.Context, mode QueryMode) context.Context {
	return context.WithValue(ctx, ctxQueryModeTypeKey{}, mode)
}

func queryModeFromContext(ctx context.Context, defaultMode QueryMode) QueryMode {
	if mode, ok := ctx.Value(ctxQueryModeTypeKey{}).(QueryMode); ok {
		return mode
	}

	return defaultMode
}

var (
	typeToString = map[QueryMode]string{
		DataQueryMode:      "data",
		ScanQueryMode:      "scan",
		SchemeQueryMode:    "scheme",
		ScriptingQueryMode: "scripting",
	}
	stringToType = map[string]QueryMode{
		"data":      DataQueryMode,
		"scan":      ScanQueryMode,
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
