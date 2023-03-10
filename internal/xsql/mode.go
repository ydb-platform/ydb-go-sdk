package xsql

import "fmt"

type QueryMode int

const (
	UnknownQueryMode = QueryMode(iota)
	DataQueryMode
	ExplainQueryMode
	ScanQueryMode
	SchemeQueryMode
	ScriptingQueryMode

	DefaultQueryMode = DataQueryMode
)

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
