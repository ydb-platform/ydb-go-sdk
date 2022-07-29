package xsql

import (
	"fmt"
)

type QueryMode int

const (
	QueryModeUnknown = iota
	QueryModeData
	QueryModeExplain
	QueryModeScan
	QueryModeScheme
	QueryModeScripting

	QueryModeDefault = QueryModeData
)

var (
	typeToString = map[QueryMode]string{
		QueryModeData:      "data_query",
		QueryModeScan:      "scan_query",
		QueryModeExplain:   "explain_query",
		QueryModeScheme:    "scheme_query",
		QueryModeScripting: "scripting_query",
	}
	stringToType = map[string]QueryMode{
		"data_query":      QueryModeData,
		"scan_query":      QueryModeScan,
		"explain_query":   QueryModeExplain,
		"scheme_query":    QueryModeScheme,
		"scripting_query": QueryModeScripting,
	}
)

func (t QueryMode) String() string {
	if s, ok := typeToString[t]; ok {
		return s
	}
	return fmt.Sprintf("unknown_mode_%d", t)
}

func FromString(s string) QueryMode {
	if t, ok := stringToType[s]; ok {
		return t
	}
	return QueryModeUnknown
}
