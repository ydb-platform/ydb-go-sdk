package xtest

import "encoding/json"

func ToJSON(v any) string {
	b, _ := json.MarshalIndent(v, "", "\t") //nolint:errchkjson

	return string(b)
}
