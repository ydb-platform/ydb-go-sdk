package xtest

import "encoding/json"

func ToJSON(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "\t") //nolint:errchkjson

	return string(b)
}
