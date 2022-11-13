package structural

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

// Metadata marshals map[string][]string value into dst and returns resulting Record
func Metadata(metadata map[string][]string, dst Record) Record {
	for key, value := range metadata {
		dst = dst.Strings(key, value)
	}
	return dst
}

// EndpointInfoSlice marshals []trace.EndpointInfo value into dst and returns resulting Array
func EndpointInfoSlice(slice []trace.EndpointInfo, dst Array) Array {
	for _, info := range slice {
		dst = dst.Stringer(info)
	}
	return dst
}
