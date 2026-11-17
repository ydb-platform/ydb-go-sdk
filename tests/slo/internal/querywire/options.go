package querywire

import ydb "github.com/ydb-platform/ydb-go-sdk/v3"

// Option selects the Query Service result decoder for this workload.
func Option(enabled bool) ydb.Option {
	return ydb.WithQueryProtoWireDecoding(enabled)
}
