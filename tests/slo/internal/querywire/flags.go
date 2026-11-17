package querywire

import "flag"

// RegisterFlag adds the query result decoder selection to a workload's flags.
func RegisterFlag(fs *flag.FlagSet) *bool {
	return fs.Bool("query-proto-wire-decoding", false, "decode Query Service rows directly from protobuf wire bytes")
}
