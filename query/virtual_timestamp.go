package query

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/querytimestamp"

// VirtualTimestamp is the commit timestamp returned for successful StrictSerializableRW
// transactions with write effects. Values issued by the SDK retain their Query client identity.
type VirtualTimestamp = querytimestamp.VirtualTimestamp

var (
	ErrDifferentDatabaseIdentity = querytimestamp.ErrDifferentDatabaseIdentity
	ErrUnknownDatabase           = querytimestamp.ErrUnknownDatabase
)

// CommitTimestampProvider is implemented by Query results and transactions.
// The timestamp is nil when the server did not provide one. For streaming results,
// it becomes available after the final response part has been consumed (or Close has drained it).
// This optional interface leaves the existing Result and Transaction interfaces unchanged.
type CommitTimestampProvider interface {
	CommitTimestamp() *VirtualTimestamp
}
