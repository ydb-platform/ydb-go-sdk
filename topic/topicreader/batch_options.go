package topicreader

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"

// WithBatchMaxCount
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type WithBatchMaxCount int

// Apply
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (count WithBatchMaxCount) Apply(
	options topicreaderinternal.ReadMessageBatchOptions,
) topicreaderinternal.ReadMessageBatchOptions {
	options.MaxCount = int(count)
	return options
}

// WithBatchPreferMinCount set prefer min count for batch size. Sometime result batch can be less then count
// for example if internal buffer full and can't receive more messages or server stop send messages in partition
//
// count must be 1 or greater
// it will panic if count < 1
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type WithBatchPreferMinCount int

// Apply
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (count WithBatchPreferMinCount) Apply(
	options topicreaderinternal.ReadMessageBatchOptions,
) topicreaderinternal.ReadMessageBatchOptions {
	if count < 1 {
		panic("ydb: min batch size must be 1 or greater")
	}
	options.MinCount = int(count)
	return options
}
