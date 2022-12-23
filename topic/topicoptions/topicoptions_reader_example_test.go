package topicoptions_test

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func ExampleWithReaderCheckRetryErrorFunction() {
	var db ydb.Connection

	reader, err := db.Topic().StartReader(
		"consumer",
		topicoptions.ReadTopic("topic"),
		topicoptions.WithReaderCheckRetryErrorFunction(
			func(errInfo topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
				// Retry not found operations
				if ydb.IsOperationErrorNotFoundError(errInfo.Error) {
					return topicoptions.CheckErrorRetryDecisionRetry
				}

				// and use default behavior for all other errors
				return topicoptions.CheckErrorRetryDecisionDefault
			}),
	)
	_, _ = reader, err
}
