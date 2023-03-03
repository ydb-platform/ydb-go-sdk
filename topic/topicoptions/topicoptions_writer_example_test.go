package topicoptions_test

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func ExampleWithWriterCheckRetryErrorFunction() {
	var db *ydb.Driver
	writer, err := db.Topic().StartWriter(
		"",
		topicoptions.WithWriterCheckRetryErrorFunction(
			func(errInfo topicoptions.CheckErrorRetryArgs) topicoptions.CheckErrorRetryResult {
				// Retry for all transport errors
				if ydb.IsTransportError(errInfo.Error) {
					return topicoptions.CheckErrorRetryDecisionRetry
				}

				// and use default behavior for all other errors
				return topicoptions.CheckErrorRetryDecisionDefault
			}),
	)
	_, _ = writer, err
}
