package rawtopiccommon

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

// StatusCodeError reports a failed status returned by the topic server.
// It intentionally does not implement the SDK's retry error interfaces.
type StatusCodeError struct {
	Status rawydb.StatusCode
}

func (e *StatusCodeError) Error() string {
	return fmt.Sprintf("ydb: bad status from topic server: %v", e.Status)
}
