package topicwriterinternal

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
)

//nolint:lll
//go:generate mockgen -source writer_stream_interface.go -destination writer_stream_interface_mock_test.go -package topicwriterinternal -write_package_comment=false
type streamWriter interface {
	Write(ctx context.Context, messages *messageWithDataContentSlice) (rawtopicwriter.WriteResult, error)
	Close(ctx context.Context) error
}
