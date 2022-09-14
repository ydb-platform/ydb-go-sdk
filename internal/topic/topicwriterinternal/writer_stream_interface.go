package topicwriterinternal

import (
	"context"
)

//nolint:lll
//go:generate mockgen -source writer_stream_interface.go -destination writer_stream_interface_mock_test.go -package topicwriterinternal -write_package_comment=false
type StreamWriter interface {
	Write(ctx context.Context, messages []Message) error
	Close(ctx context.Context) error
}
