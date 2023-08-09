package topicreaderinternal

import (
	"context"
)

//go:generate mockgen -source batched_stream_reader_interface.go -destination batched_stream_reader_mock_test.go -package topicreaderinternal -write_package_comment=false

type batchedStreamReader interface {
	WaitInit(ctx context.Context) error
	ReadMessageBatch(ctx context.Context, opts ReadMessageBatchOptions) (*PublicBatch, error)
	Commit(ctx context.Context, commitRange commitRange) error
	CloseWithError(ctx context.Context, err error) error
}
