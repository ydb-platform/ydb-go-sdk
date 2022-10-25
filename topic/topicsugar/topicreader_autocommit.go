package topicsugar

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

var (
	errReadBatchesWithAutoCommitStopped            = xerrors.Wrap(errors.New("ydb: read batches with auto commit stopped"))
	errReadBatchesWithAutoCommitIterationCompleted = xerrors.Wrap(errors.New("ydb: read batches with auto commit iteration completed"))
)

func ReadBatchesWithAutoCommit(ctx context.Context, reader *topicreader.Reader, callback func(batch *topicreader.Batch) error) (resErr error) {
	for {
		batch, err := reader.ReadMessageBatch(ctx)
		if err != nil {
			return err
		}
		err = callback(batch)
		if err == nil {
			err = reader.Commit(ctx, batch)
		}
		if err != nil {
			return err
		}
	}
}
