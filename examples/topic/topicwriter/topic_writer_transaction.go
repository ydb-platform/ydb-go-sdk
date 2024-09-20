package topicwriter

import (
	"context"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func CopyMessagesBetweenTopicsTxWriter(
	ctx context.Context,
	db *ydb.Driver,
	reader *topicreader.Reader,
	topic string,
) error {
	return db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		writer, err := db.Topic().StartTransactionalWriter(tx, topic)
		if err != nil {
			return err
		}

		batch, err := reader.PopMessagesBatchTx(ctx, tx)
		if err != nil {
			return err
		}

		for _, mess := range batch.Messages {

			if err = writer.Write(ctx, topicwriter.Message{Data: mess}); err != nil {
				return err
			}
		}

		return nil
	}, query.WithIdempotent())
}

func TableAndTopicWithinTransaction(
	ctx context.Context,
	db *ydb.Driver,
	topicPath string,
	id int64,
) error {
	return db.Query().DoTx(ctx, func(ctx context.Context, t query.TxActor) error {
		row, err := t.QueryRow(ctx, "SELECT val FROM table WHERE id=$id", query.WithParameters(
			ydb.ParamsBuilder().
				Param("$id").Int64(id).
				Build()))
		if err != nil {
			return err
		}

		var val int64
		if err = row.Scan(&val); err != nil {
			return err
		}

		// the writer is dedicated for the transaction, it can't be used outside the transaction
		// it is no needs to close or flush the messages - it happened internally on transaction commit
		writer, err := db.Topic().StartTransactionalWriter(t, topicPath)
		if err != nil {
			return err
		}

		err = writer.Write(ctx, topicwriter.Message{
			Data: strings.NewReader(fmt.Sprintf("val: %v processed", val)),
		})

		if err != nil {
			return err
		}

		return nil
	})
}
