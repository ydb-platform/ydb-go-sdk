package table_topic_transactions

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func pumpFromTopicToTable(ctx context.Context, db *ydb.Driver, topic, consumer string) error {
	reader, err := db.Topic().StartReader(consumer, topicoptions.ReadTopic(topic))
	if err != nil {
		return err
	}

	return db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		batch, err := reader.PopMessagesBatchTx(ctx, tx)
		if err != nil {
			return err
		}

		var values []types.Value
		for _, mess := range batch.Messages {
			var event Event
			err = topicsugar.JSONUnmarshal(mess, &event)
			if err != nil {
				return err
			}
			values = append(values,
				types.StructValue(
					types.StructFieldValue("id", types.Int64Value(event.ID)),
					types.StructFieldValue("val", types.TextValue(event.Val)),
				),
			)
		}

		return tx.Exec(ctx, `
DECLARE $args AS List<Struct<
	id: Int64,
	val: Text
>>;

UPSERT INTO table
SELECT * FROM AS_TABLE($args);
`, query.WithParameters(
			ydb.ParamsBuilder().Param("$args").Any(types.ListValue(values...)).Build(),
		))
	}, query.WithIdempotent())
}

func pumpFromTableToTopic(ctx context.Context, db *ydb.Driver, topic string) error {
	writer, err := db.Topic().StartWriter(topic)

	if err != nil {
		return err
	}

	return db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		rs, err := tx.QueryResultSet(ctx, `SELECT id, val FROM table;`)
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				return err
			}

			var event Event
			err = row.ScanStruct(&event)

			content, err := json.Marshal(event)
			if err != nil {
				return err
			}
			err = writer.WriteWithTx(ctx, tx, topicwriter.Message{
				Data: bytes.NewReader(content),
			})
			if err != nil {
				return err
			}
		}
		return err
	}, query.WithIdempotent())
}

type Event struct {
	ID  int64
	Val string
}
