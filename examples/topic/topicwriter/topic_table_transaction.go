package topicwriter

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"strings"
)

func SendMessageWithinTransaction(ctx context.Context, db *ydb.Driver, writer *topicwriter.Writer) {
	db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		row, err := tx.ReadRow(ctx, "SELECT val FROM table WHERE id=$id", query.WithParameters(
			ydb.ParamsBuilder().
				Param("$id").Int64(123).
				Build()))
		if err != nil {
			return err
		}

		var val int64
		if err = row.Scan(&val); err != nil {
			return err
		}

		err = writer.WriteWithTx(ctx, tx, topicwriter.Message{
			Data: strings.NewReader(fmt.Sprintf("val: %v processed", val)),
		})
		if err != nil {
			return err
		}
		return nil
	})
}
