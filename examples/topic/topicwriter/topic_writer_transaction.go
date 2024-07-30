package topicwriter

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func SendMessageWithinTransaction(ctx context.Context, db *ydb.Driver, writer *topicwriter.Writer, id int64) error {
	return db.Query().DoTx(ctx, func(ctx context.Context, t query.TxActor) error {
		row, err := t.ReadRow(ctx, "SELECT val FROM table WHERE id=$id", query.WithParameters(
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

		err = writer.WriteWithTx(ctx, t, topicwriter.Message{
			Data: strings.NewReader(fmt.Sprintf("val: %v processed", val)),
		})
		if err != nil {
			return err
		}
		return nil
	})
}

func SendWithRecreateWriter(
	ctx context.Context,
	db *ydb.Driver,
	writerFabric func(ctx context.Context, db *ydb.Driver) (*topicwriter.Writer, error),
	ids <-chan int64,
) error {
	// second loop
	var lastData int64
	processed := true
	return db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		writer, err := writerFabric(ctx, db)
		if err != nil {
			return err
		}
		defer func() { _ = writer.Close(ctx) }()

		for {
			var id int64
			if processed {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case val, ok := <-ids:
					if !ok {
						return io.EOF
					}
					lastData = val
					processed = false
				}
			}

			id = lastData
			t, err := s.Begin(ctx, nil)
			if err != nil {
				return err
			}

			row, err := t.ReadRow(ctx, "SELECT val FROM table WHERE id=$id", query.WithParameters(
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

			err = writer.WriteWithTx(ctx, t, topicwriter.Message{
				Data: strings.NewReader(fmt.Sprintf("val: %v processed", val)),
			})
			if err != nil {
				return err
			}

			err = t.CommitTx(ctx)
			if err != nil {
				return err
			}
			processed = true
		}
	})
}
