package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const batchSize = 1000

type logMessage struct {
	App       string
	Host      string
	Timestamp time.Time
	HTTPCode  uint32
	Message   string
}

func wrap(err error, explanation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", explanation, err)
	}
	return err
}

func getLogBatch(logs []logMessage, offset int) []logMessage {
	logs = logs[:0]
	for i := 0; i < batchSize; i++ {
		message := logMessage{
			App:       fmt.Sprintf("App_%d", offset/256),
			Host:      fmt.Sprintf("192.168.0.%d", offset%256),
			Timestamp: time.Now().Add(time.Millisecond * time.Duration(offset+i%1000)),
			HTTPCode:  200,
		}
		if i%2 == 0 {
			message.Message = "GET / HTTP/1.1"
		} else {
			message.Message = "GET /images/logo.png HTTP/1.1"
		}

		logs = append(logs, message)
	}
	return logs
}

func createLogTable(ctx context.Context, c table.Client, tablePath string) error {
	log.Printf("Create table: %v\n", tablePath)
	err := c.Do(ctx,
		func(ctx context.Context, session table.Session) error {
			return session.CreateTable(ctx, tablePath,
				options.WithColumn("App", types.Optional(types.TypeUTF8)),
				options.WithColumn("Timestamp", types.Optional(types.TypeTimestamp)),
				options.WithColumn("Host", types.Optional(types.TypeUTF8)),
				options.WithColumn("HTTPCode", types.Optional(types.TypeUint32)),
				options.WithColumn("Message", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("App", "Timestamp", "Host"),
			)
		},
	)
	return wrap(err, "failed to create table")
}

func writeLogBatch(ctx context.Context, c table.Client, tablePath string, logs []logMessage) error {
	err := c.Do(ctx,
		func(ctx context.Context, session table.Session) error {
			rows := make([]types.Value, 0, len(logs))

			for _, msg := range logs {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("App", types.TextValue(msg.App)),
					types.StructFieldValue("Host", types.TextValue(msg.Host)),
					types.StructFieldValue("Timestamp", types.TimestampValueFromTime(msg.Timestamp)),
					types.StructFieldValue("HTTPCode", types.Uint32Value(msg.HTTPCode)),
					types.StructFieldValue("Message", types.TextValue(msg.Message)),
				))
			}

			return wrap(session.BulkUpsert(ctx, tablePath, types.ListValue(rows...)),
				"failed to perform bulk upsert")
		})
	return wrap(err, "failed to write log batch")
}
