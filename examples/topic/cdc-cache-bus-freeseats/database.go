package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func createTableAndCDC(ctx context.Context, db ydb.Connection, consumersCount int) {
	err := createTables(ctx, db)
	if err != nil {
		log.Fatalf("failed to create tables: %+v", err)
	}

	err = createCosumers(ctx, db, consumersCount)
	if err != nil {
		log.Fatalf("failed to create consumers: %+v", err)
	}
}

func createTables(ctx context.Context, db ydb.Connection) error {
	err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		err := s.DropTable(ctx, path.Join(db.Name(), "bus"))
		if ydb.IsOperationErrorSchemeError(err) {
			err = nil
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	_, err = db.Scripting().Execute(ctx, `
CREATE TABLE bus (id Text, freeSeats Int64, PRIMARY KEY(id));

ALTER TABLE 
	bus
ADD CHANGEFEED
	updates
WITH (
	FORMAT = 'JSON',
	MODE = 'UPDATES'
)
`, nil)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	_, err = db.Scripting().Execute(ctx, `
UPSERT INTO bus (id, freeSeats) VALUES ("bus1", 40), ("bus2", 60);
`, nil)
	if err != nil {
		return fmt.Errorf("failed insert rows: %w", err)
	}
	return nil
}

func createCosumers(ctx context.Context, db ydb.Connection, consumersCount int) error {
	for i := 0; i < consumersCount; i++ {
		err := db.Topic().Alter(ctx, "bus/updates", topicoptions.AlterWithAddConsumers(topictypes.Consumer{
			Name: consumerName(i),
		}))
		if err != nil {
			return err
		}
	}

	return nil
}

func connect() ydb.Connection {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if *ydbConnectionString != "" {
		connectionString = *ydbConnectionString
	}
	if connectionString == "" {
		connectionString = defaultConnectionString
	}

	token := os.Getenv("YDB_TOKEN")
	if *ydbToken != "" {
		token = *ydbToken
	}
	var ydbOptions []ydb.Option
	if token != "" {
		ydbOptions = append(ydbOptions, ydb.WithAccessTokenCredentials(token))
	}

	if *ydbToken != "" {
		ydbOptions = append(ydbOptions, ydb.WithAccessTokenCredentials(*ydbToken))
	}
	db, err := ydb.Open(ctx, connectionString, ydbOptions...)
	if err != nil {
		log.Fatalf("failed to create to ydb: %+v", err)
	}
	log.Printf("connected to database")
	return db
}

func consumerName(index int) string {
	return fmt.Sprintf("consumer-%v", index)
}
