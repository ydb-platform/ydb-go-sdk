package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
)

func cdcRead(ctx context.Context, db *ydb.Driver, consumerName, topicPath string) error {
	// Connect to changefeed

	log.Println("Start cdc read")
	reader, err := db.Topic().StartReader(consumerName, []topicoptions.ReadSelector{{Path: topicPath}})
	if err != nil {
		return fmt.Errorf("failed to start read feed: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = reader.Close(closeCtx)
	}()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		var event any
		err = topicsugar.JSONUnmarshal(msg, &event)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json cdc: %w", err)
		}
		log.Println("new cdc event:", event)
		err = reader.Commit(ctx, msg)
		if err != nil {
			return fmt.Errorf("failed to commit message: %w", err)
		}
	}
}
