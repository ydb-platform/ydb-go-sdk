package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
)

func cdcRead(ctx context.Context, db *ydb.Driver, consumerName, topicPath string) {
	// Connect to changefeed

	log.Println("Start cdc read")
	reader, err := db.Topic().StartReader(consumerName, []topicoptions.ReadSelector{{Path: topicPath}})
	if err != nil {
		log.Fatal("failed to start read feed", err)
	}

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			panic(fmt.Errorf("failed to read message: %w", err))
		}

		var event interface{}
		err = topicsugar.JSONUnmarshal(msg, &event)
		if err != nil {
			panic(fmt.Errorf("failed to unmarshal json cdc: %w", err))
		}
		log.Println("new cdc event:", event)
		err = reader.Commit(ctx, msg)
		if err != nil {
			panic(fmt.Errorf("failed to commit message: %w", err))
		}
	}
}
