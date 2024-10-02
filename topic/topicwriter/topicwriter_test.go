package topicwriter_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func ExampleWriter_Write() {
	ctx := context.Background()
	db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		log.Fatalf("failed ydb connection: %v", err)
	}

	writer, err := db.Topic().StartWriter("topicName")
	if err != nil {
		log.Fatalf("failed to create topic writer: %v", err)
	}

	err = writer.Write(ctx,
		topicwriter.Message{Data: strings.NewReader("1")},
		topicwriter.Message{Data: strings.NewReader("2")},
		topicwriter.Message{Data: strings.NewReader("3")},
	)
	if err != nil {
		fmt.Println("OK")
	} else {
		log.Fatalf("failed write to stream")
	}
}
