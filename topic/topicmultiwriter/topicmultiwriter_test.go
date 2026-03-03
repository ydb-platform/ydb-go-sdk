package topicmultiwriter_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicmultiwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func ExampleMultiWriter_Write() {
	ctx := context.Background()
	db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		log.Fatalf("failed ydb connection: %v", err)
	}

	multiWriter, err := db.Topic().CreateMultiWriter("topicName", topicoptions.WithProducerIDPrefix("test-producer"))
	if err != nil {
		log.Fatalf("failed to create topic producer: %v", err)
	}

	msg1 := topicmultiwriter.Message{Key: "key-1"}
	msg1.Data = strings.NewReader("1")
	msg2 := topicmultiwriter.Message{Key: "key-2"}
	msg2.Data = strings.NewReader("2")
	msg3 := topicmultiwriter.Message{Key: "key-3"}
	msg3.Data = strings.NewReader("3")

	err = multiWriter.Write(ctx, msg1, msg2, msg3)
	if err != nil {
		fmt.Println("OK")
	} else {
		log.Fatalf("failed write to topic producer")
	}
}
