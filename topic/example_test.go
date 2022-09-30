package topic_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func Example_create_topic() {
	ctx := context.TODO()
	db, err := connectDB(ctx)
	if err != nil {
		log.Fatalf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources

	err = db.Topic().Create(ctx, "topic-path",

		// optional
		topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw, topictypes.CodecGzip),

		// optional
		topicoptions.CreateWithMinActivePartitions(3),
	)
	if err != nil {
		log.Fatalf("failed create topic: %v", err)
		return
	}
}

func Example_alter_topic() {
	ctx := context.TODO()
	db, err := connectDB(ctx)
	if err != nil {
		log.Fatalf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources

	err = db.Topic().Alter(ctx, "topic-path",
		topicoptions.AlterWithAddConsumers(topictypes.Consumer{
			Name:            "new-consumer",
			SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip}, // optional
		}),
	)
	if err != nil {
		log.Fatalf("failed alter topic: %v", err)
		return
	}
}

func Example_drop_topic() {
	ctx := context.TODO()
	db, err := connectDB(ctx)
	if err != nil {
		log.Fatalf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources

	err = db.Topic().Drop(ctx, "topic-path")
	if err != nil {
		log.Fatalf("failed drop topic: %v", err)
		return
	}
}

func Example_read_message() {
	ctx := context.TODO()
	db, err := connectDB(ctx)
	if err != nil {
		log.Fatalf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources

	reader, err := db.Topic().StartReader("consumer", topicoptions.ReadTopic("/topic/path"))
	if err != nil {
		fmt.Printf("failed start reader: %v", err)
		return
	}

	for {
		mess, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("failed start reader: %v", err)
			return
		}

		content, err := ioutil.ReadAll(mess)
		if err != nil {
			fmt.Printf("failed start reader: %v", err)
			return
		}
		fmt.Println(string(content))
	}
}

func connectDB(ctx context.Context) (ydb.Connection, error) {
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2135/local"
	}
	return ydb.Open(ctx, connectionString)
}
