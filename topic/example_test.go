package topic_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func Example_createTopic() {
	ctx := context.TODO()
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		log.Printf("failed connect: %v", err)

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
		log.Printf("failed create topic: %v", err)

		return
	}
}

func Example_alterTopic() {
	ctx := context.TODO()
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		log.Printf("failed connect: %v", err)

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
		log.Printf("failed alter topic: %v", err)

		return
	}
}

func Example_createTopicWithConsumerAvailabilityPeriod() {
	ctx := context.TODO()
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		log.Printf("failed connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources

	// Create topic with consumer that has 24-hour availability period
	// Messages for this consumer won't expire for at least 24 hours even if not committed
	err = db.Topic().Create(ctx, "topic-path",
		topicoptions.CreateWithConsumer(topictypes.Consumer{
			Name:               "my-consumer",
			Important:          true,
			AvailabilityPeriod: 24 * time.Hour, // Messages available for at least 24 hours
			SupportedCodecs:    []topictypes.Codec{topictypes.CodecRaw, topictypes.CodecGzip},
		}),
	)
	if err != nil {
		log.Printf("failed create topic: %v", err)

		return
	}
}

func Example_alterConsumerAvailabilityPeriod() {
	ctx := context.TODO()
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		log.Printf("failed connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources

	// Set availability period to 48 hours for existing consumer
	err = db.Topic().Alter(ctx, "topic-path",
		topicoptions.AlterConsumerWithAvailabilityPeriod("my-consumer", 48*time.Hour),
	)
	if err != nil {
		log.Printf("failed alter consumer availability period: %v", err)

		return
	}

	// Reset availability period to default value
	err = db.Topic().Alter(ctx, "topic-path",
		topicoptions.AlterConsumerResetAvailabilityPeriod("my-consumer"),
	)
	if err != nil {
		log.Printf("failed reset consumer availability period: %v", err)

		return
	}
}

func Example_describeTopic() {
	ctx := context.TODO()
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		log.Printf("failed connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources

	descResult, err := db.Topic().Describe(ctx, "topic-path")
	if err != nil {
		log.Printf("failed describe topic: %v", err)

		return
	}
	fmt.Printf("describe: %#v\n", descResult)
}

func Example_desrcibeTopicConsumer() {
	ctx := context.TODO()
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(
		ctx, connectionString,
	)
	if err != nil {
		log.Printf("failed connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources

	descResult, err := db.Topic().DescribeTopicConsumer(ctx, "topic-path", "new-consumer")
	if err != nil {
		log.Printf("failed describe topic consumer: %v", err)

		return
	}
	fmt.Printf("describe consumer: %#v\n", descResult)
}

func Example_dropTopic() {
	ctx := context.TODO()
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		log.Printf("failed connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources

	err = db.Topic().Drop(ctx, "topic-path")
	if err != nil {
		log.Printf("failed drop topic: %v", err)

		return
	}
}

func Example_readMessage() {
	ctx := context.TODO()
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(ctx, connectionString)
	if err != nil {
		log.Printf("failed connect: %v", err)

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

		content, err := io.ReadAll(mess)
		if err != nil {
			fmt.Printf("failed start reader: %v", err)

			return
		}
		fmt.Println(string(content))
	}
}
