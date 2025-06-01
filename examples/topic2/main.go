package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"path"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

var connectionString = flag.String("ydb", "grpc://localhost:2136/local", "")

func main() {
	flag.Parse()

	// Use 5-second timeout for connection as specified in requirements
	// Local YDB instances typically respond quickly
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect to YDB
	db, err := ydb.Open(ctx, *connectionString)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	// Construct topic path using established pattern
	topicPath := path.Join(db.Name(), "example-topic")

	// Step 1: Delete topic if exists (ignore schema errors)
	log.Println("Deleting topic (if exists)...")
	err = db.Query().Exec(ctx, `DROP TOPIC IF EXISTS `+"`"+topicPath+"`")
	if err != nil {
		panic(fmt.Errorf("drop topic error: %w", err))
	}
	log.Println("Topic deleted (if existed)")

	// Step 2: Create topic via YQL
	log.Println("Creating topic...")
	err = db.Query().Exec(ctx, `CREATE TOPIC `+"`"+topicPath+"`"+` (
		CONSUMER consumer1
	)`)
	if err != nil {
		panic(fmt.Errorf("create topic error: %w", err))
	}
	log.Println("Topic created successfully")

	// Step 3: Write 3 messages to the topic
	log.Println("Writing 3 messages...")
	writer, err := db.Topic().StartWriter(topicPath)
	if err != nil {
		panic(fmt.Errorf("start writer error: %w", err))
	}
	defer func() { _ = writer.Close(ctx) }()

	// Write 3 messages with different content
	messages := []string{"Message 1", "Message 2", "Message 3"}
	for i, content := range messages {
		message := topicwriter.Message{
			Data: bytes.NewReader([]byte(content)),
		}
		err = writer.Write(ctx, message)
		if err != nil {
			panic(fmt.Errorf("write message %d error: %w", i+1, err))
		}
		log.Printf("Message %d written successfully", i+1)
	}

	// Step 4: Read messages in batches from the topic
	log.Println("Starting batch reader...")
	reader, err := db.Topic().StartReader("consumer1",
		topicoptions.ReadTopic(topicPath),
	)
	if err != nil {
		panic(fmt.Errorf("start reader error: %w", err))
	}
	defer func() { _ = reader.Close(ctx) }()

	// Read messages in batches with 1-second timeout
	totalMessagesRead := 0

	for {
		// Create context with 1-second timeout for each read operation
		readCtx, readCancel := context.WithTimeout(ctx, 1*time.Second)

		batch, err := reader.ReadMessagesBatch(readCtx)
		readCancel()

		if err != nil {
			if readCtx.Err() == context.DeadlineExceeded {
				log.Println("Read timeout reached, no more messages available")
				break
			}
			panic(fmt.Errorf("read batch error: %w", err))
		}

		if batch == nil || len(batch.Messages) == 0 {
			log.Println("No messages in batch, continuing...")
			continue
		}

		// Process messages in the batch
		for _, msg := range batch.Messages {
			content, err := io.ReadAll(msg)
			if err != nil {
				panic(fmt.Errorf("read message content error: %w", err))
			}

			log.Printf("Message read: %s", string(content))
			log.Printf("Offset: %d", msg.Offset)
			totalMessagesRead++
		}

		log.Printf("Batch processed with %d messages", len(batch.Messages))

		// Commit the batch immediately after processing
		err = reader.Commit(batch.Context(), batch)
		if err != nil {
			panic(fmt.Errorf("commit batch error: %w", err))
		}
		log.Printf("Batch committed successfully")
	}

	log.Printf("Example completed successfully - read %d messages total", totalMessagesRead)
}
