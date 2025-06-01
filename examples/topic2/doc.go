/*
Package main demonstrates YDB topic batch write and read operations.

This example shows how to:
- Connect to YDB with a 5-second timeout
- Delete and create a topic using YQL commands
- Write 3 messages to the topic
- Read messages in batches with timeout handling
- Process and commit each batch individually

Prerequisites:
Before running this example, ensure you have:
 1. YDB running locally via Docker:
    docker run -d --rm --name ydb-local -h localhost \
    -p 2135:2135 -p 2136:2136 -p 8765:8765 \
    -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
    cr.yandex/yc/yandex-docker-local-ydb:latest

2. Web UI available at: http://localhost:8765

3. Authentication configured (see: https://ydb.tech/docs/reference/ydb-sdk/auth)

Usage:

	go run . -ydb "grpc://localhost:2136/local"

Connection timeout:
The example uses a 5-second timeout for ydb.Open() because local YDB instances
typically respond quickly. For production environments, consider longer timeouts.

Key operations demonstrated:

 1. Topic deletion via YQL (ignores schema errors):
    err := db.Query().Exec(ctx, `DROP TOPIC IF EXISTS `+"`"+topicPath+"`")

 2. Topic creation via YQL:
    err := db.Query().Exec(ctx, `CREATE TOPIC `+"`"+topicPath+"`"+` (
    CONSUMER consumer1
    )`)

 3. Writing multiple messages:
    writer, err := db.Topic().StartWriter(topicPath)
    messages := []string{"Message 1", "Message 2", "Message 3"}
    for i, content := range messages {
    message := topicwriter.Message{Data: bytes.NewReader([]byte(content))}
    err = writer.Write(ctx, message)
    }

 4. Reading and processing messages in batches with timeout:
    reader, err := db.Topic().StartReader("consumer1", topicoptions.ReadTopic(topicPath))
    for {
    readCtx, readCancel := context.WithTimeout(ctx, 1*time.Second)
    batch, err := reader.ReadMessagesBatch(readCtx)
    readCancel()
    if err != nil && readCtx.Err() == context.DeadlineExceeded {
    break // No more messages available
    }
    // Process batch messages...
    // Commit each batch immediately after processing
    err = reader.Commit(batch.Context(), batch)
    }

Expected output:

	Deleting topic (if exists)...
	Topic deleted (if existed)
	Creating topic...
	Topic created successfully
	Writing 3 messages...
	Message 1 written successfully
	Message 2 written successfully
	Message 3 written successfully
	Starting batch reader...
	Message read: Message 1
	Offset: 0
	Message read: Message 2
	Offset: 1
	Message read: Message 3
	Offset: 2
	Batch processed with 3 messages
	Batch committed successfully
	Read timeout reached, no more messages available
	Example completed successfully - read 3 messages total

Batch Processing Features:
- Uses 1-second timeout for read operations to handle cases where no more messages are available
- Demonstrates real-world batch processing patterns with proper timeout handling
- Shows how to process and commit each batch individually for immediate acknowledgment
- Continues reading until all messages are consumed or timeout occurs

Note: This example does not clean up resources to allow exploration via Web UI.
*/
package main
