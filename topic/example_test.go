package topic_test

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func Example_topic_read_message() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)
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
