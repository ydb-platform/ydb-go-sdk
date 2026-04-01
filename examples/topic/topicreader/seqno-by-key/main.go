// Command seqno-by-key reads messages from a YDB topic and checks that SeqNo is
// strictly increasing for each logical write stream identified by the message
// partitioning key (MessageGroupID, from the writer's Key), producer id, and
// partition id.
//
// Run template (from the examples module directory):
//
//	go run ./topic/topicreader/seqno-by-key/ \
//	  -ydb 'grpc://localhost:2136/local' \
//	  -topic 'database_path/topic_name' \
//	  -consumer 'my_consumer'
//
// With authentication from the environment, add: -use-env-credentials
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		log.Fatalf("invalid arguments: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var ydbOpts []ydb.Option
	if cfg.useEnvCredentials {
		ydbOpts = append(ydbOpts, environ.WithEnvironCredentials())
	}

	db, err := ydb.Open(ctx, cfg.dsn, ydbOpts...)
	if err != nil {
		log.Fatalf("ydb open: %v", err)
	}
	defer func() { _ = db.Close(context.Background()) }()

	sel := topicoptions.ReadSelector{Path: cfg.topicPath}
	if cfg.readFromStart {
		sel.ReadFrom = time.Unix(0, 0).UTC()
	}

	reader, err := db.Topic().StartReader(
		cfg.consumer,
		topicoptions.ReadSelectors{sel},
		topicoptions.WithReaderSupportSplitMergePartitions(true),
	)
	if err != nil {
		log.Fatalf("start reader: %v", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if closeErr := reader.Close(closeCtx); closeErr != nil {
			log.Printf("reader close: %v", closeErr)
		}
	}()

	if err := reader.WaitInit(ctx); err != nil {
		log.Fatalf("reader wait init: %v", err)
	}

	var (
		lastSeq   = make(map[streamKey]int64)
		total     int64
		violation error
	)

readLoop:
	for {
		if cfg.maxMessages > 0 && total >= cfg.maxMessages {
			break
		}

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break readLoop
			}
			log.Fatalf("read message: %v", err)
		}

		sk := streamKeyForMessage(msg)
		if prev, seen := lastSeq[sk]; seen {
			if msg.SeqNo <= prev {
				violation = fmt.Errorf(
					"SeqNo must strictly increase for the same partitioning key (message group id) within the same producer/partition: "+
						"key %q producer %q partition %d: got seqNo %d after %d",
					sk.messageGroupLabel(),
					msg.ProducerID,
					msg.PartitionID(),
					msg.SeqNo,
					prev,
				)
				_ = reader.Commit(msg.Context(), msg)
				break readLoop
			}
		}
		lastSeq[sk] = msg.SeqNo
		total++

		if err := reader.Commit(msg.Context(), msg); err != nil {
			log.Fatalf("commit: %v", err)
		}

		if cfg.verbose && total%1000 == 0 {
			log.Printf("messages checked: %d", total)
		}
	}

	if violation != nil {
		log.Fatalf("check failed: %v", violation)
	}
	log.Printf("ok: checked %d messages, %d distinct streams (key+producer+partition)", total, len(lastSeq))
}

// streamKey identifies a single monotonic SeqNo sequence on the server.
type streamKey struct {
	messageGroupID string
	producerID     string
	partitionID    int64
}

func streamKeyForMessage(msg *topicreader.Message) streamKey {
	return streamKey{
		messageGroupID: msg.MessageGroupID,
		producerID:     msg.ProducerID,
		partitionID:    msg.PartitionID(),
	}
}

func (k streamKey) messageGroupLabel() string {
	if k.messageGroupID != "" {
		return k.messageGroupID
	}
	return "<empty message group id>"
}

type config struct {
	dsn               string
	useEnvCredentials bool
	topicPath         string
	consumer          string
	readFromStart     bool
	maxMessages       int64
	verbose           bool
}

func (c *config) validate() error {
	if c.topicPath == "" {
		return fmt.Errorf("topic path is required (-topic)")
	}
	if c.consumer == "" {
		return fmt.Errorf("consumer name is required (-consumer)")
	}
	if c.maxMessages < 0 {
		return fmt.Errorf("max-messages must be >= 0")
	}
	return nil
}

func parseFlags() config {
	var c config

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.Usage = func() {
		out := fs.Output()
		_, _ = fmt.Fprintf(out, "Read a topic and verify SeqNo is strictly increasing per partitioning key, producer, and partition.\n\n")
		_, _ = fmt.Fprintf(out, "Usage:\n  %s [options]\n\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "Example (from the examples module root):\n")
		_, _ = fmt.Fprintf(out, "  go run ./topic/topicreader/seqno-by-key/ \\\n")
		_, _ = fmt.Fprintf(out, "    -ydb 'grpc://localhost:2136/local' \\\n")
		_, _ = fmt.Fprintf(out, "    -topic 'database_path/topic_name' \\\n")
		_, _ = fmt.Fprintf(out, "    -consumer 'my_consumer'\n\n")
		_, _ = fmt.Fprintf(out, "Options:\n")
		fs.PrintDefaults()
	}

	fs.StringVar(&c.dsn, "ydb", "grpc://localhost:2136/local", "YDB connection string")
	fs.BoolVar(&c.useEnvCredentials, "use-env-credentials", false, "Use credentials from environment (ydb-go-sdk-auth-environ)")

	fs.StringVar(&c.topicPath, "topic", "", "Topic path (required)")
	fs.StringVar(&c.consumer, "consumer", "", "Consumer name (required)")
	fs.BoolVar(&c.readFromStart, "read-from-start", true, "Read from topic beginning (ReadFrom epoch); if false, use consumer offsets only")
	fs.Int64Var(&c.maxMessages, "max-messages", 0, "Stop after this many messages (0 = until interrupted)")
	fs.BoolVar(&c.verbose, "verbose", false, "Log progress every 1000 messages")

	if err := fs.Parse(os.Args[1:]); err != nil {
		fs.Usage()
		os.Exit(2)
	}

	return c
}
