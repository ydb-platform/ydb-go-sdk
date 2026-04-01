// Command topicmultiwriter runs a load generator against a YDB topic using the
// multi-partition writer (topicmultiwriter). Each writer uses its own goroutine
// and a distinct producer ID prefix.
//
// Run template (from the examples module directory, github.com/ydb-platform/ydb-go-sdk/examples):
//
//	go run ./topic/topicmultiwriter/ \
//	  -ydb 'grpc://localhost:2136/local' \
//	  -topic 'database_path/topic_name' \
//	  -min-bytes 64 \
//	  -max-bytes 4096 \
//	  -duration 5m \
//	  -writers 8
//
// With authentication from the environment (see ydb-go-sdk-auth-environ), add:
//
//	-use-env-credentials
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		log.Fatalf("invalid arguments: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	var ydbOpts []ydb.Option
	if cfg.useEnvCredentials {
		ydbOpts = append(ydbOpts, environ.WithEnvironCredentials())
	}

	db, err := ydb.Open(ctx, cfg.dsn, ydbOpts...)
	if err != nil {
		log.Fatalf("ydb open: %v", err)
	}
	defer func() { _ = db.Close(context.Background()) }()

	topicClient := db.Topic()

	var (
		wg              sync.WaitGroup
		messagesWritten atomic.Int64
		bytesWritten    atomic.Int64
		writeErrors     atomic.Int64
		firstErr        atomic.Value // error
	)

	go printThroughputLoop(ctx, &messagesWritten, &bytesWritten)

	for w := range cfg.writers {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			if err := runWriter(ctx, topicClient, cfg, writerID, &messagesWritten, &bytesWritten, &writeErrors, &firstErr); err != nil {
				storeFirstError(&firstErr, err)
			}
		}(w)
	}

	wg.Wait()

	log.Printf(
		"done: messages=%d payload_bytes=%d write_errors=%d duration=%s",
		messagesWritten.Load(),
		bytesWritten.Load(),
		writeErrors.Load(),
		cfg.duration,
	)
	if v := firstErr.Load(); v != nil {
		if err, _ := v.(error); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			log.Fatalf("writer failed: %v", err)
		}
	}
}

func printThroughputLoop(ctx context.Context, messagesWritten, bytesWritten *atomic.Int64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var prevMsg, prevBytes int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msg := messagesWritten.Load()
			b := bytesWritten.Load()
			dMsg := msg - prevMsg
			dBytes := b - prevBytes
			prevMsg, prevBytes = msg, b
			log.Printf(
				"throughput (last 1s): %d msg/s, %d byte/s (%.2f MiB/s)",
				dMsg, dBytes, float64(dBytes)/(1024*1024),
			)
		}
	}
}

func storeFirstError(firstErr *atomic.Value, err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	firstErr.CompareAndSwap(nil, err)
}

func runWriter(
	ctx context.Context,
	topicClient topic.Client,
	cfg config,
	writerID int,
	messagesWritten, bytesWritten, writeErrors *atomic.Int64,
	firstErr *atomic.Value,
) error {
	producerPrefix := fmt.Sprintf("topicmultiwriter-workload-%d-%s", writerID, uuid.NewString())

	writer, err := topicClient.StartWriter(
		cfg.topicPath,
		topicoptions.WithWriterSetAutoSeqNo(true),
		topicoptions.WithWriteToManyPartitions(
			topicoptions.WithWriterPartitionByKey(topicoptions.BoundPartitionChooser()),
			topicoptions.WithProducerIDPrefix(producerPrefix),
		),
	)
	if err != nil {
		return fmt.Errorf("writer %d StartWriter: %w", writerID, err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if closeErr := writer.Close(closeCtx); closeErr != nil {
			storeFirstError(firstErr, fmt.Errorf("writer %d Close: %w", writerID, closeErr))
		}
	}()

	if err = writer.WaitInit(ctx); err != nil {
		return fmt.Errorf("writer %d WaitInit: %w", writerID, err)
	}

	rng := rand.New(rand.NewPCG(uint64(writerID), uint64(time.Now().UnixNano())))
	deadline := time.Now().Add(cfg.duration)

	var seq int64
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("writer %d: %w", writerID, err)
		}

		size := cfg.minMsgBytes
		if cfg.maxMsgBytes > cfg.minMsgBytes {
			size = cfg.minMsgBytes + rng.IntN(cfg.maxMsgBytes-cfg.minMsgBytes+1)
		}

		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(rng.UintN(256))
		}

		msg := topicwriter.Message{
			Data: bytes.NewReader(payload),
			Key:  fmt.Sprintf("w%d-%d", writerID, seq),
		}
		seq++

		if err := writer.Write(ctx, msg); err != nil {
			writeErrors.Add(1)
			log.Printf("writer %d Write: %v", writerID, err)

			return fmt.Errorf("writer %d Write: %w", writerID, err)
		}
		messagesWritten.Add(1)
		bytesWritten.Add(int64(size))
	}

	return nil
}

type config struct {
	dsn               string
	useEnvCredentials bool
	topicPath         string
	minMsgBytes       int
	maxMsgBytes       int
	duration          time.Duration
	writers           int
}

func (c *config) validate() error {
	if c.topicPath == "" {
		return fmt.Errorf("topic path is required")
	}
	if c.minMsgBytes < 0 || c.maxMsgBytes < 0 {
		return fmt.Errorf("message size bounds must be non-negative")
	}
	if c.minMsgBytes > c.maxMsgBytes {
		return fmt.Errorf("min message size (%d) must be <= max (%d)", c.minMsgBytes, c.maxMsgBytes)
	}
	if c.writers < 1 {
		return fmt.Errorf("writers count must be >= 1")
	}
	if c.duration <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	return nil
}

func parseFlags() config {
	var c config

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.Usage = func() {
		out := fs.Output()
		_, _ = fmt.Fprintf(out, "Load generator for YDB topic multi-writer (multiple partitions).\n\n")
		_, _ = fmt.Fprintf(out, "Usage:\n  %s [options]\n\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "Example (from the examples module root):\n")
		_, _ = fmt.Fprintf(out, "  go run ./topic/topicmultiwriter/ \\\n")
		_, _ = fmt.Fprintf(out, "    -ydb 'grpc://localhost:2136/local' \\\n")
		_, _ = fmt.Fprintf(out, "    -topic 'database_path/topic_name' \\\n")
		_, _ = fmt.Fprintf(out, "    -min-bytes 64 -max-bytes 4096 -duration 5m -writers 8\n\n")
		_, _ = fmt.Fprintf(out, "Options:\n")
		fs.PrintDefaults()
	}

	fs.StringVar(&c.dsn, "ydb", "grpc://localhost:2136/local", "YDB connection string")
	fs.BoolVar(&c.useEnvCredentials, "use-env-credentials", false, "Use credentials from environment (ydb-go-sdk-auth-environ)")

	fs.StringVar(&c.topicPath, "topic", "", "Topic path (required)")
	fs.IntVar(&c.minMsgBytes, "min-bytes", 64, "Minimum message payload size in bytes")
	fs.IntVar(&c.maxMsgBytes, "max-bytes", 4096, "Maximum message payload size in bytes")
	fs.DurationVar(&c.duration, "duration", time.Minute, "How long each writer runs")
	fs.IntVar(&c.writers, "writers", 4, "Number of concurrent writers (goroutines)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		fs.Usage()
		os.Exit(2)
	}

	return c
}
