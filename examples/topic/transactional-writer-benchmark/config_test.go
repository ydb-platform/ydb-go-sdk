package main

import (
	"io"
	"testing"
	"time"
)

func TestParseConfigBaselineDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--dsn", "grpc://localhost:2136/local",
		"--topic", "benchmark-topic",
		"--table", "benchmark-table",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}

	if cfg.Mode != writerModeMany {
		t.Fatalf("Mode = %q, want %q", cfg.Mode, writerModeMany)
	}
	if cfg.Concurrency != 4 {
		t.Fatalf("Concurrency = %d, want 4", cfg.Concurrency)
	}
	if cfg.MessagesPerTx != 1 {
		t.Fatalf("MessagesPerTx = %d, want 1", cfg.MessagesPerTx)
	}
	if cfg.MessageSize != 1024 {
		t.Fatalf("MessageSize = %d, want 1024", cfg.MessageSize)
	}
	if cfg.Warmup != 2*time.Second {
		t.Fatalf("Warmup = %s, want 2s", cfg.Warmup)
	}
	if cfg.Duration != 5*time.Second {
		t.Fatalf("Duration = %s, want 5s", cfg.Duration)
	}
	if cfg.Routing != routingModeKey {
		t.Fatalf("Routing = %q, want %q", cfg.Routing, routingModeKey)
	}
	if !cfg.AutoSeqNo {
		t.Fatal("AutoSeqNo = false, want true")
	}
	if !cfg.QueryRetries {
		t.Fatal("QueryRetries = false, want true")
	}
	if cfg.ProducerIDPrefix == "" {
		t.Fatal("ProducerIDPrefix is empty, want a per-run stable prefix")
	}
}

func TestParseConfigSupportsDisablingQueryRetriesForDiagnostics(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--dsn", "grpc://localhost:2136/local",
		"--topic", "benchmark-topic",
		"--table", "benchmark-table",
		"--query-retries=false",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.QueryRetries {
		t.Fatal("QueryRetries = true, want false")
	}
}

func TestParseConfigSupportsSingleWriterAndManualSequenceNumbers(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--dsn", "grpc://localhost:2136/local",
		"--topic", "benchmark-topic",
		"--table", "benchmark-table",
		"--mode", "single",
		"--auto-seq-no=false",
		"--producer-id-prefix=",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.Mode != writerModeSingle {
		t.Fatalf("Mode = %q, want %q", cfg.Mode, writerModeSingle)
	}
	if cfg.AutoSeqNo {
		t.Fatal("AutoSeqNo = true, want false")
	}
	if cfg.ProducerIDPrefix != "" {
		t.Fatalf("ProducerIDPrefix = %q, want empty", cfg.ProducerIDPrefix)
	}
}

func TestParseConfigPrepareOnlyAllowsZeroDuration(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--dsn", "grpc://localhost:2136/local",
		"--topic", "benchmark-topic",
		"--table", "benchmark-table",
		"--prepare-only",
		"--duration", "0",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if !cfg.Prepare {
		t.Fatal("Prepare = false, want true")
	}
}

func TestParseConfigRejectsInvalidMode(t *testing.T) {
	t.Parallel()

	_, err := parseConfig([]string{
		"--dsn", "grpc://localhost:2136/local",
		"--topic", "benchmark-topic",
		"--table", "benchmark-table",
		"--mode", "invalid",
	}, io.Discard)
	if err == nil {
		t.Fatal("parseConfig() error = nil, want invalid mode error")
	}
}

func TestParseConfigAutoSplitDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--dsn", "grpc://localhost:2136/local",
		"--topic", "benchmark-topic",
		"--table", "benchmark-table",
		"--auto-split",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}

	if cfg.Mode != writerModeMany {
		t.Fatalf("Mode = %q, want %q", cfg.Mode, writerModeMany)
	}
	if cfg.Routing != routingModeBoundedKey {
		t.Fatalf("Routing = %q, want %q", cfg.Routing, routingModeBoundedKey)
	}
	if cfg.Warmup != 0 {
		t.Fatalf("Warmup = %s, want 0", cfg.Warmup)
	}
	if cfg.Duration != 2*time.Minute {
		t.Fatalf("Duration = %s, want 2m", cfg.Duration)
	}
	if cfg.AutoSplitMaxPartitions != 64 {
		t.Fatalf("AutoSplitMaxPartitions = %d, want 64", cfg.AutoSplitMaxPartitions)
	}
	if cfg.AutoSplitWriteSpeed != 1<<20 {
		t.Fatalf("AutoSplitWriteSpeed = %d, want %d", cfg.AutoSplitWriteSpeed, 1<<20)
	}
	if cfg.AutoSplitUpUtilization != 2 {
		t.Fatalf("AutoSplitUpUtilization = %d, want 2", cfg.AutoSplitUpUtilization)
	}
	if cfg.AutoSplitStabilization != 2*time.Second {
		t.Fatalf("AutoSplitStabilization = %s, want 2s", cfg.AutoSplitStabilization)
	}
	if cfg.MaxErrors != 100 {
		t.Fatalf("MaxErrors = %d, want 100", cfg.MaxErrors)
	}
}

func TestParseConfigAutoSplitRejectsIncompatibleWriter(t *testing.T) {
	t.Parallel()

	_, err := parseConfig([]string{
		"--dsn", "grpc://localhost:2136/local",
		"--topic", "benchmark-topic",
		"--table", "benchmark-table",
		"--auto-split",
		"--mode", "single",
	}, io.Discard)
	if err == nil {
		t.Fatal("parseConfig() error = nil, want incompatible writer error")
	}
}

func TestParseConfigAutoSplitRejectsExplicitKafkaHashRouting(t *testing.T) {
	t.Parallel()

	_, err := parseConfig([]string{
		"--dsn", "grpc://localhost:2136/local",
		"--topic", "benchmark-topic",
		"--table", "benchmark-table",
		"--auto-split",
		"--routing", "key",
	}, io.Discard)
	if err == nil {
		t.Fatal("parseConfig() error = nil, want incompatible routing error")
	}
}
