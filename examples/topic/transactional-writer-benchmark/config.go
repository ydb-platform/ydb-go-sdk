package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type writerMode string

const (
	writerModeSingle writerMode = "single"
	writerModeMany   writerMode = "many"
)

type routingMode string

const (
	routingModeKey         routingMode = "key"
	routingModeBoundedKey  routingMode = "bounded-key"
	routingModePartitionID routingMode = "partition-id"
)

type config struct {
	DSN                    string
	TopicPath              string
	TablePath              string
	RunID                  string
	Label                  string
	ProducerIDPrefix       string
	Mode                   writerMode
	Routing                routingMode
	AutoSeqNo              bool
	QueryRetries           bool
	Duration               time.Duration
	Warmup                 time.Duration
	TransactionTimeout     time.Duration
	Concurrency            int
	MessagesPerTx          int
	MessageSize            int
	LatencySampleEvery     int
	MaxErrors              int
	PreparePartitions      int64
	AutoSplit              bool
	AutoSplitMaxPartitions int64
	AutoSplitWriteSpeed    int64
	AutoSplitBurstBytes    int64
	AutoSplitUpUtilization int
	AutoSplitStabilization time.Duration
	AutoSplitPollInterval  time.Duration
	Anonymous              bool
	SkipTableWrite         bool
	Prepare                bool
	PrepareOnly            bool
	CPUProfile             string
	HeapProfile            string
}

func parseConfig(args []string, stderr io.Writer) (config, error) {
	runID := defaultRunID()
	cfg := config{
		RunID:            runID,
		ProducerIDPrefix: runID,
	}
	var (
		modeName    string
		routingName string
	)

	flags := flag.NewFlagSet("transactional-writer-benchmark", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.DSN, "dsn", "", "YDB connection string")
	flags.StringVar(&cfg.TopicPath, "topic", "", "topic path relative to the database or absolute")
	flags.StringVar(&cfg.TablePath, "table", "", "state table path relative to the database or absolute")
	flags.StringVar(&cfg.RunID, "run-id", cfg.RunID, "value stored in the table primary key and report")
	flags.StringVar(&cfg.Label, "label", "", "free-form comparison label, for example master or pooled")
	flags.StringVar(
		&cfg.ProducerIDPrefix,
		"producer-id-prefix",
		cfg.ProducerIDPrefix,
		"stable producer prefix; pass an empty value to leave producer ID unspecified",
	)
	flags.StringVar(&modeName, "mode", string(writerModeMany), "writer mode: many or single")
	flags.StringVar(
		&routingName,
		"routing",
		string(routingModeKey),
		"multiwriter routing: key, bounded-key, or partition-id (ignored in single mode)",
	)
	flags.BoolVar(&cfg.AutoSeqNo, "auto-seq-no", true, "let the SDK assign sequence numbers")
	flags.BoolVar(&cfg.QueryRetries, "query-retries", true, "retry retryable Query transaction errors until the transaction deadline")
	flags.DurationVar(&cfg.Duration, "duration", 5*time.Second, "measurement duration")
	flags.DurationVar(&cfg.Warmup, "warmup", 2*time.Second, "warmup duration excluded from the result")
	flags.DurationVar(
		&cfg.TransactionTimeout,
		"transaction-timeout",
		30*time.Second,
		"deadline for one transaction",
	)
	flags.IntVar(&cfg.Concurrency, "concurrency", 4, "number of concurrent transaction workers")
	flags.IntVar(&cfg.MessagesPerTx, "messages-per-tx", 1, "topic messages written by each transaction")
	flags.IntVar(&cfg.MessageSize, "message-size", 1024, "payload bytes in each topic message")
	flags.IntVar(
		&cfg.LatencySampleEvery,
		"latency-sample-every",
		1,
		"record latency for every Nth committed transaction",
	)
	flags.IntVar(&cfg.MaxErrors, "max-errors", 10, "stop a phase after this many final transaction errors")
	flags.Int64Var(
		&cfg.PreparePartitions,
		"prepare-partitions",
		1,
		"fixed partition count used by --prepare",
	)
	flags.BoolVar(&cfg.AutoSplit, "auto-split", false, "prepare and monitor a multiwriter topic with automatic partition splitting")
	flags.Int64Var(&cfg.AutoSplitMaxPartitions, "auto-split-max-partitions", 64, "maximum active partitions for --auto-split")
	flags.Int64Var(&cfg.AutoSplitWriteSpeed, "auto-split-write-speed", 1<<20, "per-partition write speed in bytes/s for --auto-split")
	flags.Int64Var(&cfg.AutoSplitBurstBytes, "auto-split-burst-bytes", 1<<20, "per-partition write burst bytes for --auto-split")
	flags.IntVar(&cfg.AutoSplitUpUtilization, "auto-split-up-utilization", 2, "write utilization percentage that triggers scale-up")
	flags.DurationVar(&cfg.AutoSplitStabilization, "auto-split-stabilization", 2*time.Second, "write-speed stabilization window for --auto-split")
	flags.DurationVar(&cfg.AutoSplitPollInterval, "auto-split-poll-interval", 250*time.Millisecond, "topology polling interval for --auto-split")
	flags.BoolVar(&cfg.Anonymous, "anonymous", false, "disable authentication for a local YDB")
	flags.BoolVar(
		&cfg.SkipTableWrite,
		"skip-table-write",
		false,
		"measure a Topic-only transaction instead of UPSERT + Topic write",
	)
	flags.BoolVar(&cfg.Prepare, "prepare", false, "create the benchmark table and topic if absent")
	flags.BoolVar(&cfg.PrepareOnly, "prepare-only", false, "prepare the schema and exit")
	flags.StringVar(&cfg.CPUProfile, "cpu-profile", "", "write a CPU profile for the measured phase")
	flags.StringVar(&cfg.HeapProfile, "heap-profile", "", "write a heap profile after the measured phase")

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}

	cfg.Mode = writerMode(modeName)
	cfg.Routing = routingMode(routingName)
	var durationExplicit, warmupExplicit, routingExplicit, maxErrorsExplicit bool
	flags.Visit(func(flag *flag.Flag) {
		switch flag.Name {
		case "duration":
			durationExplicit = true
		case "warmup":
			warmupExplicit = true
		case "routing":
			routingExplicit = true
		case "max-errors":
			maxErrorsExplicit = true
		}
	})
	if cfg.AutoSplit {
		if !durationExplicit {
			cfg.Duration = 2 * time.Minute
		}
		if !warmupExplicit {
			cfg.Warmup = 0
		}
		if !routingExplicit {
			cfg.Routing = routingModeBoundedKey
		}
		if !maxErrorsExplicit {
			cfg.MaxErrors = 100
		}
	}
	if cfg.PrepareOnly {
		cfg.Prepare = true
	}

	if err := cfg.validate(); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func (c config) validate() error {
	switch {
	case c.DSN == "":
		return fmt.Errorf("--dsn is required")
	case c.TopicPath == "":
		return fmt.Errorf("--topic is required")
	case !c.SkipTableWrite && c.TablePath == "":
		return fmt.Errorf("--table is required unless --skip-table-write is set")
	case c.RunID == "":
		return fmt.Errorf("--run-id cannot be empty")
	case c.Mode != writerModeSingle && c.Mode != writerModeMany:
		return fmt.Errorf("--mode must be %q or %q", writerModeMany, writerModeSingle)
	case c.Routing != routingModeKey && c.Routing != routingModeBoundedKey && c.Routing != routingModePartitionID:
		return fmt.Errorf("--routing must be %q, %q, or %q", routingModeKey, routingModeBoundedKey, routingModePartitionID)
	case c.AutoSplit && c.Mode != writerModeMany:
		return fmt.Errorf("--auto-split requires --mode=%s", writerModeMany)
	case c.AutoSplit && c.Routing != routingModeBoundedKey:
		return fmt.Errorf("--auto-split requires --routing=%s", routingModeBoundedKey)
	case c.AutoSplit && c.PreparePartitions != 1:
		return fmt.Errorf("--auto-split requires --prepare-partitions=1")
	case c.AutoSplit && c.AutoSplitMaxPartitions <= 1:
		return fmt.Errorf("--auto-split-max-partitions must be greater than 1")
	case c.AutoSplit && c.AutoSplitWriteSpeed <= 0:
		return fmt.Errorf("--auto-split-write-speed must be positive")
	case c.AutoSplit && c.AutoSplitBurstBytes <= 0:
		return fmt.Errorf("--auto-split-burst-bytes must be positive")
	case c.AutoSplit && (c.AutoSplitUpUtilization <= 0 || c.AutoSplitUpUtilization > 100):
		return fmt.Errorf("--auto-split-up-utilization must be between 1 and 100")
	case c.AutoSplit && c.AutoSplitStabilization <= 0:
		return fmt.Errorf("--auto-split-stabilization must be positive")
	case c.AutoSplit && c.AutoSplitPollInterval <= 0:
		return fmt.Errorf("--auto-split-poll-interval must be positive")
	case c.Duration <= 0 && !c.PrepareOnly:
		return fmt.Errorf("--duration must be positive")
	case c.Warmup < 0:
		return fmt.Errorf("--warmup cannot be negative")
	case c.TransactionTimeout <= 0:
		return fmt.Errorf("--transaction-timeout must be positive")
	case c.Concurrency <= 0:
		return fmt.Errorf("--concurrency must be positive")
	case c.MessagesPerTx <= 0:
		return fmt.Errorf("--messages-per-tx must be positive")
	case c.MessageSize < 0:
		return fmt.Errorf("--message-size cannot be negative")
	case c.LatencySampleEvery <= 0:
		return fmt.Errorf("--latency-sample-every must be positive")
	case c.MaxErrors <= 0:
		return fmt.Errorf("--max-errors must be positive")
	case c.PreparePartitions <= 0:
		return fmt.Errorf("--prepare-partitions must be positive")
	case strings.ContainsRune(c.TopicPath, '`'):
		return fmt.Errorf("--topic cannot contain a backtick")
	case strings.ContainsRune(c.TablePath, '`'):
		return fmt.Errorf("--table cannot contain a backtick")
	}

	return nil
}

func defaultRunID() string {
	return fmt.Sprintf(
		"go-%s-%d",
		time.Now().UTC().Format("20060102T150405.000000000Z"),
		os.Getpid(),
	)
}
