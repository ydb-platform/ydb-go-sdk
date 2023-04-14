package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

var ErrWrongArgs = errors.New("wrong args")

type Config struct {
	Mode AppMode

	Endpoint string
	DB       string

	Table string

	MinPartitionsCount uint64
	MaxPartitionsCount uint64
	PartitionSize      uint64
	InitialDataCount   uint64

	PushGateway  string
	ReportPeriod int

	ReadRPS     int
	ReadTimeout int

	WriteRPS     int
	WriteTimeout int

	Time         int
	ShutdownTime int
}

func New() (*Config, error) {
	cfg := &Config{}

	if len(os.Args) < 2 {
		fmt.Print(mainHelp)
		return nil, ErrWrongArgs
	}

	fs := flag.FlagSet{}

	switch os.Args[1] {
	case "create":
		if len(os.Args) < 4 {
			fmt.Print(createHelp)
			return nil, ErrWrongArgs
		}

		cfg.Mode = CreateMode
		cfg.Time = 30

		fs.Uint64Var(&cfg.MinPartitionsCount,
			"min-partitions-count", 6, "minimum amount of partitions in table")
		fs.Uint64Var(&cfg.MaxPartitionsCount,
			"max-partitions-count", 1000, "maximum amount of partitions in table")
		fs.Uint64Var(&cfg.PartitionSize,
			"partition-size", 1, "partition size in mb")

		fs.Uint64Var(&cfg.InitialDataCount,
			"initial-data-count", 1000, "amount of initially created rows")
		fs.Uint64Var(&cfg.InitialDataCount,
			"c", 1000, "amount of initially created rows (shorthand)")
	case "cleanup":
		if len(os.Args) < 4 {
			fmt.Print(cleanupHelp)
			return nil, ErrWrongArgs
		}

		cfg.Mode = CleanupMode
		cfg.Time = 30
	case "run":
		if len(os.Args) < 4 {
			fmt.Print(runHelp)
			return nil, ErrWrongArgs
		}

		cfg.Mode = RunMode

		fs.Uint64Var(&cfg.InitialDataCount,
			"initial-data-count", 1000, "amount of initially created rows")
		fs.Uint64Var(&cfg.InitialDataCount,
			"c", 1000, "amount of initially created rows (shorthand)")

		fs.StringVar(&cfg.PushGateway, "prom-pgw", "", "prometheus push gateway")
		fs.IntVar(&cfg.ReportPeriod, "report-period", 250, "prometheus push period in milliseconds")

		fs.IntVar(&cfg.ReadRPS, "read-rps", 1000, "read RPS")
		fs.IntVar(&cfg.WriteRPS, "write-rps", 100, "write RPS")
		fs.IntVar(&cfg.ReadTimeout, "read-timeout", 10000, "read timeout milliseconds")

		fs.IntVar(&cfg.Time, "time", 600, "run time in seconds")
		fs.IntVar(&cfg.ShutdownTime, "shutdown-time", 30, "time to wait before force kill workers")
	default:
		fmt.Print(mainHelp)
		return nil, ErrWrongArgs
	}

	cfg.Endpoint = os.Args[2]
	cfg.DB = os.Args[3]

	fs.StringVar(&cfg.Table, "table-name", "testingTable", "table name")
	fs.StringVar(&cfg.Table, "t", "testingTable", "table name (shorthand)")

	fs.IntVar(&cfg.WriteTimeout, "write-timeout", 10000, "write timeout milliseconds")

	err := fs.Parse(os.Args[4:])
	if err != nil {
		return nil, err
	}

	return cfg, nil
}
