package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

var ErrWrongArgs = errors.New("error: wrong args")

type Config struct {
	Mode AppMode

	Endpoint string
	DB       string

	Table string

	PartitionsCount  uint64
	InitialDataCount uint64

	PushGateway  string
	ReportPeriod int

	ReadRPS     int
	ReadTimeout int

	WriteRPS     int
	WriteTimeout int

	Time         int
	ShutdownTime int
}

func New() (cfg Config, err error) {
	if len(os.Args) < 2 {
		fmt.Print(mainHelp)
		return cfg, ErrWrongArgs
	}

	fs := flag.FlagSet{}

	switch os.Args[1] {
	case "create":
		if len(os.Args) < 4 {
			fmt.Print(createHelp)
			return cfg, ErrWrongArgs
		}

		fs.Uint64Var(
			&cfg.PartitionsCount, "partitions-count", 6, "amount of partitions in table creation")
		fs.Uint64Var(
			&cfg.InitialDataCount, "initial-data-count", 1000, "amount of initially created rows")

		cfg.Mode = CreateMode
	case "cleanup":
		if len(os.Args) < 4 {
			fmt.Print(cleanupHelp)
			return cfg, ErrWrongArgs
		}

		cfg.Mode = CleanupMode
	case "run":
		if len(os.Args) < 4 {
			fmt.Print(runHelp)
			return cfg, ErrWrongArgs
		}

		cfg.Mode = RunMode

		fs.Uint64Var(
			&cfg.InitialDataCount, "initial-data-count", 1000, "amount of initially created rows")

		fs.StringVar(&cfg.PushGateway, "prom-pgw", "", "prometheus push gateway")
		fs.IntVar(&cfg.ReportPeriod, "report-period", 250, "prometheus push period in milliseconds")

		fs.IntVar(&cfg.ReadRPS, "read-rps", 1000, "read RPS")
		fs.IntVar(&cfg.WriteRPS, "write-rps", 100, "write RPS")
		fs.IntVar(&cfg.ReadTimeout, "read-timeout", 10000, "read timeout milliseconds")

		fs.IntVar(&cfg.Time, "time", 600, "run time in seconds")
		fs.IntVar(&cfg.ShutdownTime, "shutdown-time", 30, "time to wait before force kill workers")
	default:
		fmt.Print(mainHelp)
		return cfg, ErrWrongArgs
	}

	cfg.Endpoint = os.Args[2]
	cfg.DB = os.Args[3]

	fs.StringVar(&cfg.Table, "t", "testingTable", "table name")

	fs.IntVar(&cfg.WriteTimeout, "write-timeout", 10000, "write timeout milliseconds")

	err = fs.Parse(os.Args[4:])
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}
