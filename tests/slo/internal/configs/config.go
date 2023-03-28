package configs

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
	YDBToken string

	Table string

	PushGateway  string
	ReportPeriod int

	ReadRPS     int
	ReadTimeout int

	WriteRPS     int
	WriteTimeout int

	Time int
}

func NewConfig() (cfg Config, err error) {
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

		fs.StringVar(&cfg.PushGateway, "prom-pgw", "", "prometheus push gateway")
		fs.IntVar(&cfg.ReportPeriod, "report-period", 250, "prometheus push period in milliseconds")

		fs.IntVar(&cfg.ReadRPS, "read-rps", 1000, "read RPS")
		fs.IntVar(&cfg.ReadTimeout, "read-timeout", 10000, "read timeout milliseconds")

		fs.IntVar(&cfg.WriteRPS, "write-rps", 100, "write RPS")
		fs.IntVar(&cfg.WriteTimeout, "write-timeout", 10000, "write timeout milliseconds")

		fs.IntVar(&cfg.Time, "time", 600, "run time in seconds")
	default:
		fmt.Print(mainHelp)
		return
	}

	cfg.Endpoint = os.Args[2]
	cfg.DB = os.Args[3]

	fs.StringVar(&cfg.YDBToken, "a", "", "YDB access token credentials")
	fs.StringVar(&cfg.Table, "t", "testingTable", "table name")

	err = fs.Parse(os.Args[4:])
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}
