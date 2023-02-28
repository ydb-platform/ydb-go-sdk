package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
)

var (
	dsn      string
	prefix   string
	count    int
	interval time.Duration
	urls     = URLs{}
)

// URLs is a flag.Value implementation which holds URL's as string slice
type URLs struct {
	urls []string
}

// String returns string representation of URLs
func (u *URLs) String() string {
	return fmt.Sprintf("%v", u.urls)
}

// Set appends new value to URLs holder
func (u *URLs) Set(s string) error {
	u.urls = append(u.urls, s)
	return nil
}

func init() {
	required := []string{"ydb", "url"}
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s [options] -url=URL1 [-url=URL2 -url=URL3]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}
	flagSet.StringVar(&dsn,
		"ydb", "",
		"YDB connection string",
	)
	flagSet.StringVar(&prefix,
		"prefix", "",
		"tables prefix",
	)
	flagSet.Var(&urls,
		"url",
		"url for health check",
	)
	flagSet.IntVar(&count,
		"count", 0,
		"count of needed checks (one check per minute, -1 for busy loop, 0 for single shot)",
	)
	flagSet.DurationVar(&interval,
		"interval", time.Minute,
		"interval between checks",
	)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		flagSet.Usage()
		os.Exit(1)
	}
	flagSet.Visit(func(f *flag.Flag) {
		for i, arg := range required {
			if arg == f.Name {
				required = append(required[:i], required[i+1:]...)
			}
		}
	})
	if len(required) > 0 {
		fmt.Printf("\nSome required options not defined: %v\n\n", required)
		flagSet.Usage()
		os.Exit(1)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := getService(ctx, dsn, environ.WithEnvironCredentials(ctx))
	if err != nil {
		panic(fmt.Errorf("error on create service: %w", err))
	}
	defer s.Close(ctx)
	for i := 0; ; i++ {
		fmt.Println("check " + strconv.Itoa(i) + ":")
		if err := s.check(ctx, urls.urls); err != nil {
			panic(fmt.Errorf("error on check URLS: %w", err))
		}
		if count >= 0 && i == count {
			return
		}
		select {
		case <-time.After(interval):
			continue
		case <-ctx.Done():
			return
		}
	}
}
