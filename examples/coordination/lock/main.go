package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"
)

var (
	dsn       string
	path      string
	semaphore string
)

func init() {
	required := []string{"ydb", "path", "semaphore"}
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}
	flagSet.StringVar(&dsn,
		"ydb", "",
		"YDB connection string",
	)
	flagSet.StringVar(&path,
		"path", "",
		"coordination node path",
	)
	flagSet.StringVar(&semaphore,
		"semaphore", "",
		"semaphore name",
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
	context.WithCancel(context.Background())
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	db, err := ydb.Open(ctx, dsn,
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	err = db.Coordination().CreateNode(ctx, path, coordination.NodeConfig{
		Path:                     "",
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      coordination.ConsistencyModeStrict,
		AttachConsistencyMode:    coordination.ConsistencyModeStrict,
		RatelimiterCountersMode:  coordination.RatelimiterCountersModeDetailed,
	})
	if err != nil {
		fmt.Printf("failed to create coordination node: %v\n", err)
		return
	}

	for {
		fmt.Println("waiting for a lock...")

		session, err := db.Coordination().OpenSession(ctx, path)
		if err != nil {
			fmt.Println("failed to open session", err)
			return
		}

		lease, err := session.AcquireSemaphore(ctx, semaphore, coordination.Exclusive, options.WithEphemeral(true))
		if err != nil {
			fmt.Printf("failed to acquire semaphore: %v\n", err)
			_ = session.Close(ctx)
			continue
		}

		fmt.Println("the lock is acquired")

		wg := sync.WaitGroup{}
		wg.Add(1)
		go doWork(lease.Context(), &wg)

		select {
		case <-ctx.Done():
			fmt.Println("exiting")
			return
		case <-lease.Context().Done():
		}

		fmt.Println("the lock is released")
		wg.Wait()
	}
}

func doWork(ctx context.Context, wg *sync.WaitGroup) {
	fmt.Println("starting work")

loop:
	for {
		fmt.Println("work is in progress...")

		select {
		case <-ctx.Done():
			break loop
		case <-time.After(time.Second):
		}
	}

	fmt.Println("suspending work")

	wg.Done()
}
