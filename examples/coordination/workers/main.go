package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"
	"golang.org/x/sync/semaphore"
)

var (
	dsn             string
	path            string
	semaphorePrefix string
	taskCount       int
	capacity        int
)

//nolint:gochecknoinits
func init() {
	required := []string{"ydb", "path"}
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
	flagSet.StringVar(&semaphorePrefix,
		"semaphore-prefix", "job-",
		"semaphore prefix",
	)
	flagSet.IntVar(&taskCount,
		"tasks", 10,
		"the number of tasks",
	)
	flagSet.IntVar(&capacity,
		"capacity", 4,
		"the maximum number of tasks a worker can run",
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
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := ydb.Open(ctx, dsn,
		environ.WithEnvironCredentials(),
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

	tasks := make([]string, taskCount)
	for i := 0; i < taskCount; i++ {
		tasks[i] = fmt.Sprintf("%s%d", semaphorePrefix, i)
	}
	rand.Shuffle(taskCount, func(i int, j int) {
		tasks[i], tasks[j] = tasks[j], tasks[i]
	})

	fmt.Println("starting tasks")
	for {
		session, err := db.Coordination().Session(ctx, path)
		if err != nil {
			fmt.Println("failed to open session", err)

			return
		}

		semaphoreCtx, semaphoreCancel := context.WithCancel(ctx)
		wg := sync.WaitGroup{}
		wg.Add(taskCount)
		leaseChan := make(chan *LeaseInfo)
		sem := semaphore.NewWeighted(int64(capacity))

		for _, name := range tasks {
			go awaitSemaphore(semaphoreCtx, &wg, session, name, leaseChan, sem, semaphoreCancel)
		}

		tasksStarted := 0
	loop:
		for {
			select {
			case <-semaphoreCtx.Done():
				break loop
			case lease := <-leaseChan:
				go doWork(lease.lease, lease.semaphoreName)
				tasksStarted++
				if tasksStarted == capacity {
					break loop
				}
			case <-ctx.Done():
				fmt.Println("exiting")

				return
			}
		}

		fmt.Println("all workers are started")

		semaphoreCancel()
		wg.Wait()

		select {
		case <-ctx.Done():
			fmt.Println("exiting")

			return
		case <-session.Context().Done():
		}
	}
}

type LeaseInfo struct {
	lease         coordination.Lease
	semaphoreName string
}

func awaitSemaphore(
	ctx context.Context,
	done *sync.WaitGroup,
	session coordination.Session,
	semaphoreName string,
	leaseChan chan *LeaseInfo,
	sem *semaphore.Weighted,
	cancel context.CancelFunc) {
	defer done.Done()

	lease, err := session.AcquireSemaphore(
		ctx,
		semaphoreName,
		coordination.Exclusive,
		options.WithEphemeral(true),
	)
	if err != nil {
		if ctx.Err() != nil {
			return
		}

		fmt.Println("failed to acquire semaphore", err)
		cancel()

		return
	}

	if sem.TryAcquire(1) {
		leaseChan <- &LeaseInfo{lease: lease, semaphoreName: semaphoreName}
	} else {
		err := lease.Release()
		if err != nil {
			fmt.Println("failed to release semaphore", err)
			cancel()
		}
	}
}

func doWork(
	lease coordination.Lease,
	name string) {
	fmt.Printf("worker %s: starting\n", name)

	for {
		select {
		case <-lease.Context().Done():
			fmt.Printf("worker %s: done\n", name)
			err := lease.Release()
			if err != nil {
				fmt.Println("failed to release semaphore", err)
			}

			return
		case <-time.After(time.Second):
		}

		fmt.Printf("worker %s: in progress\n", name)
	}
}
