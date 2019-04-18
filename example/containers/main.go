package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/internal/traceutil"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

func main() {
	var (
		prefix   string
		endpoint string
	)
	config := new(ydb.DriverConfig)
	flag.StringVar(&config.Database, "database", "", "name of the database to use")
	flag.StringVar(&endpoint, "endpoint", "", "endpoint url to use")
	flag.StringVar(&prefix, "path", "", "tables path")
	flag.Parse()

	config.Credentials = ydb.AuthTokenCredentials{
		AuthToken: os.Getenv("YDB_TOKEN"),
	}

	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 64<<10)
			buf = buf[:runtime.Stack(buf, false)]
			log.Fatalf("panic recovered: %v\n%s", err, buf)
		}
		os.Exit(0)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		driverTrace ydb.DriverTrace
		clientTrace table.ClientTrace
	)
	traceutil.Stub(&driverTrace, func(name string, args ...interface{}) {
		log.Printf("[driver] %q %v", name, traceutil.ClearContext(args))
	})
	traceutil.Stub(&clientTrace, func(name string, args ...interface{}) {
		log.Printf("[client] %q %v", name, traceutil.ClearContext(args))
	})
	ctx = ydb.WithDriverTrace(ctx, driverTrace)
	ctx = table.WithClientTrace(ctx, clientTrace)

	go processSignals(map[os.Signal]func(){
		syscall.SIGINT: func() {
			if ctx.Err() != nil {
				log.Fatal("forced quit")
			}
			cancel()
		},
	})

	log.SetFlags(0)

	if err := run(ctx, endpoint, prefix, config); err != nil {
		log.Fatal(err)
	}
}

func processSignals(m map[os.Signal]func()) {
	ch := make(chan os.Signal, len(m))
	for sig := range m {
		signal.Notify(ch, sig)
	}
	for sig := range ch {
		log.Printf("signal received: %s", sig)
		m[sig]()
	}
}
