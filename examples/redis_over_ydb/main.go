// Sample RESP server backed by YDB (see README.md).
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func main() {
	addr := flag.String("addr", ":6379", "listen address (host:port)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := os.Getenv("YDB_CONNECTION_STRING")
	if dsn == "" {
		log.Fatal("set YDB_CONNECTION_STRING, e.g. grpc://localhost:2136/local")
	}

	db, err := ydb.Open(ctx, dsn)
	if err != nil {
		log.Fatalf("ydb open: %v", err)
	}
	defer func() { _ = db.Close(ctx) }()

	kv := sugar.NewKV(ctx, db)
	if tablePath := strings.TrimSpace(os.Getenv(envTablePath)); tablePath != "" {
		kv = kv.WithTable(tablePath)
	}

	if api := strings.TrimSpace(os.Getenv(envAPI)); api != "" {
		switch api := strings.ToLower(api); api {
		case "query":
			kv = kv.WithQueryAPI()
		case "kv":
			kv = kv.WithKVAPI()
		default:
			log.Fatalf("wrong YDB API: %q", api)
		}
	}

	client, err := kv.Build()
	if err != nil {
		log.Fatalf("new client: %v", err)
	}

	if lru := strings.TrimSpace(os.Getenv(envTableLRU)); lru != "" { //nolint:nestif
		v, err := strconv.Atoi(lru)
		if err != nil {
			log.Fatalf("wrong YDB KV TABLE LRU: %q", lru)
		}
		if v > 0 {
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(10 * time.Second):
						if keys, err := client.KeysSortedByLastUsage(ctx, uint64(v)); err != nil {
							log.Printf("cannot get keys sorted by last usage: %v", err)
						} else {
							if n, err := client.Del(ctx, keys...); err != nil {
								log.Printf("cannot del keys sorted by last usage: %v", err)
							} else if n > 0 {
								log.Printf("deleted %d old keys", n)
							}
						}
					}
				}
			}()
		}
	}

	srv := NewServer("tcp", *addr, client)
	ready := make(chan error, 1)
	go func() {
		if err := srv.Start(ready); err != nil {
			log.Printf("server exited: %v", err)
		}
	}()

	if err := <-ready; err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("listening on %s (api=%s); press Ctrl+C to stop",
		*addr, client.API().String(),
	)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	signal.Stop(sig)
	cancel()
	log.Println("shutdown signal received, stopping server...")

	if err := srv.Stop(); err != nil {
		log.Printf("server stop: %v", err)
	}

	log.Println("shutdown complete")
}
