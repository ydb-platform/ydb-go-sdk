// Sample RESP server backed by YDB (see README.md).
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func main() {
	addr := flag.String("addr", ":6379", "listen address (host:port)")
	flag.Parse()

	ctx := context.Background()
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
			kv = kv.WithAPI(sugar.KV_API_QUERY)
		case "kv":
			kv = kv.WithAPI(sugar.KV_API_KEY_VALUE)
		default:
			log.Fatalf("wrong YDB API: %q", api)
		}
	}

	client, err := kv.Build()
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer client.Close()

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
	log.Println("shutdown signal received, stopping server...")

	if err := srv.Stop(); err != nil {
		log.Printf("server stop: %v", err)
	}

	client.Close()
	log.Println("shutdown complete")
}
