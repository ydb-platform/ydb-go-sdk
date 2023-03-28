package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"sync"
	"time"

	"slo/internal/configs"
	"slo/internal/generator"
	"slo/internal/metrics"
	"slo/internal/workers"
	"slo/native/storage"

	"github.com/beefsack/go-rate"
)

const (
	readWorkers       = 30
	writeWorkers      = 10
	shutdownTimeLimit = 30 * time.Second
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg, err := configs.NewConfig()
	if errors.Is(err, configs.ErrWrongArgs) || errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Fatalf("Create config failed: %v", err)
	}

	st, err := storage.NewStorage(context.Background(), cfg)
	if err != nil {
		log.Fatalf("create storage failed: %v", err)
	}
	defer func() {
		err := st.Close(context.Background())
		if err != nil {
			log.Printf("close storage failed: %v", err)
		}
	}()

	log.Print("db init ok")

	switch cfg.Mode {
	case configs.CreateMode:
		err = st.CreateTable(context.Background())
		if err != nil {
			log.Printf("create table failed: %v", err)
			return
		}
		log.Print("create table ok")
		return
	case configs.CleanupMode:
		err = st.DropTable(context.Background())
		if err != nil {
			log.Printf("drop table failed: %v", err)
			return
		}
		log.Print("drop table ok")
		return
	}

	m, err := metrics.NewMetrics(cfg.PushGateway)
	if err != nil {
		log.Printf("create metrics failed: %v", err)
		return
	}

	log.Print("metrics init ok")

	gen := generator.NewGenerator(10, 20)

	entries := make(generator.Entries)
	entryIDs := make([]generator.EntryID, 0)
	entriesMutex := sync.RWMutex{}

	workChan := make(chan struct{})

	readRL := rate.New(cfg.ReadRPS, time.Second)
	for i := 0; i < readWorkers; i++ {
		go workers.Read(&st, readRL, m, entries, &entryIDs, &entriesMutex, workChan)
	}

	writeRL := rate.New(cfg.WriteRPS, time.Second)
	for i := 0; i < writeWorkers; i++ {
		go workers.Write(&st, writeRL, m, gen, entries, &entryIDs, &entriesMutex, workChan)
	}

	metricsRL := rate.New(1, time.Duration(cfg.ReportPeriod))
	go workers.Metrics(metricsRL, m)

	time.Sleep(time.Duration(cfg.Time) * time.Second)

	log.Print("shutdown started")
	close(workChan)
	log.Print("waiting for workers")

	time.AfterFunc(shutdownTimeLimit, func() {
		log.Fatal("time limit exceed, exiting")
	})

	for m.ActiveJobsCount() > 0 {
		time.Sleep(time.Millisecond)
	}

	log.Print("shutdown successful")
}
