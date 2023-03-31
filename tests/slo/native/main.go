package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/rs/zerolog"
	"log"
	"os"
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
	readWorkers  = 30
	writeWorkers = 10
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	cfg, err := configs.NewConfig()
	if errors.Is(err, configs.ErrWrongArgs) || errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		panic(fmt.Errorf("create config failed: %w", err))
	}

	st, err := storage.NewStorage(context.Background(), cfg, logger)
	if err != nil {
		panic(fmt.Errorf("ceate storage failed: %w", err))
	}
	defer func() {
		_ = st.Close(context.Background())
	}()

	log.Print("db init ok")

	switch cfg.Mode {
	case configs.CreateMode:
		err = st.CreateTable(context.Background())
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}
		log.Print("create table ok")
		return
	case configs.CleanupMode:
		err = st.DropTable(context.Background())
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}
		log.Print("drop table ok")
		return
	}

	m, err := metrics.NewMetrics(cfg.PushGateway, "native")
	if err != nil {
		log.Printf("create metrics failed: %v", err)
		return
	}

	err = m.Reset()
	if err != nil {
		log.Printf("metrics reset failed: %v", err)
		return
	}

	log.Print("metrics init ok")

	gen := generator.NewGenerator(10, 20)

	entries := make(generator.Entries)
	entryIDs := make([]generator.EntryID, 0)
	entriesMutex := sync.RWMutex{}

	workChan := make(chan struct{})

	readRL := rate.New(cfg.ReadRPS, time.Second)
	for i := 0; i < cfg.ReadRPS; i++ {
		readRL.Wait()
	}
	for i := 0; i < readWorkers; i++ {
		go workers.Read(&st, readRL, m, entries, &entryIDs, &entriesMutex, workChan)
	}

	writeRL := rate.New(cfg.WriteRPS, time.Second)
	for i := 0; i < cfg.WriteRPS; i++ {
		writeRL.Wait()
	}
	for i := 0; i < writeWorkers; i++ {
		go workers.Write(&st, writeRL, m, gen, entries, &entryIDs, &entriesMutex, workChan)
	}

	metricsRL := rate.New(1, time.Duration(cfg.ReportPeriod))
	go workers.Metrics(metricsRL, m)

	time.Sleep(time.Duration(cfg.Time) * time.Second)

	log.Print("shutdown started")
	close(workChan)
	log.Print("waiting for workers")

	time.AfterFunc(time.Duration(cfg.ShutdownTime)*time.Second, func() {
		panic(errors.New("time limit exceed, exiting"))
	})

	for m.ActiveJobsCount() > 0 {
		time.Sleep(time.Millisecond)
	}

	err = m.Reset()
	if err != nil {
		log.Printf("metrics reset failed: %v", err)
	}

	log.Print("shutdown successful")
}
