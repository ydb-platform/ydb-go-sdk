package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	"slo/internal/configs"
	"slo/internal/generator"
	"slo/internal/metrics"
	"slo/internal/workers"
	"slo/native/storage"

	"github.com/beefsack/go-rate"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()

	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Errorf("error create logger: %w", err))
	}
	defer func() {
		_ = logger.Sync()
	}()

	cfg, err := configs.NewConfig()
	if errors.Is(err, configs.ErrWrongArgs) || errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		panic(fmt.Errorf("create config failed: %w", err))
	}

	st, err := storage.New(ctx, cfg, logger, cfg.ReadRPS+cfg.WriteRPS)
	if err != nil {
		panic(fmt.Errorf("create storage failed: %w", err))
	}
	defer func() {
		_ = st.Close(ctx)
	}()

	logger.Info("db init ok")

	switch cfg.Mode {
	case configs.CreateMode:
		err = st.CreateTable(ctx)
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}
		logger.Info("create table ok")
		return
	case configs.CleanupMode:
		err = st.DropTable(ctx)
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}
		logger.Info("drop table ok")
		return
	}

	m, err := metrics.NewMetrics(cfg.PushGateway, "native")
	if err != nil {
		logger.Error(fmt.Errorf("create metrics failed: %v", err).Error())
		return
	}

	err = m.Reset()
	if err != nil {
		logger.Error(fmt.Errorf("metrics reset failed: %v", err).Error())
		return
	}

	logger.Info("metrics init ok")

	gen := generator.NewGenerator(10, 20)

	entries := make(generator.Entries)
	entryIDs := make([]generator.EntryID, 0)
	entriesMutex := sync.RWMutex{}

	workChan := make(chan struct{})

	// todo: create workers struct
	readRL := rate.New(cfg.ReadRPS, time.Second)
	for i := 0; i < cfg.ReadRPS; i++ {
		go workers.Read(&st, readRL, m, logger, entries, &entryIDs, &entriesMutex, workChan)
	}

	writeRL := rate.New(cfg.WriteRPS, time.Second)
	for i := 0; i < cfg.WriteRPS; i++ {
		go workers.Write(&st, writeRL, m, logger, gen, entries, &entryIDs, &entriesMutex, workChan)
	}

	metricsRL := rate.New(1, time.Duration(cfg.ReportPeriod))
	go workers.Metrics(metricsRL, m, logger)

	time.Sleep(time.Duration(cfg.Time) * time.Second)

	logger.Info("shutdown started")

	close(workChan)

	logger.Info("waiting for workers")

	time.AfterFunc(time.Duration(cfg.ShutdownTime)*time.Second, func() {
		panic(errors.New("time limit exceed, exiting"))
	})

	for m.ActiveJobsCount() > 0 {
		time.Sleep(time.Millisecond)
	}

	err = m.Reset()
	if err != nil {
		logger.Error(fmt.Errorf("metrics reset failed: %v", err).Error())
	}

	logger.Info("shutdown successful")
}
