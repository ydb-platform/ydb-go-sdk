package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/metrics"
	"slo/internal/workers"
	"slo/native/storage"
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

	cfg, err := config.New()
	if errors.Is(err, config.ErrWrongArgs) || errors.Is(err, flag.ErrHelp) {
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
	case config.CreateMode:
		err = st.CreateTable(ctx)
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}
		logger.Info("create table ok")
		return
	case config.CleanupMode:
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

	gen := generator.New(10, 20)

	workCtx, workCancel := context.WithCancel(ctx)
	defer workCancel()

	w := workers.New(workCtx, st, m, logger)

	readRL := rate.NewLimiter(rate.Limit(cfg.ReadRPS), 1)
	for i := 0; i < cfg.ReadRPS; i++ {
		go w.Read(readRL)
	}

	writeRL := rate.NewLimiter(rate.Limit(cfg.WriteRPS), 1)
	for i := 0; i < cfg.WriteRPS; i++ {
		go w.Write(writeRL, gen)
	}

	metricsRL := rate.NewLimiter(rate.Every(time.Duration(cfg.ReportPeriod)*time.Millisecond), 1)
	go w.Metrics(metricsRL)

	logger.Info("workers init ok")

	time.Sleep(time.Duration(cfg.Time) * time.Second)

	logger.Info("shutdown started")

	workCancel()

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
