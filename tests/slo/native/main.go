package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/metrics"
	"slo/internal/workers"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger, err := zap.NewProduction(zap.AddStacktrace(zapcore.PanicLevel))
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

	logger.Info("program started")
	defer logger.Info("shutdown successful")

	st, err := NewStorage(ctx, cfg, logger, cfg.ReadRPS+cfg.WriteRPS)
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

		gen := generator.New(0)

		g := errgroup.Group{}

		for i := uint64(0); i < cfg.InitialDataCount; i++ {
			g.Go(func() error {
				e, err := gen.Generate()
				if err != nil {
					return err
				}

				err = st.Write(ctx, e)
				if err != nil {
					return err
				}

				return nil
			})
		}

		err = g.Wait()
		if err != nil {
			panic(err)
		}

		logger.Info("entries write ok")

		return
	case config.CleanupMode:
		err = st.DropTable(ctx)
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}

		logger.Info("cleanup table ok")

		return
	}

	m, err := metrics.New(cfg.PushGateway, "native")
	if err != nil {
		logger.Error(fmt.Errorf("create metrics failed: %w", err).Error())
		return
	}
	defer func() {
		err = m.Reset()
		if err != nil {
			logger.Error(fmt.Errorf("metrics reset failed: %w", err).Error())
		}
	}()

	err = m.Reset()
	if err != nil {
		logger.Error(fmt.Errorf("metrics reset failed: %w", err).Error())
		return
	}

	logger.Info("metrics init ok")

	gen := generator.New(cfg.InitialDataCount)

	w := workers.New(cfg, st, m, logger)
	wg := sync.WaitGroup{}

	readRL := rate.NewLimiter(rate.Limit(cfg.ReadRPS), 1)
	wg.Add(cfg.ReadRPS)
	for i := 0; i < cfg.ReadRPS; i++ {
		go w.Read(ctx, &wg, readRL)
	}

	writeRL := rate.NewLimiter(rate.Limit(cfg.WriteRPS), 1)
	wg.Add(cfg.WriteRPS)
	for i := 0; i < cfg.WriteRPS; i++ {
		go w.Write(ctx, &wg, writeRL, gen)
	}

	metricsRL := rate.NewLimiter(rate.Every(time.Duration(cfg.ReportPeriod)*time.Millisecond), 1)
	wg.Add(1)
	go w.Metrics(ctx, &wg, metricsRL)

	logger.Info("workers init ok")

	time.Sleep(time.Duration(cfg.Time) * time.Second)

	logger.Info("shutdown started")

	cancel()

	logger.Info("waiting for workers")

	time.AfterFunc(time.Duration(cfg.ShutdownTime)*time.Second, func() {
		panic(errors.New("time limit exceed, exiting"))
	})

	wg.Wait()
}
