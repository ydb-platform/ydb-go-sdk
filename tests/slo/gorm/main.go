package main

import (
	"context"
	"fmt"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/workers"
)

var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewProduction(zap.AddStacktrace(zapcore.PanicLevel))
	if err != nil {
		panic(fmt.Errorf("error create logger: %w", err))
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()

	defer func() { _ = logger.Sync() }()

	cfg, err := config.New()
	if err != nil {
		panic(fmt.Errorf("create config failed: %w", err))
	}

	logger.Info("program started")
	defer logger.Info("program finished")

	ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Time)*time.Second)
	defer cancel()

	s, err := NewStorage(ctx, cfg, cfg.ReadRPS+cfg.WriteRPS)
	if err != nil {
		panic(fmt.Errorf("create storage failed: %w", err))
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(),
			time.Duration(cfg.ShutdownTime)*time.Second)
		defer shutdownCancel()

		_ = s.close(shutdownCtx)
	}()

	logger.Info("db init ok")

	switch cfg.Mode {
	case config.CreateMode:
		err = s.createTable(ctx)
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}
		logger.Info("create table ok")

		gen := generator.New(0)

		g := errgroup.Group{}

		for i := uint64(0); i < cfg.InitialDataCount; i++ {
			g.Go(func() (err error) {
				e, err := gen.Generate()
				if err != nil {
					return err
				}

				err = s.Write(ctx, e)
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
	case config.CleanupMode:
		err = s.dropTable(ctx)
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}

		logger.Info("cleanup table ok")
	case config.RunMode:
		gen := generator.New(cfg.InitialDataCount)

		w, err := workers.New(cfg, s, logger)
		if err != nil {
			panic(fmt.Errorf("create workers failed: %w", err))
		}
		defer func() {
			err := w.Close()
			if err != nil {
				panic(fmt.Errorf("workers close failed: %w", err))
			}
			logger.Info("workers close ok")
		}()

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

		wg.Wait()
	default:
		panic(fmt.Errorf("unknown mode: %v", cfg.Mode))
	}
}
