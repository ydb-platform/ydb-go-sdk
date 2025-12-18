package main

import (
	"context"
	"fmt"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/log"
	"slo/internal/workers"
)

var (
	ref     string
	label   string
	jobName = "slo_native_bulk_upsert"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()

	cfg, err := config.New()
	if err != nil {
		panic(fmt.Errorf("create config failed: %w", err))
	}

	log.Println("program started")
	defer log.Println("program finished")

	ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Time)*time.Second)
	defer cancel()

	go func() {
		<-ctx.Done()
		log.Println("exiting...")
	}()

	// pool size similar to query variant
	s, err := NewStorage(ctx, cfg, cfg.ReadRPS+cfg.WriteRPS, "no_session")
	if err != nil {
		panic(fmt.Errorf("create storage failed: %w", err))
	}
	defer func() {
		var (
			shutdownCtx    context.Context
			shutdownCancel context.CancelFunc
		)
		if cfg.ShutdownTime > 0 {
			shutdownCtx, shutdownCancel = context.WithTimeout(context.Background(),
				time.Duration(cfg.ShutdownTime)*time.Second)
		} else {
			shutdownCtx, shutdownCancel = context.WithCancel(context.Background())
		}
		defer shutdownCancel()

		_ = s.Close(shutdownCtx)
	}()

	log.Println("db init ok")
	gen := generator.NewSeeded(120394832798)

	switch cfg.Mode {
	case config.CreateMode:
		err = s.CreateTable(ctx)
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}
		log.Println("create table ok")

		g := errgroup.Group{}

		for i := uint64(0); i < cfg.InitialDataCount; i++ {
			g.Go(func() (err error) {
				e, err := gen.Generate()
				if err != nil {
					return err
				}

				_, err = s.WriteBatch(ctx, []generator.Row{e})
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

		log.Println("entries write ok")
	case config.CleanupMode:
		err = s.DropTable(ctx)
		if err != nil {
			panic(fmt.Errorf("create table failed: %w", err))
		}

		log.Println("cleanup table ok")
	case config.RunMode:
		// to wait for correct partitions boundaries
		time.Sleep(10 * time.Second)
		w, err := workers.NewWithBatch(cfg, s, ref, label, jobName)
		if err != nil {
			panic(fmt.Errorf("create workers failed: %w", err))
		}
		w.ReportNodeHintMisses(1)
		ns := s.nodeSelector.Load()
		idx, nodeID := ns.GetRandomNodeID(gen)
		log.Println("all requests to node id: ", nodeID)
		gen.SetRange(ns.LowerBounds[idx], ns.UpperBounds[idx])
		w.Gen = gen
		defer func() {
			err := w.Close()
			if err != nil {
				panic(fmt.Errorf("workers close failed: %w", err))
			}
			log.Println("workers close ok")
		}()

		// collect metrics
		estimator := NewEstimator(ctx, s)
		// run workers
		wg := sync.WaitGroup{}
		readRL := rate.NewLimiter(rate.Limit(cfg.ReadRPS), 1)
		wg.Add(cfg.ReadRPS)
		for i := 0; i < cfg.ReadRPS; i++ {
			go w.Read(ctx, &wg, readRL)
		}
		log.Println("started " + strconv.Itoa(cfg.ReadRPS) + " read workers")

		writeRL := rate.NewLimiter(rate.Limit(cfg.WriteRPS), 1)
		wg.Add(cfg.WriteRPS)
		for i := 0; i < cfg.WriteRPS; i++ {
			go w.Write(ctx, &wg, writeRL, gen)
		}
		log.Println("started " + strconv.Itoa(cfg.WriteRPS) + " write workers")
		wg.Wait()
		// check all load is sent to a single node
		ectx, ecancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ecancel()
		err = estimator.OnlyThisNode(ectx, nodeID)
		if err != nil {
			w.ReportNodeHintMisses(-1)
			time.Sleep(w.ExportInterval())
		}
	default:
		panic(fmt.Errorf("unknown mode: %v", cfg.Mode))
	}
}
