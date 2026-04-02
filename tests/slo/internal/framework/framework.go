package framework

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const defaultShutdownDuration = 30 * time.Second //nolint:mnd

type Framework struct {
	Config  *Config
	Metrics *Metrics
	Logger  *Logger
}

type Workload interface {
	Setup(ctx context.Context) error
	Run(ctx context.Context) error
	Teardown(ctx context.Context) error
}

type Warmer interface {
	Warmup(ctx context.Context) error
}

type Cooler interface {
	Cooldown(ctx context.Context) error
}

type WorkloadFactory func(ctx context.Context, fw *Framework) (Workload, error)

func Run(factory WorkloadFactory) {
	exitCode := 0

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()

	cfg, err := NewConfig()
	if err != nil {
		log.Fatalf("create config failed: %v", err)
	}

	logger := NewLogger()

	metrics, err := NewMetrics(ctx, cfg)
	if err != nil {
		log.Fatalf("create metrics failed: %v", err)
	}

	fw := &Framework{
		Config:  cfg,
		Metrics: metrics,
		Logger:  logger,
	}

	logger.Printf("program started")

	workload, err := factory(ctx, fw)
	if err != nil {
		log.Fatalf("create workload failed: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic recovered: %v", r)
			exitCode = 1
		}

		logger.SetPhase(PhaseTeardown)
		teardownCtx, teardownCancel := context.WithTimeout(context.Background(), defaultShutdownDuration)
		teardownErr := workload.Teardown(teardownCtx)
		teardownCancel()
		if teardownErr != nil {
			logger.Errorf("teardown failed: %v", teardownErr)
		}

		_ = metrics.Push(context.Background())
		_ = metrics.Close(context.Background())
		logger.Flush()

		logger.Printf("program finished")

		os.Exit(exitCode) //nolint:gocritic
	}()

	// Setup
	logger.SetPhase(PhaseSetup)
	if err = workload.Setup(ctx); err != nil {
		logger.Errorf("setup failed: %v", err)
		exitCode = 1

		return
	}
	logger.Printf("setup ok")

	// Warmup (optional)
	if warmer, ok := workload.(Warmer); ok {
		logger.SetPhase(PhaseWarmup)
		if err = warmer.Warmup(ctx); err != nil {
			logger.Errorf("warmup failed: %v", err)
			exitCode = 1

			return
		}
		logger.Printf("warmup ok")
	}

	// Run
	logger.SetPhase(PhaseRun)
	runCtx, runCancel := context.WithTimeout(ctx, cfg.RunDuration())
	err = workload.Run(runCtx)
	runCancel()
	if err != nil {
		logger.Errorf("run failed: %v", err)
		exitCode = 1

		return
	}
	logger.Printf("run ok")

	// Cooldown (optional)
	if cooler, ok := workload.(Cooler); ok {
		logger.SetPhase(PhaseCooldown)
		if err = cooler.Cooldown(ctx); err != nil {
			logger.Errorf("cooldown failed: %v", err)
			// cooldown errors are not fatal
		} else {
			logger.Printf("cooldown ok")
		}
	}

	logger.Printf("workload completed successfully")
}
