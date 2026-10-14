package kv

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"slo/internal/framework"
	"slo/internal/generator"
)

func TestFinalErrorWorkloadRunReturnsFinalReadError(t *testing.T) {
	errFinal := errors.New("final read error after SDK retries")
	called := make(chan struct{}, 1)
	db := &finalErrorDatabase{read: func(context.Context) error {
		select {
		case called <- struct{}{}:
		default:
		}

		return errFinal
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := finalErrorWorkload(t, db)
	result := make(chan error, 1)
	go func() { result <- w.Run(ctx) }()

	select {
	case <-called:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("read operation was not invoked")
	}
	select {
	case err := <-result:
		if !errors.Is(err, errFinal) {
			t.Fatalf("Run() error = %v, want final read error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not finish after cancellation")
	}
}

func TestFinalErrorBatchWorkloadRunReturnsFinalWriteDeadline(t *testing.T) {
	called := make(chan struct{}, 1)
	db := &finalErrorBatchDatabase{write: func(context.Context) error {
		select {
		case called <- struct{}{}:
		default:
		}

		return context.DeadlineExceeded
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	params := finalErrorParams()
	params.ReadRPS = 0
	w := NewBatch(finalErrorFramework(), params, 1, db)
	result := make(chan error, 1)
	go func() { result <- w.Run(ctx) }()

	select {
	case <-called:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("batch write operation was not invoked")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run() error = %v, want deadline exceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not finish after cancellation")
	}
}

func TestFinalErrorWorkloadSuccessfulFinalOutcome(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := &finalErrorDatabase{attempts: 2, read: func(context.Context) error {
		cancel()

		return nil
	}}
	if err := finalErrorWorkload(t, db).Run(ctx); err != nil {
		t.Fatalf("successful final outcome returned %v", err)
	}
}

func TestFinalErrorWorkloadExpectedShutdown(t *testing.T) {
	called := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := &finalErrorDatabase{read: func(ctx context.Context) error {
		close(called)
		<-ctx.Done()

		return ctx.Err()
	}}
	result := make(chan error, 1)
	workload := finalErrorWorkload(t, db)
	go func() { result <- workload.Run(ctx) }()
	select {
	case <-called:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("read operation was not invoked")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("expected shutdown returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workload did not finish after shutdown")
	}
}

func TestFinalErrorProcessExit(t *testing.T) {
	if mode := os.Getenv("SLO_FINAL_ERROR_CHILD_MODE"); mode != "" {
		framework.Run(func(_ context.Context, fw *framework.Framework) (framework.Workload, error) {
			db := &finalErrorDatabase{read: func(context.Context) error {
				if mode == "failure" {
					return errors.New("terminal read failure")
				}

				return nil
			}}
			if mode == "shutdown-deadline" {
				db.read = func(ctx context.Context) error {
					<-ctx.Done()

					return context.DeadlineExceeded
				}
			}

			return New(fw, finalErrorParams(), db), nil
		})

		return
	}
	for _, mode := range []string{"success", "failure", "shutdown-deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFinalErrorProcessExit$")
			command.Env = append(os.Environ(), "SLO_FINAL_ERROR_CHILD_MODE="+mode,
				"YDB_ENDPOINT=grpc://unused:2136", "YDB_DATABASE=/unused",
				"WORKLOAD_DURATION=1", "OTEL_EXPORTER_OTLP_ENDPOINT=", "PROMETHEUS_URL=")
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("process did not finish: %v\n%s", ctx.Err(), output)
			}
			if mode == "success" {
				if err != nil {
					t.Fatalf("healthy process failed: %v\n%s", err, output)
				}

				return
			}
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
				t.Fatalf("terminal error process = %v, want exit 1\n%s", err, output)
			}
		})
	}
}

type finalErrorDatabase struct {
	read     func(context.Context) error
	attempts int
	mu       sync.Mutex
	rows     map[generator.RowID]generator.Row
}

func (d *finalErrorDatabase) CreateTable(context.Context) error { return nil }
func (d *finalErrorDatabase) DropTable(context.Context) error   { return nil }
func (d *finalErrorDatabase) Read(ctx context.Context, id generator.RowID) (generator.Row, int, error) {
	err := d.read(ctx)
	d.mu.Lock()
	defer d.mu.Unlock()

	return CloneRow(d.rows[id]), max(1, d.attempts), err
}

func (d *finalErrorDatabase) Write(_ context.Context, row generator.Row) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.rows == nil {
		d.rows = make(map[generator.RowID]generator.Row)
	}
	d.rows[row.ID] = CloneRow(row)

	return 1, nil
}

func (d *finalErrorDatabase) Close(context.Context) error { return nil }

type finalErrorBatchDatabase struct {
	write func(context.Context) error
}

func (d *finalErrorBatchDatabase) CreateTable(context.Context) error { return nil }
func (d *finalErrorBatchDatabase) DropTable(context.Context) error   { return nil }
func (d *finalErrorBatchDatabase) ReadBatch(context.Context, []generator.RowID) ([]generator.Row, int, error) {
	return nil, 1, nil
}

func (d *finalErrorBatchDatabase) WriteBatch(ctx context.Context, _ []generator.Row) (int, error) {
	return 1, d.write(ctx)
}

func (d *finalErrorBatchDatabase) Close(context.Context) error { return nil }

func finalErrorWorkload(t *testing.T, db *finalErrorDatabase) *Workload {
	t.Helper()

	w := New(finalErrorFramework(), finalErrorParams(), db)
	if err := w.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}

	return w
}

func finalErrorFramework() *framework.Framework {
	metrics, _ := framework.NewMetrics(context.Background(), &framework.Config{})

	return &framework.Framework{Metrics: metrics, Logger: framework.NewLogger()}
}

func finalErrorParams() Params {
	return Params{ReadRPS: 1, WriteRPS: 1, PrefillCount: 1, RetryBudget: finalErrorRetryBudget{}}
}

type finalErrorRetryBudget struct{}

func (finalErrorRetryBudget) Acquire(context.Context) error { return nil }
func (finalErrorRetryBudget) Stop()                         {}
