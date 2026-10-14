package rmw

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"slo/internal/framework"
	"slo/internal/kv"
)

func TestWorkloadPlannedStopSettlesAndPreservesFinalCause(t *testing.T) {
	cause := errors.New("final transaction cause")
	for _, tc := range []struct {
		name        string
		final       error
		snapshotErr error
		corrupt     bool
	}{
		{name: "healthy"},
		{name: "final-error-and-missing-ledger", final: cause},
		{name: "final-cancellation-is-not-planned-success", final: context.Canceled},
		{name: "final-error-and-snapshot-error", final: cause, snapshotErr: errors.New("snapshot failed")},
		{name: "lost-update-fails-workload", corrupt: true},
		{name: "snapshot-error-fails-workload", snapshotErr: errors.New("snapshot failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, boundCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer boundCancel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &lifecycleStore{
				memoryStore: newMemory(), boundDone: bound.Done(),
				started: make(chan struct{}, 2), release: make(chan struct{}),
				final: tc.final, snapshotErr: tc.snapshotErr, corrupt: tc.corrupt,
			}
			metrics, err := framework.NewMetrics(ctx, &framework.Config{})
			if err != nil {
				t.Fatal(err)
			}
			workload := New(&framework.Framework{Metrics: metrics, Logger: framework.NewLogger()},
				kv.Params{WriteRPS: 1_000_000}, store, 2, 1)
			if err := workload.Setup(ctx); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() { result <- workload.Run(ctx) }()
			// Both writers are in admitted calls before scheduling is stopped. The
			// release is an event, not a delay guessing when cancellation settles.
			for range 2 {
				select {
				case <-store.started:
				case <-bound.Done():
					t.Fatal(bound.Err())
				}
			}
			cancel()
			close(store.release)
			select {
			case err = <-result:
			case <-bound.Done():
				t.Fatal(bound.Err())
			}
			if tc.final != nil {
				if !errors.Is(err, tc.final) {
					t.Fatalf("got %v, want original cause %v", err, tc.final)
				}
			} else if tc.snapshotErr != nil {
				if !errors.Is(err, tc.snapshotErr) {
					t.Fatalf("got %v, want snapshot error %v", err, tc.snapshotErr)
				}
			} else if (err != nil) != tc.corrupt {
				t.Fatalf("corrupt=%v, error=%v", tc.corrupt, err)
			}
			if store.calls.Load() != 2 || store.snapshots != 1 || store.active.Load() != 0 {
				t.Fatalf("calls=%d, snapshots=%d, active=%d", store.calls.Load(), store.snapshots, store.active.Load())
			}
		})
	}
}

func TestWorkloadRejectsInvalidScenarioOptions(t *testing.T) {
	for _, tc := range []struct {
		workers  int
		counters uint64
		rps      uint
	}{{1, 1, 1}, {2, 0, 1}, {2, 1, 0}} {
		w := New(nil, kv.Params{WriteRPS: tc.rps}, nil, tc.workers, tc.counters)
		if err := w.Setup(context.Background()); err == nil {
			t.Fatalf("invalid options accepted: %+v", tc)
		}
	}
}

// lifecycleStore blocks at an exact admitted-operation event, and verifies that
// final snapshot reads cannot race live writers or inherit scheduling cancel.
type lifecycleStore struct {
	*memoryStore

	boundDone   <-chan struct{}
	started     chan struct{}
	release     chan struct{}
	final       error
	snapshotErr error
	corrupt     bool
	active      atomic.Int64
	calls       atomic.Int64
	snapshots   int
}

func (*lifecycleStore) Setup(context.Context, uint64) error { return nil }
func (*lifecycleStore) Drop(context.Context) error          { return nil }
func (*lifecycleStore) Close(context.Context) error         { return nil }

func (s *lifecycleStore) Apply(ctx context.Context, op Operation) (int, error) {
	s.calls.Add(1)
	s.active.Add(1)
	defer s.active.Add(-1)
	s.started <- struct{}{}
	select {
	case <-s.release:
	case <-s.boundDone:
		return 1, context.DeadlineExceeded
	}
	if err := ctx.Err(); err != nil {
		return 1, err
	}
	if op.ID == 1 && s.final != nil {
		return 1, s.final
	}

	return s.memoryStore.Apply(ctx, op)
}

func (s *lifecycleStore) Snapshot(ctx context.Context) (State, int, error) {
	s.snapshots++
	if ctx.Err() != nil || s.active.Load() != 0 {
		return State{}, 1, errors.New("snapshot before settlement or with canceled context")
	}
	if s.snapshotErr != nil {
		return State{}, 1, s.snapshotErr
	}
	state, attempts, err := s.memoryStore.Snapshot(ctx)
	if s.corrupt {
		state.Counters[0].Value = 0
	}

	return state, attempts, err
}
