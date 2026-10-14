package rmw

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"
)

func TestApplyContention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := newMemory()
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	store.beforeCounter = func(attempt int) error {
		if attempt != 1 {
			return nil
		}
		arrived <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	operations := []Operation{{ID: 1, Delta: 1}, {ID: 2, Delta: 1}}
	results := make(chan error, len(operations))
	for _, op := range operations {
		go func() {
			_, err := store.Apply(ctx, op)
			results <- err
		}()
	}
	for range operations {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(release)
	for range operations {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	state, _, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Check(operations, 1, state); err != nil {
		t.Fatal(err)
	}
	if store.conflicts == 0 {
		t.Fatal("the barrier must produce a stale serializable transaction")
	}
}

func TestApplyStableIDAcrossRetry(t *testing.T) {
	for _, mode := range []string{"rollback", "persisted-ambiguous-commit"} {
		t.Run(mode, func(t *testing.T) {
			store := newMemory()
			store.retryFirst = true
			store.persistFirst = mode == "persisted-ambiguous-commit"
			op := Operation{ID: 73, Delta: 1}
			attempts, err := store.Apply(context.Background(), op)
			if err != nil {
				t.Fatal(err)
			}
			if attempts != 2 || len(store.observedIDs) != 2 || store.observedIDs[0] != op.ID || store.observedIDs[1] != op.ID {
				t.Fatalf("attempts=%d, observed IDs=%v", attempts, store.observedIDs)
			}
			wantWrites := 2
			if store.persistFirst {
				wantWrites = 1
			}
			if store.writes != wantWrites {
				t.Fatalf("writes=%d, want %d", store.writes, wantWrites)
			}
			state, _, err := store.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := Check([]Operation{op}, 1, state); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCheckExactState(t *testing.T) {
	expected := []Operation{{ID: 10, Counter: 0, Delta: 1}, {ID: 11, Counter: 1, Delta: 1}}
	for _, name := range []string{
		"healthy", "lost-update", "sum-preserving-lost-update", "wrong-version", "missing-ledger", "extra-ledger",
		"duplicate-ledger", "wrong-id", "wrong-counter", "wrong-delta",
		"missing-counter", "extra-counter", "duplicate-counter",
	} {
		t.Run(name, func(t *testing.T) {
			state := State{
				Counters: []Counter{{ID: 0, Value: 1, Version: 1}, {ID: 1, Value: 1, Version: 1}},
				Ledger:   append([]Operation(nil), expected...),
			}
			switch name {
			case "healthy":
			case "lost-update":
				state.Counters[0].Value = 0
				state.Counters[0].Version = 0
			case "sum-preserving-lost-update":
				state.Counters[0].Value = 0
				state.Counters[1].Value = 2
			case "wrong-version":
				state.Counters[0].Version = 0
			case "missing-ledger":
				state.Ledger = state.Ledger[:1]
			case "extra-ledger":
				state.Ledger = append(state.Ledger, Operation{ID: 12, Delta: 1})
			case "duplicate-ledger":
				state.Ledger = append(state.Ledger, expected[0])
			case "wrong-id":
				state.Ledger[0].ID = 12
			case "wrong-counter":
				state.Ledger[0].Counter = 1
			case "wrong-delta":
				state.Ledger[0].Delta = 2
			case "missing-counter":
				state.Counters = state.Counters[:1]
			case "extra-counter":
				state.Counters = append(state.Counters, Counter{ID: 2})
			case "duplicate-counter":
				state.Counters = append(state.Counters, state.Counters[0])
			}
			err := Check(expected, 2, state)
			if (err == nil) != (name == "healthy") {
				t.Fatalf("scenario %s: got error %v", name, err)
			}
		})
	}
}

func TestApplyExistingLedgerPayloadAndOriginalErrors(t *testing.T) {
	op := Operation{ID: 4, Delta: 1}
	store := newMemory()
	store.ledger[op.ID] = Operation{ID: op.ID, Delta: 9}
	if _, err := store.Apply(context.Background(), op); err == nil {
		t.Fatal("incorrect deduplication payload accepted")
	}
	if store.writes != 0 {
		t.Fatal("incorrect ledger payload must not mutate data")
	}
	for _, stage := range []string{"ledger", "counter", "write"} {
		t.Run(stage, func(t *testing.T) {
			cause := errors.New(stage)
			tx := &memoryTx{counters: map[uint64]Counter{0: {}}, ledger: map[uint64]Operation{}, failStage: stage, cause: cause}
			if err := Apply(context.Background(), tx, op); !errors.Is(err, cause) {
				t.Fatalf("got %v, want original cause %v", err, cause)
			}
		})
	}
}

// memoryStore is a bounded optimistic transaction seam, not an SDK retry or YDB
// emulator. It exercises the production Apply callback and Check implementation.
// A revision conflict discards the transaction; the ambiguity mode first persists
// the exact transaction, then invokes the same callback with the same operation.
type memoryStore struct {
	mu            sync.Mutex
	counters      map[uint64]Counter
	ledger        map[uint64]Operation
	revision      uint64
	conflicts     int
	writes        int
	observedIDs   []uint64
	retryFirst    bool
	persistFirst  bool
	beforeCounter func(int) error
}

func newMemory() *memoryStore {
	return &memoryStore{counters: map[uint64]Counter{0: {}}, ledger: make(map[uint64]Operation)}
}

func (m *memoryStore) begin(attempt int) (*memoryTx, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return &memoryTx{
		counters: maps.Clone(m.counters), ledger: maps.Clone(m.ledger),
		beforeCounter: m.beforeCounter, attempt: attempt,
	}, m.revision
}

func (m *memoryStore) commit(tx *memoryTx, revision uint64, op Operation, persist bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observedIDs = append(m.observedIDs, op.ID)
	m.writes += tx.writes
	if m.revision != revision {
		m.conflicts++

		return false
	}
	if persist && tx.writes != 0 {
		m.counters, m.ledger = tx.counters, tx.ledger
		m.revision++
	}

	return true
}

func (m *memoryStore) Apply(ctx context.Context, op Operation) (int, error) {
	for attempt := 1; attempt <= 64; attempt++ {
		if err := ctx.Err(); err != nil {
			return attempt, err
		}
		tx, revision := m.begin(attempt)
		if err := Apply(ctx, tx, op); err != nil {
			return attempt, err
		}
		retry := m.retryFirst && attempt == 1
		if !m.commit(tx, revision, op, !retry || m.persistFirst) || retry {
			continue
		}

		return attempt, nil
	}

	return 64, errors.New("bounded test transaction exhausted")
}

func (m *memoryStore) Snapshot(context.Context) (State, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var state State
	for _, counter := range m.counters {
		state.Counters = append(state.Counters, counter)
	}
	for _, op := range m.ledger {
		state.Ledger = append(state.Ledger, op)
	}

	return state, 1, nil
}

type memoryTx struct {
	counters      map[uint64]Counter
	ledger        map[uint64]Operation
	beforeCounter func(int) error
	attempt       int
	writes        int
	failStage     string
	cause         error
}

func (tx *memoryTx) Ledger(_ context.Context, id uint64) (Operation, bool, error) {
	if tx.failStage == "ledger" {
		return Operation{}, false, tx.cause
	}
	op, found := tx.ledger[id]

	return op, found, nil
}

func (tx *memoryTx) Counter(_ context.Context, id uint64) (Counter, error) {
	if tx.failStage == "counter" {
		return Counter{}, tx.cause
	}
	if tx.beforeCounter != nil {
		if err := tx.beforeCounter(tx.attempt); err != nil {
			return Counter{}, err
		}
	}
	counter, found := tx.counters[id]
	if !found {
		return Counter{}, errors.New("missing counter")
	}

	return counter, nil
}

func (tx *memoryTx) Write(_ context.Context, counter Counter, op Operation) error {
	if tx.failStage == "write" {
		return tx.cause
	}
	if _, exists := tx.ledger[op.ID]; exists {
		return errors.New("duplicate insert")
	}
	tx.counters[counter.ID] = counter
	tx.ledger[op.ID] = op
	tx.writes++

	return nil
}
