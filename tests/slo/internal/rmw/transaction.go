// Package rmw implements the separate concurrent transactional counter scenario.
package rmw

import (
	"context"
	"fmt"
)

// Operation identifies a logical increment, not a transaction callback attempt.
type Operation struct {
	ID      uint64
	Counter uint64
	Delta   uint64
}

// Counter is the independently observable persisted counter state.
type Counter struct {
	ID      uint64
	Value   uint64
	Version uint64
}

// State contains full rows, rather than maps, so validation detects duplicates.
type State struct {
	Counters []Counter
	Ledger   []Operation
}

// Transaction is the data access inside one SDK serializable RW transaction.
// Write must update the counter and insert the ledger entry in that transaction.
type Transaction interface {
	Ledger(ctx context.Context, id uint64) (Operation, bool, error)
	Counter(ctx context.Context, id uint64) (Counter, error)
	Write(ctx context.Context, counter Counter, op Operation) error
}

// Apply is invoked by the SDK's transaction callback on every attempt. The caller
// captures op before entering DoTx, including retries following ambiguous commit.
func Apply(ctx context.Context, tx Transaction, op Operation) error {
	previous, found, err := tx.Ledger(ctx, op.ID)
	if err != nil {
		return err
	}
	if found {
		if previous != op {
			return fmt.Errorf("ledger mismatch for operation %d: got %+v, want %+v", op.ID, previous, op)
		}

		return nil
	}

	counter, err := tx.Counter(ctx, op.Counter)
	if err != nil {
		return err
	}
	if counter.ID != op.Counter {
		return fmt.Errorf("counter mismatch: got %d, want %d", counter.ID, op.Counter)
	}
	counter.Value += op.Delta
	counter.Version++

	return tx.Write(ctx, counter, op)
}

// Check validates exact operation identities and payloads as well as per-counter
// values and versions. Expected operations are retained independently of reads.
func Check(expected []Operation, counterCount uint64, state State) error {
	want := make(map[uint64]Operation, len(expected))
	totals := make([]Counter, counterCount)
	for id := range counterCount {
		totals[id].ID = id
	}
	for _, op := range expected {
		if _, duplicate := want[op.ID]; duplicate || op.Counter >= counterCount || op.Delta != 1 {
			return fmt.Errorf("invalid expected operation: %+v", op)
		}
		want[op.ID] = op
		totals[op.Counter].Value += op.Delta
		totals[op.Counter].Version++
	}

	seen := make(map[uint64]bool, len(state.Ledger))
	for _, op := range state.Ledger {
		original, ok := want[op.ID]
		if !ok || seen[op.ID] || op != original {
			return fmt.Errorf("unexpected, duplicate or incorrect ledger entry: %+v", op)
		}
		seen[op.ID] = true
	}
	if len(seen) != len(want) {
		return fmt.Errorf("ledger has %d unique operations, want %d", len(seen), len(want))
	}

	seenCounters := make(map[uint64]bool, len(state.Counters))
	for _, counter := range state.Counters {
		if counter.ID >= counterCount || seenCounters[counter.ID] || counter != totals[counter.ID] {
			return fmt.Errorf("incorrect, duplicate or unexpected counter: %+v", counter)
		}
		seenCounters[counter.ID] = true
	}
	if uint64(len(seenCounters)) != counterCount {
		return fmt.Errorf("snapshot has %d counters, want %d", len(seenCounters), counterCount)
	}

	return nil
}
