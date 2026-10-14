package kv

import (
	"fmt"

	"slo/internal/generator"
)

// ControlRow retains an independent image of immutable generation 1.
// Only Setup (or node-hints Warmup) writes these keys; load writes exclude them.
func ControlRow(row generator.Row) generator.Row {
	row = CloneRow(row)
	generation := uint64(1)
	row.PayloadHash = &generation

	return row
}

func CloneRow(row generator.Row) generator.Row {
	row.PayloadStr = cloneValue(row.PayloadStr)
	row.PayloadHash = cloneValue(row.PayloadHash)
	row.PayloadDouble = cloneValue(row.PayloadDouble)
	row.PayloadTimestamp = cloneValue(row.PayloadTimestamp)

	return row
}

func cloneValue[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copyValue := *value

	return &copyValue
}

func equalValue[T comparable](expected, actual *T) bool {
	return expected != nil && actual != nil && *expected == *actual
}

// CheckRow runs after the completed SDK operation, never inside its retry loop.
// Hash is server-derived routing (Digest::NumericHash(id)), not a generated
// payload field; keyed adapters constrain it in WHERE and id-only tables omit it.
func CheckRow(expected, actual generator.Row) error {
	if actual.ID != expected.ID {
		return fmt.Errorf("data mismatch: id = %d, want %d", actual.ID, expected.ID)
	}
	if !equalValue(expected.PayloadHash, actual.PayloadHash) {
		return fmt.Errorf("data mismatch: id %d generation", expected.ID)
	}
	if !equalValue(expected.PayloadStr, actual.PayloadStr) {
		return fmt.Errorf("data mismatch: id %d payload_str", expected.ID)
	}
	if !equalValue(expected.PayloadDouble, actual.PayloadDouble) {
		return fmt.Errorf("data mismatch: id %d payload_double", expected.ID)
	}
	if expected.PayloadTimestamp == nil || actual.PayloadTimestamp == nil ||
		!expected.PayloadTimestamp.Equal(*actual.PayloadTimestamp) {
		return fmt.Errorf("data mismatch: id %d payload_timestamp", expected.ID)
	}

	return nil
}

// CheckRows compares sets: repeated requested keys name one expected row.
// ReadRows adapters deduplicate request keys, so duplicate returned rows fail.
func CheckRows(expected, actual []generator.Row) error {
	want := make(map[generator.RowID]generator.Row, len(expected))
	for _, row := range expected {
		want[row.ID] = row
	}
	for _, row := range actual {
		expectedRow, ok := want[row.ID]
		if !ok {
			return fmt.Errorf("data mismatch: unexpected or duplicate id %d", row.ID)
		}
		if err := CheckRow(expectedRow, row); err != nil {
			return err
		}
		delete(want, row.ID)
	}
	if len(want) != 0 {
		return fmt.Errorf("data mismatch: %d missing rows", len(want))
	}

	return nil
}
