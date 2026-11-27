package main

import (
	"errors"
	"reflect"
	"testing"
)

func TestValidatePayloadRejectsMissingAndCorruptValues(t *testing.T) {
	good := "confirmed_1"
	bad := "stale_generation"
	for _, actual := range []*string{nil, &bad} {
		if err := validatePayload(good, actual); !errors.Is(err, errDataMismatch) {
			t.Fatalf("expected data mismatch, got %v", err)
		}
	}
	if err := validatePayload(good, &good); err != nil {
		t.Fatal(err)
	}
}

func TestAuditChecksEveryAcknowledgedValueAndAllowsUncertainCommits(t *testing.T) {
	confirmed := map[uint64]string{1: "one", 2: "two", 3: "three"}
	stored := map[uint64]string{1: "one", 2: "stale", 4: "unacknowledged"}
	if missing := missingRows(confirmed, stored); !reflect.DeepEqual(missing, []uint64{2, 3}) {
		t.Fatalf("unexpected missing/corrupt rows: %v", missing)
	}
	stored[2], stored[3] = "two", "three"
	if missing := missingRows(confirmed, stored); len(missing) != 0 {
		t.Fatalf("extra unacknowledged row must not fail audit: %v", missing)
	}
}
