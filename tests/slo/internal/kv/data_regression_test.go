package kv

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path"
	"sync"
	"testing"
	"time"

	"slo/internal/framework"
	"slo/internal/generator"
)

func TestControlledRowChecksEveryPayloadField(t *testing.T) {
	expected := dataFixture(7)
	for _, field := range []string{
		"healthy", "missing", "id", "generation", "string", "double", "timestamp",
		"null", "null-generation", "null-double", "null-timestamp",
	} {
		t.Run(field, func(t *testing.T) {
			actual := CloneRow(expected)
			mutateData(field, &actual)
			if err := CheckRow(expected, actual); (err == nil) != (field == "healthy") {
				t.Fatalf("CheckRow() = %v, field %s", err, field)
			}
		})
	}
}

func TestControlledBatchChecksKeySet(t *testing.T) {
	a, b := dataFixture(1), dataFixture(2)
	for _, tc := range []struct {
		name string
		rows []generator.Row
		ok   bool
	}{
		{name: "unordered duplicates requested", rows: []generator.Row{b, a}, ok: true},
		{name: "missing", rows: []generator.Row{a}},
		{name: "duplicate returned", rows: []generator.Row{a, a, b}},
		{name: "unexpected", rows: []generator.Row{a, b, dataFixture(3)}},
		{name: "absent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := CheckRows([]generator.Row{a, b, a}, tc.rows); (err == nil) != tc.ok {
				t.Fatalf("CheckRows() = %v, want success %v", err, tc.ok)
			}
		})
	}
}

func TestExpectedImageDoesNotAliasWrite(t *testing.T) {
	row := ControlRow(dataFixture(1))
	expected := CloneRow(row)
	mutateData("string", &row)
	if err := CheckRow(expected, row); err == nil {
		t.Fatal("mutating write image also changed independent expectation")
	}
}

func TestGeneratorStoragePrecisionAndReservedRange(t *testing.T) {
	gen := generator.NewSeeded(1)
	gen.SetRange(11, 20) // control id 10 is outside the ordinary load range
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 32 {
				row, err := gen.Generate()
				if err != nil {
					t.Error(err)

					return
				}
				if row.ID < 11 || row.ID >= 20 || row.PayloadTimestamp.Nanosecond()%1000 != 0 {
					t.Errorf("invalid stored image or control collision: %+v", row)
				}
			}
		})
	}
	group.Wait()
	row, err := generator.New(10).Generate()
	if err != nil || row.PayloadTimestamp.Nanosecond()%1000 != 0 {
		t.Fatalf("sequential generator precision: %v, %v", row, err)
	}
}

func TestTableNamespaceIsolatesSameRefProcesses(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{oldArgs[0]}
	t.Cleanup(func() { os.Args = oldArgs })
	fw := &framework.Framework{Config: &framework.Config{Database: "/Root/db", Label: "kv", Ref: "same"}}
	a := ParseParams(fw, "test", nil)
	b := ParseParams(fw, "test", nil)
	defer a.RetryBudget.Stop()
	defer b.RetryBudget.Stop()
	if a.TablePath == b.TablePath || path.Dir(a.TablePath) != "/Root/db/kv" ||
		path.Dir(b.TablePath) != "/Root/db/kv" {
		t.Fatalf("namespaces are not isolated: %q, %q", a.TablePath, b.TablePath)
	}
	if fw.Config.Ref != "same" {
		t.Fatal("table isolation changed metric ref")
	}
}

func TestControlledWorkloadReturnsDataMismatchAndPreservesSDKError(t *testing.T) {
	for _, field := range []string{"missing", "id", "generation", "string", "double", "timestamp", "null", "sdk-error"} {
		t.Run(field, func(t *testing.T) {
			params := finalErrorParams()
			params.WriteRPS = 0
			db := &dataDatabase{mode: field}
			w := New(finalErrorFramework(), params, db)
			if err := w.Setup(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := w.Run(ctx)
			if err == nil || errors.Is(err, ctx.Err()) {
				t.Fatalf("wrong controlled-read verdict: %v", err)
			}
			if field == "sdk-error" && !errors.Is(err, errDataSDK) {
				t.Fatalf("original SDK error replaced: %v", err)
			}
		})
	}
}

func TestControlledBatchWorkloadReturnsDataMismatch(t *testing.T) {
	params := finalErrorParams()
	params.WriteRPS = 0
	db := &dataDatabase{mode: "missing"}
	w := NewBatch(finalErrorFramework(), params, 2, db)
	if err := w.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := w.Run(ctx); err == nil || errors.Is(err, ctx.Err()) {
		t.Fatalf("missing batch did not fail: %v", err)
	}
}

func TestControlledBatchAcceptsRecoveredCompleteResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	params := finalErrorParams()
	params.WriteRPS = 0
	db := &dataDatabase{readDone: cancel}
	w := NewBatch(finalErrorFramework(), params, 2, db)
	if err := w.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- w.Run(ctx) }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("recovered complete batch failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch did not finish after exact read completion")
	}
}

func TestFailedWriteNeverPublishesExpectedImage(t *testing.T) {
	db := &dataDatabase{mode: "write-error"}
	w := New(finalErrorFramework(), finalErrorParams(), db)
	if err := w.Setup(context.Background()); !errors.Is(err, errDataSDK) {
		t.Fatalf("Setup() = %v", err)
	}
	if w.expected[0].PayloadHash != nil {
		t.Fatal("failed write published a readable expectation")
	}
}

func TestControlledLoadNeverOverwritesPrefill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	params := finalErrorParams()
	params.ReadRPS = 0
	db := &dataDatabase{written: make(chan generator.RowID, 1), cancel: cancel}
	w := New(finalErrorFramework(), params, db)
	if err := w.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- w.Run(ctx) }()
	select {
	case id := <-db.written:
		if id < params.PrefillCount {
			t.Fatalf("load overwrote control id %d", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("load did not write")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("load did not settle")
	}
}

func TestControlledDataProcessExit(t *testing.T) {
	if mode := os.Getenv("SLO_DATA_CHILD_MODE"); mode != "" {
		framework.Run(func(_ context.Context, fw *framework.Framework) (framework.Workload, error) {
			params := finalErrorParams()
			params.WriteRPS = 0

			return New(fw, params, &dataDatabase{mode: mode}), nil
		})

		return
	}
	for _, mode := range []string{"missing", "generation", "string", "double", "timestamp", "null", "id"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestControlledDataProcessExit$")
			command.Env = append(os.Environ(), "SLO_DATA_CHILD_MODE="+mode,
				"YDB_ENDPOINT=grpc://unused:2136", "YDB_DATABASE=/unused",
				"WORKLOAD_DURATION=60", "OTEL_EXPORTER_OTLP_ENDPOINT=", "PROMETHEUS_URL=")
			output, err := command.CombinedOutput()
			var exitError *exec.ExitError
			if ctx.Err() != nil || !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
				t.Fatalf("corrupt data process = %v, want exit 1\n%s", err, output)
			}
		})
	}
}

var errDataSDK = errors.New("original SDK final error")

func dataFixture(id generator.RowID) generator.Row {
	text, generation, number, timestamp := "payload", uint64(1), 0.25, time.Unix(1700000000, 123456000).UTC()

	return generator.Row{
		ID: id, PayloadStr: &text, PayloadHash: &generation,
		PayloadDouble: &number, PayloadTimestamp: &timestamp,
	}
}

func mutateData(mode string, row *generator.Row) {
	switch mode {
	case "missing":
		*row = generator.Row{}
	case "id":
		row.ID++
	case "generation":
		*row.PayloadHash = 0
	case "string":
		*row.PayloadStr += "corrupt"
	case "double":
		*row.PayloadDouble++
	case "timestamp":
		*row.PayloadTimestamp = row.PayloadTimestamp.Add(time.Microsecond)
	case "null":
		row.PayloadStr = nil
	case "null-generation":
		row.PayloadHash = nil
	case "null-double":
		row.PayloadDouble = nil
	case "null-timestamp":
		row.PayloadTimestamp = nil
	}
}

type dataDatabase struct {
	finalErrorDatabase

	mode     string
	written  chan generator.RowID
	cancel   context.CancelFunc
	readDone context.CancelFunc
}

func (d *dataDatabase) Read(_ context.Context, id generator.RowID) (generator.Row, int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	row := CloneRow(d.rows[id])
	if d.mode == "sdk-error" {
		return generator.Row{}, 2, errDataSDK
	}
	mutateData(d.mode, &row)

	return row, 2, nil
}

func (d *dataDatabase) Write(ctx context.Context, row generator.Row) (int, error) {
	if d.mode == "write-error" {
		return 1, errDataSDK
	}
	attempts, err := d.finalErrorDatabase.Write(ctx, row)
	if row.ID > 0 && d.written != nil {
		d.written <- row.ID
		d.cancel()
	}

	return attempts, err
}

func (d *dataDatabase) WriteBatch(ctx context.Context, rows []generator.Row) (int, error) {
	for _, row := range rows {
		if _, err := d.Write(ctx, row); err != nil {
			return 1, err
		}
	}

	return 1, nil
}

func (d *dataDatabase) ReadBatch(ctx context.Context, ids []generator.RowID) ([]generator.Row, int, error) {
	if d.mode == "missing" {
		return nil, 2, nil
	}
	seen := make(map[generator.RowID]bool)
	rows := make([]generator.Row, 0, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		row, _, err := d.Read(ctx, id)
		if err != nil {
			return nil, 2, err
		}
		rows = append(rows, row)
	}
	if d.readDone != nil {
		d.readDone()
	}

	return rows, 2, nil
}
