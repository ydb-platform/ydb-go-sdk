package arrow

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func TestNewDecoderOwnership(t *testing.T) {
	for _, test := range []struct {
		name      string
		column    Column
		count     int64
		fail      error
		cancel    bool
		wantError bool
	}{
		{name: "success", column: Column{Name: "value", Type: types.Int32}, count: 1},
		{name: "column count", column: Column{Name: "value", Type: types.Int32}, count: 2, wantError: true},
		{name: "column name", column: Column{Name: "other", Type: types.Int32}, count: 1, wantError: true},
		{name: "column type", column: Column{Name: "value", Type: types.Text}, count: 1, wantError: true},
		{
			name: "read error", column: Column{Name: "value", Type: types.Int32}, count: 1,
			fail: io.ErrUnexpectedEOF, wantError: true,
		},
		{
			name: "cancel between records", column: Column{Name: "value", Type: types.Int32}, count: 1,
			cancel: true, wantError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			records := []*testRecord{
				{count: 1, data: &testArray{values: []int32{42}}},
				{count: test.count, data: &testArray{values: []int32{43}}},
			}
			reader := &testReader{records: records, fail: test.fail}
			if test.cancel {
				reader.afterRead = cancel
			}
			decode := NewDecoder(func(io.Reader, ...int) (*testReader, error) { return reader, nil })
			// Failures on the second record must also release the first retained record.
			columns := []Column{{Name: "value", Type: types.Int32}}
			reader.beforeRead = func(i int) {
				if i == 1 {
					columns[0] = test.column
				}
			}
			batches, err := decode(ctx, columns, strings.NewReader(""))
			if (err != nil) != test.wantError {
				t.Fatalf("decode error=%v", err)
			}
			if test.fail != nil && !errors.Is(err, test.fail) {
				t.Fatalf("lost error: %v", err)
			}
			if test.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if reader.released != 1 {
				t.Fatalf("reader released %d times", reader.released)
			}
			if test.wantError {
				if len(batches) != 0 {
					t.Fatal("returned partial batches on error")
				}
			}
			if !test.wantError {
				if len(batches) != 2 {
					t.Fatalf("batches=%d", len(batches))
				}
				for i, batch := range batches {
					if records[i].refs != 1 {
						t.Fatal("batch lost its retained record")
					}
					var got int32
					if err := batch.Scan(0, 0, &got); err != nil || got != int32(42+i) {
						t.Fatalf("scan=%d: %v", got, err)
					}
					batch.Release()
					batch.Release()
				}
			}
			for _, record := range records {
				if record.refs != 0 {
					t.Fatalf("leaked references: %d", record.refs)
				}
			}
		})
	}
}

func TestNewDecoderOptions(t *testing.T) {
	opts := []int{42}
	calls := 0
	decode := NewDecoder(func(_ io.Reader, options ...int) (*testReader, error) {
		calls++
		if len(options) != 1 || options[0] != 42 {
			t.Fatalf("options=%v", options)
		}

		return &testReader{}, nil
	}, opts...)
	opts[0] = 43
	for range 2 {
		batches, err := decode(t.Context(), nil, strings.NewReader(""))
		if err != nil || len(batches) != 0 {
			t.Fatalf("empty IPC: %v, %v", batches, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := decode(ctx, nil, strings.NewReader("")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if calls != 2 {
		t.Fatalf("factory calls=%d", calls)
	}
}

func TestNewDecoderFactoryError(t *testing.T) {
	expected := errors.New("factory error")
	decode := NewDecoder(func(io.Reader, ...int) (*testReader, error) { return nil, expected })
	if batches, err := decode(t.Context(), nil, strings.NewReader("")); !errors.Is(err, expected) || len(batches) != 0 {
		t.Fatalf("factory: %v, %v", batches, err)
	}
}

type testArray struct{ values []int32 }

func (a *testArray) Len() int          { return len(a.values) }
func (a *testArray) IsNull(int) bool   { return false }
func (a *testArray) NullN() int        { return 0 }
func (a *testArray) Value(i int) int32 { return a.values[i] }

type testRecord struct {
	data  *testArray
	count int64
	refs  int
}

func (r *testRecord) Retain()               { r.refs++ }
func (r *testRecord) Release()              { r.refs-- }
func (r *testRecord) NumRows() int64        { return int64(r.data.Len()) }
func (r *testRecord) NumCols() int64        { return r.count }
func (r *testRecord) ColumnName(int) string { return "value" }
func (r *testRecord) Column(int) *testArray { return r.data }

type testReader struct {
	records    []*testRecord
	current    *testRecord
	index      int
	released   int
	fail       error
	beforeRead func(int)
	afterRead  func()
}

func (r *testReader) Read() (*testRecord, error) {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.index == len(r.records) {
		if r.fail != nil {
			return nil, r.fail
		}

		return nil, io.EOF
	}
	if r.beforeRead != nil {
		r.beforeRead(r.index)
	}
	r.current = r.records[r.index]
	r.current.Retain()
	r.index++
	if r.afterRead != nil {
		r.afterRead()
	}

	return r.current, nil
}

func (r *testReader) Release() {
	r.released++
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
}
