package transactionalwriterbenchmark

import (
	"reflect"
	"testing"
)

func TestParseBenchmarkPartitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    []int64
		wantErr bool
	}{
		{
			name:  "one partition count",
			value: "64",
			want:  []int64{64},
		},
		{
			name:  "multiple partition counts",
			value: "64, 128,512",
			want:  []int64{64, 128, 512},
		},
		{
			name:    "empty list",
			value:   "",
			wantErr: true,
		},
		{
			name:    "empty item",
			value:   "64,,128",
			wantErr: true,
		},
		{
			name:    "zero",
			value:   "0",
			wantErr: true,
		},
		{
			name:    "negative",
			value:   "-1",
			wantErr: true,
		},
		{
			name:    "not a number",
			value:   "many",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseBenchmarkPartitions(test.value)
			if test.wantErr {
				if err == nil {
					t.Fatalf("parseBenchmarkPartitions(%q) error = nil", test.value)
				}

				return
			}
			if err != nil {
				t.Fatalf("parseBenchmarkPartitions(%q): %v", test.value, err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("parseBenchmarkPartitions(%q) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}
