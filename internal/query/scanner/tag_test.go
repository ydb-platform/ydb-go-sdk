package scanner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func TestParseFieldTag(t *testing.T) {
	tests := []struct {
		name     string
		tagValue string
		want     structFieldTag
	}{
		{
			name:     "just column name",
			tagValue: "my_column",
			want:     structFieldTag{columnName: "my_column", ydbType: ""},
		},
		{
			name:     "column name with type annotation",
			tagValue: "my_column,type:List<Text>",
			want:     structFieldTag{columnName: "my_column", ydbType: "List<Text>"},
		},
		{
			name:     "skip field marker",
			tagValue: "-",
			want:     structFieldTag{columnName: "-", ydbType: ""},
		},
		{
			name:     "empty tag",
			tagValue: "",
			want:     structFieldTag{columnName: "", ydbType: ""},
		},
		{
			name:     "with spaces",
			tagValue: "my_column , type:List<Text> ",
			want:     structFieldTag{columnName: "my_column", ydbType: "List<Text>"},
		},
		{
			name:     "optional type",
			tagValue: "id,type:Optional<Uint64>",
			want:     structFieldTag{columnName: "id", ydbType: "Optional<Uint64>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseFieldTag(tt.tagValue)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseYDBType(t *testing.T) {
	tests := []struct {
		name    string
		typeStr string
		want    types.Type
		wantErr bool
	}{
		{
			name:    "Bool",
			typeStr: "Bool",
			want:    types.Bool,
		},
		{
			name:    "Int64",
			typeStr: "Int64",
			want:    types.Int64,
		},
		{
			name:    "Uint64",
			typeStr: "Uint64",
			want:    types.Uint64,
		},
		{
			name:    "Text",
			typeStr: "Text",
			want:    types.Text,
		},
		{
			name:    "Utf8 alias",
			typeStr: "Utf8",
			want:    types.Text,
		},
		{
			name:    "String as Bytes",
			typeStr: "String",
			want:    types.Bytes,
		},
		{
			name:    "Bytes",
			typeStr: "Bytes",
			want:    types.Bytes,
		},
		{
			name:    "Date",
			typeStr: "Date",
			want:    types.Date,
		},
		{
			name:    "Timestamp",
			typeStr: "Timestamp",
			want:    types.Timestamp,
		},
		{
			name:    "List<Text>",
			typeStr: "List<Text>",
			want:    types.NewList(types.Text),
		},
		{
			name:    "List<Uint64>",
			typeStr: "List<Uint64>",
			want:    types.NewList(types.Uint64),
		},
		{
			name:    "Optional<Text>",
			typeStr: "Optional<Text>",
			want:    types.NewOptional(types.Text),
		},
		{
			name:    "Optional<Uint64>",
			typeStr: "Optional<Uint64>",
			want:    types.NewOptional(types.Uint64),
		},
		{
			name:    "List<Optional<Text>>",
			typeStr: "List<Optional<Text>>",
			want:    types.NewList(types.NewOptional(types.Text)),
		},
		{
			name:    "Optional<List<Text>>",
			typeStr: "Optional<List<Text>>",
			want:    types.NewOptional(types.NewList(types.Text)),
		},
		{
			name:    "Dict<Text,Uint64>",
			typeStr: "Dict<Text,Uint64>",
			want:    types.NewDict(types.Text, types.Uint64),
		},
		{
			name:    "Dict with spaces",
			typeStr: "Dict<Text, Uint64>",
			want:    types.NewDict(types.Text, types.Uint64),
		},
		{
			name:    "Dict<Text,List<Uint64>>",
			typeStr: "Dict<Text,List<Uint64>>",
			want:    types.NewDict(types.Text, types.NewList(types.Uint64)),
		},
		{
			name:    "empty string error",
			typeStr: "",
			wantErr: true,
		},
		{
			name:    "unknown type error",
			typeStr: "UnknownType",
			wantErr: true,
		},
		{
			name:    "invalid Dict format",
			typeStr: "Dict<Text>",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseYDBType(tt.typeStr)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.True(t, tt.want.String() == got.String(), "expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestFindTopLevelComma(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want int
	}{
		{
			name: "simple comma",
			s:    "Text,Uint64",
			want: 4,
		},
		{
			name: "no comma",
			s:    "Text",
			want: -1,
		},
		{
			name: "comma inside brackets",
			s:    "List<Text>,Uint64",
			want: 10,
		},
		{
			name: "nested brackets with comma",
			s:    "List<Dict<Text,Int64>>,Uint64",
			want: 22,
		},
		{
			name: "multiple top-level commas",
			s:    "Text,Uint64,Bool",
			want: 4, // Returns first one
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findTopLevelComma(tt.s)
			require.Equal(t, tt.want, got)
		})
	}
}
