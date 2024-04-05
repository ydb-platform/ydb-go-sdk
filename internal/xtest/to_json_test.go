package xtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToJSON(t *testing.T) {
	for _, tt := range []struct {
		name string
		v    interface{}
		s    string
	}{
		{
			name: CurrentFileLine(),
			v:    int64(123),
			s:    "123",
		},
		{
			name: CurrentFileLine(),
			v: struct {
				A string
				B int64
				C bool
			}{
				A: "123",
				B: 123,
				C: true,
			},
			s: "{\n\t\"A\": \"123\",\n\t\"B\": 123,\n\t\"C\": true\n}",
		},
		{
			name: CurrentFileLine(),
			v: map[string]struct {
				A string
				B int64
				C bool
			}{
				"abc": {
					A: "123",
					B: 123,
					C: true,
				},
				"def": {
					A: "456",
					B: 456,
					C: false,
				},
			},
			s: "{\n\t\"abc\": {\n\t\t\"A\": \"123\",\n\t\t\"B\": 123,\n\t\t\"C\": true\n\t},\n\t\"def\": {\n\t\t\"A\": \"456\",\n\t\t\"B\": 456,\n\t\t\"C\": false\n\t}\n}", //nolint:lll
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.s, ToJSON(tt.v))
		})
	}
}
