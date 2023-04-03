package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexAny(t *testing.T) {
	for _, tt := range []struct {
		s         string
		ss        []string
		index     int
		substring string
	}{
		{
			s: `
SELECT 1, 2, 3
-- some comment
/* another comment */
SELECT ?
SELECT $1
`,
			ss: []string{
				"--",
				"/*",
				"?",
				"$",
			},
			index:     16,
			substring: "--",
		},
		{
			s: `
/* another comment */
SELECT 1, 2, 3
-- some comment
SELECT ?
SELECT $1
`,
			ss: []string{
				"--",
				"/*",
				"?",
				"$",
			},
			index:     1,
			substring: "/*",
		},
		{
			s: `
SELECT ?
/* another comment */
SELECT 1, 2, 3
-- some comment
SELECT $1
`,
			ss: []string{
				"--",
				"/*",
				"?",
				"$",
			},
			index:     8,
			substring: "?",
		},
		{
			s: `
SELECT $1
SELECT ?
/* another comment */
SELECT 1, 2, 3
-- some comment
`,
			ss: []string{
				"--",
				"/*",
				"?",
				"$",
			},
			index:     8,
			substring: "$",
		},
	} {
		t.Run("", func(t *testing.T) {
			index, substring := indexAny(tt.s, tt.ss...)
			require.Equal(t, tt.index, index)
			require.Equal(t, tt.substring, substring)
		})
	}
}

func TestIndexNotEscaped(t *testing.T) {
	for _, tt := range []struct {
		s        string
		ss       string
		idxOpen  int
		idxClose int
	}{
		{
			s:        "a = 'a= \\'a\\''",
			ss:       "'",
			idxOpen:  4,
			idxClose: 13,
		},
		{
			s:        " '\\'a\\''",
			ss:       "'",
			idxOpen:  1,
			idxClose: 7,
		},
	} {
		t.Run("", func(t *testing.T) {
			idxOpen := indexNotEscaped(tt.s, tt.ss)
			require.Equal(t, tt.idxOpen, idxOpen)
			idxClose := indexNotEscaped(tt.s[idxOpen+1:], tt.ss)
			require.Equal(t, tt.idxClose, idxOpen+idxClose+1)
		})
	}
}
