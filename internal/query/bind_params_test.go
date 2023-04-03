package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBindParams(t *testing.T) {
	for _, tt := range []struct {
		query string
		t     paramType
		want  string
	}{
		{
			query: "SELECT ?, value FROM tbl WHERE query = `SELECT ?, value FROM tbl WHERE query = \"?\";`;",
			t:     paramTypePositional,
			want:  "SELECT $p0, value FROM tbl WHERE query = `SELECT ?, value FROM tbl WHERE query = \"?\";`;",
		},
		{
			query: "SELECT $1, value FROM tbl WHERE query = `SELECT $1, value FROM tbl WHERE query = \"$2\";`;",
			t:     paramTypeNumeric,
			want:  "SELECT $p0, value FROM tbl WHERE query = `SELECT $1, value FROM tbl WHERE query = \"$2\";`;",
		},
		{
			query: `SELECT $1, value FROM tbl WHERE query = "SELECT $1, value FROM tbl WHERE query = \"$2\";";`,
			t:     paramTypeNumeric,
			want:  `SELECT $p0, value FROM tbl WHERE query = "SELECT $1, value FROM tbl WHERE query = \"$2\";";`,
		},
		{
			query: `SELECT $1, value FROM tbl WHERE query = 'SELECT $1, value FROM tbl WHERE query = \'$2\';';`,
			t:     paramTypeNumeric,
			want:  `SELECT $p0, value FROM tbl WHERE query = 'SELECT $1, value FROM tbl WHERE query = \'$2\';';`,
		},
		{
			query: `SELECT $1, value FROM tbl WHERE id = '1';`,
			t:     paramTypeNumeric,
			want:  `SELECT $p0, value FROM tbl WHERE id = '1';`,
		},
		{
			query: "SELECT $1, value FROM tbl WHERE id = `$2`;",
			t:     paramTypeNumeric,
			want:  "SELECT $p0, value FROM tbl WHERE id = `$2`;",
		},
		{
			query: "SELECT $1, value FROM tbl WHERE id = `2`;",
			t:     paramTypeNumeric,
			want:  "SELECT $p0, value FROM tbl WHERE id = `2`;",
		},
		{
			query: `SELECT $1, value FROM tbl WHERE id = "$2";`,
			t:     paramTypeNumeric,
			want:  `SELECT $p0, value FROM tbl WHERE id = "$2";`,
		},
		{
			query: `SELECT $1, value FROM tbl WHERE id = "1";`,
			t:     paramTypeNumeric,
			want:  `SELECT $p0, value FROM tbl WHERE id = "1";`,
		},
		{
			query: `SELECT $1, value FROM tbl WHERE id = '$2';`,
			t:     paramTypeNumeric,
			want:  `SELECT $p0, value FROM tbl WHERE id = '$2';`,
		},
		{
			query: `SELECT $1, value FROM tbl WHERE id = '1';`,
			t:     paramTypeNumeric,
			want:  `SELECT $p0, value FROM tbl WHERE id = '1';`,
		},
		{
			query: `
-- line comment with question symbol $3 
SELECT $22
/* multiline comment 
with question symbol $15 
*/
SELECT $1
`,
			t: paramTypeNumeric,
			want: `
-- line comment with question symbol $3 
SELECT $p21
/* multiline comment 
with question symbol $15 
*/
SELECT $p0
`,
		},
		{
			query: `
-- line comment with question symbol $3 
SELECT $22
/* multiline comment 
with question symbol $15 
*/
SELECT $1 -- comment`,
			t: paramTypeNumeric,
			want: `
-- line comment with question symbol $3 
SELECT $p21
/* multiline comment 
with question symbol $15 
*/
SELECT $p0 -- comment`,
		},
		{
			query: `
-- line comment with question symbol ? 
SELECT ?
/* multiline comment 
with question symbol ? 
*/
SELECT ?
`,
			t: paramTypePositional,
			want: `
-- line comment with question symbol ? 
SELECT $p0
/* multiline comment 
with question symbol ? 
*/
SELECT $p1
`,
		},
		{
			query: `
-- line comment with question symbol ? 
SELECT ? /* multiline comment 
with question symbol ? 
*/ SELECT ? -- another comment
`,
			t: paramTypePositional,
			want: `
-- line comment with question symbol ? 
SELECT $p0 /* multiline comment 
with question symbol ? 
*/ SELECT $p1 -- another comment
`,
		},
	} {
		t.Run("", func(t *testing.T) {
			query, err := bindParams(tt.query, tt.t, nil)
			require.NoError(t, err)
			require.Equal(t, tt.want, query)
		})
	}
}
