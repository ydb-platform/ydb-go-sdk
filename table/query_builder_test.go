package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_queryBuilder_Query(t *testing.T) {
	for _, tt := range []struct {
		builder queryBuilder
		query   string
	}{
		{
			builder: NewQuery("SELECT $val AS value;").
				WithTablePathPrefix("/root/").
				WithUint64Param("val", 42),
			query: `PRAGMA TablePathPrefix("/root/");
DECLARE $val AS Uint64;
SELECT $val AS value;`,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.builder.Query(), tt.query)
		})
	}
}
