package sugar

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type testDbName string

func (dbName testDbName) Name() string {
	return string(dbName)
}

func TestNormalizePath(t *testing.T) {
	for i, tt := range []struct {
		db           dbName
		pathToRemove string
		exp          string
	}{
		{
			db:           testDbName("/local"),
			pathToRemove: "/local/test",
			exp:          "/local/test",
		},
		{
			db:           testDbName("/local"),
			pathToRemove: "/local///test",
			exp:          "/local/test",
		},
		{
			db:           testDbName("/local"),
			pathToRemove: "test",
			exp:          "/local/test",
		},
		{
			db:           testDbName("/local"),
			pathToRemove: "/test",
			exp:          "/local/test",
		},
		{
			db:           testDbName("/local"),
			pathToRemove: "/path/to/tbl",
			exp:          "/local/path/to/tbl",
		},
		{
			db:           testDbName("/local"),
			pathToRemove: "/path///to//tbl",
			exp:          "/local/path/to/tbl",
		},
	} {
		t.Run(fmt.Sprintf("%d: {%q,%q,%q}", i, tt.db, tt.pathToRemove, tt.exp), func(t *testing.T) {
			require.Equal(t, tt.exp, normalizePath(tt.db, tt.pathToRemove))
		})
	}
}
