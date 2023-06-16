package scheme

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryTypePrintf(t *testing.T) {
	require.Equal(t, "[Table ColumnTable]", fmt.Sprintf("%v", []EntryType{EntryTable, EntryColumnTable}))
}
