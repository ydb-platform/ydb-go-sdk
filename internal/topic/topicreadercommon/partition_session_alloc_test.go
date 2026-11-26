package topicreadercommon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionSessionFirstAssignmentWithoutMeter(t *testing.T) {
	session := NewPartitionSession(t.Context(), "topic", 1, 1, "", 1, 0, 0)
	t.Cleanup(session.Close)

	allocs := testing.AllocsPerRun(10000, func() {
		var storage PartitionSessionStorage
		require.NoError(t, storage.Add(session))
	})

	t.Logf("first Add without meter: %.0f allocs", allocs)
	require.Equal(t, float64(4), allocs, "match the allocation count at e652fb388")
}
