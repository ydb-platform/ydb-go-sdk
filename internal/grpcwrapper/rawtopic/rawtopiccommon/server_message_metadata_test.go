package rawtopiccommon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

func TestServerMessageMetadataInterface_Equals(t *testing.T) {
	t.Run("NilComparison", func(t *testing.T) {
		var meta1, meta2 *ServerMessageMetadata
		require.True(t, meta1.Equals(meta2)) // both nil

		meta1 = &ServerMessageMetadata{}
		require.False(t, meta1.Equals(meta2)) // one nil
		require.False(t, meta2.Equals(meta1)) // reversed nil
	})

	t.Run("IdenticalEmpty", func(t *testing.T) {
		meta1 := &ServerMessageMetadata{}
		meta2 := &ServerMessageMetadata{}
		require.True(t, meta1.Equals(meta2))
		require.True(t, meta2.Equals(meta1)) // symmetric
	})

	t.Run("IdenticalWithStatus", func(t *testing.T) {
		meta1 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		}
		meta2 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		}
		require.True(t, meta1.Equals(meta2))
	})

	t.Run("DifferentStatus", func(t *testing.T) {
		meta1 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
		}
		meta2 := &ServerMessageMetadata{
			Status: rawydb.StatusInternalError,
		}
		require.False(t, meta1.Equals(meta2))
	})

	t.Run("IdenticalWithIssues", func(t *testing.T) {
		issues := rawydb.Issues{
			{Code: 100, Message: "test issue"},
		}
		meta1 := &ServerMessageMetadata{
			Issues: issues,
		}
		meta2 := &ServerMessageMetadata{
			Issues: issues,
		}
		require.True(t, meta1.Equals(meta2))
	})

	t.Run("DifferentIssues", func(t *testing.T) {
		meta1 := &ServerMessageMetadata{
			Issues: rawydb.Issues{
				{Code: 100, Message: "issue1"},
			},
		}
		meta2 := &ServerMessageMetadata{
			Issues: rawydb.Issues{
				{Code: 200, Message: "issue2"},
			},
		}
		require.False(t, meta1.Equals(meta2))
	})

	t.Run("IdenticalStatusAndIssues", func(t *testing.T) {
		meta1 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: rawydb.Issues{
				{Code: 100, Message: "warning"},
				{Code: 200, Message: "info"},
			},
		}
		meta2 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: rawydb.Issues{
				{Code: 100, Message: "warning"},
				{Code: 200, Message: "info"},
			},
		}
		require.True(t, meta1.Equals(meta2))
	})

	t.Run("ComplexNestedIssues", func(t *testing.T) {
		meta1 := &ServerMessageMetadata{
			Status: rawydb.StatusInternalError,
			Issues: rawydb.Issues{
				{
					Code:    100,
					Message: "parent issue",
					Issues: rawydb.Issues{
						{Code: 101, Message: "child issue 1"},
						{Code: 102, Message: "child issue 2"},
					},
				},
			},
		}
		meta2 := &ServerMessageMetadata{
			Status: rawydb.StatusInternalError,
			Issues: rawydb.Issues{
				{
					Code:    100,
					Message: "parent issue",
					Issues: rawydb.Issues{
						{Code: 101, Message: "child issue 1"},
						{Code: 102, Message: "child issue 2"},
					},
				},
			},
		}
		require.True(t, meta1.Equals(meta2))
	})

	t.Run("DifferentNestedIssues", func(t *testing.T) {
		meta1 := &ServerMessageMetadata{
			Status: rawydb.StatusInternalError,
			Issues: rawydb.Issues{
				{
					Code:    100,
					Message: "parent issue",
					Issues: rawydb.Issues{
						{Code: 101, Message: "child issue 1"},
					},
				},
			},
		}
		meta2 := &ServerMessageMetadata{
			Status: rawydb.StatusInternalError,
			Issues: rawydb.Issues{
				{
					Code:    100,
					Message: "parent issue",
					Issues: rawydb.Issues{
						{Code: 102, Message: "child issue 2"}, // different nested issue
					},
				},
			},
		}
		require.False(t, meta1.Equals(meta2))
	})
}

func TestServerMessageMetadataImpl_EdgeCases(t *testing.T) {
	t.Run("EmptyVsNilIssues", func(t *testing.T) {
		meta1 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: nil,
		}
		meta2 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: make(rawydb.Issues, 0),
		}
		require.True(t, meta1.Equals(meta2)) // nil slice equals empty slice
	})

	t.Run("OneFieldDifferent", func(t *testing.T) {
		// Same status, different issues
		meta1 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: rawydb.Issues{{Code: 1, Message: "test"}},
		}
		meta2 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: rawydb.Issues{{Code: 2, Message: "test"}},
		}
		require.False(t, meta1.Equals(meta2))

		// Different status, same issues
		meta3 := &ServerMessageMetadata{
			Status: rawydb.StatusInternalError,
			Issues: rawydb.Issues{{Code: 1, Message: "test"}},
		}
		meta4 := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: rawydb.Issues{{Code: 1, Message: "test"}},
		}
		require.False(t, meta3.Equals(meta4))
	})

	t.Run("SelfComparison", func(t *testing.T) {
		meta := &ServerMessageMetadata{
			Status: rawydb.StatusSuccess,
			Issues: rawydb.Issues{
				{Code: 100, Message: "test"},
			},
		}
		require.True(t, meta.Equals(meta)) // self comparison
	})
}
