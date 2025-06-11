package rawydb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue_Equals(t *testing.T) {
	t.Run("NilComparison", func(t *testing.T) {
		var issue1, issue2 *Issue
		require.True(t, issue1.Equals(issue2)) // both nil

		issue1 = &Issue{Code: 1, Message: "test"}
		require.False(t, issue1.Equals(issue2)) // one nil
		require.False(t, issue2.Equals(issue1)) // reversed nil
	})

	t.Run("IdenticalIssues", func(t *testing.T) {
		issue1 := &Issue{
			Code:    100,
			Message: "test message",
		}
		issue2 := &Issue{
			Code:    100,
			Message: "test message",
		}
		require.True(t, issue1.Equals(issue2))
		require.True(t, issue2.Equals(issue1)) // symmetric
	})

	t.Run("DifferentCode", func(t *testing.T) {
		issue1 := &Issue{Code: 100, Message: "test"}
		issue2 := &Issue{Code: 200, Message: "test"}
		require.False(t, issue1.Equals(issue2))
	})

	t.Run("DifferentMessage", func(t *testing.T) {
		issue1 := &Issue{Code: 100, Message: "test1"}
		issue2 := &Issue{Code: 100, Message: "test2"}
		require.False(t, issue1.Equals(issue2))
	})

	t.Run("NestedIssues", func(t *testing.T) {
		issue1 := &Issue{
			Code:    100,
			Message: "parent",
			Issues: Issues{
				{Code: 101, Message: "child1"},
				{Code: 102, Message: "child2"},
			},
		}
		issue2 := &Issue{
			Code:    100,
			Message: "parent",
			Issues: Issues{
				{Code: 101, Message: "child1"},
				{Code: 102, Message: "child2"},
			},
		}
		require.True(t, issue1.Equals(issue2))
	})

	t.Run("DifferentNestedIssues", func(t *testing.T) {
		issue1 := &Issue{
			Code:    100,
			Message: "parent",
			Issues: Issues{
				{Code: 101, Message: "child1"},
			},
		}
		issue2 := &Issue{
			Code:    100,
			Message: "parent",
			Issues: Issues{
				{Code: 102, Message: "child2"},
			},
		}
		require.False(t, issue1.Equals(issue2))
	})

	t.Run("DeeplyNestedIssues", func(t *testing.T) {
		issue1 := &Issue{
			Code:    100,
			Message: "root",
			Issues: Issues{
				{
					Code:    101,
					Message: "level1",
					Issues: Issues{
						{Code: 201, Message: "level2"},
					},
				},
			},
		}
		issue2 := &Issue{
			Code:    100,
			Message: "root",
			Issues: Issues{
				{
					Code:    101,
					Message: "level1",
					Issues: Issues{
						{Code: 201, Message: "level2"},
					},
				},
			},
		}
		require.True(t, issue1.Equals(issue2))

		// Different deep nested
		issue3 := &Issue{
			Code:    100,
			Message: "root",
			Issues: Issues{
				{
					Code:    101,
					Message: "level1",
					Issues: Issues{
						{Code: 202, Message: "level2"}, // different code
					},
				},
			},
		}
		require.False(t, issue1.Equals(issue3))
	})
}

func TestIssues_Equals(t *testing.T) {
	t.Run("EmptySlices", func(t *testing.T) {
		var issues1, issues2 Issues
		require.True(t, issues1.Equals(issues2))
	})

	t.Run("NilVsEmpty", func(t *testing.T) {
		var issues1 Issues
		issues2 := make(Issues, 0)
		require.True(t, issues1.Equals(issues2)) // nil slice equals empty slice
	})

	t.Run("DifferentLength", func(t *testing.T) {
		issues1 := Issues{
			{Code: 1, Message: "first"},
		}
		issues2 := Issues{
			{Code: 1, Message: "first"},
			{Code: 2, Message: "second"},
		}
		require.False(t, issues1.Equals(issues2))
	})

	t.Run("SameLengthDifferentContent", func(t *testing.T) {
		issues1 := Issues{
			{Code: 1, Message: "first"},
			{Code: 2, Message: "second"},
		}
		issues2 := Issues{
			{Code: 1, Message: "first"},
			{Code: 3, Message: "third"}, // different second element
		}
		require.False(t, issues1.Equals(issues2))
	})

	t.Run("IdenticalContent", func(t *testing.T) {
		issues1 := Issues{
			{Code: 1, Message: "first"},
			{Code: 2, Message: "second"},
		}
		issues2 := Issues{
			{Code: 1, Message: "first"},
			{Code: 2, Message: "second"},
		}
		require.True(t, issues1.Equals(issues2))
		require.True(t, issues2.Equals(issues1)) // symmetric
	})

	t.Run("ComplexNestedStructure", func(t *testing.T) {
		issues1 := Issues{
			{
				Code:    1,
				Message: "root1",
				Issues: Issues{
					{Code: 11, Message: "child1.1"},
					{Code: 12, Message: "child1.2"},
				},
			},
			{
				Code:    2,
				Message: "root2",
				Issues: Issues{
					{Code: 21, Message: "child2.1"},
				},
			},
		}
		issues2 := Issues{
			{
				Code:    1,
				Message: "root1",
				Issues: Issues{
					{Code: 11, Message: "child1.1"},
					{Code: 12, Message: "child1.2"},
				},
			},
			{
				Code:    2,
				Message: "root2",
				Issues: Issues{
					{Code: 21, Message: "child2.1"},
				},
			},
		}
		require.True(t, issues1.Equals(issues2))
	})
}
