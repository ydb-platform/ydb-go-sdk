package xerrors

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

func Test_iterateByIssues(t *testing.T) {
	t.Run("check stopping", func(t *testing.T) {
		t.Run("stopped", func(t *testing.T) {
			counter := 0

			stopped := iterateByIssues(Operation(WithStatusCode(Ydb.StatusIds_ABORTED), WithIssues([]*Ydb_Issue.IssueMessage{
				{
					Message:   "issue one",
					IssueCode: 1,
					Position: &Ydb_Issue.IssueMessage_Position{
						Row:    15,
						Column: 3,
						File:   "",
					},
				},
				{
					Message:   "issue two",
					IssueCode: 2,
					Issues: []*Ydb_Issue.IssueMessage{
						{
							Message:   "issue three",
							IssueCode: 3,
							Position: &Ydb_Issue.IssueMessage_Position{
								Row:    16,
								Column: 4,
								File:   "test.yql",
							},
						},
						{
							Message:   "issue four",
							IssueCode: 4,
						},
					},
				},
			})),
				func(message string, code Ydb.StatusIds_StatusCode, severity uint32) (stop bool) {
					counter++

					return code == 2
				},
			)

			require.True(t, stopped)
			require.Equal(t, 2, counter)
		})
		t.Run("non stopped", func(t *testing.T) {
			counter := 0

			stopped := iterateByIssues(Operation(WithStatusCode(Ydb.StatusIds_ABORTED), WithIssues([]*Ydb_Issue.IssueMessage{
				{
					Message:   "issue one",
					IssueCode: 1,
					Position: &Ydb_Issue.IssueMessage_Position{
						Row:    15,
						Column: 3,
						File:   "",
					},
				},
				{
					Message:   "issue two",
					IssueCode: 2,
					Issues: []*Ydb_Issue.IssueMessage{
						{
							Message:   "issue three",
							IssueCode: 3,
							Position: &Ydb_Issue.IssueMessage_Position{
								Row:    16,
								Column: 4,
								File:   "test.yql",
							},
						},
						{
							Message:   "issue four",
							IssueCode: 4,
						},
					},
				},
			})),
				func(message string, code Ydb.StatusIds_StatusCode, severity uint32) (stop bool) {
					counter++

					return code == 100500
				},
			)

			require.False(t, stopped)
			require.Equal(t, 4, counter)
		})
	})
}
