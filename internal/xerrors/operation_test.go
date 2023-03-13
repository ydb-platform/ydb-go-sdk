package xerrors

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

func TestIsOperationError(t *testing.T) {
	for _, code := range [...]Ydb.StatusIds_StatusCode{
		Ydb.StatusIds_BAD_REQUEST,
		Ydb.StatusIds_BAD_SESSION,
	} {
		for _, err := range []error{
			&operationError{code: code},
			Operation(WithStatusCode(code)),
			fmt.Errorf("wrapped: %w", &operationError{code: code}),
		} {
			t.Run("", func(t *testing.T) {
				if !IsOperationError(err, code) {
					t.Errorf("expected %v to be operationError with code=%v", err, code)
				}
			})
		}
	}
}

func Test_operationError_Error(t *testing.T) {
	for _, tt := range []struct {
		err  error
		text string
	}{
		{
			err:  Operation(WithStatusCode(Ydb.StatusIds_BAD_REQUEST)),
			text: "operation/BAD_REQUEST (code = 400010)",
		},
		{
			err:  Operation(WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
			text: "operation/BAD_SESSION (code = 400100)",
		},
		{
			err: Operation(WithStatusCode(Ydb.StatusIds_PRECONDITION_FAILED), WithIssues([]*Ydb_Issue.IssueMessage{
				{
					Message:   "issue one",
					IssueCode: 1,
				},
				{
					Message:   "issue two",
					IssueCode: 2,
					Issues: []*Ydb_Issue.IssueMessage{
						{
							Message:   "issue three",
							IssueCode: 3,
						},
						{
							Message:   "issue four",
							IssueCode: 4,
						},
					},
				},
			})),
			text: "operation/PRECONDITION_FAILED (code = 400120, issues = [{#1 issue one},{#2 issue two [{#3 issue three},{#4 issue four}]}])", //nolint:lll
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.text, tt.err.Error())
		})
	}
}
