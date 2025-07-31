package xerrors

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

func TestIsOperationError(t *testing.T) {
	for _, tt := range []struct {
		err   error
		codes []Ydb.StatusIds_StatusCode
		match bool
	}{
		// check only operation error with any ydb status code
		{
			err:   &operationError{code: Ydb.StatusIds_BAD_REQUEST},
			match: true,
		},
		{
			err:   fmt.Errorf("wrapped: %w", &operationError{code: Ydb.StatusIds_BAD_REQUEST}),
			match: true,
		},
		{
			err: Join(
				fmt.Errorf("test"),
				&operationError{code: Ydb.StatusIds_BAD_REQUEST},
				Retryable(fmt.Errorf("test")),
			),
			match: true,
		},
		// match ydb status code
		{
			err:   &operationError{code: Ydb.StatusIds_BAD_REQUEST},
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_BAD_REQUEST},
			match: true,
		},
		{
			err:   fmt.Errorf("wrapped: %w", &operationError{code: Ydb.StatusIds_BAD_REQUEST}),
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_BAD_REQUEST},
			match: true,
		},
		{
			err: Join(
				fmt.Errorf("test"),
				&operationError{code: Ydb.StatusIds_BAD_REQUEST},
				Retryable(fmt.Errorf("test")),
			),
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_BAD_REQUEST},
			match: true,
		},
		// no match ydb status code
		{
			err:   &operationError{code: Ydb.StatusIds_BAD_REQUEST},
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_ABORTED},
			match: false,
		},
		{
			err:   fmt.Errorf("wrapped: %w", &operationError{code: Ydb.StatusIds_BAD_REQUEST}),
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_ABORTED},
			match: false,
		},
		{
			err: Join(
				fmt.Errorf("test"),
				&operationError{code: Ydb.StatusIds_BAD_REQUEST},
				Retryable(fmt.Errorf("test")),
			),
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_ABORTED},
			match: false,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.match, IsOperationError(tt.err, tt.codes...))
		})
	}
}

func TestIsOperationErrorTransactionLocksInvalidated(t *testing.T) {
	for _, tt := range [...]struct {
		err   error
		isTLI bool
	}{
		{
			err: Operation(
				WithStatusCode(Ydb.StatusIds_ABORTED),
				WithIssues([]*Ydb_Issue.IssueMessage{{
					IssueCode: IssueCodeTransactionLocksInvalidated,
				}}),
			),
			isTLI: true,
		},
		{
			err: Operation(
				WithStatusCode(Ydb.StatusIds_OVERLOADED),
				WithIssues([]*Ydb_Issue.IssueMessage{{
					IssueCode: IssueCodeTransactionLocksInvalidated,
				}}),
			),
			isTLI: false,
		},
		{
			err: Operation(
				WithStatusCode(Ydb.StatusIds_ABORTED),
			),
			isTLI: false,
		},
		{
			err: Operation(
				WithStatusCode(Ydb.StatusIds_ABORTED),
				WithIssues([]*Ydb_Issue.IssueMessage{{
					Issues: []*Ydb_Issue.IssueMessage{{
						IssueCode: IssueCodeTransactionLocksInvalidated,
					}},
				}}),
			),
			isTLI: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.isTLI, IsOperationErrorTransactionLocksInvalidated(tt.err))
		})
	}
}

func Test_operationError_Error(t *testing.T) {
	for _, tt := range []struct {
		err  error
		text string
	}{
		{
			err:  Operation(WithStatusCode(Ydb.StatusIds_BAD_REQUEST), WithAddress("localhost")),
			text: "operation/BAD_REQUEST (code = 400010, address = localhost)",
		},
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
			text: "operation/PRECONDITION_FAILED (code = 400120, issues = [{15:3 => #1 'issue one'},{#2 'issue two' [{test.yql:16:4 => #3 'issue three'},{#4 'issue four'}]}])", //nolint:lll
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.text, tt.err.Error())
		})
	}
}
