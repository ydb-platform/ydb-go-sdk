package xerrors

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestIsOperationError(t *testing.T) {
	for _, tt := range []struct {
		name  string
		err   error
		codes []Ydb.StatusIds_StatusCode
		match bool
	}{
		// check only operation error with any ydb status code
		{
			name:  xtest.CurrentFileLine(),
			err:   &operationError{code: Ydb.StatusIds_BAD_REQUEST},
			match: true,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("wrapped: %w", &operationError{code: Ydb.StatusIds_BAD_REQUEST}),
			match: true,
		},
		{
			name: xtest.CurrentFileLine(),
			err: Join(
				fmt.Errorf("test"),
				&operationError{code: Ydb.StatusIds_BAD_REQUEST},
				Retryable(fmt.Errorf("test")),
			),
			match: true,
		},
		// match ydb status code
		{
			name:  xtest.CurrentFileLine(),
			err:   &operationError{code: Ydb.StatusIds_BAD_REQUEST},
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_BAD_REQUEST},
			match: true,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("wrapped: %w", &operationError{code: Ydb.StatusIds_BAD_REQUEST}),
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_BAD_REQUEST},
			match: true,
		},
		{
			name: xtest.CurrentFileLine(),
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
			name:  xtest.CurrentFileLine(),
			err:   &operationError{code: Ydb.StatusIds_BAD_REQUEST},
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_ABORTED},
			match: false,
		},
		{
			name:  xtest.CurrentFileLine(),
			err:   fmt.Errorf("wrapped: %w", &operationError{code: Ydb.StatusIds_BAD_REQUEST}),
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_ABORTED},
			match: false,
		},
		{
			name: xtest.CurrentFileLine(),
			err: Join(
				fmt.Errorf("test"),
				&operationError{code: Ydb.StatusIds_BAD_REQUEST},
				Retryable(fmt.Errorf("test")),
			),
			codes: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_ABORTED},
			match: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.match, IsOperationError(tt.err, tt.codes...))
		})
	}
}

func TestIsOperationErrorTransactionLocksInvalidated(t *testing.T) {
	for _, tt := range [...]struct {
		name  string
		err   error
		isTLI bool
	}{
		{
			name: xtest.CurrentFileLine(),
			err: Operation(
				WithStatusCode(Ydb.StatusIds_ABORTED),
				WithIssues([]*Ydb_Issue.IssueMessage{{
					IssueCode: issueCodeTransactionLocksInvalidated,
				}}),
			),
			isTLI: true,
		},
		{
			name: xtest.CurrentFileLine(),
			err: Operation(
				WithStatusCode(Ydb.StatusIds_OVERLOADED),
				WithIssues([]*Ydb_Issue.IssueMessage{{
					IssueCode: issueCodeTransactionLocksInvalidated,
				}}),
			),
			isTLI: false,
		},
		{
			name: xtest.CurrentFileLine(),
			err: Operation(
				WithStatusCode(Ydb.StatusIds_ABORTED),
			),
			isTLI: false,
		},
		{
			name: xtest.CurrentFileLine(),
			err: Operation(
				WithStatusCode(Ydb.StatusIds_ABORTED),
				WithIssues([]*Ydb_Issue.IssueMessage{{
					Issues: []*Ydb_Issue.IssueMessage{{
						IssueCode: issueCodeTransactionLocksInvalidated,
					}},
				}}),
			),
			isTLI: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.isTLI, IsOperationErrorTransactionLocksInvalidated(tt.err))
		})
	}
}

func Test_operationError_Error(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		text string
	}{
		{
			name: xtest.CurrentFileLine(),
			err:  Operation(WithStatusCode(Ydb.StatusIds_BAD_REQUEST), WithAddress("localhost")),
			text: "operation/BAD_REQUEST (code = 400010, address = localhost)",
		},
		{
			name: xtest.CurrentFileLine(),
			err:  Operation(WithStatusCode(Ydb.StatusIds_BAD_REQUEST), WithNodeID(100500)),
			text: "operation/BAD_REQUEST (code = 400010, nodeID = 100500)",
		},
		{
			name: xtest.CurrentFileLine(),
			err:  Operation(WithStatusCode(Ydb.StatusIds_BAD_REQUEST)),
			text: "operation/BAD_REQUEST (code = 400010)",
		},
		{
			name: xtest.CurrentFileLine(),
			err:  Operation(WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
			text: "operation/BAD_SESSION (code = 400100)",
		},
		{
			name: xtest.CurrentFileLine(),
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
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.text, tt.err.Error())
		})
	}
}
