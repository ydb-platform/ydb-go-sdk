package errors

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

// NewWithIssues returns error which contains child issues
func NewWithIssues(text string, issues ...error) error {
	return &errorWithIssues{
		reason: text,
		issues: issues,
	}
}

type errorWithIssues struct {
	reason string
	issues []error
}

func (e *errorWithIssues) isYdbError() {}

func (e *errorWithIssues) Error() string {
	var b bytes.Buffer
	b.WriteString(e.reason)
	b.WriteString(", issues: [")
	for i, issue := range e.issues {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString(issue.Error())
	}
	b.WriteString("]")
	return b.String()
}

// Issue struct
type Issue struct {
	Message  string
	Code     uint32
	Severity uint32
}

type IssueIterator []*Ydb_Issue.IssueMessage

func (it IssueIterator) Len() int {
	return len(it)
}

func (it IssueIterator) Get(i int) (issue Issue, nested IssueIterator) {
	x := it[i]
	if xs := x.Issues; len(xs) > 0 {
		nested = IssueIterator(xs)
	}
	return Issue{
		Message:  x.GetMessage(),
		Code:     x.GetIssueCode(),
		Severity: x.GetSeverity(),
	}, nested
}
