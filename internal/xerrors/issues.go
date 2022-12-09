package xerrors

import (
	"bytes"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

const primaryKeyConstraintViolationCode = Ydb.StatusIds_StatusCode(2012)

// NewWithIssues returns error which contains child issues
func NewWithIssues(text string, issues ...error) error {
	err := &errorWithIssues{
		reason: text,
	}
	for i := range issues {
		if issues[i] != nil {
			err.issues = append(err.issues, issues[i])
		}
	}
	return err
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

func IterateByIssues(err error, it func(message string, code Ydb.StatusIds_StatusCode, severity uint32)) {
	var o *operationError
	if !errors.As(err, &o) {
		return
	}
	iterate(o.Issues(), it)
}

func iterate(
	issues []*Ydb_Issue.IssueMessage,
	it func(message string, code Ydb.StatusIds_StatusCode, severity uint32),
) {
	for _, issue := range issues {
		it(issue.GetMessage(), Ydb.StatusIds_StatusCode(issue.GetIssueCode()), issue.GetSeverity())
		iterate(issue.GetIssues(), it)
	}
}

func IsPrimaryKeyConstraintViolation(err error) (conflictDetected bool) {
	var o *operationError
	if !errors.As(err, &o) {
		return false
	}
	iterate(o.Issues(), func(message string, code Ydb.StatusIds_StatusCode, severity uint32) {
		if code == primaryKeyConstraintViolationCode {
			conflictDetected = true
		}
	})
	return conflictDetected
}
