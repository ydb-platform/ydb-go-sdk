package errors

import "bytes"

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
