package errors

const (
	DefaultMaxIssuesLen = 10
)

func Prepend(issues []error, err error, maxLen int) []error {
	if len(issues) > 0 && Is(err, issues[len(issues)-1]) {
		return issues
	}
	if len(issues) >= maxLen {
		issues = issues[len(issues)-maxLen:]
	}
	issues = append([]error{err}, issues...)
	return issues
}

func Contains(issues []error, err error) bool {
	for _, issue := range issues {
		if Is(issue, err) {
			return true
		}
	}
	return false
}

func ContainsOpError(issues []error, code StatusCode) bool {
	for _, issue := range issues {
		if IsOpError(issue, code) {
			return true
		}
	}
	return false
}
