package xerrors

import (
	"fmt"
	"testing"
)

func TestStackTraceError(t *testing.T) {
	for _, test := range []struct {
		error error
		text  string
	}{
		{
			error: WithStackTrace(fmt.Errorf("TestError")),
			// nolint:lll
			text: "TestError at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestStackTraceError(stacktrace_test.go:14)`",
		},
		{
			error: WithStackTrace(fmt.Errorf("TestError%s", "Printf")),
			// nolint:lll
			text: "TestErrorPrintf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestStackTraceError(stacktrace_test.go:19)`",
		},
		{
			error: WithStackTrace(fmt.Errorf("TestError%s", "Printf")),
			// nolint:lll
			text: "TestErrorPrintf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestStackTraceError(stacktrace_test.go:24)`",
		},
	} {
		t.Run(test.text, func(t *testing.T) {
			if test.error.Error() != test.text {
				t.Fatalf("unexpected text of error: \"%s\", exp: \"%s\"", test.error.Error(), test.text)
			}
		})
	}
}
