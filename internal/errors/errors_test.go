package errors

import (
	"fmt"
	"testing"
)

func TestError(t *testing.T) {
	for _, test := range []struct {
		error error
		text  string
	}{
		{
			error: WithStackTrace(fmt.Errorf("TestError")),
			text:  "TestError at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestError(errors_test.go:14)`",
		},
		{
			error: WithStackTrace(fmt.Errorf("TestError%s", "Printf")),
			text:  "TestErrorPrintf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestError(errors_test.go:18)`",
		},
		{
			error: WithStackTrace(fmt.Errorf("TestError%s", "Printf")),
			text:  "TestErrorPrintf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestError(errors_test.go:22)`",
		},
	} {
		t.Run(test.text, func(t *testing.T) {
			if test.error.Error() != test.text {
				t.Fatalf("unexpected text of error: \"%s\", exp: \"%s\"", test.error.Error(), test.text)
			}
		})
	}
}
