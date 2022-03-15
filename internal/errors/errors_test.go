package errors

import (
	"fmt"
	"testing"
)

type managedWrappingError struct {
	text           string
	withStackTrace bool
}

func (e *managedWrappingError) Error() string {
	return e.text
}

func (e *managedWrappingError) WithStackTrace() bool {
	return e.withStackTrace
}

func TestError(t *testing.T) {
	for _, test := range []struct {
		error error
		text  string
	}{
		{
			error: Error(fmt.Errorf("TestError")),
			text:  "TestError at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestError(errors_test.go:27)`",
		},
		{
			error: Errorf("TestError%s", "Printf"),
			text:  "TestErrorPrintf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestError(errors_test.go:31)`",
		},
		{
			error: ErrorfSkip(0, "TestError%s", "Printf"),
			text:  "TestErrorPrintf at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestError(errors_test.go:35)`",
		},
		{
			error: Error(&managedWrappingError{
				text:           "no wrapped managedWrappingError",
				withStackTrace: false,
			}),
			text: "no wrapped managedWrappingError",
		},
		{
			error: Error(&managedWrappingError{
				text:           "wrapped managedWrappingError",
				withStackTrace: true,
			}),
			// nolint:lll
			text: "wrapped managedWrappingError at `github.com/ydb-platform/ydb-go-sdk/v3/internal/errors.TestError(errors_test.go:46)`",
		},
	} {
		t.Run(test.text, func(t *testing.T) {
			if test.error.Error() != test.text {
				t.Fatalf("unexpected text of error: \"%s\", exp: \"%s\"", test.error.Error(), test.text)
			}
		})
	}
}
