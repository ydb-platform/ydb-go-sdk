package errors

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsYdb(t *testing.T) {
	for _, test := range []struct {
		error      error
		isYdbError bool
	}{
		{
			error:      WithStackTrace(fmt.Errorf("TestError")),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(fmt.Errorf("TestError%s", "Printf")),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(fmt.Errorf("TestError%s", "Printf")),
			isYdbError: true,
		},
		{
			error:      fmt.Errorf("TestError%s", "Printf"),
			isYdbError: false,
		},
		{
			error:      errors.New("TestError"),
			isYdbError: false,
		},
	} {
		t.Run(test.error.Error(), func(t *testing.T) {
			if IsYdb(test.error) != test.isYdbError {
				t.Fatalf("unexpected check ydb error: %v, want: %v", IsYdb(test.error), test.isYdbError)
			}
		})
	}
}
