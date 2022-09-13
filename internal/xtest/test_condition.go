package xtest

import (
	"os"
	"testing"
)

// enableAllTestsFlag set the env var for run all tests
// some of them may take a lot ot time (hours) or use functions
// not published to common test docker image
const enableAllTestsFlag = "YDB_GO_SDK_ENABLE_ALL_TESTS"

func AllowByFlag(t testing.TB, flag string) {
	if os.Getenv(flag) != "" {
		return
	}
	if os.Getenv(enableAllTestsFlag) != "" {
		return
	}
	t.Skipf("Skip test, because it need flag to run: '%v'", flag)
}
