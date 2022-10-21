package testutil

import (
	"os"
	"testing"
)

// Unsetenv calls os.Unsetenv. It also resets variable value with tb.Cleanup.
// If the variable is not set initially, Unsetenv does nothing.
func Unsetenv(tb testing.TB, key string) {
	value, exists := os.LookupEnv(key)
	if !exists {
		return
	}
	_ = os.Unsetenv(key)
	tb.Cleanup(func() {
		os.Setenv(key, value)
	})
}
