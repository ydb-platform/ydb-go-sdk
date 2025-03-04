package xtest

import (
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func goroutineStack(all bool) []byte {
	for i := 1 << 16; ; i *= 2 {
		bb := make([]byte, i)
		if n := runtime.Stack(bb, all); n < i {
			return bb[:n]
		}
	}
}

func checkGoroutinesLeak(onLeak func(goroutines []string)) {
	currentGoroutine := string(regexp.MustCompile(`^goroutine \d+ `).Find(goroutineStack(false)))
	goroutines := strings.Split(string(goroutineStack(true)), "\n\n")
	unexpectedGoroutines := make([]string, 0, len(goroutines))

	for _, g := range goroutines {
		if strings.HasPrefix(g, currentGoroutine) {
			continue
		}
		stack := strings.Split(g, "\n")
		firstFunction := stack[1]
		state := strings.Trim(
			regexp.MustCompile(`\[.*\]`).FindString(
				regexp.MustCompile(`^goroutine \d+ \[.*\]`).FindString(stack[0]),
			), "[]",
		)
		switch {
		case strings.HasPrefix(firstFunction, "testing.RunTests"),
			strings.HasPrefix(firstFunction, "testing.(*T).Run"),
			strings.HasPrefix(firstFunction, "testing.(*T).Parallel"),
			strings.HasPrefix(firstFunction, "testing.runFuzzing"),
			strings.HasPrefix(firstFunction, "testing.runFuzzTests"):
			if strings.Contains(state, "chan receive") {
				continue
			}

		case strings.HasPrefix(firstFunction, "runtime.goexit") && state == "syscall":
			continue

		case strings.HasPrefix(firstFunction, "os/signal.signal_recv"),
			strings.HasPrefix(firstFunction, "os/signal.loop"):
			if strings.Contains(g, "runtime.ensureSigM") {
				continue
			}
		}

		unexpectedGoroutines = append(unexpectedGoroutines, g)
	}
	if l := len(unexpectedGoroutines); l > 0 {
		onLeak(goroutines)
	}
}

func CheckGoroutinesLeak(tb testing.TB) {
	tb.Helper()
	checkGoroutinesLeak(func(goroutines []string) {
		tb.Helper()
		tb.Errorf("found %d unexpected goroutines:\n%s",
			len(goroutines),
			strings.Join(goroutines, "\n"),
		)
	})
}
