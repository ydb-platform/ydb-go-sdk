package xtest

import (
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func findGoroutinesLeak() error {
	var bb []byte
	for size := 1 << 16; ; size *= 2 {
		bb = make([]byte, size)
		if n := runtime.Stack(bb, true); n < size {
			bb = bb[:n]

			break
		}
	}
	goroutines := strings.Split(string(bb), "\n\n")
	unexpectedGoroutines := make([]string, 0, len(goroutines))

	for i, g := range goroutines {
		if i == 0 {
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

		case strings.HasPrefix(firstFunction, "runtime.goexit"):
			switch state {
			case "syscall", "runnable":
				continue
			}

		case strings.HasPrefix(firstFunction, "os/signal.signal_recv"),
			strings.HasPrefix(firstFunction, "os/signal.loop"):
			continue

		case strings.Contains(g, "runtime.ensureSigM"):
			continue

		case strings.Contains(g, "runtime.ReadTrace"):
			continue
		}

		unexpectedGoroutines = append(unexpectedGoroutines, g)
	}
	if l := len(unexpectedGoroutines); l > 0 {
		return fmt.Errorf("found %d unexpected goroutines:\n%s", len(goroutines), strings.Join(goroutines, "\n"))
	}

	return nil
}

func CheckGoroutinesLeak(tb testing.TB) {
	if err := findGoroutinesLeak(); err != nil {
		tb.Errorf("leak goroutines detected: %v", err)
	}
}
