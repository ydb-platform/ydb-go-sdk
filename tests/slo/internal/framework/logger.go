package framework

import (
	"fmt"
	"sync"
	"time"
)

type Phase string

const (
	PhaseSetup    Phase = "setup"
	PhaseWarmup   Phase = "warmup"
	PhaseRun      Phase = "run"
	PhaseCooldown Phase = "cooldown"
	PhaseTeardown Phase = "teardown"
)

type Logger struct {
	mu    sync.Mutex
	phase Phase

	// error compression
	lastTime  time.Time
	lastError string
	lastCount int
}

func NewLogger() *Logger {
	return &Logger{}
}

func (l *Logger) SetPhase(phase Phase) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.flushLocked()
	l.phase = phase
}

func (l *Logger) prefix() string {
	ts := time.Now().Format(time.RFC3339)
	if l.phase != "" {
		return fmt.Sprintf("[%s] [%s] ", ts, l.phase)
	}

	return fmt.Sprintf("[%s] ", ts)
}

func (l *Logger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.flushLocked()
	fmt.Printf(l.prefix()+format+"\n", args...)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf(format, args...)

	if msg == l.lastError {
		l.lastCount++

		return
	}

	l.flushLocked()
	l.lastError = msg
	l.lastCount = 1
	l.lastTime = time.Now()

	fmt.Printf(l.prefix()+"ERROR: %s\n", msg)
}

func (l *Logger) flushLocked() {
	if l.lastCount > 1 {
		elapsed := time.Since(l.lastTime).Truncate(time.Millisecond)
		fmt.Printf(l.prefix()+"ERROR: (repeated %d times in %s)\n", l.lastCount-1, elapsed)
	}

	l.lastError = ""
	l.lastCount = 0
}

func (l *Logger) Flush() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.flushLocked()
}
