package structural

import "sync"

type poolLogger struct {
	l Logger
	p *sync.Pool
}

// WithPool wraps Logger with sync.Pool used for records.
// If called on already wrapped Logger, WithPool is a no-op.
func WithPool(l Logger) Logger {
	if _, ok := l.(poolLogger); ok {
		return l
	}
	if s, ok := l.(Skipper); ok {
		l = s.WithCallerSkip(1)
	}
	p := &sync.Pool{}
	p.New = func() any {
		return &poolRecord{
			Record: l.Record(),
			p:      p,
		}
	}
	return poolLogger{
		l: l,
		p: p,
	}
}

func (pr poolLogger) Record() Record {
	return pr.p.Get().(*poolRecord)
}

func (pr poolLogger) WithName(name string) Logger {
	// child loggers use the same pool as the original one
	return poolLogger{
		l: pr.l.WithName(name),
		p: pr.p,
	}
}

// poolRecord is a wrapper of Record used by poolLogger
type poolRecord struct {
	Record
	p *sync.Pool
}

func (pr *poolRecord) Message(msg string) {
	pr.Record.Message(msg)
	pr.Reset()
	pr.p.Put(pr)
}
