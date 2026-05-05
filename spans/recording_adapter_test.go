package spans

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// recordedSpan is a minimal in-memory representation of a span recorded by the
// recordingAdapter. It is sufficient for asserting names, attributes and
// failure status in unit tests without depending on a real OTel SDK.
type recordedSpan struct {
	name      string
	startAttr []kv.KeyValue
	endAttr   []kv.KeyValue
	logs      []string
	err       error
	errAttr   []kv.KeyValue
	ended     bool
}

func (s *recordedSpan) ID() (string, bool)      { return "", true }
func (s *recordedSpan) TraceID() (string, bool) { return "", true }

func (s *recordedSpan) Link(_ Span, _ ...KeyValue) {}

func (s *recordedSpan) Log(msg string, _ ...KeyValue) {
	s.logs = append(s.logs, msg)
}

func (s *recordedSpan) Warn(_ error, _ ...KeyValue) {}

func (s *recordedSpan) Error(err error, attrs ...KeyValue) {
	s.err = err
	s.errAttr = append(s.errAttr, attrs...)
}

func (s *recordedSpan) End(attrs ...KeyValue) {
	s.endAttr = append(s.endAttr, attrs...)
	s.ended = true
}

// attr returns the value associated with the given key, looking through
// startAttr, endAttr and errAttr (in that order). Returns nil if no such key
// has been set.
func (s *recordedSpan) attr(key string) any {
	for _, group := range [][]kv.KeyValue{s.startAttr, s.endAttr, s.errAttr} {
		for _, kv := range group {
			if kv.Key() == key {
				return kv.AnyValue()
			}
		}
	}

	return nil
}

// recordingAdapter is a spans.Adapter implementation that captures every span
// created via Start, in order, for use in unit tests.
type recordingAdapter struct {
	mu    sync.Mutex
	spans []*recordedSpan
}

func (r *recordingAdapter) Details() trace.Details { return trace.DetailsAll }

func (r *recordingAdapter) SpanFromContext(_ context.Context) Span {
	return &recordedSpan{}
}

func (r *recordingAdapter) Start(
	ctx context.Context, name string, attrs ...KeyValue,
) (context.Context, Span) {
	r.mu.Lock()
	defer r.mu.Unlock()
	span := &recordedSpan{
		name:      name,
		startAttr: append([]kv.KeyValue{}, attrs...),
	}
	r.spans = append(r.spans, span)

	return ctx, span
}

// snapshot returns a copy of the spans slice for thread-safe iteration.
func (r *recordingAdapter) snapshot() []*recordedSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*recordedSpan, len(r.spans))
	copy(out, r.spans)

	return out
}

// byName returns the list of spans with the given name.
func (r *recordingAdapter) byName(name string) []*recordedSpan {
	out := make([]*recordedSpan, 0)
	for _, s := range r.snapshot() {
		if s.name == name {
			out = append(out, s)
		}
	}

	return out
}
