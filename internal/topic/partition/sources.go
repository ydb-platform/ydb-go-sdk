package partition

import "sync"

// Sources owns one shared Source per topic for a single client.
type Sources struct {
	describe TopicDescriber
	sources  map[string]*Source
	mu       sync.Mutex
}

func NewSources(describe TopicDescriber) *Sources {
	return &Sources{describe: describe, sources: make(map[string]*Source)}
}

// Get returns the same Source for a topic throughout this collection's lifetime.
// It does not check whether the topic exists or load metadata; NewRouter reports Describe errors.
func (s *Sources) Get(topicPath string) *Source {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.sources[topicPath]; ok {
		return existing
	}

	source := &Source{
		topicPath:     topicPath,
		describe:      s.describe,
		subscriptions: make(map[*subscription]struct{}),
	}
	s.sources[topicPath] = source

	return source
}
