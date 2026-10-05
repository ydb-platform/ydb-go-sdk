package topology

import "sync"

// Registry owns one shared Topic per topic for a single client.
type Registry struct {
	describe   TopicDescriber
	topologies map[string]*Topic
	mu         sync.Mutex
}

// NewRegistry creates a client-scoped collection of topic topologies.
func NewRegistry(describe TopicDescriber) *Registry {
	return &Registry{describe: describe, topologies: make(map[string]*Topic)}
}

// Get returns the same Topic for a topic throughout this collection's lifetime.
// It does not check whether the topic exists or load metadata; Partitions reports Describe errors.
func (r *Registry) Get(topicPath string) *Topic {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.topologies[topicPath]; ok {
		return existing
	}

	topology := &Topic{topicPath: topicPath, describe: r.describe}
	r.topologies[topicPath] = topology

	return topology
}
