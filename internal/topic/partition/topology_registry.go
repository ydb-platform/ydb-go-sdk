package partition

import "sync"

// TopologyRegistry owns one shared TopicTopology per topic for a single client.
type TopologyRegistry struct {
	describe   TopicDescriber
	topologies map[string]*TopicTopology
	mu         sync.Mutex
}

// NewTopologyRegistry creates a client-scoped collection of topic topologies.
func NewTopologyRegistry(describe TopicDescriber) *TopologyRegistry {
	return &TopologyRegistry{describe: describe, topologies: make(map[string]*TopicTopology)}
}

// Get returns the same TopicTopology for a topic throughout this collection's lifetime.
// It does not check whether the topic exists or load metadata; Partitions reports Describe errors.
func (r *TopologyRegistry) Get(topicPath string) *TopicTopology {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.topologies[topicPath]; ok {
		return existing
	}

	topology := &TopicTopology{topicPath: topicPath, describe: r.describe}
	r.topologies[topicPath] = topology

	return topology
}
