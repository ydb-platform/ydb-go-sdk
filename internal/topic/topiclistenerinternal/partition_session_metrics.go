package topiclistenerinternal

func (lr *TopicListenerReconnector) PartitionSessionCounts() (map[string]int64, error) {
	lr.m.Lock()
	stream := lr.streamListener
	lr.m.Unlock()
	if stream == nil || lr.closing.Load() || stream.closing.Load() {
		return map[string]int64{}, nil
	}

	return stream.sessions.ActiveCountsByTopic(), nil
}
