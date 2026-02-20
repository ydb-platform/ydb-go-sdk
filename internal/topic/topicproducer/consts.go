package topicproducer

import "time"

const (
	defaultInFlightMessagesBufferSize = 1000
	defaultSubWriterIdleTimeout       = 30 * time.Second
	infiniteTimeout                   = time.Duration(time.Hour * 24 * 365)
)
