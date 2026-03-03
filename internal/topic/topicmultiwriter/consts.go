package topicmultiwriter

import "time"

const (
	defaultInFlightMessagesBufferSize = 1000
	defaultWriterIdleTimeout          = 30 * time.Second
	infiniteTimeout                   = time.Hour * 24 * 365
)
