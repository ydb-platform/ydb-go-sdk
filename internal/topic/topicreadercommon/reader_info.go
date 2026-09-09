package topicreadercommon

// ReaderInfo contains attributes shared by topic reader implementations.
type ReaderInfo struct {
	Endpoint string
	Database string
	Consumer string
	// ReaderName is nil when the application did not configure a name.
	ReaderName *string
	// Listener selects the listener detail mask; it is never a metric label.
	Listener bool
}
