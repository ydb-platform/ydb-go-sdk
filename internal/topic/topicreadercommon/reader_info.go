package topicreadercommon

// ReaderInfo contains attributes shared by topic reader implementations.
type ReaderInfo struct {
	Endpoint string
	Database string
	Consumer string
}
