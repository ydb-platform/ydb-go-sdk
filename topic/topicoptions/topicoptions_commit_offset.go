package topicoptions

// CommitOffsetOption is an option for Topic().CommitOffset() call.
type CommitOffsetOption func(*CommitOffsetOptions)

// CommitOffsetOptions holds optional parameters for CommitOffset.
type CommitOffsetOptions struct {
	ReadSessionID string
}

// WithCommitOffsetReadSessionID associates the commit with the given read session.
// Passing the current reader's session ID (from reader.ReadSessionID()) prevents
// the server from interrupting the active read session for the partition.
// If not set, the server will interrupt the active read session.
func WithCommitOffsetReadSessionID(id string) CommitOffsetOption {
	return func(o *CommitOffsetOptions) {
		o.ReadSessionID = id
	}
}
