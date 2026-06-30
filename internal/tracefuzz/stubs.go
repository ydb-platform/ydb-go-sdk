package tracefuzz

import (
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type fuzzCall string

func (c fuzzCall) String() string { return string(c) }

type fuzzSession struct {
	id     string
	nodeID uint32
	status string
}

func (s *fuzzSession) ID() string     { return s.id }
func (s *fuzzSession) NodeID() uint32 { return s.nodeID }
func (s *fuzzSession) Status() string { return s.status }

type fuzzTx struct{ id string }

func (t *fuzzTx) ID() string { return t.id }

type fuzzEndpoint struct {
	nodeID     uint32
	address    string
	location   string
	loadFactor float32
	updated    time.Time
	localDC    bool
}

func (e *fuzzEndpoint) NodeID() uint32         { return e.nodeID }
func (e *fuzzEndpoint) Address() string        { return e.address }
func (e *fuzzEndpoint) Location() string       { return e.location }
func (e *fuzzEndpoint) LoadFactor() float32    { return e.loadFactor }
func (e *fuzzEndpoint) LastUpdated() time.Time { return e.updated }
func (e *fuzzEndpoint) LocalDC() bool          { return e.localDC }
func (e *fuzzEndpoint) String() string         { return e.address }

type fuzzConnState struct {
	code  int
	valid bool
	text  string
}

func (s *fuzzConnState) String() string { return s.text }
func (s *fuzzConnState) IsValid() bool  { return s.valid }
func (s *fuzzConnState) Code() int      { return s.code }

type fuzzStringer string

func (s fuzzStringer) String() string { return string(s) }

type fuzzTableQueryParameters string

func (p fuzzTableQueryParameters) String() string { return string(p) }

type fuzzTableDataQuery struct {
	id  string
	yql string
}

func (q *fuzzTableDataQuery) String() string { return q.yql }
func (q *fuzzTableDataQuery) ID() string     { return q.id }
func (q *fuzzTableDataQuery) YQL() string    { return q.yql }

type fuzzTableResultErr struct {
	err error
}

func (r *fuzzTableResultErr) Err() error { return r.err }

type fuzzTableResult struct {
	fuzzTableResultErr

	count int
}

func (r *fuzzTableResult) ResultSetCount() int { return r.count }

type fuzzScriptingQueryParameters string

func (p fuzzScriptingQueryParameters) String() string { return string(p) }

type fuzzScriptingResultErr struct {
	err error
}

func (r *fuzzScriptingResultErr) Err() error { return r.err }

type fuzzScriptingResult struct {
	fuzzScriptingResultErr

	count int
}

func (r *fuzzScriptingResult) ResultSetCount() int { return r.count }

type fuzzIssue struct {
	message  string
	code     uint32
	severity uint32
}

func (i *fuzzIssue) GetMessage() string   { return i.message }
func (i *fuzzIssue) GetIssueCode() uint32 { return i.code }
func (i *fuzzIssue) GetSeverity() uint32  { return i.severity }

type fuzzTopicCommitMessage struct {
	commits []trace.TopicReaderStreamCommitInfo
}

func (m *fuzzTopicCommitMessage) GetCommitsInfo() []trace.TopicReaderStreamCommitInfo {
	return m.commits
}

type fuzzTopicDataResponse struct {
	bytesSize int
}

func (r *fuzzTopicDataResponse) GetBytesSize() int { return r.bytesSize }
func (r *fuzzTopicDataResponse) GetPartitionBatchMessagesCounts() (partitionCount, batchCount, messagesCount int) {
	return 1, 1, 1
}

type fuzzTopicInitRequest struct {
	consumer string
	topics   []string
}

func (r *fuzzTopicInitRequest) GetConsumer() string { return r.consumer }
func (r *fuzzTopicInitRequest) GetTopics() []string { return r.topics }

type fuzzTopicWriterAcks struct {
	count int
}

func (a *fuzzTopicWriterAcks) GetAcks() struct {
	AcksCount        int
	SeqNoMin         int64
	SeqNoMax         int64
	WrittenOffsetMin int64
	WrittenOffsetMax int64
	WrittenCount     int
	WrittenInTxCount int
	SkipCount        int
} {
	return struct {
		AcksCount        int
		SeqNoMin         int64
		SeqNoMax         int64
		WrittenOffsetMin int64
		WrittenOffsetMax int64
		WrittenCount     int
		WrittenInTxCount int
		SkipCount        int
	}{AcksCount: a.count}
}

func newFuzzError(f *Fuzzer) error {
	switch f.Choice(4) {
	case 0:
		return nil
	case 1:
		return fmt.Errorf("%s", f.String())
	case 2:
		return errors.New("fuzz-static")
	default:
		return fmt.Errorf("fuzz-%d", f.Int())
	}
}
