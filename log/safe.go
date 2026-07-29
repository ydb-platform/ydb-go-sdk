package log

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiface"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func isNil(v any) bool {
	return xiface.IsNil(v)
}

func safeSessionID(s interface{ ID() string }) string {
	if isNil(s) {
		return ""
	}

	return s.ID()
}

func safeSessionStatus(s interface{ Status() string }) string {
	if isNil(s) {
		return ""
	}

	return s.Status()
}

func safeTxID(tx interface{ ID() string }) string {
	if isNil(tx) {
		return ""
	}

	return tx.ID()
}

func safeResultErr(r interface{ Err() error }) error {
	if isNil(r) {
		return nil
	}

	return r.Err()
}

func safeResultSetCount(r interface{ ResultSetCount() int }) int {
	if isNil(r) {
		return 0
	}

	return r.ResultSetCount()
}

func safeCommitInfos(m interface {
	GetCommitsInfo() []trace.TopicReaderStreamCommitInfo
},
) []trace.TopicReaderStreamCommitInfo {
	if isNil(m) {
		return nil
	}

	return m.GetCommitsInfo()
}

func safeTopicConsumer(r interface{ GetConsumer() string }) string {
	if isNil(r) {
		return ""
	}

	return r.GetConsumer()
}

func safeTopicTopics(r interface{ GetTopics() []string }) []string {
	if isNil(r) {
		return nil
	}

	return r.GetTopics()
}

func safeDataResponseCounts(r interface {
	GetPartitionBatchMessagesCounts() (partitionCount, batchCount, messagesCount int)
},
) (partitionCount, batchCount, messagesCount int) {
	if isNil(r) {
		return 0, 0, 0
	}

	return r.GetPartitionBatchMessagesCounts()
}

func safeDataResponseBytes(r interface{ GetBytesSize() int }) int {
	if isNil(r) {
		return 0
	}

	return r.GetBytesSize()
}

func safeWriterAcks(r interface {
	GetAcks() struct {
		AcksCount        int
		SeqNoMin         int64
		SeqNoMax         int64
		WrittenOffsetMin int64
		WrittenOffsetMax int64
		WrittenCount     int
		WrittenInTxCount int
		SkipCount        int
	}
}) struct {
	AcksCount        int
	SeqNoMin         int64
	SeqNoMax         int64
	WrittenOffsetMin int64
	WrittenOffsetMax int64
	WrittenCount     int
	WrittenInTxCount int
	SkipCount        int
} {
	if isNil(r) {
		return struct {
			AcksCount        int
			SeqNoMin         int64
			SeqNoMax         int64
			WrittenOffsetMin int64
			WrittenOffsetMax int64
			WrittenCount     int
			WrittenInTxCount int
			SkipCount        int
		}{}
	}

	return r.GetAcks()
}

func safeIssueMessage(i trace.Issue) string {
	if isNil(i) {
		return ""
	}

	return i.GetMessage()
}

func safeIssueCode(i trace.Issue) uint32 {
	if isNil(i) {
		return 0
	}

	return i.GetIssueCode()
}

// issueWithChildren matches YDB protobuf issues carried by driver traces.
// Other trace.Issue implementations are logged as leaf issues.
type issueWithChildren interface {
	GetIssues() []*Ydb_Issue.IssueMessage
}

const (
	// maxIssueLogEntries limits all entries per top-level tree, including the truncation marker.
	maxIssueLogEntries = 20
	moreIssuesMessage  = "more issues omitted"
)

type issueLog struct {
	Message   string     `json:"message"`
	Code      uint32     `json:"code"`
	Severity  uint32     `json:"severity"`
	Issues    []issueLog `json:"issues,omitempty"`
	Truncated bool       `json:"truncated,omitempty"`
}

func makeIssueLog(i trace.Issue) issueLog {
	remaining := maxIssueLogEntries
	countRemaining := maxIssueLogEntries
	if issueLogExceedsLimit(i, &countRemaining) {
		remaining--
	}
	result, _ := makeIssueLogWithLimit(i, &remaining)

	return result
}

func issueLogExceedsLimit(i trace.Issue, remaining *int) bool {
	if isNil(i) {
		return false
	}
	if *remaining == 0 {
		return true
	}
	*remaining = *remaining - 1
	if issue, ok := i.(issueWithChildren); ok {
		for _, child := range issue.GetIssues() {
			if issueLogExceedsLimit(child, remaining) {
				return true
			}
		}
	}

	return false
}

func makeIssueLogWithLimit(i trace.Issue, remaining *int) (result issueLog, truncated bool) {
	*remaining = *remaining - 1
	result.Message = safeIssueMessage(i)
	result.Code = safeIssueCode(i)
	if !isNil(i) {
		result.Severity = i.GetSeverity()
	}
	if issue, ok := i.(issueWithChildren); ok {
		for _, child := range issue.GetIssues() {
			if isNil(child) {
				continue
			}
			if *remaining == 0 {
				result.Issues = append(result.Issues, issueLog{
					Message:   moreIssuesMessage,
					Truncated: true,
				})

				return result, true
			}
			childLog, childTruncated := makeIssueLogWithLimit(child, remaining)
			result.Issues = append(result.Issues, childLog)
			if childTruncated {
				return result, true
			}
		}
	}

	return result, false
}

func safeConnState(s trace.ConnState) string {
	if isNil(s) {
		return ""
	}

	return s.String()
}

func safeEndpointString(e trace.EndpointInfo) string {
	if isNil(e) {
		return ""
	}

	return e.String()
}

func safeEndpointNodeID(e trace.EndpointInfo) int64 {
	if isNil(e) {
		return 0
	}

	return int64(e.NodeID())
}

func safeEndpointAddress(e trace.EndpointInfo) string {
	if isNil(e) {
		return ""
	}

	return e.Address()
}

func safeEndpointLocation(e trace.EndpointInfo) string {
	if isNil(e) {
		return ""
	}

	return e.Location()
}
