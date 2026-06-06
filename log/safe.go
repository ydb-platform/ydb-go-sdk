package log

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiface"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func isNil(v any) bool {
	return xiface.IsNil(v)
}

func safeStringer(s fmt.Stringer) string {
	if isNil(s) {
		return ""
	}

	return s.String()
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
}) []trace.TopicReaderStreamCommitInfo {
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
}) (partitionCount, batchCount, messagesCount int) {
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
