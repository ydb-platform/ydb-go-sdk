package topicreaderinternal

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errBadSessionWhileMessageBatchCreate       = xerrors.Wrap(errors.New("ydb: bad session while messages batch create"))        //nolint:lll
	errBadMessageOffsetWhileMessageBatchCreate = xerrors.Wrap(errors.New("ydb: bad message offset while messages batch create")) //nolint:lll
)

// PublicBatch is ordered group of message from one partition
type PublicBatch struct {
	empty.DoNotCopy

	Messages []*PublicMessage

	commitRange commitRange // от всех сообщений батча
}

func newBatch(session *partitionSession, messages []*PublicMessage) (*PublicBatch, error) {
	for i := 0; i < len(messages); i++ {
		msg := messages[i]

		if msg.commitRange.partitionSession == nil {
			msg.commitRange.partitionSession = session
		}
		if session != msg.commitRange.partitionSession {
			return nil, xerrors.WithStackTrace(errBadSessionWhileMessageBatchCreate)
		}

		if i == 0 {
			continue
		}

		prev := messages[i-1]
		if prev.commitRange.commitOffsetEnd != msg.commitRange.commitOffsetStart {
			return nil, xerrors.WithStackTrace(errBadMessageOffsetWhileMessageBatchCreate)
		}
	}

	offset := commitRange{
		partitionSession: session,
	}
	if len(messages) > 0 {
		offset.commitOffsetStart = messages[0].commitRange.commitOffsetStart
		offset.commitOffsetEnd = messages[len(messages)-1].commitRange.commitOffsetEnd
	}

	return &PublicBatch{
		Messages:    messages,
		commitRange: offset,
	}, nil
}

func newBatchFromStream(
	decoders decoderMap,
	session *partitionSession,
	sb rawtopicreader.Batch, //nolint:gocritic
) (*PublicBatch, error) {
	messages := make([]*PublicMessage, len(sb.MessageData))
	prevOffset := session.lastReceivedMessageOffset()
	for i := range sb.MessageData {
		sMess := &sb.MessageData[i]

		dstMess := &PublicMessage{}
		messages[i] = dstMess
		dstMess.CreatedAt = sMess.CreatedAt
		dstMess.MessageGroupID = sMess.MessageGroupID
		dstMess.Offset = sMess.Offset.ToInt64()
		dstMess.SeqNo = sMess.SeqNo
		dstMess.WrittenAt = sb.WrittenAt
		dstMess.ProducerID = sb.ProducerID
		dstMess.WriteSessionMetadata = sb.WriteSessionMeta

		dstMess.rawDataLen = len(sMess.Data)
		dstMess.data = createReader(decoders, sb.Codec, sMess.Data)
		dstMess.UncompressedSize = int(sMess.UncompressedSize)

		dstMess.commitRange.partitionSession = session
		dstMess.commitRange.commitOffsetStart = prevOffset + 1
		dstMess.commitRange.commitOffsetEnd = sMess.Offset + 1

		if len(sMess.MetadataItems) > 0 {
			dstMess.Metadata = make(map[string][]byte, len(sMess.MetadataItems))
			for metadataIndex := range sMess.MetadataItems {
				dstMess.Metadata[sMess.MetadataItems[metadataIndex].Key] = sMess.MetadataItems[metadataIndex].Value
			}
		}

		prevOffset = sMess.Offset
	}

	session.setLastReceivedMessageOffset(prevOffset)

	return newBatch(session, messages)
}

// Context is cancelled when code should stop to process messages batch
// for example - lost connection to server or receive stop partition signal without graceful flag
func (m *PublicBatch) Context() context.Context {
	return m.commitRange.partitionSession.Context()
}

// Topic is path of source topic of the messages in the batch
func (m *PublicBatch) Topic() string {
	return m.partitionSession().Topic
}

// PartitionID of messages in the batch
func (m *PublicBatch) PartitionID() int64 {
	return m.partitionSession().PartitionID
}

func (m *PublicBatch) partitionSession() *partitionSession {
	return m.commitRange.partitionSession
}

func (m *PublicBatch) getCommitRange() PublicCommitRange {
	return m.commitRange.getCommitRange()
}

func (m *PublicBatch) append(b *PublicBatch) (*PublicBatch, error) {
	var res *PublicBatch
	if m == nil {
		res = &PublicBatch{}
	} else {
		res = m
	}

	if res.commitRange.partitionSession != b.commitRange.partitionSession {
		return nil, xerrors.WithStackTrace(errors.New("ydb: bad partition session for merge"))
	}

	if res.commitRange.commitOffsetEnd != b.commitRange.commitOffsetStart {
		return nil, xerrors.WithStackTrace(errors.New("ydb: bad offset interval for merge"))
	}

	res.Messages = append(res.Messages, b.Messages...)
	res.commitRange.commitOffsetEnd = b.commitRange.commitOffsetEnd
	return res, nil
}

func (m *PublicBatch) cutMessages(count int) (head, rest *PublicBatch) {
	switch {
	case count == 0:
		return nil, m
	case count >= len(m.Messages):
		return m, nil
	default:
		// slice[0:count:count] - limit slice capacity and prevent overwrite rest by append messages to head
		// explicit 0 need for prevent typos, when type slice[count:count] instead of slice[:count:count]
		head, _ = newBatch(m.commitRange.partitionSession, m.Messages[0:count:count])
		rest, _ = newBatch(m.commitRange.partitionSession, m.Messages[count:])
		return head, rest
	}
}

func (m *PublicBatch) isEmpty() bool {
	return m == nil || len(m.Messages) == 0
}

func splitBytesByMessagesInBatches(batches []*PublicBatch, totalBytesCount int) error {
	restBytes := totalBytesCount

	cutBytes := func(want int) int {
		switch {
		case restBytes == 0:
			return 0
		case want >= restBytes:
			res := restBytes
			restBytes = 0
			return res
		default:
			restBytes -= want
			return want
		}
	}

	messagesCount := 0
	for batchIndex := range batches {
		messagesCount += len(batches[batchIndex].Messages)
		for messageIndex := range batches[batchIndex].Messages {
			message := batches[batchIndex].Messages[messageIndex]
			message.bufferBytesAccount = cutBytes(batches[batchIndex].Messages[messageIndex].rawDataLen)
		}
	}

	if messagesCount == 0 {
		if totalBytesCount == 0 {
			return nil
		}

		return xerrors.NewWithIssues("ydb: can't split bytes to zero length messages count")
	}

	overheadPerMessage := restBytes / messagesCount
	overheadRemind := restBytes % messagesCount

	for batchIndex := range batches {
		for messageIndex := range batches[batchIndex].Messages {
			msg := batches[batchIndex].Messages[messageIndex]

			msg.bufferBytesAccount += cutBytes(overheadPerMessage)
			if overheadRemind > 0 {
				msg.bufferBytesAccount += cutBytes(1)
			}
		}
	}

	return nil
}
