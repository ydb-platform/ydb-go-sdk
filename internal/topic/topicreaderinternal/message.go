package topicreaderinternal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xbytes"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errMessageWasReadEarly = xerrors.Wrap(errors.New("ydb: message was read early"))

// PublicErrUnexpectedCodec return when try to read message content with unknown codec
var PublicErrUnexpectedCodec = errors.New("unexpected codec") //nolint:revive,stylecheck

// PublicMessage is representation of topic message
type PublicMessage struct {
	empty.DoNotCopy

	SeqNo                int64
	CreatedAt            time.Time
	MessageGroupID       string
	WriteSessionMetadata map[string]string
	Offset               int64
	WrittenAt            time.Time
	ProducerID           string
	Metadata             map[string][]byte // Metadata, nil if no metadata

	commitRange        commitRange
	data               oneTimeReader
	rawDataLen         int
	bufferBytesAccount int
	UncompressedSize   int // as sent by sender, server/sdk doesn't check the field. It may be empty or wrong.
	dataConsumed       bool
}

func (m *PublicMessage) Context() context.Context {
	return m.commitRange.session().Context()
}

func (m *PublicMessage) Topic() string {
	return m.commitRange.session().Topic
}

func (m *PublicMessage) PartitionID() int64 {
	return m.commitRange.session().PartitionID
}

func (m *PublicMessage) getCommitRange() PublicCommitRange {
	return m.commitRange.getCommitRange()
}

// UnmarshalTo can call most once per message, it read all data from internal reader and
// call PublicMessageContentUnmarshaler.UnmarshalYDBTopicMessage with uncompressed content
func (m *PublicMessage) UnmarshalTo(dst PublicMessageContentUnmarshaler) error {
	if m.dataConsumed {
		return xerrors.WithStackTrace(errMessageWasReadEarly)
	}

	m.dataConsumed = true
	return callbackOnReaderContent(globalReadMessagePool, m, m.UncompressedSize, dst)
}

// Read implements io.Reader
// Read uncompressed message content
// return topicreader.UnexpectedCodec if message compressed with unknown codec
func (m *PublicMessage) Read(p []byte) (n int, err error) {
	m.dataConsumed = true
	return m.data.Read(p)
}

// PublicMessageContentUnmarshaler is interface for unmarshal message content
type PublicMessageContentUnmarshaler interface {
	// UnmarshalYDBTopicMessage MUST NOT use data after return.
	// If you need content after return from Consume - copy data content to
	// own slice with copy(dst, data)
	UnmarshalYDBTopicMessage(data []byte) error
}

func createReader(decoders decoderMap, codec rawtopiccommon.Codec, rawBytes []byte) oneTimeReader {
	reader, err := decoders.Decode(codec, bytes.NewReader(rawBytes))
	if err != nil {
		reader = errorReader{
			err: fmt.Errorf("failed to decode message with codec '%v': %w", codec, err),
		}
	}

	return newOneTimeReader(reader)
}

type errorReader struct {
	err error
}

func (u errorReader) Read(p []byte) (n int, err error) {
	return 0, u.err
}

type PublicMessageBuilder struct {
	mess *PublicMessage
}

func NewPublicMessageBuilder() *PublicMessageBuilder {
	res := &PublicMessageBuilder{}
	res.initMessage()
	return res
}

func (pmb *PublicMessageBuilder) initMessage() {
	pmb.mess = &PublicMessage{
		commitRange: commitRange{partitionSession: newPartitionSession(
			context.Background(),
			"",
			0,
			0,
			"",
			0,
			0,
		)},
	}
}

// Seqno set message Seqno
func (pmb *PublicMessageBuilder) Seqno(seqNo int64) *PublicMessageBuilder {
	pmb.mess.SeqNo = seqNo
	return pmb
}

// CreatedAt set message CreatedAt
func (pmb *PublicMessageBuilder) CreatedAt(createdAt time.Time) *PublicMessageBuilder {
	pmb.mess.CreatedAt = createdAt
	return pmb
}

func (pmb *PublicMessageBuilder) Metadata(metadata map[string][]byte) *PublicMessageBuilder {
	pmb.mess.Metadata = make(map[string][]byte, len(metadata))
	for key, val := range metadata {
		pmb.mess.Metadata[key] = xbytes.Clone(val)
	}
	return pmb
}

// MessageGroupID set message MessageGroupID
func (pmb *PublicMessageBuilder) MessageGroupID(messageGroupID string) *PublicMessageBuilder {
	pmb.mess.MessageGroupID = messageGroupID
	return pmb
}

// WriteSessionMetadata set message WriteSessionMetadata
func (pmb *PublicMessageBuilder) WriteSessionMetadata(writeSessionMetadata map[string]string) *PublicMessageBuilder {
	pmb.mess.WriteSessionMetadata = writeSessionMetadata
	return pmb
}

// Offset set message Offset
func (pmb *PublicMessageBuilder) Offset(offset int64) *PublicMessageBuilder {
	pmb.mess.Offset = offset
	return pmb
}

// WrittenAt set message WrittenAt
func (pmb *PublicMessageBuilder) WrittenAt(writtenAt time.Time) *PublicMessageBuilder {
	pmb.mess.WrittenAt = writtenAt
	return pmb
}

// ProducerID set message ProducerID
func (pmb *PublicMessageBuilder) ProducerID(producerID string) *PublicMessageBuilder {
	pmb.mess.ProducerID = producerID
	return pmb
}

// DataAndUncompressedSize set message uncompressed content and field UncompressedSize
func (pmb *PublicMessageBuilder) DataAndUncompressedSize(data []byte) *PublicMessageBuilder {
	copyData := make([]byte, len(data))
	copy(copyData, data)
	pmb.mess.data = oneTimeReader{reader: bytes.NewReader(data)}
	pmb.mess.dataConsumed = false
	pmb.mess.rawDataLen = len(copyData)
	pmb.mess.UncompressedSize = len(copyData)
	return pmb
}

// UncompressedSize set message UncompressedSize
func (pmb *PublicMessageBuilder) UncompressedSize(uncompressedSize int) *PublicMessageBuilder {
	pmb.mess.UncompressedSize = uncompressedSize
	return pmb
}

// Context set message Context
func (pmb *PublicMessageBuilder) Context(ctx context.Context) {
	pmb.mess.commitRange.partitionSession.ctx = ctx
}

// Topic set message Topic
func (pmb *PublicMessageBuilder) Topic(topic string) {
	pmb.mess.commitRange.partitionSession.Topic = topic
}

// PartitionID set message PartitionID
func (pmb *PublicMessageBuilder) PartitionID(partitionID int64) {
	pmb.mess.commitRange.partitionSession.PartitionID = partitionID
}

// Build return builded message and reset internal state for create new message
func (pmb *PublicMessageBuilder) Build() *PublicMessage {
	mess := pmb.mess
	pmb.initMessage()
	return mess
}
