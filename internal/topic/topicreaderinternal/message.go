package topicreaderinternal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// PublicErrUnexpectedCodec return when try to read message content with unknown codec
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
var PublicErrUnexpectedCodec = errors.New("unexpected codec") // nolint

// PublicMessage is representation of topic message
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PublicMessage struct {
	empty.DoNotCopy

	SeqNo                int64
	CreatedAt            time.Time
	MessageGroupID       string
	WriteSessionMetadata map[string]string
	Offset               int64
	WrittenAt            time.Time
	ProducerID           string

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
		return xerrors.NewYdbErrWithStackTrace("ydb: message was read early")
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
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PublicMessageContentUnmarshaler interface {
	// UnmarshalYDBTopicMessage MUST NOT use data after return.
	// If you need content after return from Consume - copy data content to
	// own slice with copy(dst, data)
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
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
