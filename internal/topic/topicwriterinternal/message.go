package topicwriterinternal

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Message struct {
	SeqNo        int64
	CreatedAt    time.Time
	Data         io.Reader
	Partitioning PublicPartitioning
}

type PublicPartitioning struct {
	messageGroupID string
	partitionID    int64
	hasPartitionID bool
}

func (p PublicPartitioning) ToRaw() rawtopicwriter.Partitioning {
	if p.hasPartitionID {
		return rawtopicwriter.NewPartitioningPartitionID(p.partitionID)
	}
	return rawtopicwriter.NewPartitioningMessageGroup(p.messageGroupID)
}

func NewPartitioningWithMessageGroupID(id string) PublicPartitioning {
	return PublicPartitioning{
		messageGroupID: id,
	}
}

func NewPartitioningWithPartitionID(id int64) PublicPartitioning {
	return PublicPartitioning{
		partitionID:    id,
		hasPartitionID: true,
	}
}

type messageWithDataContent struct {
	Message

	encoders            *EncoderMap
	hasRawContent       bool
	rawBuf              bytes.Buffer
	hasEncodedContent   bool
	bufCodec            rawtopiccommon.Codec
	bufEncoded          bytes.Buffer
	bufUncompressedSize int64
}

func (m *messageWithDataContent) GetEncodedBytes(codec rawtopiccommon.Codec) ([]byte, error) {
	if codec == rawtopiccommon.CodecRaw && m.hasRawContent {
		return m.rawBuf.Bytes(), nil
	}

	if codec == m.bufCodec {
		return m.bufEncoded.Bytes(), nil
	}
	m.bufEncoded.Reset()

	writer, err := m.encoders.CreateLazyEncodeWriter(codec, &m.bufEncoded)
	if err != nil {
		return nil, xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: failed create encoder for message, codec '%v': %w", codec, err)))
	}
	_, err = writer.Write(m.rawBuf.Bytes())
	if err == nil {
		err = writer.Close()
	}
	if err != nil {
		return nil, xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: failed to compress message, codec '%v': %w", codec, err)))
	}
	return m.bufEncoded.Bytes(), nil
}

func (m *messageWithDataContent) readDataToRawBuf() error {
	m.rawBuf.Reset()
	m.hasRawContent = true
	if m.Data != nil {
		writtenBytes, err := io.Copy(&m.rawBuf, m.Data)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		m.Data = nil
		m.bufUncompressedSize = writtenBytes
	}
	return nil
}

func (m *messageWithDataContent) readDataToTargetCodec(codec rawtopiccommon.Codec) error {
	m.hasEncodedContent = true
	m.bufCodec = codec
	m.bufEncoded.Reset()

	encoder, err := m.encoders.CreateLazyEncodeWriter(codec, &m.bufEncoded)
	if err != nil {
		return err
	}

	reader := m.Data
	if reader == nil {
		reader = &bytes.Reader{}
	}
	bytesCount, err := io.Copy(encoder, reader)
	if err == nil {
		err = encoder.Close()
	}
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: failed compress message with codec '%v': %w", codec, err)))
	}
	m.bufUncompressedSize = bytesCount
	return nil
}

func newMessageDataWithContent(mess Message, encoders *EncoderMap, targetCodec rawtopiccommon.Codec) (res messageWithDataContent, err error) {
	res.encoders = encoders
	res.Message = mess

	if targetCodec == rawtopiccommon.CodecUNSPECIFIED || targetCodec == rawtopiccommon.CodecRaw {
		err = res.readDataToRawBuf()
	} else {
		err = res.readDataToTargetCodec(targetCodec)
	}

	return res, xerrors.WithStackTrace(err)
}
