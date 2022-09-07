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

	encoders            EncoderMap
	rawBuf              *bytes.Buffer
	bufCodec            rawtopiccommon.Codec
	bufEncoded          *bytes.Buffer
	bufUncompressedSize int64
}

func (m *messageWithDataContent) SetEncoders(encoders EncoderMap) {
	m.encoders = encoders
}

func (m *messageWithDataContent) GetEncodedBytes(codec rawtopiccommon.Codec) ([]byte, error) {
	if codec == rawtopiccommon.CodecRaw && m.rawBuf != nil {
		return m.rawBuf.Bytes(), nil
	}

	if codec == m.bufCodec {
		return m.bufEncoded.Bytes(), nil
	}

	if m.bufEncoded == nil {
		m.bufEncoded = newBuffer()
	} else {
		m.bufEncoded.Reset()
	}

	writer, err := m.encoders.CreateLazyEncodeWriter(codec, m.bufEncoded)
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

func newMessageDataWithContent(mess Message) (res messageWithDataContent, err error) {
	res.Message = mess
	res.Data = nil

	res.rawBuf = newBuffer()
	res.bufCodec = rawtopiccommon.CodecUNSPECIFIED

	if mess.Data != nil {
		writtenBytes, err := io.Copy(res.rawBuf, mess.Data)
		if err != nil {
			return messageWithDataContent{}, xerrors.WithStackTrace(err)
		}
		res.bufUncompressedSize = writtenBytes
	}
	return res, xerrors.WithStackTrace(err)
}

// messageWithDataContentSlice with buffer use for prevent allocation while send messsages from
// Writer to WriterImpl, because it is hot way and slice need for every call
// if messages sended one by one without additional buffer - it need for every message
type messageWithDataContentSlice struct {
	m []messageWithDataContent
}
