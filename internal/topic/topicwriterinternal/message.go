package topicwriterinternal

import (
	"bytes"
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

	buf                 *bytes.Buffer
	bufCodec            rawtopiccommon.Codec
	bufUncompressedSize int64
}

func newMessageDataWithContent(mess Message) (res messageWithDataContent, err error) {
	res.Message = mess
	res.buf = newBuffer()
	res.bufCodec = rawtopiccommon.CodecRaw
	if mess.Data != nil {
		_, err = io.Copy(res.buf, res.Data)
	}
	res.Data = nil
	return res, xerrors.WithStackTrace(err)
}

// messageWithDataContentSlice with buffer use for prevent allocation while send messsages from
// Writer to WriterImpl, because it is hot way and slice need for every call
// if messages sended one by one without additional buffer - it need for every message
type messageWithDataContentSlice struct {
	m []messageWithDataContent
}
