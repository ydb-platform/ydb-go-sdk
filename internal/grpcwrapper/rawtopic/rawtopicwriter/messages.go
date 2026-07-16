package rawtopicwriter

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errWriteResultProtoIsNil             = xerrors.Wrap(errors.New("ydb: write result proto is nil"))
	errWriteResultResponseWriteAckIsNil  = xerrors.Wrap(errors.New("ydb: write result response write ack is nil"))
	errWriteResultResponseStatisticIsNil = xerrors.Wrap(errors.New("ydb: write result response statistic is nil"))
)

type InitRequest struct {
	clientMessageImpl

	Path             string
	ProducerID       string
	WriteSessionMeta map[string]string

	Partitioning Partitioning

	GetLastSeqNo bool
}

func (r *InitRequest) toProto() (*Ydb_Topic.StreamWriteMessage_InitRequest, error) {
	res := Ydb_Topic.StreamWriteMessage_InitRequest_builder{
		Path:             r.Path,
		ProducerId:       r.ProducerID,
		WriteSessionMeta: r.WriteSessionMeta,
		GetLastSeqNo:     r.GetLastSeqNo,
	}.Build()

	err := r.Partitioning.setToProtoInitRequest(res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Partitioning is struct because it included in per-message structure and
// places on hot-path for write messages
// structure will work and compile-optimization better then interface
type Partitioning struct {
	Type           PartitioningType
	MessageGroupID string
	PartitionID    int64
	Generation     int64
}

func NewPartitioningMessageGroup(messageGroupID string) Partitioning {
	return Partitioning{
		Type:           PartitioningMessageGroupID,
		MessageGroupID: messageGroupID,
	}
}

func NewPartitioningPartitionID(partitionID int64) Partitioning {
	return Partitioning{
		Type:        PartitioningPartitionID,
		PartitionID: partitionID,
	}
}

func NewPartitioningPartitionWithGeneration(partitionID, generation int64) Partitioning {
	return Partitioning{
		Type:        PartitioningPartitionWithGeneration,
		PartitionID: partitionID,
		Generation:  generation,
	}
}

func (p *Partitioning) setToProtoInitRequest(r *Ydb_Topic.StreamWriteMessage_InitRequest) error {
	switch p.Type {
	case PartitioningUndefined:
		r.ClearPartitioning()
	case PartitioningMessageGroupID:
		r.SetMessageGroupId(p.MessageGroupID)
	case PartitioningPartitionID:
		r.SetPartitionId(p.PartitionID)
	case PartitioningPartitionWithGeneration:
		r.SetPartitionWithGeneration(Ydb_Topic.PartitionWithGeneration_builder{
			PartitionId: p.PartitionID,
			Generation:  p.Generation,
		}.Build())
	default:
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: unexpected partition type while set to init request: %v",
			p.Type,
		)))
	}

	return nil
}

func (p *Partitioning) setToProtoMessage(m *Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData) error {
	switch p.Type {
	case PartitioningUndefined:
		m.ClearPartitioning()
	case PartitioningMessageGroupID:
		m.SetMessageGroupId(p.MessageGroupID)
	case PartitioningPartitionID:
		m.SetPartitionId(p.PartitionID)
	case PartitioningPartitionWithGeneration:
		m.SetPartitionWithGeneration(Ydb_Topic.PartitionWithGeneration_builder{
			PartitionId: p.PartitionID,
			Generation:  p.Generation,
		}.Build())
	default:
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: unexpected partition type while set to message proto: %v",
			p.Type,
		)))
	}

	return nil
}

type PartitioningType int

const (
	PartitioningUndefined PartitioningType = iota
	PartitioningMessageGroupID
	PartitioningPartitionID
	PartitioningPartitionWithGeneration
)

type InitResult struct {
	serverMessageImpl
	rawtopiccommon.ServerMessageMetadata

	LastSeqNo       int64
	SessionID       string
	PartitionID     int64
	SupportedCodecs rawtopiccommon.SupportedCodecs
}

func (r *InitResult) mustFromProto(response *Ydb_Topic.StreamWriteMessage_InitResponse) {
	r.SessionID = response.GetSessionId()
	r.PartitionID = response.GetPartitionId()
	r.LastSeqNo = response.GetLastSeqNo()
	r.SupportedCodecs.MustFromProto(response.GetSupportedCodecs())
}

type WriteRequest struct {
	clientMessageImpl

	Messages []MessageData
	Codec    rawtopiccommon.Codec
	Tx       rawtopiccommon.TransactionIdentity
}

func (r *WriteRequest) toProto() (p *Ydb_Topic.StreamWriteMessage_WriteRequest, err error) {
	messages := make([]*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData, len(r.Messages))

	for i := range r.Messages {
		messages[i], err = r.Messages[i].ToProto()
		if err != nil {
			return nil, err
		}
	}

	return Ydb_Topic.StreamWriteMessage_WriteRequest_builder{
		Messages: messages,
		Codec:    int32(r.Codec.ToProto()),
		Tx:       r.Tx.ToProto(),
	}.Build(), nil
}

var writeRequestClientMessageSize = proto.Size(Ydb_Topic.StreamWriteMessage_FromClient_builder{
	WriteRequest: &Ydb_Topic.StreamWriteMessage_WriteRequest{},
}.Build())

func (r *WriteRequest) Size() int {
	if mess, err := r.toProto(); err == nil {
		size := proto.Size(mess) + writeRequestClientMessageSize

		return size
	}

	return 0
}

func (r *WriteRequest) FillCache() *WriteRequest {
	r.Size()

	return r
}

func (r *WriteRequest) Cut(count int) (head *WriteRequest, rest *WriteRequest) {
	if count >= len(r.Messages) {
		return r, nil
	}

	rest = &WriteRequest{}
	*rest = *r
	r.Messages, rest.Messages = r.Messages[:count], r.Messages[count:]

	return r, rest
}

type MessageData struct {
	SeqNo            int64
	CreatedAt        time.Time
	UncompressedSize int64
	Partitioning     Partitioning
	MetadataItems    []rawtopiccommon.MetadataItem
	Data             []byte

	size  int
	proto *Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData
}

func (d *MessageData) ToProto() (*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData, error) {
	if d.proto != nil {
		return d.proto, nil
	}

	res := Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData_builder{
		SeqNo:            d.SeqNo,
		CreatedAt:        timestamppb.New(d.CreatedAt),
		Data:             d.Data,
		UncompressedSize: d.UncompressedSize,
	}.Build()
	err := d.Partitioning.setToProtoMessage(res)
	if err != nil {
		return nil, err
	}

	for i := range d.MetadataItems {
		res.SetMetadataItems(append(res.GetMetadataItems(), Ydb_Topic.MetadataItem_builder{
			Key:   d.MetadataItems[i].Key,
			Value: d.MetadataItems[i].Value,
		}.Build()))
	}

	d.proto = res

	return d.proto, nil
}

func (d *MessageData) ProtoWireSizeBytes() int {
	if d.size == 0 {
		if p, err := d.ToProto(); err == nil {
			d.size = proto.Size(p)
		}
	}

	return d.size
}

type WriteResult struct {
	serverMessageImpl
	rawtopiccommon.ServerMessageMetadata

	Acks            []WriteAck
	PartitionID     int64
	WriteStatistics WriteStatistics
}

func (r *WriteResult) fromProto(response *Ydb_Topic.StreamWriteMessage_WriteResponse) error {
	if response == nil {
		return xerrors.WithStackTrace(errWriteResultProtoIsNil)
	}
	r.Acks = make([]WriteAck, len(response.GetAcks()))
	for i := range response.GetAcks() {
		if err := r.Acks[i].fromProto(response.GetAcks()[i]); err != nil {
			return err
		}
	}
	r.PartitionID = response.GetPartitionId()

	return r.WriteStatistics.fromProto(response.GetWriteStatistics())
}

// GetAcks implemtnts trace.TopicWriterResultMessagesInfoAcks interface
func (r *WriteResult) GetAcks() (res traceAck) {
	res.AcksCount = len(r.Acks)
	if res.AcksCount > 0 {
		res.SeqNoMin = r.Acks[0].SeqNo
		res.WrittenOffsetMin = r.Acks[0].MessageWriteStatus.WrittenOffset
	}
	for i := range r.Acks {
		ack := &r.Acks[i]
		switch ack.MessageWriteStatus.Type {
		case WriteStatusTypeWritten:
			res.WrittenCount++
		case WriteStatusTypeSkipped:
			res.SkipCount++
		case WriteStatusTypeWrittenInTx:
			res.WrittenInTxCount++
		}

		if ack.SeqNo < res.SeqNoMin {
			res.SeqNoMin = ack.SeqNo
		} else if ack.SeqNo > res.SeqNoMax {
			res.SeqNoMax = ack.SeqNo
		}

		if ack.MessageWriteStatus.WrittenOffset < res.SeqNoMin {
			res.WrittenOffsetMin = ack.MessageWriteStatus.WrittenOffset
		} else if ack.MessageWriteStatus.WrittenOffset > res.WrittenOffsetMax {
			res.WrittenOffsetMax = ack.MessageWriteStatus.WrittenOffset
		}
	}

	return res
}

type traceAck = struct {
	AcksCount        int
	SeqNoMin         int64
	SeqNoMax         int64
	WrittenOffsetMin int64
	WrittenOffsetMax int64
	WrittenCount     int
	WrittenInTxCount int
	SkipCount        int
}

type WriteAck struct {
	SeqNo              int64
	MessageWriteStatus MessageWriteStatus
}

func (wa *WriteAck) fromProto(pb *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck) error {
	if pb == nil {
		return xerrors.WithStackTrace(errWriteResultResponseWriteAckIsNil)
	}
	wa.SeqNo = pb.GetSeqNo()

	return wa.MessageWriteStatus.fromProto(pb)
}

// MessageWriteStatus is struct because it included in per-message structure and
// places on hot-path for write messages
// structure will work and compile-optimization better then interface
type MessageWriteStatus struct {
	Type          WriteStatusType
	WrittenOffset int64
	SkippedReason WriteStatusSkipReason
}

func (s *MessageWriteStatus) fromProto(pb *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck) error {
	switch pb.WhichMessageWriteStatus() {
	case Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Written_case:
		s.Type = WriteStatusTypeWritten
		s.WrittenOffset = pb.GetWritten().GetOffset()

		return nil
	case Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Skipped_case:
		s.Type = WriteStatusTypeSkipped
		s.SkippedReason = WriteStatusSkipReason(pb.GetSkipped().GetReason())

		return nil

	case Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_WrittenInTx_case:
		s.Type = WriteStatusTypeWrittenInTx

		return nil

	default:
		return xerrors.WithStackTrace(xerrors.Wrap(
			fmt.Errorf("ydb: unexpected write status type: %v", pb.WhichMessageWriteStatus()),
		))
	}
}

type WriteStatusType int

const (
	WriteStatusTypeWritten WriteStatusType = iota + 1
	WriteStatusTypeSkipped
	WriteStatusTypeWrittenInTx
)

func (t WriteStatusType) String() string {
	switch t {
	case WriteStatusTypeSkipped:
		return "Skipped"
	case WriteStatusTypeWritten:
		return "Written"
	case WriteStatusTypeWrittenInTx:
		return "WrittenInTx"
	default:
		return strconv.Itoa(int(t))
	}
}

type WriteStatusSkipReason int

const (
	WriteStatusSkipReasonUnspecified    = WriteStatusSkipReason(Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Skipped_REASON_UNSPECIFIED)     //nolint:lll
	WriteStatusSkipReasonAlreadyWritten = WriteStatusSkipReason(Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Skipped_REASON_ALREADY_WRITTEN) //nolint:lll
)

type WriteStatistics struct {
	PersistingTime     time.Duration
	MinQueueWaitTime   time.Duration
	MaxQueueWaitTime   time.Duration
	TopicQuotaWaitTime time.Duration
}

func (s *WriteStatistics) fromProto(statistics *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteStatistics) error {
	if statistics == nil {
		return xerrors.WithStackTrace(errWriteResultResponseStatisticIsNil)
	}

	s.PersistingTime = statistics.GetPersistingTime().AsDuration()
	s.MinQueueWaitTime = statistics.GetMinQueueWaitTime().AsDuration()
	s.MaxQueueWaitTime = statistics.GetMaxQueueWaitTime().AsDuration()
	s.TopicQuotaWaitTime = statistics.GetTopicQuotaWaitTime().AsDuration()

	return nil
}

type UpdateTokenRequest struct {
	clientMessageImpl

	rawtopiccommon.UpdateTokenRequest
}

type UpdateTokenResponse struct {
	rawtopiccommon.UpdateTokenResponse

	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata
}
