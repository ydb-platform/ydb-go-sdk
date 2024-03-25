package rawtopicwriter

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
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
	res := &Ydb_Topic.StreamWriteMessage_InitRequest{
		Path:             r.Path,
		ProducerId:       r.ProducerID,
		WriteSessionMeta: r.WriteSessionMeta,
		GetLastSeqNo:     r.GetLastSeqNo,
	}

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

func (p *Partitioning) setToProtoInitRequest(r *Ydb_Topic.StreamWriteMessage_InitRequest) error {
	switch p.Type {
	case PartitioningUndefined:
		r.Partitioning = nil
	case PartitioningMessageGroupID:
		r.Partitioning = &Ydb_Topic.StreamWriteMessage_InitRequest_MessageGroupId{
			MessageGroupId: p.MessageGroupID,
		}
	case PartitioningPartitionID:
		r.Partitioning = &Ydb_Topic.StreamWriteMessage_InitRequest_PartitionId{
			PartitionId: p.PartitionID,
		}
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
		m.Partitioning = nil
	case PartitioningMessageGroupID:
		m.Partitioning = &Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData_MessageGroupId{
			MessageGroupId: p.MessageGroupID,
		}
	case PartitioningPartitionID:
		m.Partitioning = &Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData_PartitionId{
			PartitionId: p.PartitionID,
		}
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
}

func (r *WriteRequest) toProto() (p *Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest, err error) {
	messages := make([]*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData, len(r.Messages))

	for i := range r.Messages {
		messages[i], err = r.Messages[i].ToProto()
		if err != nil {
			return nil, err
		}
	}

	res := &Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest{
		WriteRequest: &Ydb_Topic.StreamWriteMessage_WriteRequest{
			Messages: messages,
			Codec:    int32(r.Codec.ToProto()),
		},
	}

	return res, nil
}

type MessageData struct {
	SeqNo            int64
	CreatedAt        time.Time
	UncompressedSize int64
	Partitioning     Partitioning
	MetadataItems    []rawtopiccommon.MetadataItem
	Data             []byte
}

func (d *MessageData) ToProto() (*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData, error) {
	res := &Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData{
		SeqNo:            d.SeqNo,
		CreatedAt:        timestamppb.New(d.CreatedAt),
		Data:             d.Data,
		UncompressedSize: d.UncompressedSize,
	}
	err := d.Partitioning.setToProtoMessage(res)
	if err != nil {
		return nil, err
	}

	for i := range d.MetadataItems {
		res.MetadataItems = append(res.GetMetadataItems(), &Ydb_Topic.MetadataItem{
			Key:   d.MetadataItems[i].Key,
			Value: d.MetadataItems[i].Value,
		})
	}

	return res, nil
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

type WriteAck struct {
	SeqNo              int64
	MessageWriteStatus MessageWriteStatus
}

func (wa *WriteAck) fromProto(pb *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck) error {
	if pb == nil {
		return xerrors.WithStackTrace(errWriteResultResponseWriteAckIsNil)
	}
	wa.SeqNo = pb.GetSeqNo()

	return wa.MessageWriteStatus.fromProto(pb.GetMessageWriteStatus())
}

// MessageWriteStatus is struct because it included in per-message structure and
// places on hot-path for write messages
// structure will work and compile-optimization better then interface
type MessageWriteStatus struct {
	Type          WriteStatusType
	WrittenOffset int64
	SkippedReason WriteStatusSkipReason
}

func (s *MessageWriteStatus) fromProto(status interface{}) error {
	switch v := status.(type) {
	case *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Written_:
		s.Type = WriteStatusTypeWritten
		s.WrittenOffset = v.Written.GetOffset()

		return nil
	case *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Skipped_:
		s.Type = WriteStatusTypeSkipped
		s.SkippedReason = WriteStatusSkipReason(v.Skipped.GetReason())

		return nil
	default:
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: unexpected write status type: %v", reflect.TypeOf(v))))
	}
}

type WriteStatusType int

const (
	WriteStatusTypeUnknown WriteStatusType = iota
	WriteStatusTypeWritten
	WriteStatusTypeSkipped
)

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
