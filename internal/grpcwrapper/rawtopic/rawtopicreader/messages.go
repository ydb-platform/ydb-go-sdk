package rawtopicreader

import (
	"errors"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type PartitionSessionID int64

func (id *PartitionSessionID) FromInt64(v int64) {
	*id = PartitionSessionID(v)
}

func (id PartitionSessionID) ToInt64() int64 {
	return int64(id)
}

type Offset int64

func NewOffset(v int64) Offset {
	return Offset(v)
}

func (offset *Offset) FromInt64(v int64) {
	*offset = Offset(v)
}

func (offset Offset) ToInt64() int64 {
	return int64(offset)
}

type OptionalOffset struct {
	Offset   Offset
	HasValue bool
}

func (offset *OptionalOffset) FromInt64Pointer(v *int64) {
	if v == nil {
		offset.HasValue = false
		offset.Offset.FromInt64(-1)
	} else {
		offset.HasValue = true
		offset.Offset.FromInt64(*v)
	}
}

func (offset *OptionalOffset) FromInt64(v int64) {
	offset.FromInt64Pointer(&v)
}

func (offset OptionalOffset) ToInt64() int64 {
	return offset.Offset.ToInt64()
}

func (offset OptionalOffset) ToInt64Pointer() *int64 {
	if offset.HasValue {
		v := offset.Offset.ToInt64()
		return &v
	}
	return nil
}

//
// UpdateTokenRequest
//

type UpdateTokenRequest struct {
	clientMessageImpl

	rawtopiccommon.UpdateTokenRequest
}

func (r *UpdateTokenRequest) toProto() *Ydb_Topic.UpdateTokenRequest {
	return &Ydb_Topic.UpdateTokenRequest{
		Token: r.Token,
	}
}

type UpdateTokenResponse struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	rawtopiccommon.UpdateTokenResponse
}

//
// InitRequest
//

type InitRequest struct {
	clientMessageImpl

	TopicsReadSettings []TopicReadSettings

	Consumer string
}

func (g *InitRequest) toProto() *Ydb_Topic.StreamReadMessage_InitRequest {
	p := &Ydb_Topic.StreamReadMessage_InitRequest{
		Consumer: g.Consumer,
	}

	p.TopicsReadSettings = make([]*Ydb_Topic.StreamReadMessage_InitRequest_TopicReadSettings, len(g.TopicsReadSettings))
	for topicSettingsIndex := range g.TopicsReadSettings {
		srcTopicSettings := &g.TopicsReadSettings[topicSettingsIndex]
		dstTopicSettings := &Ydb_Topic.StreamReadMessage_InitRequest_TopicReadSettings{}
		p.TopicsReadSettings[topicSettingsIndex] = dstTopicSettings

		dstTopicSettings.Path = srcTopicSettings.Path
		dstTopicSettings.MaxLag = srcTopicSettings.MaxLag.ToProto()
		dstTopicSettings.ReadFrom = srcTopicSettings.ReadFrom.ToProto()
	}

	return p
}

type TopicReadSettings struct {
	Path         string
	PartitionsID []int64

	MaxLag   rawoptional.Duration
	ReadFrom rawoptional.Time
}

type InitResponse struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	SessionID string
}

func (g *InitResponse) fromProto(p *Ydb_Topic.StreamReadMessage_InitResponse) {
	g.SessionID = p.SessionId
}

//
// ReadRequest
//

type ReadRequest struct {
	clientMessageImpl

	BytesSize int
}

func (r *ReadRequest) toProto() *Ydb_Topic.StreamReadMessage_ReadRequest {
	return &Ydb_Topic.StreamReadMessage_ReadRequest{BytesSize: int64(r.BytesSize)}
}

type ReadResponse struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	BytesSize     int
	PartitionData []PartitionData
}

func (r *ReadResponse) fromProto(p *Ydb_Topic.StreamReadMessage_ReadResponse) error {
	if p == nil {
		return xerrors.NewYdbErrWithStackTrace("ydb: unexpected nil Ydb_Topic.StreamReadMessage_ReadResponse")
	}
	r.BytesSize = int(p.BytesSize)

	r.PartitionData = make([]PartitionData, len(p.PartitionData))
	for partitionIndex := range p.PartitionData {
		srcPartition := p.PartitionData[partitionIndex]
		if srcPartition == nil {
			return xerrors.NewYdbErrWithStackTrace("ydb: unexpected nil partition data")
		}
		dstPartition := &r.PartitionData[partitionIndex]
		dstPartition.PartitionSessionID.FromInt64(srcPartition.PartitionSessionId)

		dstPartition.Batches = make([]Batch, len(srcPartition.Batches))

		for batchIndex := range srcPartition.Batches {
			srcBatch := srcPartition.Batches[batchIndex]
			if srcBatch == nil {
				return xerrors.NewYdbErrWithStackTrace("ydb: unexpected nil batch in partition data")
			}
			dstBatch := &dstPartition.Batches[batchIndex]

			dstBatch.ProducerID = srcBatch.ProducerId
			dstBatch.WriteSessionMeta = srcBatch.WriteSessionMeta
			dstBatch.Codec.MustFromProto(Ydb_Topic.Codec(srcBatch.Codec))

			dstBatch.WrittenAt = srcBatch.WrittenAt.AsTime()

			dstBatch.MessageData = make([]MessageData, len(srcBatch.MessageData))
			for messageIndex := range srcBatch.MessageData {
				srcMessage := srcBatch.MessageData[messageIndex]
				if srcMessage == nil {
					return xerrors.NewYdbErrWithStackTrace("ydb: unexpected message nil in partition data")
				}
				dstMessage := &dstBatch.MessageData[messageIndex]

				dstMessage.Offset.FromInt64(srcMessage.Offset)
				dstMessage.SeqNo = srcMessage.SeqNo
				dstMessage.CreatedAt = srcMessage.CreatedAt.AsTime()
				dstMessage.Data = srcMessage.Data
				dstMessage.UncompressedSize = srcMessage.UncompressedSize
				dstMessage.MessageGroupID = srcMessage.MessageGroupId
			}
		}
	}
	return nil
}

type PartitionData struct {
	PartitionSessionID PartitionSessionID

	Batches []Batch
}
type Batch struct {
	Codec rawtopiccommon.Codec

	ProducerID       string
	WriteSessionMeta map[string]string // nil if session meta is empty
	WrittenAt        time.Time

	MessageData []MessageData
}

type MessageData struct {
	Offset           Offset
	SeqNo            int64
	CreatedAt        time.Time
	Data             []byte
	UncompressedSize int64
	MessageGroupID   string
}

//
// CommitOffsetRequest
//

type CommitOffsetRequest struct {
	clientMessageImpl

	CommitOffsets []PartitionCommitOffset
}

func (r *CommitOffsetRequest) toProto() *Ydb_Topic.StreamReadMessage_CommitOffsetRequest {
	res := &Ydb_Topic.StreamReadMessage_CommitOffsetRequest{}
	res.CommitOffsets = make(
		[]*Ydb_Topic.StreamReadMessage_CommitOffsetRequest_PartitionCommitOffset,
		len(r.CommitOffsets),
	)

	for sessionIndex := range r.CommitOffsets {
		srcPartitionCommitOffset := &r.CommitOffsets[sessionIndex]
		dstCommitOffset := &Ydb_Topic.StreamReadMessage_CommitOffsetRequest_PartitionCommitOffset{
			PartitionSessionId: srcPartitionCommitOffset.PartitionSessionID.ToInt64(),
		}
		res.CommitOffsets[sessionIndex] = dstCommitOffset

		dstCommitOffset.Offsets = make([]*Ydb_Topic.OffsetsRange, len(srcPartitionCommitOffset.Offsets))
		for offsetIndex := range srcPartitionCommitOffset.Offsets {
			dstCommitOffset.Offsets[offsetIndex] = srcPartitionCommitOffset.Offsets[offsetIndex].ToProto()
		}
	}
	return res
}

type PartitionCommitOffset struct {
	PartitionSessionID PartitionSessionID
	Offsets            []OffsetRange
}

type OffsetRange struct {
	Start Offset
	End   Offset
}

func (r *OffsetRange) FromProto(p *Ydb_Topic.OffsetsRange) error {
	if p == nil {
		return xerrors.NewYdbErrWithStackTrace("ydb: unexpected protobuf nil offsets")
	}

	r.Start.FromInt64(p.Start)
	r.End.FromInt64(p.End)
	return nil
}

func (r *OffsetRange) ToProto() *Ydb_Topic.OffsetsRange {
	return &Ydb_Topic.OffsetsRange{
		Start: r.Start.ToInt64(),
		End:   r.End.ToInt64(),
	}
}

type CommitOffsetResponse struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	PartitionsCommittedOffsets []PartitionCommittedOffset
}

func (r *CommitOffsetResponse) fromProto(proto *Ydb_Topic.StreamReadMessage_CommitOffsetResponse) error {
	r.PartitionsCommittedOffsets = make([]PartitionCommittedOffset, len(proto.PartitionsCommittedOffsets))
	for i := range r.PartitionsCommittedOffsets {
		srcCommitted := proto.PartitionsCommittedOffsets[i]
		if srcCommitted == nil {
			return xerrors.WithStackTrace(errors.New("unexpected nil while parse commit offset response"))
		}
		dstCommitted := &r.PartitionsCommittedOffsets[i]

		dstCommitted.PartitionSessionID.FromInt64(srcCommitted.PartitionSessionId)
		dstCommitted.CommittedOffset.FromInt64(srcCommitted.CommittedOffset)
	}

	return nil
}

type PartitionCommittedOffset struct {
	PartitionSessionID PartitionSessionID
	CommittedOffset    Offset
}

//
// PartitionSessionStatusRequest
//

type PartitionSessionStatusRequest struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
}

func (r *PartitionSessionStatusRequest) toProto() *Ydb_Topic.StreamReadMessage_PartitionSessionStatusRequest {
	return &Ydb_Topic.StreamReadMessage_PartitionSessionStatusRequest{
		PartitionSessionId: r.PartitionSessionID.ToInt64(),
	}
}

type PartitionSessionStatusResponse struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	PartitionSessionID     PartitionSessionID
	PartitionOffsets       OffsetRange
	WriteTimeHighWatermark time.Time
}

func (r *PartitionSessionStatusResponse) fromProto(
	p *Ydb_Topic.StreamReadMessage_PartitionSessionStatusResponse,
) error {
	r.PartitionSessionID.FromInt64(p.GetPartitionSessionId())
	if err := r.PartitionOffsets.FromProto(p.GetPartitionOffsets()); err != nil {
		return err
	}
	r.WriteTimeHighWatermark = p.GetWriteTimeHighWatermark().AsTime()
	return nil
}

//
// StartPartitionSessionRequest
//

type StartPartitionSessionRequest struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	PartitionSession PartitionSession
	CommittedOffset  Offset
	PartitionOffsets OffsetRange
}

func (r *StartPartitionSessionRequest) fromProto(p *Ydb_Topic.StreamReadMessage_StartPartitionSessionRequest) error {
	if p == nil {
		return xerrors.NewYdbErrWithStackTrace("ydb: unexpected proto nil start partition session request")
	}

	if p.PartitionSession == nil {
		return xerrors.NewYdbErrWithStackTrace(
			"ydb: unexpected proto nil partition session in start partition session request",
		)
	}
	r.PartitionSession.PartitionID = p.PartitionSession.PartitionId
	r.PartitionSession.Path = p.PartitionSession.Path
	r.PartitionSession.PartitionSessionID.FromInt64(p.PartitionSession.PartitionSessionId)

	r.CommittedOffset.FromInt64(p.CommittedOffset)

	return r.PartitionOffsets.FromProto(p.PartitionOffsets)
}

type PartitionSession struct {
	PartitionSessionID PartitionSessionID
	Path               string // Topic path of partition
	PartitionID        int64
}

type StartPartitionSessionResponse struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
	ReadOffset         OptionalOffset
	CommitOffset       OptionalOffset
}

func (r *StartPartitionSessionResponse) toProto() *Ydb_Topic.StreamReadMessage_StartPartitionSessionResponse {
	res := &Ydb_Topic.StreamReadMessage_StartPartitionSessionResponse{
		PartitionSessionId: r.PartitionSessionID.ToInt64(),
		ReadOffset:         r.ReadOffset.ToInt64Pointer(),
		CommitOffset:       r.CommitOffset.ToInt64Pointer(),
	}
	return res
}

//
// StopPartitionSessionRequest
//

type StopPartitionSessionRequest struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	PartitionSessionID PartitionSessionID
	Graceful           bool
	CommittedOffset    Offset
}

func (r *StopPartitionSessionRequest) fromProto(proto *Ydb_Topic.StreamReadMessage_StopPartitionSessionRequest) error {
	if proto == nil {
		return xerrors.NewWithIssues("ydb: unexpected grpc nil stop partition session request")
	}
	r.PartitionSessionID.FromInt64(proto.PartitionSessionId)
	r.Graceful = proto.Graceful
	r.CommittedOffset.FromInt64(proto.CommittedOffset)
	return nil
}

type StopPartitionSessionResponse struct {
	//nolint:unused
	clientMessageImpl

	PartitionSessionID PartitionSessionID
}
