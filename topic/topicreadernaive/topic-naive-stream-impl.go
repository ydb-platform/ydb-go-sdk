package topicreadernaive

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const tickInterval = time.Millisecond * 100

var clientPartitionSessionID atomic.Int64

type topicNaiveStreamReader struct {
	id             string
	readerID       int64
	stream         Ydb_Topic_V1.TopicService_StreamReadClient
	topicSelectors []*topicreadercommon.PublicReadSelector
	consumerName   string

	worker       background.Worker
	decoders     topicreadercommon.DecoderMap
	sendMessages chan *Ydb_Topic.StreamReadMessage_FromClient
	auth         credentials.Credentials
	commitMode   topicreadercommon.PublicCommitMode
	tracer       *trace.Topic

	m              xsync.Mutex
	partitions     *topicreadercommon.PartitionSessionStorage
	messageBatches []*topicreadercommon.PublicBatch
}

//nolint:funlen
func newTopicNaiveStreamReader(
	cred credentials.Credentials,
	grpcClient Ydb_Topic_V1.TopicServiceClient,
	topicName []*topicreadercommon.PublicReadSelector,
	consumerName string,
	bufferSize int64,
	decoders topicreadercommon.DecoderMap,
	tracer *trace.Topic,
) (*topicNaiveStreamReader, error) {
	res := &topicNaiveStreamReader{
		id:             uuid.New().String(),
		topicSelectors: topicName,
		consumerName:   consumerName,
		sendMessages:   make(chan *Ydb_Topic.StreamReadMessage_FromClient, 1000),
		auth:           cred,
		partitions:     &topicreadercommon.PartitionSessionStorage{},
		decoders:       decoders,
		tracer:         tracer,
	}

	stream, err := grpcClient.StreamRead(res.worker.Context())
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb failed to open naive topic reader: %w", err))
	}

	res.stream = stream

	wrapperInitMessage := topicreadercommon.CreateInitMessage(res.consumerName, res.topicSelectors)
	initRequest := &Ydb_Topic.StreamReadMessage_FromClient_InitRequest{InitRequest: wrapperInitMessage.TmpPublicToProto()}

	err = stream.Send(&Ydb_Topic.StreamReadMessage_FromClient{ClientMessage: initRequest})
	if err != nil {
		_ = stream.CloseSend()

		return nil, err
	}

	resp, err := stream.Recv()
	if err != nil {
		_ = stream.CloseSend()

		return nil, err
	}

	if resp.GetStatus() != Ydb.StatusIds_SUCCESS {
		_ = stream.CloseSend()

		return nil, fmt.Errorf("ydb: failed naive stream reader: %w", xerrors.WithStackTrace(xerrors.Operation(
			xerrors.WithStatusCode(resp.GetStatus()),
			xerrors.WithIssues(resp.GetIssues()),
		)))
	}

	res.worker.Start("naive reader stream read loop", res.messagesReadLoop)
	res.worker.Start("naive stream send loop", res.sendLoop)
	res.worker.Start("naive reader auth token loop", res.authTokenLoop)

	res.sendMessages <- &Ydb_Topic.StreamReadMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_ReadRequest{
			ReadRequest: &Ydb_Topic.StreamReadMessage_ReadRequest{BytesSize: bufferSize},
		},
	}

	return res, nil
}

//nolint:funlen
func (r *topicNaiveStreamReader) messagesReadLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		msg, err := r.stream.Recv()
		if err != nil {
			_ = r.worker.Close(ctx, xerrors.WithStackTrace(
				fmt.Errorf("ydb naive reader failed to receive grpc message: %w", err)),
			)

			return
		}

		switch m := msg.GetServerMessage().(type) {
		case *Ydb_Topic.StreamReadMessage_FromServer_ReadResponse:
			respWrap := &rawtopicreader.ReadResponse{}
			err := respWrap.TmpPublicFromProto(m.ReadResponse)
			if err != nil {
				_ = r.worker.Close(ctx, xerrors.WithStackTrace(
					fmt.Errorf("ydb naive reader failed convert batch messages from grpc to wrap: %w", err)),
				)
			}

			trace.TopicOnReaderReceiveDataResponse(r.tracer, r.id, -1, respWrap)

			r.m.WithLock(func() {
				batches, err := topicreadercommon.ReadRawBatchesToPublicBatches(respWrap, r.partitions, r.decoders)
				if err != nil {
					_ = r.worker.Close(ctx, xerrors.WithStackTrace(
						fmt.Errorf("ydb naive reader failed to convert messfrom wrap to publis: %w", err),
					))

					return
				}

				if err = checkEqualsMessages(m, batches); err != nil {
					_ = r.worker.Close(ctx, err)

					return
				}

				r.messageBatches = append(r.messageBatches, batches...)
			})
		case *Ydb_Topic.StreamReadMessage_FromServer_StartPartitionSessionRequest:
			session := topicreadercommon.NewPartitionSession(
				ctx,
				m.StartPartitionSessionRequest.GetPartitionSession().GetPath(),
				m.StartPartitionSessionRequest.GetPartitionSession().GetPartitionId(),
				r.readerID,
				r.id,
				rawtopicreader.PartitionSessionID(m.StartPartitionSessionRequest.GetPartitionSession().GetPartitionSessionId()),
				clientPartitionSessionID.Add(1),
				rawtopiccommon.Offset(m.StartPartitionSessionRequest.GetCommittedOffset()),
			)
			err = r.partitions.Add(session)
			if err != nil {
				_ = r.worker.Close(ctx, xerrors.WithStackTrace(
					fmt.Errorf("ydb naive reader failed to add partition session: %w", err),
				))

				return
			}
			r.sendMessages <- &Ydb_Topic.StreamReadMessage_FromClient{
				ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_StartPartitionSessionResponse{
					StartPartitionSessionResponse: &Ydb_Topic.StreamReadMessage_StartPartitionSessionResponse{
						PartitionSessionId: m.StartPartitionSessionRequest.GetPartitionSession().GetPartitionSessionId(),
					},
				},
			}

		case *Ydb_Topic.StreamReadMessage_FromServer_CommitOffsetResponse:
			r.m.WithLock(func() {
				for _, resp := range m.CommitOffsetResponse.GetPartitionsCommittedOffsets() {
					partition, err := r.partitions.Get(rawtopicreader.PartitionSessionID(resp.GetPartitionSessionId()))
					if err != nil {
						_ = r.worker.Close(ctx, xerrors.WithStackTrace(
							fmt.Errorf("ydb naive reader can't get session for accept partition commit: %w", err)),
						)

						return
					}

					partition.SetCommittedOffsetForward(rawtopiccommon.Offset(resp.GetCommittedOffset()))
				}
			})
		case *Ydb_Topic.StreamReadMessage_FromServer_StopPartitionSessionRequest:
			r.sendMessages <- &Ydb_Topic.StreamReadMessage_FromClient{
				ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_StopPartitionSessionResponse{
					StopPartitionSessionResponse: &Ydb_Topic.StreamReadMessage_StopPartitionSessionResponse{
						PartitionSessionId: m.StopPartitionSessionRequest.GetPartitionSessionId(),
					},
				},
			}
		}
	}
}

func checkEqualsMessages(
	grpcMess *Ydb_Topic.StreamReadMessage_FromServer_ReadResponse,
	batches []*topicreadercommon.PublicBatch,
) error {
	type messKey struct {
		PartitionSessionID int64
		Offset             int64
	}

	cnt := 0
	for _, batch := range batches {
		cnt += len(batch.Messages)
	}

	grpcMap := make(map[messKey]empty.Struct, cnt)
	for _, partData := range grpcMess.ReadResponse.GetPartitionData() {
		for _, grpcBatch := range partData.GetBatches() {
			for _, grpcMessData := range grpcBatch.GetMessageData() {
				k := messKey{
					PartitionSessionID: partData.GetPartitionSessionId(),
					Offset:             grpcMessData.GetOffset(),
				}
				grpcMap[k] = empty.Struct{}
			}
		}
	}

	for _, batch := range batches {
		for _, mess := range batch.Messages {
			cr := topicreadercommon.GetCommitRange(mess)
			k := messKey{
				PartitionSessionID: int64(cr.PartitionSession.StreamPartitionSessionID),
				Offset:             mess.Offset,
			}

			if _, ok := grpcMap[k]; ok {
				delete(grpcMap, k)
			} else {
				return xerrors.WithStackTrace(fmt.Errorf("ydb naive topic check messages unreaded message: %#v", k))
			}
		}
	}

	if len(grpcMap) > 0 {
		return xerrors.WithStackTrace(fmt.Errorf("ydb naive topic check messages lost messages: %#v", grpcMap))
	}

	return nil
}

func (r *topicNaiveStreamReader) sendLoop(ctx context.Context) {
	done := ctx.Done()
	for {
		select {
		case <-done:
			return
		case m := <-r.sendMessages:
			err := r.stream.Send(m)
			if err != nil {
				_ = r.worker.Close(ctx, xerrors.WithStackTrace(
					fmt.Errorf("ydb naive reader failed write message to stream: %w", err)),
				)

				return
			}
		}
	}
}

func (r *topicNaiveStreamReader) authTokenLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			token, err := r.auth.Token(ctx)
			if err != nil {
				continue
			}

			r.sendMessages <- &Ydb_Topic.StreamReadMessage_FromClient{
				ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_UpdateTokenRequest{
					UpdateTokenRequest: &Ydb_Topic.UpdateTokenRequest{
						Token: token,
					},
				},
			}
		}
	}
}

func (r *topicNaiveStreamReader) WaitClose(ctx context.Context) (reason error) {
	select {
	case <-r.worker.StopDone():
		return r.worker.CloseReason()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *topicNaiveStreamReader) ReadBatch(ctx context.Context) (batch *topicreadercommon.PublicBatch, _ error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	batch = r.getFirstBatch()
	if batch != nil {
		return batch, nil
	}

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			batch = r.getFirstBatch()
			if batch != nil {
				batchSize := topicreadercommon.GetBatchSizeBytes(batch)
				r.sendMessages <- &Ydb_Topic.StreamReadMessage_FromClient{
					ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_ReadRequest{
						ReadRequest: &Ydb_Topic.StreamReadMessage_ReadRequest{BytesSize: int64(batchSize)},
					},
				}

				return batch, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.worker.Done():
			return nil, fmt.Errorf("ydb naive topic reader stream worker stopped: %w", r.worker.CloseReason())
		}
	}
}

func (r *topicNaiveStreamReader) getFirstBatch() (batch *topicreadercommon.PublicBatch) {
	r.m.WithLock(func() {
		if len(r.messageBatches) > 0 {
			batch = r.messageBatches[0]
			r.messageBatches = r.messageBatches[1:]
		}
	})

	return batch
}

//nolint:funlen
func (r *topicNaiveStreamReader) Commit(ctx context.Context, cr topicreadercommon.CommitRange) (resErr error) {
	traceCtx := ctx
	onDone := trace.TopicOnReaderCommit(
		r.tracer,
		&traceCtx,
		cr.PartitionSession.Topic,
		cr.PartitionSession.PartitionID,
		cr.PartitionSession.StreamPartitionSessionID.ToInt64(),
		cr.CommitOffsetStart.ToInt64(),
		cr.CommitOffsetEnd.ToInt64(),
	)
	defer func() {
		onDone(resErr)
	}()

	ctx = traceCtx
	if r.commitMode == topicreadercommon.CommitModeNone {
		return xerrors.WithStackTrace(fmt.Errorf("ydb naive topic reader called commit for commit mode none"))
	}

	r.sendMessages <- &Ydb_Topic.StreamReadMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_CommitOffsetRequest{
			CommitOffsetRequest: &Ydb_Topic.StreamReadMessage_CommitOffsetRequest{
				CommitOffsets: []*Ydb_Topic.StreamReadMessage_CommitOffsetRequest_PartitionCommitOffset{
					{
						PartitionSessionId: cr.PartitionSession.StreamPartitionSessionID.ToInt64(),
						Offsets: []*Ydb_Topic.OffsetsRange{
							{
								Start: cr.CommitOffsetStart.ToInt64(),
								End:   cr.CommitOffsetEnd.ToInt64(),
							},
						},
					},
				},
			},
		},
	}
	trace.TopicOnReaderSendCommitMessage(r.tracer, &topicreadercommon.CommitRanges{
		Ranges: []topicreadercommon.CommitRange{cr},
	})

	if r.commitMode == topicreadercommon.CommitModeAsync {
		return nil
	}

	partition, err := r.partitions.Get(cr.PartitionSession.StreamPartitionSessionID)
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb naive topic reader can't get partition session for commit: %w", err))
	}

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if partition.CommittedOffset() >= cr.CommitOffsetEnd {
				return nil
			}
		case <-partition.Context().Done():
			return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
				"ydb naive topic reader: %w",
				topicreadercommon.PublicErrCommitSessionToExpiredSession,
			)))
		}
	}
}

func (r *topicNaiveStreamReader) Close(reason error) error {
	_ = r.stream.CloseSend()

	return r.worker.Close(context.Background(), xerrors.WithStackTrace(reason))
}
