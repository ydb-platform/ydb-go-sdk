package topicresearch_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

const streamReadResponseIdleTimeout = 2 * time.Second

func initializeStreamReadSteps(sc *godog.ScenarioContext) {
	sc.Step(
		`^TopicService\.StreamRead: InitRequest\{consumer: ([^,}]+), partition_ids: \[0\]\}$`,
		stepOpenStreamRead,
	)
	sc.Step(
		`^TopicService\.StreamRead: StartPartitionSessionResponse\{read_offset: 0\}$`,
		stepStartReadPartition,
	)
	sc.Step(
		`^TopicService\.StreamRead: ReadRequest\{bytes_size: (\d+)\}$`,
		stepReadTopicMessages,
	)
}

func stepOpenStreamRead(ctx context.Context, consumer string) error {
	research, err := researchFromContext(ctx)
	if err != nil {
		return err
	}
	if research.readStream != nil {
		return errors.New("a StreamRead session is already active")
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := Ydb_Topic_V1.NewTopicServiceClient(ydb.GRPCConn(research.world.driver)).StreamRead(streamCtx)
	if err != nil {
		cancel()

		return fmt.Errorf("open StreamRead: %w", err)
	}
	research.readStream = stream
	research.readCancel = cancel
	research.startReadReceiver(streamCtx)

	request := &Ydb_Topic.StreamReadMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_InitRequest{
			InitRequest: &Ydb_Topic.StreamReadMessage_InitRequest{
				TopicsReadSettings: []*Ydb_Topic.StreamReadMessage_InitRequest_TopicReadSettings{{
					Path:         research.world.topicPath,
					PartitionIds: []int64{0},
				}},
				Consumer: consumer,
			},
		},
	}
	research.observeStreamReadRequest(request)
	if err := stream.Send(request); err != nil {
		research.observeStreamReadEnd(err)

		return nil
	}

	initObserved := false
	for !initObserved || !research.readPartitionReady {
		response, recvErr := research.receiveRead(ctx)
		if recvErr != nil {
			return ctx.Err()
		}
		if response.GetStatus() != Ydb.StatusIds_SUCCESS {
			return nil
		}
		if response.GetInitResponse() != nil {
			initObserved = true
		}
		if start := response.GetStartPartitionSessionRequest(); start != nil {
			research.readPartitionSessionID = start.GetPartitionSession().GetPartitionSessionId()
			research.readPartitionReady = true
		}
	}

	return nil
}

func stepStartReadPartition(ctx context.Context) error {
	research, err := researchFromContext(ctx)
	if err != nil {
		return err
	}
	if research.readStream == nil || !research.readPartitionReady {
		research.observe("Research client has no StartPartitionSessionRequest to answer.")

		return nil
	}

	readOffset := int64(0)
	request := &Ydb_Topic.StreamReadMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_StartPartitionSessionResponse{
			StartPartitionSessionResponse: &Ydb_Topic.StreamReadMessage_StartPartitionSessionResponse{
				PartitionSessionId: research.readPartitionSessionID,
				ReadOffset:         &readOffset,
			},
		},
	}
	research.observeStreamReadRequest(request)
	if sendErr := research.readStream.Send(request); sendErr != nil {
		research.observeStreamReadEnd(sendErr)
	}

	return nil
}

func stepReadTopicMessages(ctx context.Context, bytesSizeText string) error {
	research, err := researchFromContext(ctx)
	if err != nil {
		return err
	}
	if research.readStream == nil || !research.readPartitionReady {
		research.observe("Research client has no active partition session for ReadRequest.")

		return nil
	}
	bytesSize, err := strconv.ParseInt(bytesSizeText, 10, 64)
	if err != nil {
		return fmt.Errorf("parse StreamRead ReadRequest bytes_size: %w", err)
	}

	request := &Ydb_Topic.StreamReadMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_ReadRequest{
			ReadRequest: &Ydb_Topic.StreamReadMessage_ReadRequest{BytesSize: bytesSize},
		},
	}
	research.observeStreamReadRequest(request)
	if sendErr := research.readStream.Send(request); sendErr != nil {
		research.observeStreamReadEnd(sendErr)

		return nil
	}

	for {
		windowCtx, cancel := context.WithTimeout(ctx, streamReadResponseIdleTimeout)
		response, recvErr := research.receiveRead(windowCtx)
		cancel()
		if recvErr != nil {
			return ctx.Err()
		}
		if response.GetStatus() != Ydb.StatusIds_SUCCESS {
			return nil
		}
	}
}

func (r *streamWriteResearch) startReadReceiver(ctx context.Context) {
	r.readResults = make(chan streamReadReceive, 1)
	r.readDone = make(chan struct{})
	go func() {
		defer close(r.readDone)
		defer close(r.readResults)
		for {
			message, err := r.readStream.Recv()
			if err != nil {
				if ctx.Err() == nil {
					r.observeStreamReadEnd(err)
				}
			} else {
				r.observeStreamReadResponse(message)
			}
			select {
			case r.readResults <- streamReadReceive{message: message, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
}

func (r *streamWriteResearch) receiveRead(ctx context.Context) (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	select {
	case result, ok := <-r.readResults:
		if !ok {
			return nil, io.EOF
		}

		return result.message, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type streamReadReceive struct {
	message *Ydb_Topic.StreamReadMessage_FromServer
	err     error
}

func (r *streamWriteResearch) observeStreamReadRequest(message *Ydb_Topic.StreamReadMessage_FromClient) {
	r.mu.Lock()
	defer r.mu.Unlock()

	messageName := protobufMessageName(message)
	details := "{}"
	if request := message.GetInitRequest(); request != nil {
		messageName = protobufMessageName(request)
		settings := make([]string, 0, len(request.GetTopicsReadSettings()))
		for _, topic := range request.GetTopicsReadSettings() {
			settings = append(settings, fmt.Sprintf(
				"{path=%q, partition_ids=%v}",
				topic.GetPath(),
				topic.GetPartitionIds(),
			))
		}
		details = fmt.Sprintf(
			"consumer=%q, topics_read_settings=[%s]",
			request.GetConsumer(),
			strings.Join(settings, "; "),
		)
	}
	if response := message.GetStartPartitionSessionResponse(); response != nil {
		messageName = protobufMessageName(response)
		details = fmt.Sprintf(
			"partition_session_id=%d, read_offset=%d",
			response.GetPartitionSessionId(),
			response.GetReadOffset(),
		)
	}
	if request := message.GetReadRequest(); request != nil {
		messageName = protobufMessageName(request)
		details = fmt.Sprintf("bytes_size=%d", request.GetBytesSize())
	}
	description := fmt.Sprintf(
		"gRPC client → server /Ydb.Topic.V1.TopicService/StreamRead #1 / %s: %s.",
		messageName,
		details,
	)
	r.appendProtocolEventLocked(
		"client -> /Ydb.Topic.V1.TopicService/StreamRead #1",
		description,
		message,
	)
}

func (r *streamWriteResearch) observeStreamReadResponse(message *Ydb_Topic.StreamReadMessage_FromServer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	messageName := protobufMessageName(message)
	details := fmt.Sprintf("status=%s%s", message.GetStatus(), summarizeQueryIssues(message.GetIssues()))
	if response := message.GetInitResponse(); response != nil {
		messageName = protobufMessageName(response)
		details = fmt.Sprintf("status=%s, session_id=%q", message.GetStatus(), response.GetSessionId())
	}
	if request := message.GetStartPartitionSessionRequest(); request != nil {
		messageName = protobufMessageName(request)
		partitionSession := request.GetPartitionSession()
		details = fmt.Sprintf(
			"status=%s, partition_session_id=%d, path=%q, partition_id=%d, "+
				"committed_offset=%d, partition_offsets={start=%d, end=%d}",
			message.GetStatus(),
			partitionSession.GetPartitionSessionId(),
			partitionSession.GetPath(),
			partitionSession.GetPartitionId(),
			request.GetCommittedOffset(),
			request.GetPartitionOffsets().GetStart(),
			request.GetPartitionOffsets().GetEnd(),
		)
	}
	if response := message.GetReadResponse(); response != nil {
		messageName = protobufMessageName(response)
		partitions := make([]string, 0, len(response.GetPartitionData()))
		for _, partition := range response.GetPartitionData() {
			messages := make([]string, 0)
			for _, batch := range partition.GetBatches() {
				for _, data := range batch.GetMessageData() {
					messages = append(messages, fmt.Sprintf(
						"{offset=%d, seq_no=%d, data=%q}",
						data.GetOffset(),
						data.GetSeqNo(),
						string(data.GetData()),
					))
				}
			}
			partitions = append(partitions, fmt.Sprintf(
				"{partition_session_id=%d, messages=[%s]}",
				partition.GetPartitionSessionId(),
				strings.Join(messages, "; "),
			))
		}
		details = fmt.Sprintf(
			"status=%s, bytes_size=%d, partition_data=[%s]",
			message.GetStatus(),
			response.GetBytesSize(),
			strings.Join(partitions, "; "),
		)
	}
	description := fmt.Sprintf(
		"gRPC client ← server /Ydb.Topic.V1.TopicService/StreamRead #1 / %s: %s.",
		messageName,
		details,
	)
	r.appendProtocolEventLocked(
		"server -> /Ydb.Topic.V1.TopicService/StreamRead #1",
		description,
		message,
	)
}

func (r *streamWriteResearch) observeStreamReadEnd(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) && r.readCancel == nil {
		return
	}
	if errors.Is(err, io.EOF) {
		r.observe("gRPC client ← server /Ydb.Topic.V1.TopicService/StreamRead #1 / Recv: EOF.")

		return
	}
	r.observe(fmt.Sprintf(
		"gRPC client ← server /Ydb.Topic.V1.TopicService/StreamRead #1 / Recv: %v.",
		err,
	))
}
