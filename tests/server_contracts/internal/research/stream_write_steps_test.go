package research_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const streamWriteStepPrefix = `^TopicService\.StreamWrite(?: "([^"]+)")?: `

const streamWriteRequestStepPattern = streamWriteStepPrefix + `WriteRequest(?:\{([^}]*)\})? messages:$`

func initializeStreamWriteSteps(sc *godog.ScenarioContext) {
	sc.Step(`^research runner: pipeline TopicService\.StreamWrite requests$`, stepPipelineWrites)
	sc.Step(
		streamWriteStepPrefix+`InitRequest\{([^}]*)\}$`,
		stepOpenStreamWrite,
	)
	sc.Step(
		streamWriteRequestStepPattern,
		stepSendWriteRequest,
	)
	sc.Step(
		`^research runner: withhold the next TopicService\.StreamWrite(?: "([^"]+)")? WriteResponse$`,
		stepHideNextWriteResponseFromScenario,
	)
	sc.Step(
		streamWriteStepPrefix+`CloseSend before consuming the recorded WriteResponse$`,
		stepCloseStreamWriteSessionBeforeAwaitingResponses,
	)
	sc.Step(streamWriteStepPrefix+`CloseSend$`, stepCloseCurrentStreamWriteSession)
	sc.Step(
		`^TopicService\.DescribeTopic: DescribeTopicRequest with include_stats=true$`,
		stepInspectTopicPartition,
	)
}

func stepOpenStreamWrite(ctx context.Context, name, parameters string) error {
	research, err := ensureStreamWriteResearch(ctx)
	if err != nil {
		return err
	}

	request, err := parseInitRequestParameters(parameters)
	if err != nil {
		return err
	}

	session, err := research.newWriteSession(name)
	if err != nil {
		return err
	}

	return openStreamWrite(ctx, session, request)
}

func parseInitRequestParameters(
	parameters string,
) (*Ydb_Topic.StreamWriteMessage_InitRequest, error) {
	request := &Ydb_Topic.StreamWriteMessage_InitRequest{}
	parameters = strings.TrimSpace(parameters)
	if parameters == "" {
		return request, nil
	}
	fields := strings.Split(parameters, ",")
	seen := make(map[string]struct{}, len(fields))
	for fieldIndex, fieldValue := range fields {
		parts := strings.SplitN(fieldValue, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"InitRequest parameter %d must have the form field: value",
				fieldIndex+1,
			)
		}
		field := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if _, exists := seen[field]; exists {
			return nil, fmt.Errorf("InitRequest parameter %q is repeated", field)
		}
		seen[field] = struct{}{}

		switch field {
		case "producer_id":
			if value == "" {
				return nil, errors.New("InitRequest producer_id must not be empty; omit it instead")
			}
			request.ProducerId = value
		case "get_last_seq_no":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("parse InitRequest get_last_seq_no: %w", err)
			}
			request.GetLastSeqNo = parsed
		case "partition_id":
			parsed, parseErr := strconv.ParseInt(value, 10, 64)
			if parseErr != nil {
				return nil, fmt.Errorf("parse InitRequest partition_id: %w", parseErr)
			}
			request.Partitioning = &Ydb_Topic.StreamWriteMessage_InitRequest_PartitionId{PartitionId: parsed}
		default:
			return nil, fmt.Errorf("unsupported InitRequest parameter %q", field)
		}
	}

	return request, nil
}

func ensureStreamWriteResearch(ctx context.Context) (*streamWriteResearch, error) {
	world, err := worldFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if world.research != nil {
		return world.research, nil
	}

	research := &streamWriteResearch{
		world: world,
		liveLogf: func(format string, args ...any) {
			godog.Logf(ctx, format, args...)
		},
	}
	world.research = research
	if world.observer != nil {
		world.observer.SetEventSink(research.observeGRPCEvent)
	}
	godog.Logf(ctx, "Live gRPC exchange (observed order):")

	return research, nil
}

func stepSendWriteRequest(ctx context.Context, name, parameters string, table *godog.Table) error {
	session, err := writeSessionFromContext(ctx, name)
	if err != nil {
		return err
	}
	if session.stream == nil {
		return errors.New("StreamWrite session is not initialized")
	}
	request, err := buildWriteRequest(parameters, table, session.namedTransactions)
	if err != nil {
		return err
	}
	if err := session.send(ctx, request); err == nil {
		// One table is one request, irrespective of how many messages it contains.
		session.addPendingResponse()
	} else if ctx.Err() != nil {
		return ctx.Err()
	}
	if session.pipelineWrites {
		return nil
	}
	// A transport error is an observation too; still drain any final server response.
	if err := session.observeWriteResponses(ctx, session.takePendingResponses()); err != nil {
		return err
	}

	return session.observeStreamEndOrIdle(ctx)
}

func stepPipelineWrites(ctx context.Context) error {
	research, err := ensureStreamWriteResearch(ctx)
	if err != nil {
		return err
	}
	research.pipelineWrites = true

	return nil
}

func (r *streamWriteResearch) drainPendingWrites(ctx context.Context) error {
	for _, session := range r.writeSessions {
		if err := session.observeWriteResponses(ctx, session.takePendingResponses()); err != nil {
			return err
		}
	}

	return nil
}

func buildWriteRequest(
	parameters string,
	table *godog.Table,
	transactions map[string]*leasedQueryTransaction,
) (*Ydb_Topic.StreamWriteMessage_FromClient, error) {
	request := &Ydb_Topic.StreamWriteMessage_WriteRequest{Codec: int32(Ydb_Topic.Codec_CODEC_RAW)}
	if parameters = strings.TrimSpace(parameters); parameters != "" {
		field, name, ok := strings.Cut(parameters, ":")
		if !ok || strings.TrimSpace(field) != "txId" || strings.Contains(name, ",") {
			return nil, errors.New("WriteRequest parameters must have the form txId: transaction alias")
		}
		name = strings.TrimSpace(name)
		transaction := transactions[name]
		if transaction == nil {
			return nil, fmt.Errorf("WriteRequest refers to unopened Query transaction %q", name)
		}
		request.Tx = &Ydb_Topic.TransactionIdentity{
			Id: transaction.transaction.ID(), Session: transaction.sessionID,
		}
	}
	var err error
	request.Messages, err = parseWriteMessages(table)
	if err != nil {
		return nil, err
	}

	return &Ydb_Topic.StreamWriteMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest{WriteRequest: request},
	}, nil
}

func parseWriteMessages(table *godog.Table) ([]*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData, error) {
	if table == nil || len(table.Rows) < 2 {
		return nil, errors.New("message table must contain a header and at least one message")
	}

	columns := make(map[string]int, len(table.Rows[0].Cells))
	for index, cell := range table.Rows[0].Cells {
		switch cell.Value {
		case "data", "seq_no":
		case "tx", "txId":
			return nil, errors.New("transaction belongs to WriteRequest{txId: alias}, not to a message table column")
		default:
			return nil, fmt.Errorf("unsupported message table column %q", cell.Value)
		}
		if _, exists := columns[cell.Value]; exists {
			return nil, fmt.Errorf("message table column %q is repeated", cell.Value)
		}
		columns[cell.Value] = index
	}
	dataColumn, hasData := columns["data"]
	if !hasData {
		return nil, errors.New(`message table must contain a "data" column`)
	}
	sequenceNumberColumn, hasSequenceNumber := columns["seq_no"]

	messages := make([]*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData, 0, len(table.Rows)-1)
	for rowIndex, row := range table.Rows[1:] {
		if len(row.Cells) != len(columns) {
			return nil, fmt.Errorf("message table row %d must have %d cells", rowIndex+1, len(columns))
		}
		var sequenceNumber *int64
		if hasSequenceNumber {
			value := row.Cells[sequenceNumberColumn].Value
			if value != "" {
				parsed, parseErr := strconv.ParseInt(value, 10, 64)
				if parseErr != nil {
					return nil, fmt.Errorf("parse seq_no in message table row %d: %w", rowIndex+1, parseErr)
				}
				sequenceNumber = &parsed
			}
		}
		messages = append(messages, writeMessageData(row.Cells[dataColumn].Value, sequenceNumber))
	}

	return messages, nil
}

func stepHideNextWriteResponseFromScenario(ctx context.Context, name string) error {
	research, err := writeSessionFromContext(ctx, name)
	if err != nil {
		return err
	}
	research.setHideNextWriteResponse()
	research.observe(fmt.Sprintf(
		"Research client will observe the next WriteResponse on StreamWrite %s "+
			"but will not deliver it to the scenario.", research.label()))

	return nil
}

func writeMessageData(
	payload string,
	sequenceNumber *int64,
) *Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData {
	message := &Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData{
		CreatedAt:        timestamppb.Now(),
		Data:             []byte(payload),
		UncompressedSize: int64(len(payload)),
	}
	if sequenceNumber != nil {
		message.SeqNo = *sequenceNumber
	}

	return message
}

func nonTransactionalWriteRequest(
	payload string,
	sequenceNumber *int64,
) *Ydb_Topic.StreamWriteMessage_FromClient {
	return &Ydb_Topic.StreamWriteMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest{
			WriteRequest: &Ydb_Topic.StreamWriteMessage_WriteRequest{
				Messages: []*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData{writeMessageData(payload, sequenceNumber)},
				Codec:    int32(Ydb_Topic.Codec_CODEC_RAW),
			},
		},
	}
}

func stepCloseCurrentStreamWriteSession(ctx context.Context, name string) error {
	research, err := writeSessionFromContext(ctx, name)
	if err != nil {
		return err
	}
	if research.stream == nil {
		return errors.New("StreamWrite session is not initialized")
	}

	streamLabel := research.label()
	research.observe(fmt.Sprintf(
		"gRPC client → server /Ydb.Topic.V1.TopicService/StreamWrite %s / CloseSend.",
		streamLabel,
	))
	if err := research.closeStream(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		research.observe(fmt.Sprintf(
			"gRPC client → server /Ydb.Topic.V1.TopicService/StreamWrite %s / CloseSend: %v.",
			streamLabel,
			err,
		))
	}

	return nil
}

func stepCloseStreamWriteSessionBeforeAwaitingResponses(ctx context.Context, name string) error {
	research, err := writeSessionFromContext(ctx, name)
	if err != nil {
		return err
	}
	research.takePendingResponses()

	timer := time.NewTimer(streamResponseIdleTimeout)
	defer timer.Stop()
	select {
	case <-research.receiveDone:
	case <-timer.C:
		research.observe(fmt.Sprintf(
			"/Ydb.Topic.V1.TopicService/StreamWrite %s produced no withheld "+
				"Ydb.Topic.StreamWriteMessage.WriteResponse within %s; the research client now cancels the stream.",
			research.label(),
			streamResponseIdleTimeout,
		))
	case <-ctx.Done():
		return ctx.Err()
	}
	_ = research.takeHideNextWriteResponse()

	return stepCloseCurrentStreamWriteSession(ctx, name)
}
