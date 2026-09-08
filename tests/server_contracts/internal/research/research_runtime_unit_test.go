package research_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cucumber/godog"
	messages "github.com/cucumber/messages/go/v34"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestInitRequestParametersPreservePartitioning(t *testing.T) {
	for _, test := range []struct {
		parameters string
		want       *Ydb_Topic.StreamWriteMessage_InitRequest
	}{
		{parameters: "", want: &Ydb_Topic.StreamWriteMessage_InitRequest{}},
		{
			parameters: "producer_id: producer, get_last_seq_no: false",
			want:       &Ydb_Topic.StreamWriteMessage_InitRequest{ProducerId: "producer"},
		},
		{
			parameters: "partition_id: 0",
			want: &Ydb_Topic.StreamWriteMessage_InitRequest{
				Partitioning: &Ydb_Topic.StreamWriteMessage_InitRequest_PartitionId{PartitionId: 0},
			},
		},
		{
			parameters: "producer_id: producer, partition_id: 7, get_last_seq_no: true",
			want: &Ydb_Topic.StreamWriteMessage_InitRequest{
				ProducerId: "producer", GetLastSeqNo: true,
				Partitioning: &Ydb_Topic.StreamWriteMessage_InitRequest_PartitionId{PartitionId: 7},
			},
		},
	} {
		t.Run(test.parameters, func(t *testing.T) {
			request, err := parseInitRequestParameters(test.parameters)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := proto.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			decoded := &Ydb_Topic.StreamWriteMessage_InitRequest{}
			if err := proto.Unmarshal(encoded, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(decoded, test.want) {
				t.Fatalf("decoded InitRequest is %v, want %v", decoded, test.want)
			}
		})
	}
}

func TestInvalidInitRequestParameters(t *testing.T) {
	for _, parameters := range []string{
		"producer_id: A, producer_id: B",
		"partition_id: 0, partition_id: 1",
		"get_last_seq_no: true, get_last_seq_no: false",
		"get_last_seq_no: invalid", "partition_id: invalid", "producer_id:",
		"producer_id", "unknown: value",
	} {
		t.Run(parameters, func(t *testing.T) {
			if request, err := parseInitRequestParameters(parameters); err == nil || request != nil {
				t.Fatalf("invalid parameters yielded request=%v error=%v", request, err)
			}
		})
	}
}

func TestWriteResponseReportsPartitionID(t *testing.T) {
	for _, partitionID := range []int64{0, 7} {
		for _, withIssues := range []bool{false, true} {
			t.Run(fmt.Sprintf("partition=%d/issues=%t", partitionID, withIssues), func(t *testing.T) {
				response := writeAckForTest(1)
				response.GetWriteResponse().PartitionId = partitionID
				if withIssues {
					response.Issues = []*Ydb_Issue.IssueMessage{{IssueCode: 17, Message: "diagnostic"}}
				}
				research := &streamWriteResearch{}
				research.observeStreamWriteResponse("#1 [A]", response)
				report := research.HumanReadableReport()
				want := fmt.Sprintf(
					"WriteResponse: status=SUCCESS, partition_id=%d, acks=[seq_no=1, result=unknown]", partitionID)
				if !strings.Contains(report, want) {
					t.Fatalf("response partition or ACK is missing: %s", report)
				}
				if withIssues && !strings.Contains(report, "issues=[issue #17: diagnostic]") {
					t.Fatalf("response issues are missing: %s", report)
				}
			})
		}
	}
}

func TestStreamErrorDoesNotInventPartitionID(t *testing.T) {
	research := &streamWriteResearch{}
	research.observeStreamWriteResponse("#1", &Ydb_Topic.StreamWriteMessage_FromServer{
		Status: Ydb.StatusIds_BAD_REQUEST,
		Issues: []*Ydb_Issue.IssueMessage{{IssueCode: 17, Message: "diagnostic"}},
	})
	report := research.HumanReadableReport()
	if strings.Contains(report, "partition_id=") || !strings.Contains(report,
		"FromServer: status=BAD_REQUEST, issues=[issue #17: diagnostic]") {
		t.Fatalf("unexpected stream error description: %s", report)
	}
}

func TestWriteRequestStepSendsWholeTableOnce(t *testing.T) {
	for _, name := range []string{"", "A"} {
		t.Run("stream="+name, func(t *testing.T) {
			research := &streamWriteResearch{namedTransactions: map[string]*leasedQueryTransaction{
				"Transaction A": {transaction: queryTransactionIDStub{id: "real-tx"}, sessionID: "real-session"},
			}}
			_, wire := startControlledSession(t, research, name)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx = context.WithValue(ctx, worldContextKey{}, &researchWorld{research: research})
			result := make(chan error, 1)
			go func() {
				result <- stepSendWriteRequest(ctx, name, "txId: Transaction A", messageTableForTest(
					[]string{"data", "seq_no"}, []string{"first", "1"}, []string{"second", "2"},
				))
			}()
			var sent *Ydb_Topic.StreamWriteMessage_FromClient
			select {
			case sent = <-wire.sent:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			encoded, err := proto.Marshal(sent)
			if err != nil {
				t.Fatal(err)
			}
			decoded := &Ydb_Topic.StreamWriteMessage_FromClient{}
			if err := proto.Unmarshal(encoded, decoded); err != nil {
				t.Fatal(err)
			}
			request := decoded.GetWriteRequest()
			if len(request.GetMessages()) != 2 || request.GetTx().GetId() != "real-tx" ||
				request.GetTx().GetSession() != "real-session" {
				t.Fatalf("table did not become one transactional request: %v", request)
			}
			for i, payload := range []string{"first", "second"} {
				message := request.GetMessages()[i]
				if string(message.GetData()) != payload || message.GetSeqNo() != int64(i+1) {
					t.Fatalf("message %d changed order or contents: %v", i, message)
				}
			}
			response := writeAckForTest(1)
			response.GetWriteResponse().Acks = append(response.GetWriteResponse().Acks,
				&Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck{SeqNo: 2})
			wire.received <- streamWriteReceive{message: response}
			wire.received <- streamWriteReceive{err: io.EOF}
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case extra := <-wire.sent:
				t.Fatalf("table produced another Send: %v", extra)
			default:
			}
			if report := research.HumanReadableReport(); !strings.Contains(report,
				`messages=[{data="first", seq_no=1}; {data="second", seq_no=2}]`) {
				t.Fatalf("trace does not show the message list: %s", report)
			}
		})
	}
}

func TestWriteRequestMessageDefaults(t *testing.T) {
	for _, table := range []*godog.Table{
		messageTableForTest([]string{"data"}, []string{""}, []string{"payload"}),
		messageTableForTest([]string{"seq_no", "data"}, []string{"", ""}, []string{"0", "payload"}),
	} {
		request, err := buildWriteRequest("", table, nil)
		if err != nil {
			t.Fatal(err)
		}
		batch := request.GetWriteRequest()
		if batch.GetTx() != nil || batch.GetCodec() != int32(Ydb_Topic.Codec_CODEC_RAW) || len(batch.GetMessages()) != 2 {
			t.Fatalf("unexpected request defaults: %v", batch)
		}
		for _, message := range batch.GetMessages() {
			if message.GetSeqNo() != 0 || message.GetUncompressedSize() != int64(len(message.GetData())) ||
				message.GetCreatedAt().CheckValid() != nil || message.GetPartitioning() != nil {
				t.Fatalf("unexpected message defaults: %v", message)
			}
		}
	}
}

func TestInvalidWriteRequestNeverSendsPartialBatch(t *testing.T) {
	for _, test := range []struct {
		name, parameters string
		rows             [][]string
	}{
		{name: "empty table"},
		{name: "header only", rows: [][]string{{"data"}}},
		{name: "missing data", rows: [][]string{{"seq_no"}, {"1"}}},
		{name: "duplicate column", rows: [][]string{{"data", "data"}, {"a", "b"}}},
		{name: "unknown column", rows: [][]string{{"data", "unknown"}, {"a", "b"}}},
		{name: "transaction column", rows: [][]string{{"data", "tx"}, {"a", "Transaction A"}}},
		{name: "transaction ID column", rows: [][]string{{"data", "txId"}, {"a", "Transaction A"}}},
		{name: "short row", rows: [][]string{{"data", "seq_no"}, {"first", "1"}, {"second"}}},
		{name: "long row", rows: [][]string{{"data"}, {"first"}, {"second", "2"}}},
		{name: "invalid sequence", rows: [][]string{{"data", "seq_no"}, {"first", "1"}, {"second", "invalid"}}},
		{name: "overflow", rows: [][]string{{"data", "seq_no"}, {"a", "9223372036854775808"}}},
		{name: "missing transaction", parameters: "txId: missing", rows: [][]string{{"data"}, {"a"}}},
		{name: "empty transaction", parameters: "txId:", rows: [][]string{{"data"}, {"a"}}},
		{name: "unknown parameter", parameters: "tx: A", rows: [][]string{{"data"}, {"a"}}},
		{name: "duplicate parameter", parameters: "txId: A, txId: B", rows: [][]string{{"data"}, {"a"}}},
		{name: "malformed parameter", parameters: "txId", rows: [][]string{{"data"}, {"a"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			research := &streamWriteResearch{}
			_, wire := startControlledSession(t, research, "")
			ctx := context.WithValue(context.Background(), worldContextKey{}, &researchWorld{research: research})
			if err := stepSendWriteRequest(ctx, "", test.parameters, messageTableForTest(test.rows...)); err == nil {
				t.Fatal("accepted an invalid request")
			}
			select {
			case sent := <-wire.sent:
				t.Fatalf("sent part of an invalid batch: %v", sent)
			default:
			}
		})
	}
}

func TestTopicFixtureStepParameters(t *testing.T) {
	pattern := regexp.MustCompile(emptyTopicStepPattern)
	for _, test := range []struct {
		step       string
		partitions int64
		consumer   string
	}{
		{step: "an empty topic", partitions: 1},
		{step: "an empty topic with 2 partitions", partitions: 2},
		{step: `an empty topic with consumer "reader" for observation`, partitions: 1, consumer: "reader"},
		{step: `an empty topic with 3 partitions with consumer "reader" for observation`, partitions: 3, consumer: "reader"},
	} {
		t.Run(test.step, func(t *testing.T) {
			matches := pattern.FindStringSubmatch(test.step)
			if len(matches) != 3 {
				t.Fatalf("step does not match fixture vocabulary: %q", test.step)
			}
			count, err := parseTopicPartitionCount(matches[1])
			if err != nil {
				t.Fatal(err)
			}
			if count != test.partitions || matches[2] != test.consumer {
				t.Fatalf("parsed partitions=%d consumer=%q, want %d %q", count, matches[2], test.partitions, test.consumer)
			}
		})
	}
	for _, value := range []string{"0", "-1", "text", "9223372036854775808"} {
		if _, err := parseTopicPartitionCount(value); err == nil {
			t.Errorf("accepted invalid partition count %q", value)
		}
	}
}

func TestFormatTopicPartitionStats(t *testing.T) {
	partitions := []topictypes.PartitionInfo{
		{PartitionID: 5, PartitionStats: topictypes.PartitionStats{PartitionsOffset: topictypes.OffsetRange{End: 3}}},
		{PartitionID: 1, PartitionStats: topictypes.PartitionStats{PartitionsOffset: topictypes.OffsetRange{End: 2}}},
	}
	want := `Decoded Ydb.Topic.DescribeTopicResult: path="/local/topic", partitions=[` +
		`{partition_id=5, partition_stats={end_offset=3}}; {partition_id=1, partition_stats={end_offset=2}}].`
	if got := formatTopicPartitionStats("/local/topic", partitions); got != want {
		t.Fatalf("unexpected partition statistics: %s, want %s", got, want)
	}
	want = `Decoded Ydb.Topic.DescribeTopicResult: path="/local/topic", partitions=[].`
	if got := formatTopicPartitionStats("/local/topic", nil); got != want {
		t.Fatalf("unexpected empty partition statistics: %s, want %s", got, want)
	}
}

func TestStreamWritePumpsSendAndReceiveConcurrently(t *testing.T) {
	streamCtx, streamCancel := context.WithCancel(context.Background())
	stream := newControlledStreamWriteClient(streamCtx)
	research := &streamWriteSession{
		streamWriteResearch: &streamWriteResearch{},
		number:              1,
		stream:              stream,
		streamCancel:        streamCancel,
	}
	research.startStreamPumps()

	select {
	case <-stream.receiveStarted:
	case <-time.After(time.Second):
		t.Fatal("receive pump did not call Recv before the first Send")
	}

	request := &Ydb_Topic.StreamWriteMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamWriteMessage_FromClient_InitRequest{
			InitRequest: &Ydb_Topic.StreamWriteMessage_InitRequest{Path: "/local/topic"},
		},
	}
	if err := research.send(context.Background(), request); err != nil {
		t.Fatalf("send through pump: %v", err)
	}
	select {
	case actual := <-stream.sent:
		if actual != request {
			t.Fatal("send pump changed the request pointer")
		}
	case <-time.After(time.Second):
		t.Fatal("send pump did not call Send")
	}

	response := &Ydb_Topic.StreamWriteMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_InitResponse{
			InitResponse: &Ydb_Topic.StreamWriteMessage_InitResponse{SessionId: "stream-session"},
		},
	}
	stream.received <- streamWriteReceive{message: response}
	actual, err := research.receive(context.Background())
	if err != nil {
		t.Fatalf("receive through pump: %v", err)
	}
	if actual != response {
		t.Fatal("receive pump changed the response pointer")
	}

	writeRequests := []*Ydb_Topic.StreamWriteMessage_FromClient{
		transactionalWriteRequest("transaction-1", "tx-1", "query-session-1"),
		transactionalWriteRequest("transaction-2", "tx-2", "query-session-2"),
	}
	for _, writeRequest := range writeRequests {
		if err := research.send(context.Background(), writeRequest); err != nil {
			t.Fatalf("send WriteRequest through pump without a server response: %v", err)
		}
	}
	for i, expected := range writeRequests {
		select {
		case actual := <-stream.sent:
			if actual != expected {
				t.Fatalf("send pump changed WriteRequest %d", i+1)
			}
		case <-time.After(time.Second):
			t.Fatalf("send pump waited for a response before WriteRequest %d", i+1)
		}
	}

	report := research.HumanReadableReport()
	requestPosition := strings.Index(report, "client → server")
	responsePosition := strings.Index(report, "client ← server")
	if requestPosition < 0 || responsePosition < 0 || requestPosition >= responsePosition {
		t.Fatalf("events are not reported in observed order:\n%s", report)
	}
	firstWritePosition := strings.Index(report, `data="transaction-1"`)
	secondWritePosition := strings.Index(report, `data="transaction-2"`)
	if firstWritePosition < 0 || secondWritePosition < 0 || firstWritePosition >= secondWritePosition {
		t.Fatalf("write requests are not reported in send order:\n%s", report)
	}
	for _, expected := range []string{
		"/Ydb.Topic.V1.TopicService/StreamWrite",
		"Ydb.Topic.StreamWriteMessage.InitRequest",
		"Ydb.Topic.StreamWriteMessage.InitResponse",
		"Ydb.Topic.StreamWriteMessage.WriteRequest",
	} {
		if !strings.Contains(report, expected) {
			t.Fatalf("report does not contain protocol name %q:\n%s", expected, report)
		}
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
	defer cleanupCancel()
	if err := research.closeStream(cleanupCtx); err != nil {
		t.Fatalf("close stream pumps: %v", err)
	}
}

func TestResearchAnnotatesRealIDsWithStableAliases(t *testing.T) {
	research := &streamWriteResearch{
		transactionAliases: map[string]string{"real-tx-id": "Transaction A"},
		sessionAliases:     map[string]string{"real-session-id": "Query session A"},
	}

	got := research.annotateKnownIDsLocked(
		`session_id="real-session-id", tx_id="real-tx-id"`,
	)
	want := `session_id="real-session-id" [Query session A], tx_id="real-tx-id" [Transaction A]`
	if got != want {
		t.Fatalf("annotated IDs are %q, want %q", got, want)
	}
}

func TestWriteAfterCloseReturnsEOF(t *testing.T) {
	research := &streamWriteResearch{}
	session, _ := startControlledSession(t, research, "A")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.closeStream(ctx); err != nil {
		t.Fatal(err)
	}
	if err := session.send(ctx, nonTransactionalWriteRequest("after-close", nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("send on a closed stream: %v", err)
	}
}

func TestReadObservationTimeoutKeepsOneReceiver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 4)
	responses := make(chan *Ydb_Topic.StreamReadMessage_FromServer)
	research := &streamWriteResearch{readStream: &controlledStreamReadClient{
		receive: func() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
			started <- struct{}{}
			select {
			case response := <-responses:
				return response, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}}
	research.startReadReceiver(ctx)
	t.Cleanup(func() {
		cancel()
		select {
		case <-research.readDone:
		case <-time.After(time.Second):
			t.Error("read receiver did not stop after cancellation")
		}
	})
	<-started
	for range 2 {
		window, stop := context.WithTimeout(ctx, time.Millisecond)
		_, err := research.receiveRead(window)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("idle receive returned %v", err)
		}
	}
	select {
	case <-started:
		t.Fatal("an idle observation started a concurrent Recv")
	default:
	}
	response := &Ydb_Topic.StreamReadMessage_FromServer{Status: Ydb.StatusIds_SUCCESS}
	responses <- response
	window, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if received, err := research.receiveRead(window); err != nil || received != response {
		t.Fatalf("late response was lost: %v, %v", received, err)
	}
}

func TestResearchReportsStreamReadMessagesInServerOrder(t *testing.T) {
	research := &streamWriteResearch{}
	research.observeStreamReadResponse(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_ReadResponse{
			ReadResponse: &Ydb_Topic.StreamReadMessage_ReadResponse{
				PartitionData: []*Ydb_Topic.StreamReadMessage_ReadResponse_PartitionData{{
					PartitionSessionId: 42,
					Batches: []*Ydb_Topic.StreamReadMessage_ReadResponse_Batch{{
						MessageData: []*Ydb_Topic.StreamReadMessage_ReadResponse_MessageData{
							{Offset: 0, SeqNo: 2, Data: []byte("transaction-b-message")},
							{Offset: 1, SeqNo: 1, Data: []byte("transaction-a-message")},
						},
					}},
				}},
			},
		},
	})

	report := research.HumanReadableReport()
	secondTransaction := strings.Index(report, `offset=0, seq_no=2, data="transaction-b-message"`)
	firstTransaction := strings.Index(report, `offset=1, seq_no=1, data="transaction-a-message"`)
	if secondTransaction < 0 || firstTransaction < 0 || secondTransaction >= firstTransaction {
		t.Fatalf("StreamRead messages are not reported in server order:\n%s", report)
	}
}

func TestNamedStreamWriteSessionsAreIndependent(t *testing.T) {
	research := &streamWriteResearch{}
	a, wireA := startControlledSession(t, research, "A")
	b, wireB := startControlledSession(t, research, "B")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := research.newWriteSession("A"); err == nil {
		t.Fatal("replaced an active stream with the same alias")
	}
	ctx = context.WithValue(ctx, worldContextKey{}, &researchWorld{research: research})
	if selected, err := writeSessionFromContext(ctx, "A"); err != nil || selected != a {
		t.Fatalf("alias A selected %p, error %v", selected, err)
	}
	if _, err := writeSessionFromContext(ctx, ""); err == nil {
		t.Fatal("unnamed step implicitly selected a named stream")
	}
	a.addPendingResponse()
	if b.takePendingResponses() != 0 || a.takePendingResponses() != 1 {
		t.Fatal("streams share pending-response state")
	}
	a.setHideNextWriteResponse()
	if b.takeHideNextWriteResponse() || !a.takeHideNextWriteResponse() {
		t.Fatal("streams share response-withholding state")
	}

	requestA := nonTransactionalWriteRequest("message A", nil)
	requestB := nonTransactionalWriteRequest("message B", nil)
	results := make(chan error, 2)
	go func() { results <- a.send(ctx, requestA) }()
	go func() { results <- b.send(ctx, requestB) }()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if <-wireA.sent != requestA || <-wireB.sent != requestB {
		t.Fatal("request routed to the wrong stream")
	}
	responseA, responseB := writeAckForTest(1), writeAckForTest(2)
	wireB.received <- streamWriteReceive{message: responseB}
	wireA.received <- streamWriteReceive{message: responseA}
	if got, err := a.receive(ctx); err != nil || got != responseA {
		t.Fatalf("stream A received %p, error %v", got, err)
	}
	if got, err := b.receive(ctx); err != nil || got != responseB {
		t.Fatalf("stream B received %p, error %v", got, err)
	}
	if err := a.closeStream(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.send(ctx, requestB); err != nil {
		t.Fatalf("closing A broke B: %v", err)
	}
	reopened, _ := startControlledSession(t, research, "A")
	if reopened == a || reopened.label() != "#3 [A]" || a.label() != "#1 [A]" {
		t.Fatal("reopening an alias reused a live session object or its stream number")
	}
	if err := research.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, session := range research.writeSessions {
		select {
		case <-session.receiveDone:
		default:
			t.Fatalf("cleanup left %s receiving", session.label())
		}
	}
	for _, label := range []string{"StreamWrite #1 [A]", "StreamWrite #2 [B]"} {
		if !strings.Contains(research.HumanReadableReport(), label) {
			t.Fatalf("shared timeline is missing %s", label)
		}
	}
}

func TestUnselectedStreamKeepsReceiving(t *testing.T) {
	research := &streamWriteResearch{}
	session, wire := startControlledSession(t, research, "A")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		for seqNo := int64(1); seqNo <= 64; seqNo++ {
			select {
			case wire.received <- streamWriteReceive{message: writeAckForTest(seqNo)}:
			case <-ctx.Done():
				return
			}
		}
		select {
		case wire.received <- streamWriteReceive{err: io.EOF}:
		case <-ctx.Done():
		}
	}()
	select {
	case <-session.receiveDone:
	case <-ctx.Done():
		t.Fatal("receiver blocked while no scenario step was consuming its responses")
	}
	if !strings.Contains(research.HumanReadableReport(), "seq_no=64") {
		t.Fatal("response was not recorded before the scenario consumed it")
	}
	for seqNo := int64(1); seqNo <= 64; seqNo++ {
		response, err := session.receive(ctx)
		if err != nil || response.GetWriteResponse().GetAcks()[0].GetSeqNo() != seqNo {
			t.Fatalf("lost or reordered response %d: %v", seqNo, err)
		}
	}
	if _, err := session.receive(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("lost terminal stream error: %v", err)
	}
}

type queryTransactionIDStub struct {
	query.Transaction

	id string
}

func (tx queryTransactionIDStub) ID() string { return tx.id }

func messageTableForTest(rows ...[]string) *godog.Table {
	table := &godog.Table{}
	for _, values := range rows {
		row := &messages.PickleTableRow{}
		for _, value := range values {
			row.Cells = append(row.Cells, &messages.PickleTableCell{Value: value})
		}
		table.Rows = append(table.Rows, row)
	}

	return table
}

func startControlledSession(
	t *testing.T, research *streamWriteResearch, name string,
) (*streamWriteSession, *controlledStreamWriteClient) {
	t.Helper()
	session, err := research.newWriteSession(name)
	if err != nil {
		t.Fatal(err)
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	wire := newControlledStreamWriteClient(streamCtx)
	session.stream = wire
	session.streamCancel = cancel
	session.startStreamPumps()
	t.Cleanup(func() {
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := session.closeStream(ctx); err != nil {
			t.Errorf("close %s: %v", session.label(), err)
		}
	})
	select {
	case <-wire.receiveStarted:
	case <-time.After(time.Second):
		t.Fatal("stream receiver did not start")
	}

	return session, wire
}

func writeAckForTest(seqNo int64) *Ydb_Topic.StreamWriteMessage_FromServer {
	return &Ydb_Topic.StreamWriteMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_WriteResponse{
			WriteResponse: &Ydb_Topic.StreamWriteMessage_WriteResponse{
				Acks: []*Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck{{SeqNo: seqNo}},
			},
		},
	}
}

type controlledStreamWriteClient struct {
	ctx            context.Context //nolint:containedctx // Implements the gRPC stream's Context method.
	receiveStarted chan struct{}
	receiveOnce    sync.Once
	sent           chan *Ydb_Topic.StreamWriteMessage_FromClient
	received       chan streamWriteReceive
}

type controlledStreamReadClient struct {
	Ydb_Topic_V1.TopicService_StreamReadClient

	receive func() (*Ydb_Topic.StreamReadMessage_FromServer, error)
}

func (s *controlledStreamReadClient) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	return s.receive()
}

func newControlledStreamWriteClient(ctx context.Context) *controlledStreamWriteClient {
	return &controlledStreamWriteClient{
		ctx:            ctx,
		receiveStarted: make(chan struct{}),
		sent:           make(chan *Ydb_Topic.StreamWriteMessage_FromClient, 2),
		received:       make(chan streamWriteReceive, 1),
	}
}

func (s *controlledStreamWriteClient) Send(message *Ydb_Topic.StreamWriteMessage_FromClient) error {
	select {
	case <-s.receiveStarted:
	default:
		return errors.New("Send called before the receive pump started")
	}

	select {
	case s.sent <- message:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *controlledStreamWriteClient) Recv() (*Ydb_Topic.StreamWriteMessage_FromServer, error) {
	s.receiveOnce.Do(func() {
		close(s.receiveStarted)
	})
	select {
	case result := <-s.received:
		return result.message, result.err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *controlledStreamWriteClient) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (s *controlledStreamWriteClient) Trailer() metadata.MD {
	return nil
}

func (s *controlledStreamWriteClient) CloseSend() error {
	return nil
}

func (s *controlledStreamWriteClient) Context() context.Context {
	return s.ctx
}

func (s *controlledStreamWriteClient) SendMsg(any) error {
	return errors.New("SendMsg is not used by this test")
}

func (s *controlledStreamWriteClient) RecvMsg(any) error {
	return errors.New("RecvMsg is not used by this test")
}
