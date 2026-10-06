package research_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"

	"github.com/cucumber/godog"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/tests/server_contracts/internal/grpcclient"
)

// Protocol messages are cloned when recorded, before the test client can
// withhold an ACK. Assertions inspect typed messages, never formatted log text.
type protocolEvent struct {
	direction string
	message   proto.Message
}

type contractMessage struct {
	partition int64
	seqNo     int64
	data      string
}

func initializeContractSteps(sc *godog.ScenarioContext) {
	sc.Step(`^contract: StreamWrite "([^"]*)" has exactly these ACKs:$`, contractACKs)
	sc.Step(`^contract: StreamWrite "([^"]*)" rejected the write with (\w+) and no ACK$`, contractRejectedWrite)
	sc.Step(`^contract: (Commit|Rollback) of "([^"]+)" returned (\w+)$`, contractTransactionResult)
	sc.Step(`^contract: topic contains exactly:$`, contractTopicMessages)
	sc.Step(`^contract: partition (\d+) is inactive with active children \[([0-9, ]+)\]$`, contractSplit)
}

func (r *streamWriteResearch) recordedProtocol() []protocolEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.protocolEvents)
}

func (session *streamWriteSession) contractResponses() []*Ydb_Topic.StreamWriteMessage_FromServer {
	var responses []*Ydb_Topic.StreamWriteMessage_FromServer
	for _, event := range session.recordedProtocol() {
		if event.direction != "server -> /Ydb.Topic.V1.TopicService/StreamWrite "+session.label() {
			continue
		}
		if response, ok := event.message.(*Ydb_Topic.StreamWriteMessage_FromServer); ok {
			responses = append(responses, response)
		}
	}

	return responses
}

func contractACKs(ctx context.Context, name string, table *godog.Table) error {
	rows, err := contractTable(table, "seq_no", "result")
	if err != nil {
		return err
	}
	session, err := writeSessionFromContext(ctx, name)
	if err != nil {
		return err
	}
	responses, err := session.awaitContractACKs(ctx, len(rows))
	if err != nil {
		return err
	}

	return checkContractACKs(responses, rows)
}

func (session *streamWriteSession) awaitContractACKs(
	ctx context.Context, expected int,
) ([]*Ydb_Topic.StreamWriteMessage_FromServer, error) {
	// ACK count, not WriteRequest count, determines when the observation is complete.
	session.takePendingResponses()
	var receiveErr error
	for {
		responses := session.contractResponses()
		acks, err := contractObservedACKs(responses)
		if err != nil {
			return nil, err
		}
		if len(acks) >= expected {
			return responses, nil
		}
		if receiveErr != nil {
			return nil, fmt.Errorf("StreamWrite %s: received %d of %d ACKs: %w",
				session.label(), len(acks), expected, receiveErr)
		}
		_, receiveErr = session.receive(ctx)
	}
}

func checkContractACKs(responses []*Ydb_Topic.StreamWriteMessage_FromServer, rows [][]string) error {
	actual, err := contractObservedACKs(responses)
	if err != nil {
		return err
	}
	expected := slices.Clone(rows)
	compare := func(a, b []string) int { return slices.Compare(a, b) }
	slices.SortFunc(actual, compare)
	slices.SortFunc(expected, compare)
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("ACK sequence numbers/results/counts: got %v, want %v", actual, expected)
	}

	return nil
}

func contractObservedACKs(responses []*Ydb_Topic.StreamWriteMessage_FromServer) ([][]string, error) {
	var actual [][]string
	initialized := false
	for _, response := range responses {
		if response.GetStatus() != Ydb.StatusIds_SUCCESS {
			return nil, fmt.Errorf("unexpected StreamWrite response: %v", response)
		}
		if response.GetInitResponse() != nil {
			if initialized || len(actual) != 0 {
				return nil, errors.New("unexpected InitResponse order")
			}
			initialized = true

			continue
		}
		write := response.GetWriteResponse()
		if !initialized || write == nil {
			return nil, fmt.Errorf("unexpected response before/after Init: %v", response)
		}
		for _, ack := range write.GetAcks() {
			result := "unknown"
			switch ack.GetMessageWriteStatus().(type) {
			case *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Written_:
				result = "written"
			case *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_WrittenInTx_:
				result = "written_in_tx"
			case *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Skipped_:
				result = "skipped"
			}
			actual = append(actual, []string{strconv.FormatInt(ack.GetSeqNo(), 10), result})
		}
	}
	if !initialized {
		return nil, errors.New("no successful InitResponse")
	}

	return actual, nil
}

func contractRejectedWrite(ctx context.Context, name, statusName string) error {
	session, err := writeSessionFromContext(ctx, name)
	if err != nil {
		return err
	}
	if err := session.observeWriteResponses(ctx, session.takePendingResponses()); err != nil {
		return err
	}
	responses := session.contractResponses()
	code, ok := Ydb.StatusIds_StatusCode_value[statusName]
	if !ok || code == int32(Ydb.StatusIds_SUCCESS) {
		return fmt.Errorf("invalid rejection status %q", statusName)
	}
	if len(responses) != 2 || responses[0].GetStatus() != Ydb.StatusIds_SUCCESS || responses[0].GetInitResponse() == nil ||
		responses[1].GetStatus() != Ydb.StatusIds_StatusCode(code) || responses[1].GetWriteResponse() != nil {
		return fmt.Errorf("want successful Init followed by %s without WriteResponse, got %v", statusName, responses)
	}

	return nil
}

func contractTransactionResult(ctx context.Context, operation, name, statusName string) error {
	_, transaction, err := namedQueryTransactionFromContext(ctx, name)
	if err != nil {
		return err
	}
	code, ok := Ydb.StatusIds_StatusCode_value[statusName]
	if !ok {
		return fmt.Errorf("unknown transaction status %q", statusName)
	}
	if !transaction.finished || transaction.completionOperation != operation {
		return fmt.Errorf("transaction %q has no completed %s RPC", name, operation)
	}
	var statusErr *grpcclient.StatusError
	if (statusName == "SUCCESS" && transaction.completionErr == nil) ||
		(statusName != "SUCCESS" && errors.As(transaction.completionErr, &statusErr) &&
			statusErr.Status == Ydb.StatusIds_StatusCode(code)) {
		return nil
	}

	if transaction.completionErr == nil {
		return fmt.Errorf("%s of %q: want %s, got SUCCESS", operation, name, statusName)
	}

	return fmt.Errorf("%s of %q: want %s, got %w", operation, name, statusName, transaction.completionErr)
}

func contractTopicMessages(ctx context.Context, table *godog.Table) error {
	columns := []string{"seq_no", "data"}
	withPartition := table != nil && len(table.Rows) > 0 && len(table.Rows[0].Cells) == 3
	if withPartition {
		columns = append([]string{"partition_id"}, columns...)
	}
	rows, err := contractTable(table, columns...)
	if err != nil {
		return err
	}
	var expected []contractMessage
	for _, row := range rows {
		message := contractMessage{}
		if withPartition {
			message.partition, err = contractNonnegativeInt(row[0])
			if err != nil {
				return err
			}
			row = row[1:]
		}
		message.seqNo, err = contractNonnegativeInt(row[0])
		if err != nil {
			return err
		}
		message.data = row[1]
		expected = append(expected, message)
	}
	research, err := researchFromContext(ctx)
	if err != nil {
		return err
	}
	// The scenario explicitly performs StreamRead. Missing/failed/timed-out
	// observations cannot satisfy a nonempty expected result.
	actual, err := contractReadMessages(research.recordedProtocol())
	if err != nil {
		return err
	}
	if !withPartition {
		for i := range actual {
			actual[i].partition = 0
		}
	}
	// Preserve message order within each partition, ignoring arrival order between partitions.
	sortMessages := func(messages []contractMessage) {
		slices.SortStableFunc(messages, func(a, b contractMessage) int {
			return cmp.Compare(a.partition, b.partition)
		})
	}
	sortMessages(actual)
	sortMessages(expected)
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("topic messages %v: got %+v, want %+v", columns, actual, expected)
	}

	return nil
}

func contractReadMessages(events []protocolEvent) ([]contractMessage, error) {
	partitions := make(map[int64]int64)
	var messages []contractMessage
	for _, event := range events {
		response, ok := event.message.(*Ydb_Topic.StreamReadMessage_FromServer)
		if !ok {
			continue
		}
		if response.GetStatus() != Ydb.StatusIds_SUCCESS {
			return nil, fmt.Errorf("StreamRead failed: %v", response)
		}
		if start := response.GetStartPartitionSessionRequest(); start != nil {
			partition := start.GetPartitionSession()
			partitions[partition.GetPartitionSessionId()] = partition.GetPartitionId()
		}
		for _, data := range response.GetReadResponse().GetPartitionData() {
			partition, exists := partitions[data.GetPartitionSessionId()]
			if !exists {
				return nil, fmt.Errorf("unknown read partition session %d", data.GetPartitionSessionId())
			}
			for _, batch := range data.GetBatches() {
				if batch.GetCodec() != int32(Ydb_Topic.Codec_CODEC_RAW) {
					return nil, fmt.Errorf("unexpected read codec %d", batch.GetCodec())
				}
				for _, message := range batch.GetMessageData() {
					messages = append(messages, contractMessage{
						partition: partition, seqNo: message.GetSeqNo(), data: string(message.GetData()),
					})
				}
			}
		}
	}

	return messages, nil
}

func contractSplit(ctx context.Context, parent int64, childrenText string) error {
	children, err := parseReadPartitionIDs(childrenText)
	if err != nil {
		return err
	}
	world, err := worldFromContext(ctx)
	if err != nil {
		return err
	}
	description, err := world.DescribeTopic(ctx, false)
	if err != nil {
		return err
	}
	foundParent, foundChildren := false, 0
	for _, partition := range description.GetPartitions() {
		if partition.GetPartitionId() == parent {
			actual := slices.Clone(partition.GetChildPartitionIds())
			slices.Sort(actual)
			slices.Sort(children)
			foundParent = !partition.GetActive() && slices.Equal(actual, children)
		}
		if slices.Contains(children, partition.GetPartitionId()) && partition.GetActive() &&
			len(partition.GetChildPartitionIds()) == 0 && slices.Contains(partition.GetParentPartitionIds(), parent) {
			foundChildren++
		}
	}
	if !foundParent || foundChildren != len(children) {
		return fmt.Errorf("want inactive parent %d with active children %v, got %+v",
			parent, children, description.GetPartitions())
	}

	return nil
}

func contractTable(table *godog.Table, columns ...string) ([][]string, error) {
	if table == nil || len(table.Rows) < 2 {
		return nil, errors.New("contract table needs a header and at least one row")
	}
	var rows [][]string
	for i, row := range table.Rows {
		if len(row.Cells) != len(columns) {
			return nil, fmt.Errorf("contract table row %d: want columns %v", i, columns)
		}
		var values []string
		for _, cell := range row.Cells {
			values = append(values, cell.Value)
		}
		if i == 0 {
			if !slices.Equal(values, columns) {
				return nil, fmt.Errorf("contract table columns: got %v, want %v", values, columns)
			}

			continue
		}
		rows = append(rows, values)
	}

	return rows, nil
}

func contractNonnegativeInt(value string) (int64, error) {
	number, err := strconv.ParseInt(value, 10, 64)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("expected nonnegative integer, got %q", value)
	}

	return number, nil
}
