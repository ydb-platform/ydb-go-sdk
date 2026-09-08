package research_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cucumber/godog"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type streamWriteResearch struct {
	world *researchWorld

	writeSessions          []*streamWriteSession
	namedStreams           map[string]*streamWriteSession
	readStream             Ydb_Topic_V1.TopicService_StreamReadClient
	readCancel             context.CancelFunc
	readResults            chan streamReadReceive
	readDone               chan struct{}
	readPartitionSessionID int64
	readPartitionReady     bool

	transactions       []*leasedQueryTransaction
	namedTransactions  map[string]*leasedQueryTransaction
	transactionAliases map[string]string
	sessionAliases     map[string]string

	mu         sync.Mutex
	timeline   []string
	transcript []string
	liveLogf   func(string, ...any)
}

type leasedQueryTransaction struct {
	transaction query.Transaction
	sessionID   string
	release     chan struct{}
	done        chan struct{}
	doErr       error
	releaseOnce sync.Once
	finished    bool
}

type leasedQueryTransactionResult struct {
	transaction *leasedQueryTransaction
	err         error
}

type streamWriteSend struct {
	message *Ydb_Topic.StreamWriteMessage_FromClient
	result  chan error
}

type streamWriteReceive struct {
	message *Ydb_Topic.StreamWriteMessage_FromServer
	err     error
}

const streamResponseIdleTimeout = 2 * time.Second

func initializeConcurrentTransactionSteps(sc *godog.ScenarioContext) {
	sc.Step(
		`^QueryService\.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "([^"]+)"$`,
		stepNamedQueryTransactionOpen,
	)
	sc.Step(
		`^QueryService\.CommitTransaction: CommitTransactionRequest for "([^"]+)"$`,
		stepNamedQueryTransactionCommit,
	)
	sc.Step(
		`^QueryService\.RollbackTransaction: RollbackTransactionRequest for "([^"]+)"$`,
		stepNamedQueryTransactionRollback,
	)
	sc.Step(
		`^QueryService\.CommitTransaction concurrently: CommitTransactionRequest for "([^"]+)" and "([^"]+)"$`,
		stepNamedQueryTransactionsCommitConcurrently,
	)
}

func stepNamedQueryTransactionOpen(ctx context.Context, name string) error {
	research, err := ensureStreamWriteResearch(ctx)
	if err != nil {
		return err
	}
	if research.namedTransactions == nil {
		research.namedTransactions = make(map[string]*leasedQueryTransaction)
	}
	if _, exists := research.namedTransactions[name]; exists {
		return fmt.Errorf("Query transaction %q is already open", name)
	}

	transaction, err := leaseQueryTransaction(ctx, research.world.driver)
	if err != nil {
		return fmt.Errorf("begin Query transaction %q: %w", name, err)
	}
	research.transactions = append(research.transactions, transaction)
	research.namedTransactions[name] = transaction
	research.registerTransactionAlias(name, transaction)

	return nil
}

func stepNamedQueryTransactionCommit(ctx context.Context, name string) error {
	_, transaction, err := namedQueryTransactionFromContext(ctx, name)
	if err != nil {
		return err
	}
	_ = transaction.transaction.CommitTx(ctx)
	transaction.finished = true

	return releaseQueryTransaction(ctx, name, transaction)
}

func stepNamedQueryTransactionRollback(ctx context.Context, name string) error {
	_, transaction, err := namedQueryTransactionFromContext(ctx, name)
	if err != nil {
		return err
	}
	_ = transaction.transaction.Rollback(ctx)
	transaction.finished = true

	return releaseQueryTransaction(ctx, name, transaction)
}

func stepNamedQueryTransactionsCommitConcurrently(ctx context.Context, firstName, secondName string) error {
	_, first, err := namedQueryTransactionFromContext(ctx, firstName)
	if err != nil {
		return err
	}
	_, second, err := namedQueryTransactionFromContext(ctx, secondName)
	if err != nil {
		return err
	}

	type commitResult struct {
		name        string
		transaction *leasedQueryTransaction
	}
	start := make(chan struct{})
	results := make(chan commitResult, 2)
	for _, item := range []struct {
		name        string
		transaction *leasedQueryTransaction
	}{
		{name: firstName, transaction: first},
		{name: secondName, transaction: second},
	} {
		go func() {
			<-start
			_ = item.transaction.transaction.CommitTx(ctx)
			item.transaction.finished = true
			results <- commitResult{name: item.name, transaction: item.transaction}
		}()
	}
	close(start)

	var releaseErr error
	for range 2 {
		result := <-results
		releaseErr = errors.Join(releaseErr, releaseQueryTransaction(ctx, result.name, result.transaction))
	}

	return releaseErr
}

func namedQueryTransactionFromContext(
	ctx context.Context,
	name string,
) (*streamWriteResearch, *leasedQueryTransaction, error) {
	research, err := researchFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	transaction := research.namedTransactions[name]
	if transaction == nil {
		return nil, nil, fmt.Errorf("Query transaction %q is not open", name)
	}

	return research, transaction, nil
}

func releaseQueryTransaction(ctx context.Context, name string, transaction *leasedQueryTransaction) error {
	transaction.Release()
	if err := transaction.Wait(ctx); err != nil {
		return fmt.Errorf("release Query session for transaction %q: %w", name, err)
	}

	return nil
}

func openStreamWrite(
	ctx context.Context,
	session *streamWriteSession,
	initRequest *Ydb_Topic.StreamWriteMessage_InitRequest,
) error {
	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := Ydb_Topic_V1.NewTopicServiceClient(ydb.GRPCConn(session.world.driver)).StreamWrite(streamCtx)
	if err != nil {
		streamCancel()

		return fmt.Errorf("open StreamWrite: %w", err)
	}
	session.stream = stream
	session.streamCancel = streamCancel
	session.startStreamPumps()

	initRequest.Path = session.world.topicPath
	request := &Ydb_Topic.StreamWriteMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamWriteMessage_FromClient_InitRequest{
			InitRequest: initRequest,
		},
	}
	if err := session.send(ctx, request); err != nil {
		return ctx.Err()
	}
	_, err = session.receive(ctx)
	if err != nil {
		return ctx.Err()
	}

	return nil
}

func stepInspectTopicPartition(ctx context.Context) error {
	research, err := ensureStreamWriteResearch(ctx)
	if err != nil {
		return err
	}
	description, err := research.world.driver.Topic().Describe(
		ctx,
		research.world.topicPath,
		topicoptions.IncludePartitionStats(),
	)
	if err != nil {
		return fmt.Errorf("describe Topic after writes: %w", err)
	}
	research.observe(formatTopicPartitionStats(research.world.topicPath, description.Partitions))

	return nil
}

func formatTopicPartitionStats(topicPath string, partitions []topictypes.PartitionInfo) string {
	details := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		details = append(details, fmt.Sprintf(
			"{partition_id=%d, partition_stats={end_offset=%d}}",
			partition.PartitionID,
			partition.PartitionStats.PartitionsOffset.End,
		))
	}

	return fmt.Sprintf("Decoded Ydb.Topic.DescribeTopicResult: path=%q, partitions=[%s].",
		topicPath, strings.Join(details, "; "))
}

func leaseQueryTransaction(ctx context.Context, driver *ydb.Driver) (*leasedQueryTransaction, error) {
	transaction := &leasedQueryTransaction{
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	ready := make(chan leasedQueryTransactionResult, 1)

	go func() {
		callbackStarted := false
		err := driver.Query().Do(ctx, func(attemptCtx context.Context, session query.Session) error {
			callbackStarted = true
			tx, err := session.Begin(attemptCtx, query.TxSettings(query.WithSerializableReadWrite()))
			if err != nil {
				ready <- leasedQueryTransactionResult{err: err}

				return err
			}
			sessionAware, ok := tx.(interface{ SessionID() string })
			if !ok {
				ready <- leasedQueryTransactionResult{err: fmt.Errorf(
					"Query transaction %T does not expose its session ID",
					tx,
				)}

				return nil
			}
			transaction.transaction = tx
			transaction.sessionID = sessionAware.SessionID()
			ready <- leasedQueryTransactionResult{transaction: transaction}

			select {
			case <-transaction.release:
			case <-attemptCtx.Done():
			}

			return nil
		}, query.WithIdempotent(false))
		if !callbackStarted {
			ready <- leasedQueryTransactionResult{err: err}
		}
		transaction.doErr = err
		close(transaction.done)
	}()

	select {
	case result := <-ready:
		return result.transaction, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func transactionalWriteRequest(
	payload string,
	transactionID string,
	sessionID string,
) *Ydb_Topic.StreamWriteMessage_FromClient {
	return transactionalWriteRequestWithSequence(payload, transactionID, sessionID, nil)
}

func transactionalWriteRequestWithSequence(
	payload string,
	transactionID string,
	sessionID string,
	sequenceNumber *int64,
) *Ydb_Topic.StreamWriteMessage_FromClient {
	request := nonTransactionalWriteRequest(payload, sequenceNumber)
	request.GetWriteRequest().Tx = &Ydb_Topic.TransactionIdentity{Id: transactionID, Session: sessionID}

	return request
}

func researchFromContext(ctx context.Context) (*streamWriteResearch, error) {
	world, err := worldFromContext(ctx)
	if err != nil {
		return nil, err
	}
	research := world.research
	if research == nil {
		return nil, errors.New("StreamWrite research is not initialized")
	}

	return research, nil
}

func (r *streamWriteResearch) observeStreamWriteRequest(
	streamLabel string,
	message *Ydb_Topic.StreamWriteMessage_FromClient,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	description := fmt.Sprintf(
		"gRPC client → server /Ydb.Topic.V1.TopicService/StreamWrite %s / %s.",
		streamLabel,
		protobufMessageName(message),
	)
	if request := message.GetInitRequest(); request != nil {
		fields := []string{
			fmt.Sprintf("path=%q", request.GetPath()),
		}
		if request.GetProducerId() != "" {
			fields = append(fields, fmt.Sprintf("producer_id=%q", request.GetProducerId()))
		}
		switch partitioning := request.GetPartitioning().(type) {
		case *Ydb_Topic.StreamWriteMessage_InitRequest_PartitionId:
			fields = append(fields, fmt.Sprintf("partition_id=%d", partitioning.PartitionId))
		case *Ydb_Topic.StreamWriteMessage_InitRequest_PartitionWithGeneration:
			fields = append(fields, fmt.Sprintf(
				"partition_with_generation={partition_id=%d, generation=%d}",
				partitioning.PartitionWithGeneration.GetPartitionId(),
				partitioning.PartitionWithGeneration.GetGeneration(),
			))
		}
		if request.GetGetLastSeqNo() {
			fields = append(fields, "get_last_seq_no=true")
		}
		description = fmt.Sprintf(
			"gRPC client → server /Ydb.Topic.V1.TopicService/StreamWrite %s / %s: %s.",
			streamLabel,
			protobufMessageName(request),
			strings.Join(fields, ", "),
		)
	}
	if request := message.GetWriteRequest(); request != nil {
		description = fmt.Sprintf(
			"gRPC client → server /Ydb.Topic.V1.TopicService/StreamWrite %s / %s: %s.",
			streamLabel,
			protobufMessageName(request),
			r.describeWriteRequestLocked(request),
		)
	}

	r.appendProtocolEventLocked(
		fmt.Sprintf("client -> /Ydb.Topic.V1.TopicService/StreamWrite %s", streamLabel),
		description,
		message,
	)
}

func (r *streamWriteResearch) describeWriteRequestLocked(request *Ydb_Topic.StreamWriteMessage_WriteRequest) string {
	fields := make([]string, 0, 2)
	if tx := request.GetTx(); tx != nil {
		fields = append(fields, fmt.Sprintf("tx={id=%s, session=%s}",
			aliasedID(tx.GetId(), r.transactionAliases), aliasedID(tx.GetSession(), r.sessionAliases)))
	}
	messages := make([]string, 0, len(request.GetMessages()))
	for _, message := range request.GetMessages() {
		messages = append(messages, fmt.Sprintf("{data=%q, seq_no=%d}", message.GetData(), message.GetSeqNo()))
	}
	fields = append(fields, "messages=["+strings.Join(messages, "; ")+"]")

	return strings.Join(fields, ", ")
}

func aliasedID(id string, aliases map[string]string) string {
	value := fmt.Sprintf("%q", id)
	if alias := aliases[id]; alias != "" {
		value += " [" + alias + "]"
	}

	return value
}

func (r *streamWriteResearch) observeStreamWriteResponse(
	streamLabel string,
	message *Ydb_Topic.StreamWriteMessage_FromServer,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	description := fmt.Sprintf(
		"gRPC client ← server /Ydb.Topic.V1.TopicService/StreamWrite %s / %s: status=%s.",
		streamLabel,
		protobufMessageName(message),
		message.GetStatus(),
	)
	if response := message.GetInitResponse(); response != nil {
		description = fmt.Sprintf(
			"gRPC client ← server /Ydb.Topic.V1.TopicService/StreamWrite %s / %s: "+
				"status=%s, session_id=%q, partition_id=%d, last_seq_no=%d.",
			streamLabel,
			protobufMessageName(response),
			message.GetStatus(),
			response.GetSessionId(),
			response.GetPartitionId(),
			response.GetLastSeqNo(),
		)
	}
	if response := message.GetWriteResponse(); response != nil {
		acknowledgements := make([]string, 0, len(response.GetAcks()))
		for _, acknowledgement := range response.GetAcks() {
			writeResult := "unknown"
			switch acknowledgement.GetMessageWriteStatus().(type) {
			case *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Written_:
				writeResult = "written"
			case *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Skipped_:
				writeResult = "skipped"
			case *Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_WrittenInTx_:
				writeResult = "written_in_tx"
			}
			acknowledgements = append(acknowledgements, fmt.Sprintf(
				"seq_no=%d, result=%s",
				acknowledgement.GetSeqNo(),
				writeResult,
			))
		}
		description = fmt.Sprintf(
			"gRPC client ← server /Ydb.Topic.V1.TopicService/StreamWrite %s / %s: "+
				"status=%s, partition_id=%d, acks=[%s].",
			streamLabel,
			protobufMessageName(response),
			message.GetStatus(),
			response.GetPartitionId(),
			strings.Join(acknowledgements, "; "),
		)
	}
	if issues := message.GetIssues(); len(issues) > 0 {
		issueDescriptions := make([]string, 0, len(issues))
		for _, issue := range issues {
			issueDescriptions = append(issueDescriptions, fmt.Sprintf(
				"issue #%d: %s",
				issue.GetIssueCode(),
				issue.GetMessage(),
			))
		}
		description = strings.TrimSuffix(description, ".") + fmt.Sprintf(
			", issues=[%s].", strings.Join(issueDescriptions, "; "))
	}

	r.appendProtocolEventLocked(
		fmt.Sprintf("server -> /Ydb.Topic.V1.TopicService/StreamWrite %s", streamLabel),
		description,
		message,
	)
}

func (r *streamWriteResearch) observeGRPCEvent(event observedGRPCEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	details := r.annotateKnownIDsLocked(event.details)
	message := ""
	if event.messageName != "" {
		message = " / " + event.messageName
	}
	r.appendLiveEventLocked(fmt.Sprintf(
		"gRPC %s %s%s: %s.",
		humanGRPCDirection(event.direction),
		queryGRPCFullMethod(event.method),
		message,
		details,
	))
}

func (r *streamWriteResearch) registerTransactionAlias(
	alias string,
	transaction *leasedQueryTransaction,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.transactionAliases == nil {
		r.transactionAliases = make(map[string]string)
	}
	if r.sessionAliases == nil {
		r.sessionAliases = make(map[string]string)
	}
	txID := transaction.transaction.ID()
	r.transactionAliases[txID] = alias
	sessionAlias := r.sessionAliases[transaction.sessionID]
	if sessionAlias == "" {
		sessionAlias = nextQuerySessionAlias(len(r.sessionAliases))
		r.sessionAliases[transaction.sessionID] = sessionAlias
	}
	r.appendLiveEventLocked(fmt.Sprintf(
		"Aliases: %s (tx_id=%q); %s (session_id=%q).",
		alias,
		txID,
		sessionAlias,
		transaction.sessionID,
	))
}

func nextQuerySessionAlias(index int) string {
	if index >= 0 && index < 26 {
		return fmt.Sprintf("Query session %c", 'A'+rune(index))
	}

	return fmt.Sprintf("Query session %d", index+1)
}

func (r *streamWriteResearch) annotateKnownIDsLocked(details string) string {
	for id, alias := range r.transactionAliases {
		field := fmt.Sprintf("tx_id=%q", id)
		details = strings.ReplaceAll(details, field, field+" ["+alias+"]")
	}
	for id, alias := range r.sessionAliases {
		field := fmt.Sprintf("session_id=%q", id)
		details = strings.ReplaceAll(details, field, field+" ["+alias+"]")
	}

	return details
}

func protobufMessageName(message proto.Message) string {
	return string(message.ProtoReflect().Descriptor().FullName())
}

func (r *streamWriteResearch) appendProtocolEventLocked(
	direction string,
	description string,
	message proto.Message,
) {
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
	if err != nil {
		r.transcript = append(r.transcript, fmt.Sprintf(
			"%s: <%T marshal error: %v>",
			direction,
			message,
			err,
		))
	} else {
		r.transcript = append(r.transcript, direction+": "+string(encoded))
	}
	r.appendLiveEventLocked(description)
}

func (r *streamWriteResearch) observe(description string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.appendLiveEventLocked(description)
}

func (r *streamWriteResearch) appendLiveEventLocked(description string) {
	r.timeline = append(r.timeline, description)
	if r.liveLogf != nil {
		r.liveLogf("%d. %s", len(r.timeline), description)
	}
}

func (r *streamWriteResearch) Transcript() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return joinTranscript(r.transcript)
}

func (r *streamWriteResearch) HumanReadableReport() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	lines := make([]string, 0, len(r.timeline))
	for i, event := range r.timeline {
		lines = append(lines, fmt.Sprintf("%d. %s", i+1, event))
	}

	return joinTranscript(lines)
}

func humanGRPCDirection(direction string) string {
	if direction == "server ->" {
		return "client ← server"
	}

	return "client → server"
}

func (r *streamWriteResearch) Close(ctx context.Context) error {
	var cleanupErr error
	if r.world != nil && r.world.observer != nil {
		r.world.observer.SetEventSink(nil)
	}
	for _, session := range r.writeSessions {
		cleanupErr = errors.Join(cleanupErr, session.closeStream(ctx))
	}
	if r.readStream != nil {
		cleanupErr = errors.Join(cleanupErr, r.readStream.CloseSend())
	}
	if r.readCancel != nil {
		r.readCancel()
	}
	if r.readDone != nil {
		select {
		case <-r.readDone:
		case <-ctx.Done():
			cleanupErr = errors.Join(cleanupErr, ctx.Err())
		}
	}
	for _, transaction := range r.transactions {
		if !transaction.finished {
			cleanupErr = errors.Join(cleanupErr, transaction.transaction.Rollback(ctx))
			transaction.finished = true
		}
		transaction.Release()
		cleanupErr = errors.Join(cleanupErr, transaction.Wait(ctx))
	}

	return cleanupErr
}

func (t *leasedQueryTransaction) Release() {
	t.releaseOnce.Do(func() {
		close(t.release)
	})
}

func (t *leasedQueryTransaction) Wait(ctx context.Context) error {
	select {
	case <-t.done:
		return t.doErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
