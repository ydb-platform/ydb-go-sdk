Feature: Producer sequence state and transactional deduplication
  These contracts use explicit positive sequence numbers; ACK does not establish a successful commit.

  Scenario: Repeated sequence numbers preserve the original payload
    * an empty topic with 1 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "A"
    * TopicService.StreamWrite "S": InitRequest{producer_id: manual}
    * client: send TopicService.StreamWrite requests without waiting for ACKs
    * TopicService.StreamWrite "S": WriteRequest{txId: A} messages:
      | data         | seq_no |
      | original-one | 1      |
      | original-two | 2      |
    * TopicService.StreamWrite "S": WriteRequest{txId: A} messages:
      | data            | seq_no |
      | replacement-two | 2      |
      | original-three  | 3      |
    Then contract: StreamWrite "S" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
      | 2      | skipped       |
      | 3      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "A"
    Then contract: Commit of "A" returned SUCCESS
    * TopicService.StreamRead: InitRequest{consumer: contract-reader}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | seq_no | data           |
      | 1      | original-one   |
      | 2      | original-two   |
      | 3      | original-three |
    * TopicService.StreamWrite "S": CloseSend

  Scenario: Two accepted transactions with one producer may conflict at commit
    * an empty topic with 1 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "B"
    * TopicService.StreamWrite "S": InitRequest{producer_id: manual}
    * client: send TopicService.StreamWrite requests without waiting for ACKs
    * TopicService.StreamWrite "S": WriteRequest{txId: A} messages:
      | data     | seq_no |
      | a-first  | 1      |
      | a-second | 2      |
    * TopicService.StreamWrite "S": WriteRequest{txId: B} messages:
      | data     | seq_no |
      | b-first  | 1      |
      | b-second | 2      |
    Then contract: StreamWrite "S" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "A"
    Then contract: Commit of "A" returned SUCCESS
    * QueryService.CommitTransaction: CommitTransactionRequest for "B"
    Then contract: Commit of "B" returned ABORTED
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "C"
    * TopicService.StreamWrite "S": WriteRequest{txId: C} messages:
      | data        | seq_no |
      | after-abort | 3      |
    Then contract: StreamWrite "S" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
      | 3      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "C"
    Then contract: Commit of "C" returned SUCCESS
    * TopicService.StreamRead: InitRequest{consumer: contract-reader}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | seq_no | data        |
      | 1      | a-first     |
      | 2      | a-second    |
      | 3      | after-abort |
    * TopicService.StreamWrite "S": CloseSend

  Scenario: Rolled-back sequence numbers remain writable after reopening
    * an empty topic with 1 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "A"
    * TopicService.StreamWrite "First": InitRequest{producer_id: manual}
    * TopicService.StreamWrite "First": WriteRequest{txId: A} messages:
      | data      | seq_no |
      | committed | 7      |
    Then contract: StreamWrite "First" has exactly these ACKs:
      | seq_no | result        |
      | 7      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "A"
    Then contract: Commit of "A" returned SUCCESS
    * TopicService.StreamWrite "First": CloseSend
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "B"
    * TopicService.StreamWrite "Second": InitRequest{producer_id: manual}
    * TopicService.StreamWrite "Second": WriteRequest{txId: B} messages:
      | data      | seq_no |
      | discarded | 8      |
    Then contract: StreamWrite "Second" has exactly these ACKs:
      | seq_no | result        |
      | 8      | written_in_tx |
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "B"
    Then contract: Rollback of "B" returned SUCCESS
    * TopicService.StreamWrite "Second": CloseSend
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "C"
    * TopicService.StreamWrite "Third": InitRequest{producer_id: manual}
    * TopicService.StreamWrite "Third": WriteRequest{txId: C} messages:
      | data           | seq_no |
      | after-rollback | 8      |
    Then contract: StreamWrite "Third" has exactly these ACKs:
      | seq_no | result        |
      | 8      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "C"
    Then contract: Commit of "C" returned SUCCESS
    * TopicService.StreamWrite "Third": CloseSend
    * TopicService.StreamRead: InitRequest{consumer: contract-reader}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | seq_no | data           |
      | 7      | committed      |
      | 8      | after-rollback |
