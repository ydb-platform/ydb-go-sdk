Feature: Independent transactions share a healthy StreamWrite
  Interleaving, commit and rollback must preserve the other transaction and permit subsequent reuse.

  Scenario: Rollback discards only its own interleaved writes
    * an empty topic with 1 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "B"
    * TopicService.StreamWrite "S": InitRequest{}
    * client: send TopicService.StreamWrite requests without waiting for ACKs
    * TopicService.StreamWrite "S": WriteRequest{txId: A} messages:
      | data        | seq_no |
      | discard-one | 1      |
      | discard-two | 2      |
    * TopicService.StreamWrite "S": WriteRequest{txId: B} messages:
      | data     | seq_no |
      | keep-one | 1      |
    * TopicService.StreamWrite "S": WriteRequest{txId: A} messages:
      | data          | seq_no |
      | discard-three | 2      |
      | discard-four  | 3      |
    * TopicService.StreamWrite "S": WriteRequest{txId: B} messages:
      | data     | seq_no |
      | keep-two | 2      |
    Then contract: StreamWrite "S" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
      | 3      | written_in_tx |
      | 2      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "B"
    Then contract: Commit of "B" returned SUCCESS
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "A"
    Then contract: Rollback of "A" returned SUCCESS
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "C"
    * TopicService.StreamWrite "S": WriteRequest{txId: C} messages:
      | data           | seq_no |
      | after-rollback | 4      |
    Then contract: StreamWrite "S" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
      | 3      | written_in_tx |
      | 2      | written_in_tx |
      | 4      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "C"
    Then contract: Commit of "C" returned SUCCESS
    * TopicService.StreamRead: InitRequest{consumer: contract-reader}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | seq_no | data           |
      | 1      | keep-one       |
      | 2      | keep-two       |
      | 4      | after-rollback |
    * TopicService.StreamWrite "S": CloseSend
