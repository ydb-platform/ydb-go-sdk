Feature: Producerless acknowledgements and delivery
  ACKs are matched by seq_no, independent of their order and response batching. Both producer identifiers are omitted.

  Scenario: Overlapping pipelined batches retain every payload and ACK
    * an empty topic with 1 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "A"
    * TopicService.StreamWrite "S": InitRequest{}
    * client: send TopicService.StreamWrite requests without waiting for ACKs
    * TopicService.StreamWrite "S": WriteRequest{txId: A} messages:
      | data   | seq_no |
      | first  | 1      |
      | second | 2      |
    * TopicService.StreamWrite "S": WriteRequest{txId: A} messages:
      | data            | seq_no |
      | repeated-second | 2      |
      | third           | 3      |
    * TopicService.StreamWrite "S": WriteRequest{txId: A} messages:
      | data           | seq_no |
      | repeated-third | 3      |
    Then contract: StreamWrite "S" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
      | 2      | written_in_tx |
      | 3      | written_in_tx |
      | 3      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "A"
    Then contract: Commit of "A" returned SUCCESS
    * TopicService.StreamRead: InitRequest{consumer: contract-reader}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | seq_no | data            |
      | 1      | first           |
      | 2      | second          |
      | 2      | repeated-second |
      | 3      | third           |
      | 3      | repeated-third  |
    * TopicService.StreamWrite "S": CloseSend

  Scenario: Two open transactions may commit identical sequence ranges
    * an empty topic with 1 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "B"
    * TopicService.StreamWrite "S": InitRequest{}
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
    Then contract: Commit of "B" returned SUCCESS
    * TopicService.StreamRead: InitRequest{consumer: contract-reader}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | seq_no | data     |
      | 1      | a-first  |
      | 2      | a-second |
      | 1      | b-first  |
      | 2      | b-second |
    * TopicService.StreamWrite "S": CloseSend
