Feature: Transaction delivery through multiple StreamWrite sessions
  Committed messages are read from every participating partition, with independent sequence spaces per stream.

  Scenario: Commit publishes all partitions without producer deduplication
    * an empty topic with 2 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "A"
    * TopicService.StreamWrite "Left": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "Right": InitRequest{partition_id: 1}
    * TopicService.StreamWrite "Left": WriteRequest{txId: A} messages:
      | data        | seq_no |
      | left-first  | 1      |
      | left-second | 2      |
    * TopicService.StreamWrite "Right": WriteRequest{txId: A} messages:
      | data         | seq_no |
      | right-first  | 1      |
      | right-second | 2      |
    Then contract: StreamWrite "Left" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
    Then contract: StreamWrite "Right" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "A"
    Then contract: Commit of "A" returned SUCCESS
    * TopicService.StreamRead: InitRequest{consumer: contract-reader, partition_ids: [0, 1]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | partition_id | seq_no | data         |
      | 0            | 1      | left-first   |
      | 0            | 2      | left-second  |
      | 1            | 1      | right-first  |
      | 1            | 2      | right-second |
    * TopicService.StreamWrite "Left": CloseSend
    * TopicService.StreamWrite "Right": CloseSend

  Scenario: Different producers participate in the same transaction
    * an empty topic with 2 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "A"
    * TopicService.StreamWrite "Left": InitRequest{producer_id: left-producer, partition_id: 0}
    * TopicService.StreamWrite "Right": InitRequest{producer_id: right-producer, partition_id: 1}
    * TopicService.StreamWrite "Left": WriteRequest{txId: A} messages:
      | data | seq_no |
      | left | 1      |
    * TopicService.StreamWrite "Right": WriteRequest{txId: A} messages:
      | data  | seq_no |
      | right | 1      |
    Then contract: StreamWrite "Left" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
    Then contract: StreamWrite "Right" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "A"
    Then contract: Commit of "A" returned SUCCESS
    * TopicService.StreamRead: InitRequest{consumer: contract-reader, partition_ids: [0, 1]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | partition_id | seq_no | data  |
      | 0            | 1      | left  |
      | 1            | 1      | right |
    * TopicService.StreamWrite "Left": CloseSend
    * TopicService.StreamWrite "Right": CloseSend
