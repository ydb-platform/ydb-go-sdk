Feature: Unsorted transactional batches and subsequent writes
  Observe how an unsorted batch affects the stream, reconnect attempts,
  subsequent ordinary writes, and commits of the associated transactions.

  # Observed on YDB main.7f40cb4 (trunk) with actual batches: tx1's messages [2,1]
  # are rejected with BAD_REQUEST (#500003, sequence numbers are unsorted), then EOF.
  # Reopening the stream and resending [2,1] produces the same error. All subsequent
  # writes on these terminated streams return EOF, including tx2 and ordinary writes.
  # All three empty transactions commit successfully; end_offset remains 0 and no
  # payloads are read. Unlike separate requests, this batch requires increasing seq_no.
  Scenario: Retry an unsorted transactional batch and observe subsequent writes
    * an empty topic with consumer "mixed-write-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "tx1"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "tx2"
    * TopicService.StreamWrite: InitRequest{producer_id: mixed-write-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: tx1} messages:
      | data          | seq_no |
      | tx1-message-1 | 2      |
      | tx1-message-2 | 1      |
    * TopicService.StreamWrite: WriteRequest{txId: tx2} messages:
      | data          | seq_no |
      | tx2-message-3 | 3      |
      | tx2-message-4 | 4      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: mixed-write-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: tx1} messages:
      | data          | seq_no |
      | tx1-message-1 | 2      |
      | tx1-message-2 | 1      |
    * TopicService.StreamWrite: WriteRequest{txId: tx2} messages:
      | data          | seq_no |
      | tx2-message-3 | 3      |
      | tx2-message-4 | 4      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "tx2"
    * QueryService.CommitTransaction: CommitTransactionRequest for "tx1"
    * TopicService.StreamWrite: WriteRequest messages:
      | data             | seq_no |
      | direct-message-5 | 3      |
      | direct-message-6 | 6      |
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "tx3"
    * TopicService.StreamWrite: WriteRequest{txId: tx3} messages:
      | data          | seq_no |
      | tx3-message-7 | 7      |
      | tx3-message-8 | 8      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "tx3"
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: mixed-write-reader, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
