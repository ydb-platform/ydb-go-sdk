Feature: Transactional batches in one StreamWrite session
  Each WriteRequest contains a messages list and one transaction identity.
  Observe the ACKs and committed offsets of two batches belonging to different transactions.

  # Observed on YDB main.7f40cb4 (trunk), rerun with pipelining: both WriteRequests
  # are sent before either ACK is observed. Each two-message WriteRequest receives
  # one WriteResponse with two written_in_tx ACKs matching its seq_no values.
  # Committing A succeeds and advances end_offset to 2 while B remains uncommitted;
  # committing B succeeds and advances it to 4. Each batch belongs to its request's tx.
  Scenario: Two transactions write separate two-message batches through one stream
    * an empty topic
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{producer_id: batch-producer, partition_id: 0, get_last_seq_no: true}
    * research runner: pipeline TopicService.StreamWrite requests
    * TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
      | data           | seq_no |
      | first-message  | 1      |
      | second-message | 2      |
    * TopicService.StreamWrite: WriteRequest{txId: Transaction B} messages:
      | data           | seq_no |
      | third-message  | 3      |
      | fourth-message | 4      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction B"
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
