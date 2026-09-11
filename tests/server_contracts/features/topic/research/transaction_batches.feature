Feature: Transactional batches in one StreamWrite session
  All write sessions specify partition_id without producer_id; sequence numbers remain explicit.
  Each WriteRequest contains a messages list and one transaction identity.
  Observe the ACKs and committed offsets of two batches belonging to different transactions.

  # Observed on YDB main.7f40cb4 (trunk, 2026-09-08), without producer_id:
  # both WriteRequests are sent before either ACK is observed. Each request receives
  # one WriteResponse with two written_in_tx ACKs matching its distinct seq_no range.
  # Commit A succeeds and advances end_offset to 2 while B remains uncommitted;
  # commit B succeeds and advances it to 4. WriteResponse has partition_id but no tx_id.
  Scenario: Two transactions write separate two-message batches through one stream
    * an empty topic
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{partition_id: 0}
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
