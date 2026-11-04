Feature: Overlapping sequence numbers between transactional batches
  All write sessions specify partition_id without producer_id; sequence numbers remain explicit.
  Keep each batch sorted but repeat sequence numbers across separate WriteRequests.

  # Observed on YDB main.7f40cb4 (trunk, 2026-09-08), without producer_id:
  # requests [1,2] and [2,3] receive written_in_tx for all four messages, including both 2s.
  # Commit succeeds, end_offset=4. StreamRead returns first-1, first-2, replacement-2,
  # next-3 at offsets 0..3: the repeated sequence number is not deduplicated.
  Scenario: Overlap two batches within one transaction
    * an empty topic with 1 partitions with consumer "research-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * TopicService.StreamWrite "P0": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "P0": WriteRequest{txId: Transaction A} messages:
      | data    | seq_no |
      | first-1 | 1      |
      | first-2 | 2      |
    * TopicService.StreamWrite "P0": WriteRequest{txId: Transaction A} messages:
      | data          | seq_no |
      | replacement-2 | 2      |
      | next-3        | 3      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.StreamWrite "P0": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: research-reader, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

  # Observed on YDB main.7f40cb4 (trunk, 2026-09-08), without producer_id:
  # both transactions receive written_in_tx ACKs for [1,2] on the same stream.
  # Both commits succeed; end_offset advances to 2 and then 4, with no MinSeqNo violation.
  # StreamRead returns a-1, a-2, b-1, b-2 at offsets 0..3. ACK seq_no values repeat
  # across the transactions, and WriteResponse has no tx_id: seq_no alone cannot
  # distinguish their ACKs. Here the steps observe ACKs between sends.
  Scenario: Overlap two batches belonging to different open transactions
    * an empty topic with 1 partitions with consumer "research-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite "P0": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "P0": WriteRequest{txId: Transaction A} messages:
      | data | seq_no |
      | a-1  | 1      |
      | a-2  | 2      |
    * TopicService.StreamWrite "P0": WriteRequest{txId: Transaction B} messages:
      | data | seq_no |
      | b-1  | 1      |
      | b-2  | 2      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction B"
    * TopicService.StreamWrite "P0": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: research-reader, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
