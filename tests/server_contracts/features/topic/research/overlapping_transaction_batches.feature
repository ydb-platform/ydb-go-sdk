Feature: Overlapping sequence numbers between transactional batches
  Keep each batch sorted but repeat sequence numbers across separate WriteRequests.

  # Observed on YDB main.7f40cb4 (trunk): [1,2] receives written_in_tx ACKs;
  # the next request [2,3] receives skipped for 2 and written_in_tx for 3.
  # Commit succeeds, end_offset=3. StreamRead returns first-1, first-2, next-3;
  # replacement-2 does not replace the earlier payload with the same sequence number.
  Scenario: Overlap two batches within one transaction
    * an empty topic with 1 partitions with consumer "research-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * TopicService.StreamWrite "P0": InitRequest{producer_id: overlap-producer, partition_id: 0, get_last_seq_no: true}
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

  # Observed on YDB main.7f40cb4 (trunk): both transactions receive written_in_tx
  # ACKs for [1,2] on the same stream and partition. A commits; B's commit is ABORTED
  # with #2011 MinSeqNo violation. Only a-1 and a-2 are read, end_offset=2.
  # ACK seq_no values are not unique across open transactions when the client reuses them;
  # WriteResponse has no tx_id, and written_in_tx does not promise a successful commit.
  Scenario: Overlap two batches belonging to different open transactions
    * an empty topic with 1 partitions with consumer "research-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite "P0": InitRequest{producer_id: overlap-producer, partition_id: 0, get_last_seq_no: true}
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
