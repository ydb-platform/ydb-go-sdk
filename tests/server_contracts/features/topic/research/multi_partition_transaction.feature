Feature: One Query transaction writes to multiple topic partitions
  All write sessions specify partition_id without producer_id; sequence numbers remain explicit.
  Observe staging, commit, ACKs, and payloads independently in both partitions.

  # Observed on YDB main.7f40cb4 (trunk, 2026-09-08), without producer_id:
  # both partitions ACK seq_no=[1,2] as written_in_tx for one tx_id;
  # both end offsets remain 0 before commit. Commit succeeds, both end offsets
  # become 2, and StreamRead returns the two original payloads from each partition.
  Scenario: Commit two batches in two partitions as one transaction
    * an empty topic with 2 partitions with consumer "research-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * TopicService.StreamWrite "P0": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "P1": InitRequest{partition_id: 1}
    * TopicService.StreamWrite "P0": WriteRequest{txId: Transaction A} messages:
      | data        | seq_no |
      | left-first  | 1      |
      | left-second | 2      |
    * TopicService.StreamWrite "P1": WriteRequest{txId: Transaction A} messages:
      | data         | seq_no |
      | right-first  | 1      |
      | right-second | 2      |
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.StreamWrite "P0": CloseSend
    * TopicService.StreamWrite "P1": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: research-reader, partition_ids: [0, 1]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
