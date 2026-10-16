Feature: Repeated sequence numbers in producerless transactional writes
  Every StreamWrite InitRequest sets partition_id without producer_id or message_group_id.
  Compare repeated numbers across WriteRequests with repeated numbers inside one request.
  Read back distinct payloads after Commit rather than treating ACKs as committed data.

  # Observed on YDB main.9b354d6 (trunk, 2026-09-11), without producer_id or message_group_id:
  # both requests receive written_in_tx for seq_no=1. Before Commit, end_offset=0.
  # Commit succeeds, end_offset=2. StreamRead returns first-message and second-message
  # at offsets 0 and 1, both with seq_no=1: neither payload is deduplicated or replaced.
  Scenario: Two single-message requests use sequence number 1 in one transaction
    * an empty topic with 1 partitions with consumer "repeated-seq-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * TopicService.StreamWrite "P0": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "P0": WriteRequest{txId: Transaction A} messages:
      | data          | seq_no |
      | first-message | 1      |
    * TopicService.StreamWrite "P0": WriteRequest{txId: Transaction A} messages:
      | data           | seq_no |
      | second-message | 1      |
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.StreamWrite "P0": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: repeated-seq-reader, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

  # Observed on YDB main.9b354d6 (trunk, 2026-09-11), without producer_id or message_group_id:
  # [1,1] gets BAD_REQUEST (#500003, sequence numbers are unsorted), then EOF, without ACKs.
  # Commit still returns SUCCESS, but end_offset stays 0 and StreamRead returns no payloads.
  # The increasing [1,2] control gets two written_in_tx ACKs and commits both payloads:
  # end_offset goes from 0 to 2, and StreamRead returns the messages at offsets 0 and 1.
  Scenario Outline: One transactional request uses sequence numbers 1 and <second_seq_no>
    * an empty topic with 1 partitions with consumer "repeated-seq-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * TopicService.StreamWrite "P0": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "P0": WriteRequest{txId: Transaction A} messages:
      | data           | seq_no          |
      | first-message  | 1               |
      | second-message | <second_seq_no> |
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.StreamWrite "P0": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: repeated-seq-reader, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

    Examples:
      | second_seq_no |
      | 1             |
      | 2             |
