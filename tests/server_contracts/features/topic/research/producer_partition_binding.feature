Feature: Explicit partition selection for the same ProducerID
  Observe whether initialization alone binds a producer to a partition,
  and compare it with initialization followed by an acknowledged write.

  # Observed on YDB main.7f40cb4 (trunk): initialization on partition 0 already
  # binds the producer. Reopening it on partition 1 returns BAD_REQUEST (#500003)
  # and EOF, even though no message was written. Reopening partition 0 succeeds
  # with last_seq_no=0; both partitions remain empty. A first write is not required
  # for the binding to survive closing the original stream.
  Scenario: Reopen one producer on partition 1 before its first write
    * an empty topic with 2 partitions
    * TopicService.StreamWrite: InitRequest{producer_id: partition-binding-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: partition-binding-producer, partition_id: 1, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest messages:
      | data                | seq_no |
      | partition-1-message | 2      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: partition-binding-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true

  # Observed on YDB main.7f40cb4 (trunk): partition 0 accepts seq_no=1 as written.
  # InitRequest for the same producer on partition 1 returns BAD_REQUEST (#500003)
  # because the producer is already bound to partition 0; the stream ends with EOF.
  # The error uses one-based PartitionGroupId values 1 and 2 for partition_id 0 and 1.
  # Reopening partition 0 succeeds with last_seq_no=1; end_offset is 1 on partition 0
  # and 0 on partition 1. Closing a session does not remove the producer binding.
  Scenario: Reopen one producer on partition 1 after writing to partition 0
    * an empty topic with 2 partitions
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite: InitRequest{producer_id: partition-binding-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest messages:
      | data                | seq_no |
      | partition-0-message | 1      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: partition-binding-producer, partition_id: 1, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest messages:
      | data                | seq_no |
      | partition-1-message | 2      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: partition-binding-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
