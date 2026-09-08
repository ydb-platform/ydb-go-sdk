Feature: Partition split with a pending transaction and complete replay
  PAUSED auto partitioning plus AlterTopic requests a real split, not just a new independent partition.
  DescribeTopic records active flags and parent/child IDs. Child IDs 1 and 2 are probed on a fresh one-partition topic.

  # Observed on YDB main.7f40cb4 (trunk): AlterTopic succeeds; partition 0 becomes
  # inactive with active children [1,2]. The old stream still ACKs seq_no=2 as
  # written_in_tx after the split, but commit is ABORTED (#2011, partition 0 inactive).
  # All end offsets remain 0. A new transaction replays both logical payloads through
  # new per-child producers and commits successfully. Each child ends at offset 1;
  # StreamRead returns staged-before-split and sent-after-split exactly once.
  Scenario: Commit after a partition split and replay all messages in a new transaction
    * an empty topic with 1 partitions with paused auto partitioning with consumer "research-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Before split"
    * TopicService.StreamWrite "Parent": InitRequest{producer_id: parent-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite "Parent": WriteRequest{txId: Before split} messages:
      | data                | seq_no |
      | staged-before-split | 1      |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Parent": WriteRequest{txId: Before split} messages:
      | data             | seq_no |
      | sent-after-split | 2      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Before split"
    * TopicService.StreamWrite "Parent": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "After split"
    * TopicService.StreamWrite "Left": InitRequest{producer_id: child-left-producer, partition_id: 1, get_last_seq_no: true}
    * TopicService.StreamWrite "Right": InitRequest{producer_id: child-right-producer, partition_id: 2, get_last_seq_no: true}
    * TopicService.StreamWrite "Left": WriteRequest{txId: After split} messages:
      | data                | seq_no |
      | staged-before-split | 1      |
    * TopicService.StreamWrite "Right": WriteRequest{txId: After split} messages:
      | data             | seq_no |
      | sent-after-split | 1      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "After split"
    * TopicService.StreamWrite "Left": CloseSend
    * TopicService.StreamWrite "Right": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: research-reader, partition_ids: [1, 2]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

  # Observed on YDB main.7f40cb4 (trunk): the recorded parent ACK is withheld,
  # AlterTopic makes parent 0 inactive with children [1,2], and explicit rollback
  # succeeds. A new transaction with new per-child producers commits both replayed
  # logical payloads: parent end_offset=0, each child end_offset=1. StreamRead
  # returns logical-left and logical-right exactly once, from different children.
  Scenario: Lose an ACK, split, rollback, and replay the entire transaction into the child partitions
    * an empty topic with 1 partitions with paused auto partitioning with consumer "research-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Attempt 1"
    * TopicService.StreamWrite "Parent": InitRequest{producer_id: parent-producer, partition_id: 0, get_last_seq_no: true}
    * research runner: withhold the next TopicService.StreamWrite "Parent" WriteResponse
    * TopicService.StreamWrite "Parent": WriteRequest{txId: Attempt 1} messages:
      | data          | seq_no |
      | logical-left  | 1      |
      | logical-right | 2      |
    * TopicService.StreamWrite "Parent": CloseSend before consuming the recorded WriteResponse
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "Attempt 1"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Attempt 2"
    * TopicService.StreamWrite "Left": InitRequest{producer_id: child-left-producer, partition_id: 1, get_last_seq_no: true}
    * TopicService.StreamWrite "Right": InitRequest{producer_id: child-right-producer, partition_id: 2, get_last_seq_no: true}
    * TopicService.StreamWrite "Left": WriteRequest{txId: Attempt 2} messages:
      | data         | seq_no |
      | logical-left | 1      |
    * TopicService.StreamWrite "Right": WriteRequest{txId: Attempt 2} messages:
      | data          | seq_no |
      | logical-right | 1      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Attempt 2"
    * TopicService.StreamWrite "Left": CloseSend
    * TopicService.StreamWrite "Right": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: research-reader, partition_ids: [1, 2]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
