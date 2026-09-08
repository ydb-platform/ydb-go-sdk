Feature: Partition-ID-only write sessions across a partition split
  Every StreamWrite InitRequest sets partition_id without producer_id.
  Compare ordinary writes and Query transactions before and after a real split.
  A fresh one-partition topic is split via PAUSED auto partitioning and AlterTopic.
  Messages still carry positive seq_no; only producer_id is omitted.

  # Observed on YDB version: main.7f40cb4 (trunk, 2026-09-08).
  # InitRequest{partition_id: 0} and the ordinary write succeed without producer_id.
  # Split makes partition 0 inactive with children 1 and 2. An ordinary write on the old
  # stream gets OVERLOADED (Write to inactive partition 0), then EOF. Reopening partition 0
  # gets OVERLOADED (Partition not choosed), then EOF. There is no transparent rerouting.
  # Producerless child sessions both write successfully; StreamRead returns the pre-split
  # parent message and one message per child, each at offset 0 in its own partition.
  Scenario: Write without a producer through old and new sessions after a split
    * an empty topic with 1 partitions with paused auto partitioning with consumer "partition-only-reader" for observation
    * TopicService.StreamWrite "Parent": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "Parent": WriteRequest messages:
      | data                | seq_no |
      | parent-before-split | 1      |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Parent": WriteRequest messages:
      | data           | seq_no |
      | old-after-split | 2      |
    * TopicService.StreamWrite "Parent": CloseSend
    * TopicService.StreamWrite "Reopened parent": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "Reopened parent": WriteRequest messages:
      | data                | seq_no |
      | reopened-after-split | 1      |
    * TopicService.StreamWrite "Reopened parent": CloseSend
    * TopicService.StreamWrite "Left": InitRequest{partition_id: 1}
    * TopicService.StreamWrite "Right": InitRequest{partition_id: 2}
    * TopicService.StreamWrite "Left": WriteRequest messages:
      | data       | seq_no |
      | left-child | 1      |
    * TopicService.StreamWrite "Right": WriteRequest messages:
      | data        | seq_no |
      | right-child | 1      |
    * TopicService.StreamWrite "Left": CloseSend
    * TopicService.StreamWrite "Right": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: partition-only-reader, partition_ids: [0, 1, 2]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

  # Observed on YDB version: main.7f40cb4 (trunk, 2026-09-08).
  # The same transaction receives written_in_tx for writes before and after split on the
  # old producerless stream. Commit returns ABORTED (#2011, Partition 0 is inactive).
  # All partition end offsets remain 0. Replaying both messages in a new transaction
  # through producerless child sessions commits successfully: one message per child,
  # parent still empty. StreamRead returns both replayed payloads once.
  Scenario: Stage without a producer before and after split and replay in the child partitions
    * an empty topic with 1 partitions with paused auto partitioning with consumer "partition-only-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Before split"
    * TopicService.StreamWrite "Parent": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "Parent": WriteRequest{txId: Before split} messages:
      | data                | seq_no |
      | staged-before-split | 1      |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Parent": WriteRequest{txId: Before split} messages:
      | data            | seq_no |
      | sent-after-split | 2      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Before split"
    * TopicService.StreamWrite "Parent": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "After split"
    * TopicService.StreamWrite "Left": InitRequest{partition_id: 1}
    * TopicService.StreamWrite "Right": InitRequest{partition_id: 2}
    * TopicService.StreamWrite "Left": WriteRequest{txId: After split} messages:
      | data                | seq_no |
      | staged-before-split | 1      |
    * TopicService.StreamWrite "Right": WriteRequest{txId: After split} messages:
      | data            | seq_no |
      | sent-after-split | 2      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "After split"
    * TopicService.StreamWrite "Left": CloseSend
    * TopicService.StreamWrite "Right": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: partition-only-reader, partition_ids: [0, 1, 2]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

  # Observed on YDB version: main.7f40cb4 (trunk, 2026-09-08).
  # The producerless stream opens on partition 0 before split, but the transaction starts
  # after split. Its first WriteRequest gets EOF without a WriteResponse; commit returns
  # ABORTED (#2011, invalid WriteId). All partition end offsets remain 0.
  Scenario: Start a transaction after split using a producerless stream opened before split
    * an empty topic with 1 partitions with paused auto partitioning
    * TopicService.StreamWrite "Parent": InitRequest{partition_id: 0}
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Started after split"
    * TopicService.StreamWrite "Parent": WriteRequest{txId: Started after split} messages:
      | data                | seq_no |
      | new-tx-on-old-stream | 1      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Started after split"
    * TopicService.StreamWrite "Parent": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
