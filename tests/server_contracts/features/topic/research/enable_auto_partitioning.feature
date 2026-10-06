Feature: Existing write sessions when auto partitioning is enabled
  Enable SCALE_UP on a topic created without auto partitioning while StreamWrite is open.
  Keep min_active_partitions and max_active_partitions at 2 to isolate the setting change
  from a partition split. Compare explicit partition routing with server-selected routing.
  Record AlterTopic status, old and fresh session responses, commits, topology, and payloads.

  # Observed on YDB version: main.db11cbd (trunk, 2026-09-09).
  # AlterTopic returns SUCCESS. All three routing modes keep the existing stream usable:
  # writes before and after enablement receive written ACKs on the same partition.
  # Fresh sessions accept seq_no=3; producer sessions report last_seq_no=2.
  # Both partitions remain active without parent/child links. StreamRead returns all
  # three payloads at offsets 0, 1, 2 in the selected partition.
  Scenario Outline: Continue ordinary writes after enabling auto partitioning with <mode>
    * an empty topic with 2 partitions with consumer "enable-reader" for observation
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {alter_auto_partitioning_settings: {set_strategy: AUTO_PARTITIONING_STRATEGY_DISABLED}}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Existing": InitRequest{<init>}
    * TopicService.StreamWrite "Existing": WriteRequest messages:
      | data          | seq_no |
      | before-enable | 1      |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {alter_auto_partitioning_settings: {set_strategy: AUTO_PARTITIONING_STRATEGY_SCALE_UP}}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Existing": WriteRequest messages:
      | data                  | seq_no |
      | existing-after-enable | 2      |
    * TopicService.StreamWrite "Existing": CloseSend
    * TopicService.StreamWrite "Fresh": InitRequest{<init>}
    * TopicService.StreamWrite "Fresh": WriteRequest messages:
      | data               | seq_no |
      | fresh-after-enable | 3      |
    * TopicService.StreamWrite "Fresh": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: enable-reader, partition_ids: [0, 1]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

    Examples:
      | mode                      | init                                                                 |
      | partition ID only         | partition_id: 0                                                      |
      | producer and partition ID | producer_id: enable-producer, partition_id: 0, get_last_seq_no: true |
      | producer-selected routing | producer_id: enable-producer, get_last_seq_no: true                  |

  # Observed on YDB version: main.db11cbd (trunk, 2026-09-09).
  # All three routing modes receive written_in_tx before and after enablement on the
  # existing stream. Both the spanning transaction and the fresh-session transaction
  # commit with SUCCESS. StreamRead returns all three payloads at offsets 0, 1, 2.
  # Enabling SCALE_UP alone does not invalidate the session or abort its transaction;
  # this experiment does not characterize a subsequent load-triggered split.
  Scenario Outline: Commit a transaction spanning auto partitioning enablement with <mode>
    * an empty topic with 2 partitions with consumer "enable-reader" for observation
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {alter_auto_partitioning_settings: {set_strategy: AUTO_PARTITIONING_STRATEGY_DISABLED}}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Spanning enablement"
    * TopicService.StreamWrite "Existing": InitRequest{<init>}
    * TopicService.StreamWrite "Existing": WriteRequest{txId: Spanning enablement} messages:
      | data          | seq_no |
      | before-enable | 1      |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {alter_auto_partitioning_settings: {set_strategy: AUTO_PARTITIONING_STRATEGY_SCALE_UP}}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Existing": WriteRequest{txId: Spanning enablement} messages:
      | data                  | seq_no |
      | existing-after-enable | 2      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Spanning enablement"
    * TopicService.StreamWrite "Existing": CloseSend
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "After enablement"
    * TopicService.StreamWrite "Fresh": InitRequest{<init>}
    * TopicService.StreamWrite "Fresh": WriteRequest{txId: After enablement} messages:
      | data               | seq_no |
      | fresh-after-enable | 3      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "After enablement"
    * TopicService.StreamWrite "Fresh": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: enable-reader, partition_ids: [0, 1]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

    Examples:
      | mode                      | init                                                                 |
      | partition ID only         | partition_id: 0                                                      |
      | producer and partition ID | producer_id: enable-producer, partition_id: 0, get_last_seq_no: true |
      | producer-selected routing | producer_id: enable-producer, get_last_seq_no: true                  |
