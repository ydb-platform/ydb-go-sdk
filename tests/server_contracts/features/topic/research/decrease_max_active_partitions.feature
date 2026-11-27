Feature: Partition merge after decreasing max_active_partitions
  Observe whether lowering the partition limit merges active partitions with PAUSED auto partitioning.
  Keep both existing streams open and use a fixed idle observation window without a write loop.
  Separately record whether SCALE_UP_AND_DOWN is accepted while lowering the limits.

  # Observed on YDB main.db11cbd (trunk, 2026-09-14).
  # AlterTopic returned BAD_REQUEST: max=1 is below min=2. Settings and both active partitions stayed unchanged.
  Scenario: Lower max_active_partitions below the current minimum
    * an empty topic with 2 partitions with paused auto partitioning
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_max_active_partitions: 1}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true

  # Observed on YDB main.db11cbd (trunk, 2026-09-14), with the race detector.
  # The initial split produced inactive partition 0 and active children 1/2.
  # Lowering min to 1 and then max to 1 both returned SUCCESS; DescribeTopic reported min=max=1.
  # Immediately, after 1s, and after 10s, both children remained active without a merged descendant.
  # Both original child streams accepted seq_no=2 with SUCCESS/written after the idle window.
  Scenario: Lower max_active_partitions after lowering the minimum and observe both existing writers
    * an empty topic with 1 partitions with paused auto partitioning
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Left": InitRequest{partition_id: 1}
    * TopicService.StreamWrite "Right": InitRequest{partition_id: 2}
    * TopicService.StreamWrite "Left": WriteRequest messages:
      | data        | seq_no |
      | left-before | 1      |
    * TopicService.StreamWrite "Right": WriteRequest messages:
      | data         | seq_no |
      | right-before | 1      |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 1}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_max_active_partitions: 1}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Left": observe server responses for 1000ms
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Left": observe server responses for 9000ms
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamWrite "Left": WriteRequest messages:
      | data       | seq_no |
      | left-after | 2      |
    * TopicService.StreamWrite "Right": WriteRequest messages:
      | data        | seq_no |
      | right-after | 2      |
    * TopicService.StreamWrite "Left": CloseSend
    * TopicService.StreamWrite "Right": CloseSend

  # Observed on YDB main.db11cbd (trunk, 2026-09-14), with the race detector.
  # AlterTopic accepted SCALE_UP_AND_DOWN together with min=max=1 and returned SUCCESS.
  # The immediate DescribeTopic reported the new strategy and limits, but both partitions stayed active.
  # This scenario checks configuration acceptance; it does not wait for load-based automatic scaling.
  Scenario: Request SCALE_UP_AND_DOWN while lowering the partition limits
    * an empty topic with 2 partitions with paused auto partitioning
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 1, set_max_active_partitions: 1, alter_auto_partitioning_settings: {set_strategy: AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN}}}
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
