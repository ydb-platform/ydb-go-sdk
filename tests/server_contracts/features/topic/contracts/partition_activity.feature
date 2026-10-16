Feature: DescribeTopic reports current partition activity
  The Active field is the server-authoritative indication that a partition can receive writes.

  Scenario: Activity reflects the initial topology and a completed split
    * an empty topic with 1 partitions with paused auto partitioning
    Then contract: DescribeTopic eventually reports exactly this partition activity:
      | partition_id | active |
      | 0            | true   |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    Then contract: AlterTopic completed successfully
    Then contract: DescribeTopic eventually reports exactly this partition activity:
      | partition_id | active |
      | 0            | false  |
      | 1            | true   |
      | 2            | true   |
