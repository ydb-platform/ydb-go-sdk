Feature: Parent write rejection and eventual child discovery after split
  The writer may reject an inactive partition before DescribeTopic exposes its children.

  Scenario: The next ordinary write on an active session for a split parent is rejected
    * an empty topic with 1 partitions with paused auto partitioning
    * TopicService.StreamWrite "Parent": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "Parent": WriteRequest messages:
      | data         | seq_no |
      | before-split | 1      |
    Then contract: StreamWrite "Parent" has exactly these ACKs:
      | seq_no | result  |
      | 1      | written |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    Then contract: AlterTopic completed successfully
    * TopicService.StreamWrite "Parent": WriteRequest messages:
      | data        | seq_no |
      | after-split | 2      |
    Then contract: StreamWrite "Parent" terminated with OVERLOADED
    * TopicService.StreamWrite "Parent": CloseSend

  Scenario: DescribeTopic exposes children after writer rejection even if its first response is stale
    * an empty topic with 1 partitions with paused auto partitioning
    * TopicService.StreamWrite "Parent": InitRequest{partition_id: 0}
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}} while probing StreamWrite "Parent" and sampling DescribeTopic every 1ms
    Then contract: AlterTopic completed successfully
    Then contract: DescribeTopic eventually shows inactive partition 0 with active children [1, 2] after writer rejection
    Then contract: StreamWrite "Parent" terminated with OVERLOADED
    * TopicService.StreamWrite "Parent": CloseSend
