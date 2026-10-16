Feature: Reusing a sequence number in one producerless write session
  This research scenario sends two consecutive WriteRequests through one
  StreamWrite session without a producer ID. Both messages use sequence number 1.

  # Observed on YDB main.7f40cb4: both messages are acknowledged as written;
  # seq_no does not deduplicate within a producerless session, and end_offset becomes 2.
  Scenario: Two consecutive messages use sequence number 1
    * an empty topic
    * TopicService.StreamWrite: InitRequest{get_last_seq_no: false}
    * TopicService.StreamWrite: WriteRequest messages:
      | data           | seq_no |
      | first-message  | 1      |
    * TopicService.StreamWrite: WriteRequest messages:
      | data           | seq_no |
      | second-message | 1      |
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
