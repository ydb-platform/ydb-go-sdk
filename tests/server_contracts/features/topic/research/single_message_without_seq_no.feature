Feature: Writing one message without an explicit sequence number
  This research scenario isolates sequence-number handling from Query transactions
  and concurrent writes. It sends one ordinary message through one StreamWrite
  session without a producer ID and without an explicit sequence number.

  # Observed on YDB main.7f40cb4: the omitted proto3 seq_no is decoded as 0;
  # StreamWrite ends with BAD_REQUEST, no WriteResponse, and end_offset remains 0.
  Scenario: One non-transactional message without a sequence number
    * an empty topic
    * TopicService.StreamWrite: InitRequest{get_last_seq_no: false}
    * TopicService.StreamWrite: WriteRequest messages:
      | data                                |
      | one-message-without-sequence-number |
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
