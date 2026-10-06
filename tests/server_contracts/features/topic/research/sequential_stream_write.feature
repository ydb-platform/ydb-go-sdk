Feature: Reusing a sequence number in consecutive producerless write sessions
  This research scenario observes whether sequence numbers carry over between
  StreamWrite sessions when no producer ID identifies the writer.

  # Observed on YDB main.7f40cb4: both sessions accept seq_no=1 as written;
  # producerless sessions do not share sequence-number state, and end_offset becomes 2.
  Scenario: Both consecutive sessions send sequence number 1
    * an empty topic
    * TopicService.StreamWrite: InitRequest{get_last_seq_no: false}
    * TopicService.StreamWrite: WriteRequest messages:
      | data                  | seq_no |
      | first-session-message | 1      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{get_last_seq_no: false}
    * TopicService.StreamWrite: WriteRequest messages:
      | data                   | seq_no |
      | second-session-message | 1      |
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
