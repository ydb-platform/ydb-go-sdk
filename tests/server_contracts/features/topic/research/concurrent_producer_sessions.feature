Feature: Concurrent StreamWrite sessions with the same ProducerID
  Keep the first stream open while initializing another stream with the same
  producer and partition, then attempt ordinary writes through both streams.

  # Observed on YDB main.7f40cb4 (trunk): A writes seq_no=1. B initializes successfully
  # with last_seq_no=1 and replaces A, which receives SESSION_EXPIRED (#500004) and EOF.
  # A's subsequent seq_no=2 send returns EOF; B writes seq_no=3 successfully.
  # Partition 0 ends at offset 2; partition 1 stays empty. These are ordinary writes,
  # not Query transactions. Opening another stream does not provide concurrent writers
  # for the same producer and partition; the new session takes ownership.
  Scenario: Write through A and B after opening B with the same producer
    * an empty topic with 2 partitions
    * TopicService.StreamWrite "A": InitRequest{producer_id: concurrent-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite "A": WriteRequest messages:
      | data                    | seq_no |
      | a-before-opening-b      | 1      |
    * TopicService.StreamWrite "B": InitRequest{producer_id: concurrent-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite "A": WriteRequest messages:
      | data                    | seq_no |
      | a-after-opening-b       | 2      |
    * TopicService.StreamWrite "B": WriteRequest messages:
      | data                    | seq_no |
      | b-after-opening-b       | 3      |
    * TopicService.StreamWrite "A": CloseSend
    * TopicService.StreamWrite "B": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
