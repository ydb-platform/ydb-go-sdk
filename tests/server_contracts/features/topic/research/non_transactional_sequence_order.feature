Feature: Out-of-order sequence numbers in non-transactional writes
  Compare seq_no=2 followed by seq_no=1 in separate requests and in one batch.
  Use a named producer or a producerless session and read the stored payloads
  and offsets. These scenarios do not use Query transactions.

  Background:
    * an empty topic with consumer "sequence-order-reader" for observation

  # Observed on YDB main.7f40cb4 (image tag: trunk): seq_no=2 received SUCCESS/written
  # before seq_no=1 was sent. The subsequent seq_no=1 received SUCCESS/skipped,
  # despite being a new payload and a sequence number never previously sent.
  # DescribeTopic reported end_offset=1. StreamRead returned only "sent-first-seq-2"
  # at offset 0 with seq_no=2. The server neither waited for the missing lower number
  # nor inserted it later: producer deduplication skipped it after accepting 2.
  # Thus seq_no is not a sorting key, but sending a lower number can discard a new
  # message; deduplication here is not limited to exact repeats of previously sent IDs.
  Scenario: Send sequence number 2 then 1 through one producer session
    * TopicService.StreamWrite: InitRequest{producer_id: sequence-order-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest messages:
      | data              | seq_no |
      | sent-first-seq-2  | 2      |
    * TopicService.StreamWrite: WriteRequest messages:
      | data              | seq_no |
      | sent-second-seq-1 | 1      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: sequence-order-reader, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

  # Observed on YDB main.7f40cb4 (trunk): the [2,1] batch is rejected atomically
  # with BAD_REQUEST (#500003, sequence numbers are unsorted), followed by EOF.
  # Sending seq_no=1 again on the terminated stream returns EOF. The topic remains
  # empty (end_offset=0), and StreamRead returns no payloads within the observation window.
  Scenario: Send sequence numbers 2 and 1 in one non-transactional producer batch
    * TopicService.StreamWrite: InitRequest{producer_id: sequence-order-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest messages:
      | data              | seq_no |
      | sent-first-seq-2   | 2      |
      | sent-second-seq-1  | 1      |
    * TopicService.StreamWrite: WriteRequest messages:
      | data              | seq_no |
      | sent-second-seq-1  | 1      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: sequence-order-reader, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

  # Observed on YDB main.7f40cb4 (image tag: trunk): both seq_no=2 and the subsequent
  # seq_no=1 received SUCCESS/written. DescribeTopic reported end_offset=2.
  # StreamRead returned "sent-first-seq-2" at offset 0 with seq_no=2, followed by
  # "sent-second-seq-1" at offset 1 with seq_no=1. Without a producer ID, these two
  # separate writes retained their send order and were not sorted by sequence number.
  # These observations concern separate WriteRequests, not an unsorted message batch.
  Scenario: Send sequence number 2 then 1 through one producerless session
    * TopicService.StreamWrite: InitRequest{partition_id: 0}
    * TopicService.StreamWrite: WriteRequest messages:
      | data             | seq_no |
      | sent-first-seq-2 | 2      |
    * TopicService.StreamWrite: WriteRequest messages:
      | data              | seq_no |
      | sent-second-seq-1 | 1      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: sequence-order-reader, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
