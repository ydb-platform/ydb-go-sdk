Feature: Lost ACK and complete retry of a multi-partition transaction
  The runner records a real WriteResponse, withholds it from the scenario, and cancels its stream.
  Rollback and replay are explicit protocol actions, not SDK retries or a simulated lost commit response.

  # Observed on YDB main.7f40cb4 (trunk): the P0 ACK is delivered, the P1 ACK is
  # recorded but withheld, and that stream is cancelled. Explicit rollback succeeds;
  # both partitions stay empty. Replacement streams with the same per-partition
  # producers report last_seq_no=0. Replaying both complete batches in Attempt 2
  # commits successfully: two payloads per partition, four total, no duplicates.
  # This injects loss of a WriteResponse, not loss of the CommitTransaction response.
  Scenario: Rollback after losing one partition ACK and replay both partitions in a new transaction
    * an empty topic with 2 partitions with consumer "research-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Attempt 1"
    * TopicService.StreamWrite "P0": InitRequest{producer_id: multi-p0, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite "P1": InitRequest{producer_id: multi-p1, partition_id: 1, get_last_seq_no: true}
    * TopicService.StreamWrite "P0": WriteRequest{txId: Attempt 1} messages:
      | data        | seq_no |
      | left-first  | 1      |
      | left-second | 2      |
    * research runner: withhold the next TopicService.StreamWrite "P1" WriteResponse
    * TopicService.StreamWrite "P1": WriteRequest{txId: Attempt 1} messages:
      | data         | seq_no |
      | right-first  | 1      |
      | right-second | 2      |
    * TopicService.StreamWrite "P1": CloseSend before consuming the recorded WriteResponse
    * TopicService.StreamWrite "P0": CloseSend
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "Attempt 1"
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Attempt 2"
    * TopicService.StreamWrite "P0": InitRequest{producer_id: multi-p0, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite "P1": InitRequest{producer_id: multi-p1, partition_id: 1, get_last_seq_no: true}
    * TopicService.StreamWrite "P0": WriteRequest{txId: Attempt 2} messages:
      | data        | seq_no |
      | left-first  | 1      |
      | left-second | 2      |
    * TopicService.StreamWrite "P1": WriteRequest{txId: Attempt 2} messages:
      | data         | seq_no |
      | right-first  | 1      |
      | right-second | 2      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Attempt 2"
    * TopicService.StreamWrite "P0": CloseSend
    * TopicService.StreamWrite "P1": CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: research-reader, partition_ids: [0, 1]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
