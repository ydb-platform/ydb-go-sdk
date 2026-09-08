Feature: Lost ACK and complete retry of a multi-partition transaction
  All write sessions specify partition_id without producer_id; sequence numbers remain explicit.
  The runner records a real WriteResponse, withholds it from the scenario, and cancels its stream.
  Rollback and replay are explicit protocol actions, not SDK retries or a simulated lost commit response.

  # Observed on YDB main.7f40cb4 (trunk, 2026-09-08), without producer_id:
  # P0's ACK is delivered, P1's real ACK is recorded but withheld, and its stream
  # is cancelled. Explicit rollback succeeds; both partitions remain empty.
  # New producerless streams replay both complete batches in Attempt 2. Commit succeeds;
  # each partition ends at offset 2, and StreamRead returns all four payloads once.
  # The absent duplicates follow a known rollback, not deduplication of a committed attempt.
  # The CommitTransaction response is not lost in this experiment.
  Scenario: Rollback after losing one partition ACK and replay both partitions in a new transaction
    * an empty topic with 2 partitions with consumer "research-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Attempt 1"
    * TopicService.StreamWrite "P0": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "P1": InitRequest{partition_id: 1}
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
    * TopicService.StreamWrite "P0": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "P1": InitRequest{partition_id: 1}
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
