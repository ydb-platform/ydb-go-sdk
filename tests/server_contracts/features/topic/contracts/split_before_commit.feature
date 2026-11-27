Feature: Accepted transactional writes may fail at Commit after split
  Actual parent/child topology establishes the split; no issue code is treated as a universal split classifier.

  Scenario: Reject the old attempt and replay all payloads through active children
    * an empty topic with 1 partitions with paused auto partitioning with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Old"
    * TopicService.StreamWrite "Parent": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "Parent": WriteRequest{txId: Old} messages:
      | data         | seq_no |
      | before-split | 1      |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    Then contract: partition 0 is inactive with active children [1, 2]
    * TopicService.StreamWrite "Parent": WriteRequest{txId: Old} messages:
      | data        | seq_no |
      | after-split | 2      |
    Then contract: StreamWrite "Parent" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
      | 2      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Old"
    Then contract: Commit of "Old" returned ABORTED
    * TopicService.StreamWrite "Parent": CloseSend
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "New"
    * TopicService.StreamWrite "Left": InitRequest{partition_id: 1}
    * TopicService.StreamWrite "Right": InitRequest{partition_id: 2}
    * TopicService.StreamWrite "Left": WriteRequest{txId: New} messages:
      | data         | seq_no |
      | before-split | 1      |
    * TopicService.StreamWrite "Right": WriteRequest{txId: New} messages:
      | data        | seq_no |
      | after-split | 1      |
    Then contract: StreamWrite "Left" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
    Then contract: StreamWrite "Right" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "New"
    Then contract: Commit of "New" returned SUCCESS
    * TopicService.StreamRead: InitRequest{consumer: contract-reader, partition_ids: [1, 2]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | partition_id | seq_no | data         |
      | 1            | 1      | before-split |
      | 2            | 1      | after-split  |
    * TopicService.StreamWrite "Left": CloseSend
    * TopicService.StreamWrite "Right": CloseSend
