Feature: Full replay after a lost WriteResponse and confirmed rollback
  The server ACK is recorded and withheld from the caller. Commit ambiguity is outside this contract.

  Scenario: Full replay after rollback delivers every payload once
    * an empty topic with 2 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Attempt 1"
    * TopicService.StreamWrite "OldLeft": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "OldRight": InitRequest{partition_id: 1}
    * TopicService.StreamWrite "OldLeft": WriteRequest{txId: Attempt 1} messages:
      | data | seq_no |
      | left | 1      |
    * client: withhold the next TopicService.StreamWrite "OldRight" WriteResponse
    * TopicService.StreamWrite "OldRight": WriteRequest{txId: Attempt 1} messages:
      | data  | seq_no |
      | right | 1      |
    * TopicService.StreamWrite "OldRight": CloseSend before consuming the recorded WriteResponse
    Then contract: StreamWrite "OldLeft" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
    Then contract: StreamWrite "OldRight" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
    * TopicService.StreamWrite "OldLeft": CloseSend
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "Attempt 1"
    Then contract: Rollback of "Attempt 1" returned SUCCESS
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Attempt 2"
    * TopicService.StreamWrite "Left": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "Right": InitRequest{partition_id: 1}
    * TopicService.StreamWrite "Left": WriteRequest{txId: Attempt 2} messages:
      | data | seq_no |
      | left | 1      |
    * TopicService.StreamWrite "Right": WriteRequest{txId: Attempt 2} messages:
      | data  | seq_no |
      | right | 1      |
    Then contract: StreamWrite "Left" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
    Then contract: StreamWrite "Right" has exactly these ACKs:
      | seq_no | result        |
      | 1      | written_in_tx |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Attempt 2"
    Then contract: Commit of "Attempt 2" returned SUCCESS
    * TopicService.StreamRead: InitRequest{consumer: contract-reader, partition_ids: [0, 1]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
    Then contract: topic contains exactly:
      | partition_id | seq_no | data  |
      | 0            | 1      | left  |
      | 1            | 1      | right |
    * TopicService.StreamWrite "Left": CloseSend
    * TopicService.StreamWrite "Right": CloseSend
