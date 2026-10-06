Feature: Sequence validation applies to each complete WriteRequest
  Invalid batches are rejected without ACKs, with or without a producer.

  Scenario Outline: Reject <case> in <mode> mode without ACKs
    * an empty topic with 1 partitions with consumer "contract-reader" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "A"
    * TopicService.StreamWrite "S": InitRequest{<producer>}
    * TopicService.StreamWrite "S": WriteRequest{txId: A} messages:
      | data   | seq_no   |
      | first  | <first>  |
      | second | <second> |
    Then contract: StreamWrite "S" rejected the write with BAD_REQUEST and no ACK
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "A"
    Then contract: Rollback of "A" returned SUCCESS
    * TopicService.StreamWrite "S": CloseSend

    Examples:
      | mode         | producer            | case       | first | second |
      | producerless |                     | zero       | 0     | 1      |
      | producerless |                     | negative   | -1    | 1      |
      | producerless |                     | repetition | 1     | 1      |
      | producerless |                     | decrease   | 2     | 1      |
      | producer     | producer_id: manual | zero       | 0     | 1      |
      | producer     | producer_id: manual | negative   | -1    | 1      |
      | producer     | producer_id: manual | repetition | 1     | 1      |
      | producer     | producer_id: manual | decrease   | 2     | 1      |
