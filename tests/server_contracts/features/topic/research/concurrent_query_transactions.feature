Feature: Sharing one StreamWrite session between Query transactions
  These research scenarios compare writes from overlapping Query transactions
  through one or two StreamWrite sessions, both without and with a ProducerID.

  # Observed on YDB main.7f40cb4: both messages are acknowledged as written_in_tx,
  # both concurrent commits succeed, and end_offset becomes 2.
  Scenario: Two concurrent Query transactions write through one StreamWrite session
    * an empty topic
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{get_last_seq_no: false}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
      | data                  | seq_no |
      | transaction-a-message | 1      |
    * TopicService.StreamWrite: WriteRequest{txId: Transaction B} messages:
      | data                  | seq_no |
      | transaction-b-message | 2      |
    * QueryService.CommitTransaction concurrently: CommitTransactionRequest for "Transaction A" and "Transaction B"
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true

  # Observed on YDB main.7f40cb4: both writes are staged, but committing B (seq_no=2)
  # before A (seq_no=1) makes B visible and aborts A with #2011 MinSeqNo violation.
  # StreamRead returns only B at offset 0, and end_offset becomes 1.
  Scenario: Observe message order with one ProducerID across two StreamWrite sessions when the second transaction commits first
    * an empty topic with consumer "research-order-reader-two-streams" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{producer_id: ordered-transactions-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
      | data                  | seq_no |
      | transaction-a-message | 1      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: ordered-transactions-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction B} messages:
      | data                  | seq_no |
      | transaction-b-message | 2      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction B"
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: research-order-reader-two-streams, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}

  # Observed on YDB main.7f40cb4: committing B (seq_no=1) before A (seq_no=2)
  # succeeds for both transactions. StreamRead returns B at offset 0 and A at offset 1;
  # topic order follows commit/sequence-number order rather than WriteRequest order.
  Scenario: Observe message order with one ProducerID across two StreamWrite sessions when sequence numbers follow commit order
    * an empty topic with consumer "research-order-reader-swapped-sequence" for observation
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{producer_id: ordered-transactions-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
      | data                  | seq_no |
      | transaction-a-message | 2      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: ordered-transactions-producer, partition_id: 0, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction B} messages:
      | data                  | seq_no |
      | transaction-b-message | 1      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction B"
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
    * TopicService.StreamRead: InitRequest{consumer: research-order-reader-swapped-sequence, partition_ids: [0]}
    * TopicService.StreamRead: StartPartitionSessionResponse{read_offset: 0}
    * TopicService.StreamRead: ReadRequest{bytes_size: 1048576}
