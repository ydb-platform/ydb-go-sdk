Feature: Server handling of ProducerID across Query transactions
  These scenarios observe the Topic and Query gRPC responses produced by YDB.

  Background:
    * an empty topic

  # Observed on YDB main.7f40cb4: the first session commits seq_no=1; the replacement
  # session reports LastSeqNo=1, commits seq_no=2, and end_offset becomes 2.
  Scenario: Observe sequential committed transactions with one ProducerID
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * TopicService.StreamWrite: InitRequest{producer_id: research-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
      | data                      | seq_no |
      | first-transaction-message | 1      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.StreamWrite: CloseSend
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{producer_id: research-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction B} messages:
      | data                       | seq_no |
      | second-transaction-message | 2      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction B"
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true

  # Observed on YDB main.7f40cb4: after the first ACK is withheld and the stream is
  # interrupted, the replacement reports LastSeqNo=0 and marks the resent seq_no=1
  # as skipped; commit still creates exactly one topic message.
  Scenario: Reconnect and resend a message with its original sequence number
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * TopicService.StreamWrite: InitRequest{producer_id: reconnect-producer, get_last_seq_no: true}
    * research runner: withhold the next TopicService.StreamWrite WriteResponse
    * TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
      | data                | seq_no |
      | reconnected-message | 1      |
    * TopicService.StreamWrite: CloseSend before consuming the recorded WriteResponse
    * TopicService.StreamWrite: InitRequest{producer_id: reconnect-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
      | data                | seq_no |
      | reconnected-message | 1      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction A"
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true

  # Observed on YDB main.7f40cb4: after seq_no=7 is written and the stream closes,
  # a replacement stream for the same producer reports LastSeqNo=7.
  Scenario: Observe LastSeqNo in a replacement producer session
    * TopicService.StreamWrite: InitRequest{producer_id: replacement-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest messages:
      | data              | seq_no |
      | persisted-message | 7      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: replacement-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true

  # Observed on YDB main.7f40cb4: while Transaction A remains open, Transaction B
  # commits seq_no=1. A new stream for the same producer then reports LastSeqNo=1.
  # InitRequest has no TransactionIdentity, so this lookup is not associated with A.
  Scenario: Observe LastSeqNo after a newer transaction commits while an older transaction remains open
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{producer_id: late-session-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction B} messages:
      | data                  | seq_no |
      | transaction-b-message | 1      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction B"
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: late-session-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: CloseSend
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "Transaction A"

  # Observed on YDB main.7f40cb4: while Transaction A remains open, Transaction B
  # stages seq_no=1 but does not commit. A new stream for the same producer reports
  # LastSeqNo=0, so an uncommitted transactional write does not advance this value.
  Scenario: Observe LastSeqNo while a newer transaction remains uncommitted
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{producer_id: uncommitted-session-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction B} messages:
      | data                  | seq_no |
      | transaction-b-message | 1      |
    * TopicService.StreamWrite: CloseSend
    * TopicService.StreamWrite: InitRequest{producer_id: uncommitted-session-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: CloseSend
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "Transaction B"
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "Transaction A"

  # Observed on YDB main.7f40cb4: rollback does not advance the producer sequence;
  # the next session reports LastSeqNo=0, reuses seq_no=1, and commits one message.
  Scenario: Rollback followed by a write from the same producer
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * TopicService.StreamWrite: InitRequest{producer_id: rollback-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
      | data                | seq_no |
      | rolled-back-message | 1      |
    * QueryService.RollbackTransaction: RollbackTransactionRequest for "Transaction A"
    * TopicService.StreamWrite: CloseSend
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{producer_id: rollback-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction B} messages:
      | data              | seq_no |
      | committed-message | 1      |
    * QueryService.CommitTransaction: CommitTransactionRequest for "Transaction B"
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true

  # Observed on YDB main.7f40cb4: both writes are staged. In the latest concurrent
  # commit run A (seq_no=1) committed before B (seq_no=2); both succeeded, end_offset=2.
  # In an earlier main.0efc910 run B won the race and A aborted with #2011 MinSeqNo violation,
  # leaving end_offset=1. The result depends on the server's actual commit order.
  Scenario: Concurrent transactions write through one producer session
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction A"
    * QueryService.BeginTransaction: BeginTransactionRequest; alias returned tx_id as "Transaction B"
    * TopicService.StreamWrite: InitRequest{producer_id: concurrent-producer, get_last_seq_no: true}
    * TopicService.StreamWrite: WriteRequest{txId: Transaction A} messages:
      | data                      | seq_no |
      | first-transaction-message | 1      |
    * TopicService.StreamWrite: WriteRequest{txId: Transaction B} messages:
      | data                       | seq_no |
      | second-transaction-message | 2      |
    * QueryService.CommitTransaction concurrently: CommitTransactionRequest for "Transaction A" and "Transaction B"
    * TopicService.StreamWrite: CloseSend
    * TopicService.DescribeTopic: DescribeTopicRequest with include_stats=true
