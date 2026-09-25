Feature: StreamWrite termination and DescribeTopic visibility during a partition split
  Observe the active parent stream and partition visibility after requesting a partition change.
  Record whether the writer is rejected; do not require rejection to finish the observation.

  # Observed on YDB main.db11cbd (2026-09-14), three runs.
  # No unsolicited error was received during the 1000ms idle observation after AlterTopic.
  # The next ordinary write returned OVERLOADED (Write to inactive partition 0), followed by EOF.
  # Observed on YDB 26.1.1.22.1b59297 (2026-09-14).
  # AlterTopic returned SUCCESS; the next ordinary write on partition 0 received a written ACK.
  Scenario: Observe an idle parent session and its next ordinary write after AlterTopic
    * an empty topic with 1 partitions with paused auto partitioning
    * TopicService.StreamWrite "Parent": InitRequest{partition_id: 0}
    * TopicService.StreamWrite "Parent": WriteRequest messages:
      | data         | seq_no |
      | before-split | 1      |
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}}
    * TopicService.StreamWrite "Parent": observe server responses for 1000ms
    * TopicService.StreamWrite "Parent": WriteRequest messages:
      | data        | seq_no |
      | after-split | 2      |
    * TopicService.StreamWrite "Parent": observe server responses for 1000ms
    * TopicService.StreamWrite "Parent": CloseSend

  # Observed on YDB main.db11cbd (2026-09-14), three runs.
  # In one run, OVERLOADED arrived at +15.901ms. DescribeTopic requested at +15.967ms
  # still returned only the active parent at +18.328ms. A request at +18.482ms returned
  # inactive parent 0 and active children 1/2 at +20.720ms; Alter completed at +18.539ms.
  # In two other runs the first DescribeTopic after OVERLOADED already contained children.
  # The lag is relative to writer rejection, not necessarily to synchronous AlterTopic completion.
  # Observed on YDB 26.1.1.22.1b59297 (2026-09-14).
  # AlterTopic returned SUCCESS, but the final description still showed only active partition 0.
  # All four probes received written ACKs; observation finished with writer_rejected=false.
  Scenario: Sample writer responses and partition visibility during AlterTopic
    * an empty topic with 1 partitions with paused auto partitioning
    * TopicService.StreamWrite "Parent": InitRequest{partition_id: 0}
    * TopicService.AlterTopic: AlterTopicRequest{alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}} while probing StreamWrite "Parent" and sampling DescribeTopic every 1ms
    * TopicService.StreamWrite "Parent": observe server responses for 1000ms
    * TopicService.StreamWrite "Parent": CloseSend
