# Known Issues

This document lists known issues that affect ydb-go-sdk tests or behavior, including upstream (YDB server) bugs and workarounds applied in this repository.

## Integration tests

### YDB server crash on 10MB topic messages (nightly)

- **Upstream issue:** [ydb-platform/ydb#19449](https://github.com/ydb-platform/ydb/issues/19449)
- **Affected test:** `TestSendMessagesLargerThenGRPCLimit` in `tests/integration/topic_read_writer_test.go`
- **Symptom:** YDB server hits a VERIFY failure in `CreateFormedBlob()` when handling topic messages of size ~10MB on **nightly** builds.
- **Workaround:** The test is skipped when `YDB_VERSION=nightly`. The test runs on YDB 25.0+ (non-nightly) where the bug is not present.
- **Action:** When the upstream issue is fixed and the fix is available in a stable YDB release, the skip in the test can be removed.
