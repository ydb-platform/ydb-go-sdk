# YDB-Go-SDK Versioning Policy

By adhering to these guidelines and exceptions, we aim to provide a stable and reliable development experience for our users while still allowing for innovation and improvement.

We endeavor to adhere to versioning guidelines as defined by [SemVer2.0.0](https://semver.org/).
Also making the following exceptions to those guidelines:
1) **Experimental**
   - We use the `// Experimental` comment for new features in the `ydb-go-sdk`. 
   - Early adopters of newest feature can report bugs and imperfections in functionality. 
   - For fix this issues we can make broken changes in experimental API. 
   - We reserve the right to remove or modify these experimental features at any time, until the removal of the "Experimental" comment.
2) **Deprecated**
   - We use the `// Deprecated` comment for deprecated features in the `ydb-go-sdk`. 
   - This helps to owr users to soft decline to use the deprecated feature without any impact on their code.
   - Deprecated features will not be removed or changed for a minimum period of **six months**.
3) **Public internals**
   - Some public packages of `ydb-go-sdk` relate to the internals.
   - `ydb-go-sdk` internals can be changed at any time.
   - That's why versioning rules of [SemVer2.0.0](https://semver.org/) does not apply to the next packages:
     - [trace](https://github.com/ydb-platform/ydb-go-sdk/tree/master/trace)
     - [log](https://github.com/ydb-platform/ydb-go-sdk/tree/master/log)
     - [metrics](https://github.com/ydb-platform/ydb-go-sdk/tree/master/metrics)
   - List of stable supported metrics are stored in [metrics/README.md](https://github.com/ydb-platform/ydb-go-sdk/tree/master/metrics) and cover with tests
