# Security analysis

The CodeQL workflow runs the `security-extended` query suite for the SDK language and GitHub Actions on pull requests, pushes, merge queues and a weekly schedule. It can also be started manually.

The SDK scan uses Go autobuild and includes the module sources and examples. Build extraction determines the scan scope; no ineffective path filters are applied.

Results are published to **Security and quality → Code scanning**, with a separate category for each language. Review initial findings and record a reason for every dismissed alert.

After merging, require both **CodeQL (go)** and **CodeQL (actions)** job checks for supported branches. Also enable **Require code scanning results → CodeQL → Security alerts: High or higher** in the branch ruleset. Successful workflow execution alone does not enforce this threshold. Track the latest analysis commit/date, extraction errors and open findings by security severity.
