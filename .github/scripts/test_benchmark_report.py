"""Behavioral checks for the pull request benchmark report."""

import unittest

from benchmark_report import render_report


SDK_CSV = """\
goos: linux
pkg: github.com/ydb-platform/ydb-go-sdk/v3/internal/pool
,master,,pr,,,
,sec/op,CI,sec/op,CI,vs base,P
Slow-2,1e-06,2%,1.2e-06,3%,+20.00%,p=0.001 n=10
Fast-2,2e-06,2%,1.8e-06,2%,-10.00%,p=0.002 n=10
NoChange-2,3e-06,2%,3.3e-06,2%,~,p=0.300 n=10
geomean,1.817e-06,,1.920e-06,,+5.67%,

,master,,pr,,,
,B/op,CI,B/op,CI,vs base,P
Slow-2,100,0%,120,0%,+20.00%,p=0.001 n=10
Fast-2,100,0%,90,0%,-10.00%,p=0.002 n=10
NoChange-2,100,0%,100,0%,~,p=1.000 n=10
"""

INTEGRATION_CSV = """\
goos: linux
pkg: github.com/ydb-platform/ydb-go-sdk/v3/tests/integration
,master,,pr,,,
,sec/op,CI,sec/op,CI,vs base,P
WriterMany/p64-2,0.01,2%,0.005,2%,-50.00%,p=0.000 n=10
WriterMany/p128-2,0.02,2%,0.01,2%,-50.00%,p=0.000 n=10
WriterSingle/p64-2,0.01,2%,0.013,2%,+30.00%,p=0.001 n=10
"""


class BenchmarkReportTests(unittest.TestCase):
    def test_combined_report_explains_status_and_keeps_details_under_packages(self):
        artifact_url = "https://github.com/ydb-platform/ydb-go-sdk/actions/runs/42/artifacts/7"
        report = render_report(
            SDK_CSV,
            INTEGRATION_CSV,
            artifact_url=artifact_url,
            master_sha="a" * 40,
            head_sha="b" * 40,
        )

        self.assertIn("🔴 Performance regressions reported", report)
        self.assertIn("### SDK", report)
        self.assertIn("### Integration", report)
        self.assertIn("internal/pool", report)
        self.assertIn("tests/integration", report)
        self.assertIn("WriterMany", report)
        self.assertIn("slower (+20.00% sec/op)", report)
        self.assertIn("allocates more memory (+20.00% B/op)", report)
        self.assertIn("faster (-50.00% sec/op)", report)
        self.assertIn("+5.9%", report)  # Includes the row marked ~ in the time trend.
        self.assertIn("-31.2%", report)
        self.assertIn("Master median", report)
        self.assertIn("PR median", report)
        self.assertIn("p &lt; 0.001", report)
        self.assertNotIn("NoChange-2", report)
        self.assertEqual(5, report.count("<details>"))
        self.assertTrue(report.rstrip().endswith(f"[Download full benchstat and raw results]({artifact_url})"))

    def test_missing_comparison_is_not_reported_as_an_improvement(self):
        sdk_csv = """\
pkg: github.com/ydb-platform/ydb-go-sdk/v3
,master,,pr,,,
,sec/op,CI,sec/op,CI,vs base,P
Removed-2,1e-06,2%
Added-2,,,1e-06,2%
"""
        report = render_report(
            sdk_csv,
            INTEGRATION_CSV.replace("-50.00%", "~").replace("+30.00%", "~"),
            artifact_url="https://github.com/example/repo/actions/runs/1/artifacts/2",
            master_sha="a" * 40,
            head_sha="b" * 40,
        )

        self.assertIn("🟡 Comparison incomplete", report)
        self.assertIn("Removed-2", report)
        self.assertIn("Added-2", report)
        self.assertIn("not comparable", report)

    def test_benchmark_names_are_escaped_before_rendering_html(self):
        sdk_csv = SDK_CSV.replace("Slow-2", "Slow<script>-2")
        report = render_report(
            sdk_csv,
            INTEGRATION_CSV,
            artifact_url="https://github.com/example/repo/actions/runs/1/artifacts/2",
            master_sha="a" * 40,
            head_sha="b" * 40,
        )

        self.assertIn("Slow&lt;script&gt;-2", report)
        self.assertNotIn("<script>", report)

    def test_invalid_csv_fails_instead_of_showing_a_green_report(self):
        with self.assertRaises(ValueError):
            render_report(
                "not benchstat output",
                INTEGRATION_CSV,
                artifact_url="https://github.com/example/repo/actions/runs/1/artifacts/2",
                master_sha="a" * 40,
                head_sha="b" * 40,
            )

    def test_mixed_result_shows_both_directions_and_requires_review(self):
        sdk_csv = """\
pkg: github.com/ydb-platform/ydb-go-sdk/v3/internal/pool
,master,,pr,,,
,sec/op,CI,sec/op,CI,vs base,P
Mixed-2,1e-06,1%,8e-07,1%,-20.00%,p=0.001 n=10

,master,,pr,,,
,B/op,CI,B/op,CI,vs base,P
Mixed-2,100,1%,120,1%,+20.00%,p=0.001 n=10
"""
        report = render_report(
            sdk_csv,
            INTEGRATION_CSV.replace("-50.00%", "~").replace("+30.00%", "~"),
            artifact_url="https://github.com/example/repo/actions/runs/1/artifacts/2",
            master_sha="a" * 40,
            head_sha="b" * 40,
        )

        self.assertIn("🔴 Performance regressions reported", report)
        self.assertIn("🔴🟢 <code>Mixed-2</code>", report)
        self.assertIn("faster (-20.00% sec/op)", report)
        self.assertIn("allocates more memory (+20.00% B/op)", report)
        self.assertIn("1 benchmarks with regressions · 0 with improvements only", report)

    def test_unclassified_metric_keeps_comparison_incomplete(self):
        sdk_csv = """\
pkg: github.com/ydb-platform/ydb-go-sdk/v3
,master,,pr,,,
,widgets/op,CI,widgets/op,CI,vs base,P
Custom-2,100,1%,110,1%,+10.00%,p=0.001 n=10
"""
        report = render_report(
            sdk_csv,
            INTEGRATION_CSV.replace("-50.00%", "~").replace("+30.00%", "~"),
            artifact_url="https://github.com/example/repo/actions/runs/1/artifacts/2",
            master_sha="a" * 40,
            head_sha="b" * 40,
        )

        self.assertIn("🟡 Comparison incomplete", report)
        self.assertIn("direction unknown", report)
        self.assertNotIn("🔴 Performance regressions reported", report)


if __name__ == "__main__":
    unittest.main()
