import unittest

from benchstat_preview import render


class BenchstatPreviewTests(unittest.TestCase):
    def test_preserves_report_content_with_ascii_symbols(self):
        report = "name │ master │ pr\ntime 1.2µs ± 3% ¹\nbytes 4²\n"

        self.assertEqual(
            render(report),
            "name | master | pr\ntime 1.2us +/- 3% [1]\nbytes 4[2]\n",
        )

    def test_rejects_unmapped_characters(self):
        with self.assertRaisesRegex(ValueError, "unmapped non-ASCII"):
            render("unexpected →")


if __name__ == "__main__":
    unittest.main()
