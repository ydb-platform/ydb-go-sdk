import pathlib
import subprocess
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("sync-slo-workload-from-current.sh")


class SyncSLOWorkloadTest(unittest.TestCase):
    def test_older_sdk_uses_standard_decoder_adapter(self):
        self.check_adapter("package ydb\n", "baseline adapter\n")

    def test_sdk_with_wire_option_keeps_current_adapter(self):
        self.check_adapter(
            "package ydb\nfunc WithQueryProtoWireDecoding(enabled bool) Option {}\n",
            "current adapter\n",
        )

    def check_adapter(self, sdk_options, expected_adapter):
        with tempfile.TemporaryDirectory(prefix="slo-sync-") as tmp:
            root = pathlib.Path(tmp)
            source = root / "current"
            target = root / "baseline"
            source_helper = source / "tests/slo/internal/querywire"
            source_workload = source / "tests/slo/native/query"
            source_helper.mkdir(parents=True)
            source_workload.mkdir(parents=True)
            (target / "tests/slo/native/query").mkdir(parents=True)
            (target / "options.go").write_text(sdk_options)
            (source_helper / "options.go").write_text("current adapter\n")
            (source_helper / "options.go.stub").write_text("baseline adapter\n")
            (source_helper / "flags.go").write_text("shared flag registration\n")
            (source_workload / "storage.go").write_text("current workload\n")
            (source / "tests/slo/go.mod").write_text("module slo\n")
            (source / "tests/slo/go.sum").write_text("")

            result = subprocess.run(
                [
                    "bash", str(SCRIPT), "--target", str(target),
                    "--source", str(source), "--workload-path", "native/query",
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            target_helper = target / "tests/slo/internal/querywire"
            self.assertEqual((target_helper / "options.go").read_text(), expected_adapter)
            self.assertEqual((target_helper / "flags.go").read_text(), "shared flag registration\n")
            self.assertEqual(
                (target / "tests/slo/native/query/storage.go").read_text(),
                "current workload\n",
            )
            self.assertEqual((source_helper / "options.go").read_text(), "current adapter\n")


if __name__ == "__main__":
    unittest.main()
