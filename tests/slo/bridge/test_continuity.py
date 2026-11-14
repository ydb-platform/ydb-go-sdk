import datetime
import unittest

from continuity import check_continuity


class ContinuityTest(unittest.TestCase):
    def test_transient_errors_do_not_fail_a_recovered_client(self):
        log, rows, events, end = fixture()
        rows.append({"event": "operation", "ts": iso(105), "ok": False})
        details, errors = check_continuity(log, rows, events, end)
        self.assertEqual(errors, [])
        self.assertEqual(len(details["unavailable_intervals"]), 4)

    def test_waiting_for_the_old_pile_to_return_is_not_recovery(self):
        log, rows, events, end = fixture()
        rows = [row for row in rows if not iso(200) <= row["ts"] < iso(220)]
        _, errors = check_continuity(log, rows, events, end)
        self.assertTrue(any("unavailable" in error for error in errors))

    def test_no_progress_after_one_recovery_fails(self):
        log, rows, events, end = fixture()
        rows = [row for row in rows if not iso(350) <= row["ts"] <= iso(410)]
        _, errors = check_continuity(log, rows, events, end)
        self.assertTrue(any("after recovery" in error for error in errors))

    def test_missing_fault_evidence_fails(self):
        log, rows, _, end = fixture()
        _, errors = check_continuity(log, rows, [], end)
        self.assertTrue(any("Missing unavailable-pile" in error for error in errors))


def iso(seconds):
    return (
        datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        + datetime.timedelta(seconds=seconds)
    ).isoformat()


def fixture():
    rows, events, log = [], [], []
    for i in range(5):
        recovered = i * 100 + 50
        log.append(f"[{iso(recovered)}] Recovered: scenario-{i}")
        rows.extend(
            {"event": "operation", "ts": iso(recovered + n), "ok": True}
            for n in (1, 2, 3)
        )
        if i:
            name = f"ydb-database-{1 + i % 2}"
            for action, t in (("stop", i * 100), ("start", i * 100 + 20)):
                events.append(
                    {
                        "Action": action,
                        "time": datetime.datetime.fromisoformat(iso(t)).timestamp(),
                        "Actor": {"Attributes": {"name": name}},
                    }
                )
            rows.extend(
                {"event": "operation", "ts": iso(i * 100 + n), "ok": True}
                for n in (5, 6, 7)
            )
    rows.extend(
        {"event": "operation", "ts": iso(n), "ok": True} for n in (495, 496, 497)
    )
    return "\n".join(log), rows, events, iso(500)


if __name__ == "__main__":
    unittest.main()
