import contextlib
from datetime import datetime, timezone
from io import StringIO
import unittest

from monitor import Monitor, completed_round


class MonitorTests(unittest.TestCase):
    def test_groups_zero_signal_and_duplicate_round(self):
        monitor = Monitor(5)
        output = StringIO()
        with contextlib.redirect_stdout(output):
            for round_id, number in enumerate([13, 29, 19, 25, 30], start=1):
                monitor.accept(round_id, number, None)
            monitor.accept(5, 30, None)
            monitor.accept(6, 0, None)
        lines = output.getvalue().splitlines()
        self.assertEqual(sum("round=" in line for line in lines), 6)
        self.assertEqual(sum("SIGNAL: G1, misses=5" in line for line in lines), 1)
        self.assertEqual(monitor.misses["G1"], 6)
        self.assertEqual(monitor.misses["G2"], 3)
        self.assertEqual(monitor.misses["G3"], 1)
        self.assertEqual(monitor.misses["G4"], 3)
        self.assertEqual(monitor.misses["G5"], 1)

    def test_gap_suspends_counter_until_hit(self):
        monitor = Monitor(2)
        with contextlib.redirect_stdout(StringIO()):
            monitor.accept(1, 1, None)
            monitor.accept(4, 0, None)
            self.assertTrue(all(value is None for value in monitor.misses.values()))
            monitor.accept(5, 13, None)
        self.assertEqual(monitor.misses["G2"], 0)
        self.assertEqual(monitor.misses["G4"], 0)
        self.assertIsNone(monitor.misses["G1"])

    def test_only_completed_live_state_is_parsed(self):
        state = {"game_id": 42, "rand_stage": "start_expected", "rand_values": [0],
                 "dttm_end": "2026-10-04T16:00:00Z"}
        event = {"type": "state", "state": {"status": "OK", "action": "start_expected", "state": state}}
        self.assertEqual(completed_round(event), (42, 0, datetime(2026, 10, 4, 16, tzinfo=timezone.utc)))
        event["state"]["action"] = "rng_value"
        self.assertIsNone(completed_round(event))


if __name__ == "__main__":
    unittest.main()
