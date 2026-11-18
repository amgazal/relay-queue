import json
import unittest
from pathlib import Path

from verify import verify


class EvidenceChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        report = json.loads(Path(__file__).with_name("results.json").read_text())
        cls.burst = next(run for run in report["runs"] if run["mode"] == "api")

    def test_internally_consistent_partial_ledger_is_rejected(self):
        partial = self.burst.copy()
        for field in ("expected", "accepted", "matched", "succeeded", "attempts"):
            partial[field] -= 1
        with self.assertRaisesRegex(ValueError, "submitted workload"):
            verify(partial)

    def test_missing_http_response_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "HTTP counts"):
            verify(self.burst | {"http_requests": self.burst["http_requests"] - 1})
