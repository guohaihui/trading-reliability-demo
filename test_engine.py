import tempfile
import unittest
from pathlib import Path
from engine import Blocked, Engine, Order, Quote, SimulatedBroker


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "orders.db"
        self.broker = SimulatedBroker()
        self.engine = Engine(self.path, self.broker, lambda: 1000.0)
        self.quote = Quote("DEMO", 100.0, 999.0)

    def tearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    def submit(self, key="order-1", qty=4, quote=None):
        return self.engine.execute(key, "DEMO", qty, quote or self.quote)

    def test_expired_session_refreshes_before_submit(self):
        self.broker.ensure_session()
        self.broker.session_valid = False
        self.assertEqual(self.submit()["status"], "ACCEPTED")
        self.assertEqual(self.broker.login_count, 2)

    def test_auth_failure_blocks_submission(self):
        self.broker.authentication_available = False
        with self.assertRaises(Blocked):
            self.submit()
        self.assertEqual(self.broker.submit_count, 0)
        self.assertIsNone(self.engine.row("order-1"))

    def test_stale_future_nan_quotes_block_submission(self):
        for quote in [Quote("DEMO", 100, 900), Quote("DEMO", 100, 1001),
                      Quote("DEMO", 100, float("nan")), Quote("DEMO", float("nan"), 999),
                      Quote("OTHER", 100, 999)]:
            with self.subTest(quote=quote), self.assertRaises(Blocked):
                self.submit(quote=quote)
        self.assertEqual(self.broker.submit_count, 0)

    def test_quantity_limit_blocks_submission(self):
        for qty in [0, -1, 11, 1.5, True]:
            with self.subTest(qty=qty), self.assertRaises(Blocked):
                self.submit(qty=qty)
        self.assertEqual(self.broker.submit_count, 0)

    def test_duplicate_signal_does_not_duplicate_order(self):
        self.submit()
        self.submit()
        self.assertEqual(self.broker.submit_count, 1)

    def test_client_id_cannot_be_reused_for_changed_intent(self):
        self.submit()
        with self.assertRaises(Blocked):
            self.submit(qty=5)
        self.assertEqual(self.broker.submit_count, 1)

    def test_lost_ack_reconciles_after_engine_restart(self):
        self.broker.timeout_after_accept = True
        self.assertEqual(self.submit()["status"], "UNKNOWN")
        self.engine.close()
        self.engine = Engine(self.path, self.broker, lambda: 1000.0)
        self.assertEqual(self.submit()["status"], "ACCEPTED")
        self.assertEqual(self.broker.submit_count, 1)

    def test_unresolved_outcome_blocks_replay_and_new_order(self):
        self.broker.expire_on_submit = True
        self.assertEqual(self.submit()["status"], "UNKNOWN")
        self.broker.expire_on_submit = False
        with self.assertRaises(Blocked):
            self.submit()
        with self.assertRaises(Blocked):
            self.submit(key="order-2")
        self.assertEqual(self.broker.submit_count, 0)

    def test_rejection_is_preserved_without_retry(self):
        self.broker.reject_next = True
        self.assertEqual(self.submit()["status"], "REJECTED")
        self.assertEqual(self.submit()["status"], "REJECTED")
        self.assertEqual(self.broker.submit_count, 1)

    def test_partial_fills_are_cumulative_not_double_counted(self):
        self.submit()
        self.broker.fill("order-1", 2)
        self.assertEqual(self.engine.reconcile("order-1")["filled"], 2)
        self.assertEqual(self.engine.reconcile("order-1")["filled"], 2)
        self.broker.fill("order-1", 4)
        self.assertEqual(self.engine.check_position("DEMO"), 4)

    def test_fill_regression_and_overfill_are_blocked(self):
        self.submit()
        self.broker.fill("order-1", 2)
        self.engine.reconcile("order-1")
        for filled in [1, 5]:
            self.broker.fill("order-1", filled)
            with self.subTest(filled=filled), self.assertRaises(Blocked):
                self.engine.reconcile("order-1")

    def test_terminal_state_cannot_regress(self):
        self.submit()
        self.broker.fill("order-1", 4)
        self.engine.reconcile("order-1")
        self.broker.orders["order-1"] = Order("order-1", "DEMO", 4, 2, "PARTIAL")
        with self.assertRaises(Blocked):
            self.engine.reconcile("order-1")

    def test_position_mismatch_blocks_next_order(self):
        self.submit()
        self.broker.fill("order-1", 4)
        self.broker.extra_position = 1
        with self.assertRaises(Blocked):
            self.submit(key="order-2")
        self.assertEqual(self.broker.submit_count, 1)

    def test_reconciliation_refreshes_expired_session(self):
        self.submit()
        self.broker.session_valid = False
        self.engine.reconcile("order-1")
        self.assertEqual(self.broker.login_count, 2)


if __name__ == "__main__":
    unittest.main()
