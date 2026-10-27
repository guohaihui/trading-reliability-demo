"""Run deterministic scenarios and optionally save a Markdown + JSON evidence report."""
import argparse
import json
import tempfile
from pathlib import Path
from engine import Blocked, Engine, Quote, SimulatedBroker


def scenario(name, action):
    with tempfile.TemporaryDirectory() as directory:
        broker = SimulatedBroker()
        engine = Engine(Path(directory) / "orders.db", broker, lambda: 1000.0)
        try:
            outcome = action(engine, broker)
            return {"scenario": name, "outcome": outcome, "submissions": broker.submit_count,
                    "events": engine.events}
        finally:
            engine.close()


def place(engine, key="demo-1"):
    return engine.execute(key, "DEMO", 4, Quote("DEMO", 100.0, 999.0))


def expired_session(engine, broker):
    broker.ensure_session()
    broker.session_valid = False
    row = place(engine)
    assert row["status"] == "ACCEPTED" and broker.login_count == 2
    return "Expired session refreshed; exactly one order accepted."


def stale_quote(engine, broker):
    try:
        engine.execute("stale-1", "DEMO", 4, Quote("DEMO", 100.0, 900.0))
    except Blocked:
        assert broker.submit_count == 0
        return "100-second-old quote blocked before submission."
    raise AssertionError("Expected stale quote to be blocked")


def lost_ack(engine, broker):
    broker.timeout_after_accept = True
    assert place(engine)["status"] == "UNKNOWN"
    path = engine.db.execute("PRAGMA database_list").fetchone()[2]
    with_restart = Engine(path, broker, lambda: 1000.0)
    try:
        row = place(with_restart)
        engine.events.extend(with_restart.events)
        assert row["status"] == "ACCEPTED" and broker.submit_count == 1
        return "New engine instance recovered persisted intent; no duplicate submission."
    finally:
        with_restart.close()


def partial_fill(engine, broker):
    place(engine)
    broker.fill("demo-1", 2)
    assert engine.reconcile("demo-1")["filled"] == 2
    assert engine.reconcile("demo-1")["filled"] == 2
    broker.fill("demo-1", 4)
    assert engine.check_position("DEMO") == 4
    return "Repeated partial-fill observation counted once; final position reconciles to 4."


def position_mismatch(engine, broker):
    place(engine)
    broker.fill("demo-1", 4)
    broker.extra_position = 1
    try:
        place(engine, "demo-2")
    except Blocked:
        assert broker.submit_count == 1
        return "Broker position 5 versus ledger 4; next order blocked."
    raise AssertionError("Expected position mismatch")


def rejected_order(engine, broker):
    broker.reject_next = True
    assert place(engine)["status"] == "REJECTED"
    assert place(engine)["status"] == "REJECTED"
    assert broker.submit_count == 1
    return "Rejected order remains rejected; replay does not resubmit."


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Save report.md and report.json here")
    args = parser.parse_args()
    results = [scenario(name, action) for name, action in [
        ("Session expiry", expired_session),
        ("Stale market data", stale_quote),
        ("Lost acknowledgement + restart", lost_ack),
        ("Partial fills", partial_fill),
        ("Position mismatch", position_mismatch),
        ("Broker rejection", rejected_order),
    ]]
    lines = ["# Offline reliability demo — execution evidence", "",
             "All inputs and broker responses are simulated. No credentials or network access.", "",
             "| Scenario | Observed result | Submit calls |", "|---|---|---|"]
    for result in results:
        lines.append(f"| {result['scenario']} | {result['outcome']} | {result['submissions']} |")
    lines += ["", "Structured event traces are in [report.json](report.json).",
              "This is demonstration evidence, not an audit of a client's system.", ""]
    report = "\n".join(lines)
    print(report)
    if args.output:
        args.output.mkdir(parents=True, exist_ok=True)
        (args.output / "report.md").write_text(report)
        (args.output / "report.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
