# Offline reliability demo — execution evidence

All inputs and broker responses are simulated. No credentials or network access.

| Scenario | Observed result | Submit calls |
|---|---|---|
| Session expiry | Expired session refreshed; exactly one order accepted. | 1 |
| Stale market data | 100-second-old quote blocked before submission. | 0 |
| Lost acknowledgement + restart | New engine instance recovered persisted intent; no duplicate submission. | 1 |
| Partial fills | Repeated partial-fill observation counted once; final position reconciles to 4. | 1 |
| Position mismatch | Broker position 5 versus ledger 4; next order blocked. | 1 |
| Broker rejection | Rejected order remains rejected; replay does not resubmit. | 1 |

Structured event traces are in [report.json](report.json).
This is demonstration evidence, not an audit of a client's system.
