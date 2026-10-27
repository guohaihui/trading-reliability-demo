"""Offline reference: persist intent, submit once, reconcile ambiguous outcomes."""
from dataclasses import dataclass
from math import isfinite
import sqlite3


class Blocked(Exception):
    """A failed precondition or reconciliation check; no new order is permitted."""


class SessionExpired(Exception):
    pass


@dataclass(frozen=True)
class Quote:
    symbol: str
    price: float
    timestamp: float


@dataclass(frozen=True)
class Order:
    client_id: str
    symbol: str
    quantity: int
    filled: int
    status: str


class SimulatedBroker:
    """Deliberate deterministic test double. No network or real trading support."""

    def __init__(self):
        self.session_valid = False
        self.authentication_available = True
        self.login_count = 0
        self.submit_count = 0
        self.orders = {}
        self.timeout_after_accept = False
        self.reject_next = False
        self.expire_on_submit = False
        self.extra_position = 0

    def authenticate(self):
        if not self.authentication_available:
            raise SessionExpired("Authentication unavailable")
        self.login_count += 1
        self.session_valid = True

    def ensure_session(self):
        if not self.session_valid:
            self.authenticate()

    def submit(self, client_id, symbol, quantity):
        if self.expire_on_submit:
            self.session_valid = False
            raise SessionExpired("Session expired before order acceptance")
        if not self.session_valid:
            raise SessionExpired("Invalid session")
        self.submit_count += 1
        if client_id in self.orders:
            raise AssertionError("Duplicate submission reached broker")
        status = "REJECTED" if self.reject_next else "ACCEPTED"
        order = Order(client_id, symbol, quantity, 0, status)
        self.orders[client_id] = order
        if self.timeout_after_accept:
            raise TimeoutError("Broker accepted order; acknowledgement was lost")
        return order

    def lookup(self, client_id):
        return self.orders.get(client_id)

    def fill(self, client_id, cumulative_quantity):
        old = self.orders[client_id]
        status = "FILLED" if cumulative_quantity == old.quantity else "PARTIAL"
        self.orders[client_id] = Order(
            old.client_id, old.symbol, old.quantity, cumulative_quantity, status
        )

    def position(self, symbol):
        return sum(o.filled for o in self.orders.values() if o.symbol == symbol) + self.extra_position


class Engine:
    """Single-worker, buy-only demonstration, with durable SQLite order intent."""

    def __init__(self, path, broker, clock, max_quote_age=5.0, max_quantity=10):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                client_id TEXT PRIMARY KEY, symbol TEXT NOT NULL,
                quantity INTEGER NOT NULL, status TEXT NOT NULL,
                filled INTEGER NOT NULL DEFAULT 0
            )
        """)
        self.db.commit()
        self.broker = broker
        self.clock = clock
        self.max_quote_age = max_quote_age
        self.max_quantity = max_quantity
        self.events = []

    def close(self):
        self.db.close()

    def log(self, event, **fields):
        self.events.append({"time": self.clock(), "event": event, **fields})

    def row(self, client_id):
        row = self.db.execute("SELECT * FROM orders WHERE client_id=?", (client_id,)).fetchone()
        return dict(row) if row else None

    def connect(self):
        try:
            self.broker.ensure_session()
        except SessionExpired as exc:
            self.log("AUTH_BLOCKED")
            raise Blocked(str(exc)) from exc

    def execute(self, client_id, symbol, quantity, quote):
        if not client_id or not symbol:
            raise Blocked("Stable client ID and symbol are required")
        if type(quantity) is not int or not 0 < quantity <= self.max_quantity:
            raise Blocked("Quantity outside configured limit")
        old = self.row(client_id)
        if old:
            if (old["symbol"], old["quantity"]) != (symbol, quantity):
                raise Blocked("Client ID reused for a different intent")
            self.log("REPLAY_RECONCILE", client_id=client_id)
            return self.reconcile(client_id)
        age = self.clock() - quote.timestamp
        if quote.symbol != symbol or not isfinite(quote.price) or quote.price <= 0:
            raise Blocked("Invalid quote")
        if not isfinite(age) or age < 0 or age > self.max_quote_age:
            self.log("QUOTE_BLOCKED", age=age)
            raise Blocked("Stale or future-dated quote")
        # Reconcile current state before allowing another intent for this symbol.
        self.check_position(symbol)
        with self.db:
            self.db.execute(
                "INSERT INTO orders VALUES (?, ?, ?, 'PENDING', 0)",
                (client_id, symbol, quantity),
            )
        self.log("INTENT_PERSISTED", client_id=client_id)
        try:
            order = self.broker.submit(client_id, symbol, quantity)
        except (TimeoutError, SessionExpired) as exc:
            with self.db:
                self.db.execute("UPDATE orders SET status='UNKNOWN' WHERE client_id=?", (client_id,))
            self.log("ACK_UNKNOWN", client_id=client_id, reason=type(exc).__name__)
            return self.row(client_id)
        return self.apply(order)

    def apply(self, order):
        old = self.row(order.client_id)
        if old is None or (old["symbol"], old["quantity"]) != (order.symbol, order.quantity):
            raise Blocked("Broker order identity does not match intent")
        allowed = {
            "PENDING": {"ACCEPTED", "PARTIAL", "FILLED", "REJECTED"},
            "UNKNOWN": {"ACCEPTED", "PARTIAL", "FILLED", "REJECTED"},
            "ACCEPTED": {"ACCEPTED", "PARTIAL", "FILLED", "REJECTED"},
            "PARTIAL": {"PARTIAL", "FILLED"},
            "FILLED": {"FILLED"},
            "REJECTED": {"REJECTED"},
        }
        if order.status not in allowed.get(old["status"], set()):
            raise Blocked("Invalid order-state transition")
        if type(order.filled) is not int or not old["filled"] <= order.filled <= old["quantity"]:
            raise Blocked("Invalid or regressing cumulative fill")
        if order.status in {"ACCEPTED", "REJECTED"} and order.filled != 0:
            raise Blocked("Unfilled order state contains fills")
        if order.status == "PARTIAL" and not 0 < order.filled < order.quantity:
            raise Blocked("Invalid partial fill")
        if order.status == "FILLED" and order.filled != order.quantity:
            raise Blocked("Filled order has incomplete quantity")
        with self.db:
            self.db.execute(
                "UPDATE orders SET status=?, filled=? WHERE client_id=?",
                (order.status, order.filled, order.client_id),
            )
        self.log("ORDER_RECONCILED", client_id=order.client_id, status=order.status, filled=order.filled)
        return self.row(order.client_id)

    def reconcile(self, client_id):
        self.connect()
        order = self.broker.lookup(client_id)
        if order is None:
            self.log("UNRESOLVED_INTENT", client_id=client_id)
            raise Blocked("Outcome unresolved; automatic resubmission is prohibited")
        return self.apply(order)

    def check_position(self, symbol):
        self.connect()
        rows = self.db.execute("SELECT client_id FROM orders WHERE symbol=?", (symbol,)).fetchall()
        for row in rows:
            self.reconcile(row["client_id"])
        expected = self.db.execute(
            "SELECT COALESCE(SUM(filled), 0) FROM orders WHERE symbol=?", (symbol,)
        ).fetchone()[0]
        observed = self.broker.position(symbol)
        if expected != observed:
            self.log("POSITION_MISMATCH", expected=expected, observed=observed)
            raise Blocked("Broker position differs from durable fill ledger")
        return expected
