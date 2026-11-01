"""CDC pipeline: JSONL change events -> orders_current (state) + orders_history (audit)."""
import argparse, csv, json, math, sqlite3, uuid
from datetime import datetime, timezone

OPS = {"I", "U", "D"}
STATUSES = {"new", "paid", "shipped", "cancelled"}
KNOWN = {"event_id", "order_id", "op", "event_ts", "customer_id", "amount", "status"}
REQUIRED = ("event_id", "order_id", "op", "event_ts")

SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (customer_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS orders_current (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT REFERENCES customers(customer_id),
  amount REAL,
  status TEXT,
  last_event_ts TEXT NOT NULL,
  last_event_id TEXT NOT NULL,
  is_deleted INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0,1))
);
CREATE TABLE IF NOT EXISTS orders_history (
  event_id TEXT PRIMARY KEY,
  order_id TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('I','U','D')),
  event_ts TEXT NOT NULL,
  customer_id TEXT, amount REAL, status TEXT,
  applied INTEGER NOT NULL CHECK (applied IN (0,1)),
  run_id TEXT NOT NULL,
  source_file TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_history_order ON orders_history(order_id, event_ts);
CREATE TABLE IF NOT EXISTS rejects (
  run_id TEXT NOT NULL,
  source_file TEXT NOT NULL,
  line_no INTEGER NOT NULL,
  reason TEXT NOT NULL,
  raw TEXT,
  PRIMARY KEY (source_file, line_no)
);
CREATE TABLE IF NOT EXISTS run_log (
  run_id TEXT PRIMARY KEY,
  source_file TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  status TEXT NOT NULL,
  input_count INTEGER DEFAULT 0,
  applied_count INTEGER DEFAULT 0,
  stale_count INTEGER DEFAULT 0,
  duplicate_count INTEGER DEFAULT 0,
  rejected_count INTEGER DEFAULT 0,
  failure_reason TEXT
);
"""

UPSERT = """
INSERT INTO orders_current
  (order_id, customer_id, amount, status, last_event_ts, last_event_id, is_deleted)
VALUES (?,?,?,?,?,?,?)
ON CONFLICT(order_id) DO UPDATE SET
  customer_id = CASE WHEN excluded.is_deleted=1 THEN orders_current.customer_id ELSE excluded.customer_id END,
  amount      = CASE WHEN excluded.is_deleted=1 THEN orders_current.amount      ELSE excluded.amount      END,
  status      = CASE WHEN excluded.is_deleted=1 THEN orders_current.status      ELSE excluded.status      END,
  last_event_ts = excluded.last_event_ts,
  last_event_id = excluded.last_event_id,
  is_deleted    = excluded.is_deleted
"""


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn


def load_customers(conn, csv_path):
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = [(r["customer_id"],) for r in csv.DictReader(f)]
    conn.executemany("INSERT OR IGNORE INTO customers VALUES (?)", rows)
    conn.commit()


def validate(raw, customers):
    """Return (event, None) or (None, reason). Timestamps normalised to UTC."""
    if not isinstance(raw, dict):
        return None, "not_an_object"
    extra = set(raw) - KNOWN
    if extra:
        return None, "unexpected_fields:" + ",".join(sorted(extra))
    for k in REQUIRED:
        v = raw.get(k)
        if not isinstance(v, str) or not v.strip():
            return None, f"missing_or_invalid:{k}"
    if raw["op"] not in OPS:
        return None, "invalid_op"
    try:
        dt = datetime.fromisoformat(raw["event_ts"].replace("Z", "+00:00"))
    except ValueError:
        return None, "bad_timestamp"
    if dt.tzinfo is None:
        return None, "bad_timestamp"
    ev = {
        "event_id": raw["event_id"], "order_id": raw["order_id"], "op": raw["op"],
        "event_ts": dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "customer_id": None, "amount": None, "status": None,
    }
    if raw["op"] != "D":
        cid, amt, st = raw.get("customer_id"), raw.get("amount"), raw.get("status")
        if not isinstance(cid, str) or not cid:
            return None, "missing_or_invalid:customer_id"
        if cid not in customers:
            return None, "invalid_reference:customer_id"
        if isinstance(amt, bool) or not isinstance(amt, (int, float)) or not math.isfinite(amt):
            return None, "missing_or_invalid:amount"
        if amt < 0:
            return None, "negative_amount"
        if st not in STATUSES:
            return None, "invalid_status"
        ev.update(customer_id=cid, amount=float(amt), status=st)
    return ev, None


def apply_event(conn, ev, run_id, src):
    """Returns 'duplicate' | 'applied' | 'stale'. History is always written (except duplicates)."""
    if conn.execute("SELECT 1 FROM orders_history WHERE event_id=?", (ev["event_id"],)).fetchone():
        return "duplicate"
    cur = conn.execute(
        "SELECT last_event_ts, last_event_id FROM orders_current WHERE order_id=?",
        (ev["order_id"],)).fetchone()
    newer = cur is None or (ev["event_ts"], ev["event_id"]) > (cur[0], cur[1])
    conn.execute(
        "INSERT INTO orders_history VALUES (?,?,?,?,?,?,?,?,?,?)",
        (ev["event_id"], ev["order_id"], ev["op"], ev["event_ts"], ev["customer_id"],
         ev["amount"], ev["status"], int(newer), run_id, src))
    if newer:
        conn.execute(UPSERT, (ev["order_id"], ev["customer_id"], ev["amount"], ev["status"],
                              ev["event_ts"], ev["event_id"], int(ev["op"] == "D")))
        return "applied"
    return "stale"


def process_file(conn, path, batch_size=5000):
    run_id = uuid.uuid4().hex[:12]
    conn.execute("INSERT INTO run_log(run_id, source_file, started_at, status) VALUES (?,?,?,'running')",
                 (run_id, path, now()))
    conn.commit()
    customers = {r[0] for r in conn.execute("SELECT customer_id FROM customers")}
    c = dict(input=0, applied=0, stale=0, duplicate=0, rejected=0)
    committed = dict(c)
    status, err = "success", None
    try:
        with open(path, encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                if not line.strip():
                    continue
                c["input"] += 1
                try:
                    raw = json.loads(line)
                    ev, reason = validate(raw, customers)
                except json.JSONDecodeError:
                    ev, reason = None, "malformed_json"
                if ev is None:
                    conn.execute("INSERT OR IGNORE INTO rejects VALUES (?,?,?,?,?)",
                                 (run_id, path, line_no, reason, line.strip()[:500]))
                    c["rejected"] += 1
                else:
                    c[apply_event(conn, ev, run_id, path)] += 1
                if c["input"] % batch_size == 0:
                    conn.commit()
                    committed = dict(c)
        conn.commit()
        committed = dict(c)
    except Exception as e:  # uncommitted batch rolls back; earlier batches stay; rerun resumes via dedupe
        conn.rollback()
        status, err = "failed", repr(e)
    conn.execute(
        "UPDATE run_log SET ended_at=?, status=?, input_count=?, applied_count=?, stale_count=?, "
        "duplicate_count=?, rejected_count=?, failure_reason=? WHERE run_id=?",
        (now(), status, committed["input"], committed["applied"], committed["stale"],
         committed["duplicate"], committed["rejected"], err, run_id))
    conn.commit()
    return {"run_id": run_id, "status": status, "failure_reason": err, **committed}


def main():
    import os, time
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="out/cdc.db")
    p.add_argument("--customers", required=True)
    p.add_argument("--input", required=True)
    p.add_argument("--batch-size", type=int, default=5000)
    a = p.parse_args()
    os.makedirs(os.path.dirname(a.db) or ".", exist_ok=True)
    conn = connect(a.db)
    load_customers(conn, a.customers)
    t = time.perf_counter()
    res = process_file(conn, a.input, a.batch_size)
    res["seconds"] = round(time.perf_counter() - t, 2)
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
