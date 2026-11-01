"""Reconciliation checks. Exit code 1 if any check fails."""
import argparse, json, sys
from cdc.pipeline import connect

CHECKS = {
    "every run: input = applied+stale+duplicate+rejected (bad runs)":
        "SELECT COUNT(*) FROM run_log WHERE input_count != applied_count+stale_count+duplicate_count+rejected_count",
    "history rows - sum(applied+stale) over runs (difference)":
        "SELECT (SELECT COUNT(*) FROM orders_history) - (SELECT COALESCE(SUM(applied_count+stale_count),0) FROM run_log)",
    "current row != newest event in history (orders)":
        """SELECT COUNT(*) FROM orders_current c JOIN (
             SELECT order_id, MAX(event_ts||'|'||event_id) k FROM orders_history GROUP BY order_id) h
           ON h.order_id=c.order_id WHERE h.k != c.last_event_ts||'|'||c.last_event_id""",
    "orders in history but missing from current":
        "SELECT COUNT(DISTINCT order_id) FROM orders_history WHERE order_id NOT IN (SELECT order_id FROM orders_current)",
    "orphan customer references in current":
        "SELECT COUNT(*) FROM orders_current WHERE customer_id IS NOT NULL AND customer_id NOT IN (SELECT customer_id FROM customers)",
}


def run_checks(conn, expected_path=None):
    results = []
    for name, sql in CHECKS.items():
        v = conn.execute(sql).fetchone()[0]
        results.append((name, v == 0, v))
    if expected_path:
        exp = json.load(open(expected_path))
        cur = {r[0]: r[1:] for r in conn.execute(
            "SELECT order_id, last_event_id, is_deleted, customer_id, amount, status FROM orders_current")}
        bad = 0
        for oid, (eid, deleted, cust, amt, st) in exp["orders"].items():
            row = cur.get(oid)
            if row is None or row[0] != eid or row[1] != deleted or (
                    not deleted and tuple(row[2:]) != (cust, amt, st)):
                bad += 1
        results.append(("final state mismatches vs generator's expected state", bad == 0, bad))
        rej = conn.execute("SELECT COUNT(*) FROM rejects").fetchone()[0]
        results.append(("rejects table - injected invalid rows (difference)", rej == exp["rejected"], rej - exp["rejected"]))
    return results


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="out/cdc.db")
    p.add_argument("--expected")
    a = p.parse_args()
    res = run_checks(connect(a.db), a.expected)
    for name, ok, val in res:
        print(f"[{'PASS' if ok else 'FAIL'}] {name} -> {val}")
    sys.exit(0 if all(ok for _, ok, _ in res) else 1)
