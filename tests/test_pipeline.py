import json
import pytest
from cdc import pipeline
from cdc.pipeline import connect, process_file


@pytest.fixture
def conn(tmp_path):
    c = connect(str(tmp_path / "t.db"))
    c.executemany("INSERT INTO customers VALUES (?)", [("C1",), ("C2",)])
    c.commit()
    return c


def ev(eid, op="I", ts="2026-01-01T00:00:00Z", oid="O1", cust="C1", amt=10.0, st="new", **kw):
    e = {"event_id": eid, "order_id": oid, "op": op, "event_ts": ts}
    if op != "D":
        e.update(customer_id=cust, amount=amt, status=st)
    e.update(kw)
    return e


def run(conn, tmp_path, events, name="in.jsonl", **kw):
    p = tmp_path / name
    p.write_text("\n".join(e if isinstance(e, str) else json.dumps(e) for e in events))
    return process_file(conn, str(p), **kw)


def row(conn, oid="O1"):
    return conn.execute("SELECT amount, status, is_deleted, last_event_id FROM orders_current WHERE order_id=?", (oid,)).fetchone()


def test_happy_path(conn, tmp_path):
    r = run(conn, tmp_path, [ev("E1"), ev("E2", "U", "2026-01-02T00:00:00Z", amt=20, st="paid")])
    assert (r["status"], r["applied"], r["rejected"]) == ("success", 2, 0)
    assert row(conn) == (20.0, "paid", 0, "E2")
    assert conn.execute("SELECT COUNT(*) FROM orders_history").fetchone()[0] == 2


def test_empty_batch(conn, tmp_path):
    r = run(conn, tmp_path, [])
    assert r["status"] == "success" and r["input"] == 0


def test_malformed_json_rejected_with_source(conn, tmp_path):
    r = run(conn, tmp_path, ['{"event_id": "E1", ', ev("E2")])
    assert (r["rejected"], r["applied"]) == (1, 1)
    reason, line_no, src = conn.execute("SELECT reason, line_no, source_file FROM rejects").fetchone()
    assert reason == "malformed_json" and line_no == 1 and src.endswith("in.jsonl")


def test_missing_required_field(conn, tmp_path):
    bad = ev("E1"); del bad["order_id"]
    r = run(conn, tmp_path, [bad])
    assert r["rejected"] == 1
    assert conn.execute("SELECT reason FROM rejects").fetchone()[0] == "missing_or_invalid:order_id"


def test_nulls_rejected(conn, tmp_path):
    r = run(conn, tmp_path, [ev("E1", amt=None), ev("E2", cust=None), ev("E3", st=None)])
    assert r["rejected"] == 3 and r["applied"] == 0


def test_invalid_reference_key(conn, tmp_path):
    r = run(conn, tmp_path, [ev("E1", cust="C999")])
    assert r["rejected"] == 1
    assert conn.execute("SELECT reason FROM rejects").fetchone()[0] == "invalid_reference:customer_id"


def test_unexpected_schema_change(conn, tmp_path):
    r = run(conn, tmp_path, [ev("E1", coupon="X")])
    assert r["rejected"] == 1
    assert conn.execute("SELECT reason FROM rejects").fetchone()[0].startswith("unexpected_fields")


def test_duplicate_event_ignored(conn, tmp_path):
    r = run(conn, tmp_path, [ev("E1"), ev("E1")])
    assert (r["applied"], r["duplicate"]) == (1, 1)
    assert conn.execute("SELECT COUNT(*) FROM orders_history").fetchone()[0] == 1


def test_out_of_order_update_does_not_overwrite(conn, tmp_path):
    run(conn, tmp_path, [ev("E2", "U", "2026-01-02T00:00:00Z", amt=20), ev("E1", "I", "2026-01-01T00:00:00Z", amt=10)])
    assert row(conn)[0] == 20.0 and row(conn)[3] == "E2"
    # late event is still kept in history, flagged not applied
    assert conn.execute("SELECT applied FROM orders_history WHERE event_id='E1'").fetchone()[0] == 0


def test_delete_is_soft_and_late_update_ignored(conn, tmp_path):
    r = run(conn, tmp_path, [ev("E1"), ev("E3", "D", "2026-01-03T00:00:00Z"),
                             ev("E2", "U", "2026-01-02T00:00:00Z", amt=99)])
    assert row(conn) == (10.0, "new", 1, "E3")
    assert r["stale"] == 1
    assert conn.execute("SELECT COUNT(*) FROM orders_history WHERE order_id='O1'").fetchone()[0] == 3


def test_timezone_offsets_normalised(conn, tmp_path):
    run(conn, tmp_path, [ev("E1", ts="2026-01-01T05:00:00+05:00", amt=1),
                         ev("E2", "U", "2026-01-01T00:00:01Z", amt=2)])
    assert row(conn)[0] == 2.0


def test_rerun_same_input_is_idempotent(conn, tmp_path):
    events = [ev("E1"), ev("E2", "U", "2026-01-02T00:00:00Z", amt=5), "garbage"]
    run(conn, tmp_path, events)
    snap = lambda: (conn.execute("SELECT * FROM orders_current ORDER BY 1").fetchall(),
                    conn.execute("SELECT COUNT(*) FROM orders_history").fetchone(),
                    conn.execute("SELECT COUNT(*) FROM rejects").fetchone())
    before = snap()
    r2 = run(conn, tmp_path, events)
    assert snap() == before
    assert (r2["duplicate"], r2["applied"]) == (2, 0)


def test_partial_failure_then_retry_matches_clean_run(conn, tmp_path, monkeypatch):
    events = [ev(f"E{i}", "I", f"2026-01-01T00:00:{i:02d}Z", oid=f"O{i}") for i in range(1, 8)]
    real = pipeline.apply_event
    calls = {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] == 5:
            raise RuntimeError("boom")
        return real(*a, **k)

    monkeypatch.setattr(pipeline, "apply_event", flaky)
    r1 = run(conn, tmp_path, events, batch_size=2)
    assert r1["status"] == "failed" and "boom" in r1["failure_reason"]
    assert conn.execute("SELECT COUNT(*) FROM orders_current").fetchone()[0] == 4  # 2 batches committed
    monkeypatch.setattr(pipeline, "apply_event", real)
    r2 = run(conn, tmp_path, events, batch_size=2)
    assert r2["status"] == "success" and r2["duplicate"] == 4 and r2["applied"] == 3
    assert conn.execute("SELECT COUNT(*) FROM orders_current").fetchone()[0] == 7


def test_reconciliation_identity_holds(conn, tmp_path):
    from cdc.reconcile import run_checks
    run(conn, tmp_path, [ev("E1"), ev("E1"), "bad", ev("E0", "U", "2025-12-31T00:00:00Z")])
    assert all(ok for _, ok, _ in run_checks(conn))
