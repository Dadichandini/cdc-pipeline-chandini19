"""Simple web interface for the CDC pipeline. Run:  streamlit run app.py"""
import os
import time
from contextlib import closing

import pandas as pd
import streamlit as st

from cdc.generate import generate
from cdc.pipeline import connect, load_customers, process_file
from cdc.reconcile import run_checks

DB = "out/ui.db"
SAMPLE = "data/sample"
os.makedirs("out", exist_ok=True)

st.set_page_config(page_title="CDC Pipeline", layout="wide")
st.title("CDC Pipeline")
st.caption("Insert/update/delete events into a current-state table with full audit history.")


def q(sql, params=()):
    with closing(connect(DB)) as conn:
        return pd.read_sql_query(sql, conn, params=params)


with st.sidebar:
    st.header("Database")
    if st.button("Reset database"):
        if os.path.exists(DB):
            os.remove(DB)
        st.success("Database cleared.")
    with closing(connect(DB)) as conn:
        n_cur = conn.execute("SELECT COUNT(*) FROM orders_current").fetchone()[0]
        n_his = conn.execute("SELECT COUNT(*) FROM orders_history").fetchone()[0]
    st.metric("Orders (current)", n_cur)
    st.metric("Events (history)", n_his)

tab_run, tab_cur, tab_his, tab_rej, tab_rec = st.tabs(
    ["Run pipeline", "Current orders", "Order history", "Rejects", "Reconciliation"])

# ---------------- Run ----------------
with tab_run:
    source = st.radio("Input", ["Sample data", "Generate new data", "Upload events.jsonl"], horizontal=True)
    batch = st.number_input("Batch size (rows per commit)", 1, 100000, 5000)
    events_path, customers_path, expected = os.path.join(SAMPLE, "events.jsonl"), os.path.join(SAMPLE, "customers.csv"), os.path.join(SAMPLE, "expected.json")
    if source == "Generate new data":
        c1, c2 = st.columns(2)
        orders = c1.number_input("Orders", 10, 50000, 500)
        seed = c2.number_input("Seed", 0, 10**6, 42)
    elif source == "Upload events.jsonl":
        up = st.file_uploader("JSON Lines file (one event per line)", type=["jsonl", "json", "txt"])
        expected = None
    if st.button("Run pipeline", type="primary"):
        if source == "Generate new data":
            out = "out/ui_data"
            generate(int(orders), int(seed), out)
            events_path, customers_path, expected = f"{out}/events.jsonl", f"{out}/customers.csv", f"{out}/expected.json"
        elif source == "Upload events.jsonl":
            if up is None:
                st.warning("Choose a file first.")
                st.stop()
            os.makedirs("out/uploads", exist_ok=True)
            events_path = f"out/uploads/{up.name}"
            with open(events_path, "wb") as f:
                f.write(up.getbuffer())
        with closing(connect(DB)) as conn:
            load_customers(conn, customers_path)
            t = time.perf_counter()
            res = process_file(conn, events_path, int(batch))
            secs = time.perf_counter() - t
        st.session_state["expected"] = expected
        (st.success if res["status"] == "success" else st.error)(
            f"Run {res['run_id']}: {res['status']} in {secs:.2f}s" + (f" ({res['failure_reason']})" if res["failure_reason"] else ""))
        cols = st.columns(5)
        for col, k in zip(cols, ["input", "applied", "stale", "duplicate", "rejected"]):
            col.metric(k, f"{res[k]:,}")
        st.caption("input = applied + stale + duplicate + rejected. Run the same input again: applied becomes 0 (idempotent).")
    st.subheader("Run log")
    st.dataframe(q("SELECT * FROM run_log ORDER BY started_at DESC LIMIT 20"), width="stretch", hide_index=True)

# ---------------- Current ----------------
with tab_cur:
    statuses = ["new", "paid", "shipped", "cancelled"]
    c1, c2, c3 = st.columns([2, 2, 1])
    sel = c1.multiselect("Status", statuses, default=statuses)
    find = c2.text_input("Order id contains")
    show_del = c3.checkbox("Show deleted", value=False)
    sql = "SELECT * FROM orders_current WHERE 1=1"
    params = []
    if not show_del:
        sql += " AND is_deleted=0"
    if sel:
        sql += f" AND (status IN ({','.join('?' * len(sel))}) OR status IS NULL)"
        params += sel
    if find:
        sql += " AND order_id LIKE ?"
        params.append(f"%{find}%")
    df = q(sql + " ORDER BY order_id LIMIT 1000", params)
    st.caption(f"Showing up to 1,000 rows ({len(df)} shown)")
    st.dataframe(df, width="stretch", hide_index=True)
    summary = q("SELECT status, COUNT(*) AS orders, ROUND(SUM(amount),2) AS revenue FROM orders_current WHERE is_deleted=0 GROUP BY status")
    if not summary.empty:
        st.subheader("Live orders by status")
        st.bar_chart(summary.set_index("status")["orders"])
        st.dataframe(summary, hide_index=True)

# ---------------- History ----------------
with tab_his:
    oid = st.text_input("Order id (for example O0000001)")
    if oid:
        cur = q("SELECT * FROM orders_current WHERE order_id=?", (oid,))
        st.write("Current state")
        st.dataframe(cur, width="stretch", hide_index=True)
        st.write("All events (applied = 0 means a late event that did not change state)")
        st.dataframe(q("SELECT event_ts, event_id, op, customer_id, amount, status, applied, run_id FROM orders_history WHERE order_id=? ORDER BY event_ts, event_id", (oid,)),
                     width="stretch", hide_index=True)
    else:
        st.info("Enter an order id to see every event for it.")
        st.dataframe(q("SELECT order_id, COUNT(*) AS events, SUM(applied=0) AS late_events FROM orders_history GROUP BY order_id HAVING late_events>0 ORDER BY late_events DESC LIMIT 20"),
                     width="stretch", hide_index=True)

# ---------------- Rejects ----------------
with tab_rej:
    rs = q("SELECT reason, COUNT(*) AS rows FROM rejects GROUP BY reason ORDER BY rows DESC")
    if rs.empty:
        st.info("No rejected rows.")
    else:
        st.bar_chart(rs.set_index("reason"))
        pick = st.selectbox("Filter by reason", ["All"] + rs["reason"].tolist())
        sql = "SELECT source_file, line_no, reason, raw FROM rejects" + ("" if pick == "All" else " WHERE reason=?")
        st.dataframe(q(sql + " ORDER BY source_file, line_no LIMIT 1000", () if pick == "All" else (pick,)),
                     width="stretch", hide_index=True)

# ---------------- Reconciliation ----------------
with tab_rec:
    if st.button("Run reconciliation checks"):
        exp = st.session_state.get("expected")
        with closing(connect(DB)) as conn:
            results = run_checks(conn, exp if exp and os.path.exists(exp) else None)
        st.dataframe(pd.DataFrame([{"check": n, "result": "PASS" if ok else "FAIL", "value": v} for n, ok, v in results]),
                     width="stretch", hide_index=True)
        (st.success if all(ok for _, ok, _ in results) else st.error)(
            "All checks passed." if all(ok for _, ok, _ in results) else "Some checks failed.")
        if not exp:
            st.caption("Answer-key comparison is only available for sample or generated data.")
