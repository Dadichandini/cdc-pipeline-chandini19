"""Seeded synthetic data generator. Labels/expected results go to a SEPARATE file."""
import argparse, csv, json, os, random
from datetime import datetime, timedelta, timezone

STATUSES = ["new", "paid", "shipped", "cancelled"]
BAD_KINDS = ["malformed_json", "missing_field", "invalid_ref", "invalid_op",
             "negative_amount", "bad_ts", "extra_field", "null_amount"]


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate(orders, seed, out_dir):
    rnd = random.Random(seed)
    os.makedirs(out_dir, exist_ok=True)
    custs = [f"C{i:04d}" for i in range(1, 501)]
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events, expected, n = [], {}, 0

    def emit(oid, op, t, cust, amt, st):
        nonlocal n
        n += 1
        e = {"event_id": f"E{n:09d}", "order_id": oid, "op": op, "event_ts": iso(t)}
        if op != "D":
            e.update(customer_id=cust, amount=amt, status=st)
        events.append(e)
        return e["event_id"]

    for o in range(orders):
        oid = f"O{o:07d}"
        t = base + timedelta(seconds=rnd.randint(0, 10_000_000))
        cust = rnd.choice(custs)
        amt = rnd.choice([0.0, 0.01, round(rnd.uniform(5, 500), 2)])  # boundary values
        st, deleted = "new", 0
        last = emit(oid, "I", t, cust, amt, st)
        for _ in range(rnd.randint(0, 3)):
            t += timedelta(seconds=rnd.randint(1, 86400))
            amt, st = round(rnd.uniform(5, 500), 2), rnd.choice(STATUSES)
            last = emit(oid, "U", t, cust, amt, st)
        if rnd.random() < 0.1:
            t += timedelta(seconds=rnd.randint(1, 86400))
            last, deleted = emit(oid, "D", t, None, None, None), 1
        expected[oid] = [last, deleted, cust, amt, st]

    lines = [json.dumps(e) for e in events]
    for _ in range(len(lines) // 20):  # out-of-order: swap with a later event
        i = rnd.randrange(len(lines))
        j = min(len(lines) - 1, i + rnd.randint(1, 2000))
        lines[i], lines[j] = lines[j], lines[i]
    dups = rnd.sample(lines, len(lines) // 50)  # exact duplicate events
    for d in dups:
        lines.insert(rnd.randint(0, len(lines)), d)

    n_bad = max(1, len(events) // 30)
    for i in range(n_bad):
        kind = BAD_KINDS[i % len(BAD_KINDS)]
        e = {"event_id": f"B{i:07d}", "order_id": f"O{rnd.randrange(orders):07d}", "op": "U",
             "event_ts": iso(base + timedelta(seconds=rnd.randint(0, 10_000_000))),
             "customer_id": rnd.choice(custs), "amount": 10.0, "status": "paid"}
        if kind == "malformed_json":
            line = '{"event_id": "B%07d", "order_id": ' % i
        else:
            if kind == "missing_field": del e["order_id"]
            elif kind == "invalid_ref": e["customer_id"] = "C9999"
            elif kind == "invalid_op": e["op"] = "X"
            elif kind == "negative_amount": e["amount"] = -5
            elif kind == "bad_ts": e["event_ts"] = "not-a-date"
            elif kind == "extra_field": e["coupon"] = "SAVE10"
            elif kind == "null_amount": e["amount"] = None
            line = json.dumps(e)
        lines.insert(rnd.randint(0, len(lines)), line)

    with open(f"{out_dir}/events.jsonl", "w") as f:
        f.write("\n".join(lines) + "\n")
    with open(f"{out_dir}/customers.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id"])
        for c in custs:
            w.writerow([c])
    with open(f"{out_dir}/expected.json", "w") as f:  # labels kept apart from input
        json.dump({"lines": len(lines), "unique_valid": len(events), "duplicates": len(dups),
                   "rejected": n_bad, "orders": expected}, f)
    return len(lines)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--orders", type=int, default=60)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data/sample")
    a = p.parse_args()
    print("lines:", generate(a.orders, a.seed, a.out))
