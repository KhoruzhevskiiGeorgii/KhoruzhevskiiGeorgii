from __future__ import annotations

import csv
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DATA = "https://data-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
OUT = Path("price_recovery_output")
OUT.mkdir(exist_ok=True)
WALLETS = json.loads(Path("price_wallets.json").read_text())
SAMPLES_PER_WALLET = 12


def get_json(base, path, params, retries=4, timeout=15):
    url = base + path + "?" + urlencode({key: value for key, value in params.items() if value is not None})
    error = None
    for attempt in range(retries + 1):
        try:
            request = Request(url, headers={"User-Agent": "polymarket-price-persistence-recovery/0.1"})
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except Exception as exc:
            error = exc
            if attempt < retries:
                time.sleep(min(0.35 * (2 ** attempt), 4))
    raise RuntimeError(f"{url}: {error}")


def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def timestamp(value):
    value = number(value)
    if value is None or value <= 0:
        return None
    if value > 10_000_000_000:
        value /= 1000
    return int(value)


def price_at_or_after(history, target, tolerance=180):
    points = []
    for point in history:
        point_ts = timestamp(point.get("t"))
        price = number(point.get("p"))
        if point_ts is None or price is None or point_ts < target or point_ts - target > tolerance:
            continue
        points.append((point_ts, price))
    return min(points)[1] if points else None


def get_requests(wallet):
    activity = get_json(
        DATA,
        "/activity",
        {
            "user": wallet,
            "type": "TRADE",
            "limit": 500,
            "offset": 0,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        },
    )
    first_by_market = {}
    for row in sorted(activity, key=lambda item: timestamp(item.get("timestamp")) or 0):
        if str(row.get("side") or "").upper() != "BUY":
            continue
        condition = str(row.get("conditionId") or "")
        asset = str(row.get("asset") or "")
        entered_at = timestamp(row.get("timestamp"))
        entry_price = number(row.get("price"))
        if condition and asset and entered_at is not None and entry_price is not None:
            first_by_market.setdefault(condition, row)
    selected = sorted(first_by_market.values(), key=lambda item: timestamp(item.get("timestamp")) or 0, reverse=True)[:SAMPLES_PER_WALLET]
    return [
        {
            "wallet": wallet,
            "timestamp": timestamp(row.get("timestamp")),
            "condition_id": row.get("conditionId"),
            "asset": str(row.get("asset") or ""),
            "title": row.get("title"),
            "outcome": row.get("outcome"),
            "entry_price": number(row.get("price")),
        }
        for row in selected
    ]


def analyze(request_row):
    entered_at = request_row["timestamp"]
    asset = request_row["asset"]
    entry_price = request_row["entry_price"]
    error = None
    try:
        payload = get_json(
            CLOB,
            "/prices-history",
            {
                "market": asset,
                "startTs": entered_at - 60,
                "endTs": entered_at + 600,
                "fidelity": 1,
            },
            retries=3,
            timeout=12,
        )
        history = payload.get("history", [])
    except Exception as exc:
        history = []
        error = repr(exc)
    price_1m = price_at_or_after(history, entered_at + 60)
    price_5m = price_at_or_after(history, entered_at + 300)
    return {
        **request_row,
        "price_1m": price_1m,
        "price_5m": price_5m,
        "adverse_1m": price_1m - entry_price if price_1m is not None else None,
        "adverse_5m": price_5m - entry_price if price_5m is not None else None,
        "history_points": len(history),
        "error": error,
        "source_url": (
            f"{CLOB}/prices-history?market={asset}&startTs={entered_at-60}&endTs={entered_at+600}&fidelity=1"
        ),
    }


def write_csv(path, rows):
    rows = list(rows)
    if not rows:
        path.write_text("")
        return
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    requests = []
    activity_errors = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_requests, wallet): wallet for wallet in WALLETS}
        for future in as_completed(futures):
            wallet = futures[future]
            try:
                wallet_requests = future.result()
                requests.extend(wallet_requests)
                print(f"activity wallet={wallet} requests={len(wallet_requests)}", flush=True)
            except Exception as exc:
                activity_errors.append({"wallet": wallet, "stage": "activity", "error": repr(exc)})

    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(analyze, row) for row in requests]
        for index, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            if index % 25 == 0 or index == len(futures):
                print(f"price_history={index}/{len(futures)}", flush=True)

    results.sort(key=lambda row: (row["wallet"], -(row["timestamp"] or 0)))
    write_csv(OUT / "recovered_price_persistence.csv", results)
    write_csv(OUT / "activity_errors.csv", activity_errors)
    summary = {
        "wallets_requested": len(WALLETS),
        "activity_errors": len(activity_errors),
        "price_requests": len(requests),
        "rows": len(results),
        "rows_with_1m": sum(row["price_1m"] is not None for row in results),
        "rows_with_5m": sum(row["price_5m"] is not None for row in results),
        "price_errors": sum(bool(row["error"]) for row in results),
        "median_adverse_1m": statistics.median([row["adverse_1m"] for row in results if row["adverse_1m"] is not None]) if any(row["adverse_1m"] is not None for row in results) else None,
        "median_adverse_5m": statistics.median([row["adverse_5m"] for row in results if row["adverse_5m"] is not None]) if any(row["adverse_5m"] is not None for row in results) else None,
    }
    (OUT / "price_recovery_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
