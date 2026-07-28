from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polymarket_scan_remote as scan

scan.CONCURRENCY = 24
scan.PRICE_SAMPLES = 8


def fast_get_json(base, path, params, retries=2, timeout=10):
    url = base + path + "?" + urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
    cache_path = scan.cache_path(url)
    if cache_path.exists():
        return json.loads(cache_path.read_text())["payload"]
    error = None
    for attempt in range(retries + 1):
        try:
            request = Request(url, headers={"User-Agent": "polymarket-copytrade-scan/0.3"})
            with urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode())
            cache_path.write_text(json.dumps({"url": url, "retrieved_at": scan.NOW.isoformat(), "payload": payload}, ensure_ascii=False))
            return payload
        except Exception as exc:
            error = exc
            if attempt < retries:
                time.sleep(min(0.25 * (2 ** attempt), 1))
    raise RuntimeError(f"{url}: {error}")


def fast_price_history(asset, start, end):
    return fast_get_json(
        scan.CLOB,
        "/prices-history",
        {"market": asset, "startTs": start, "endTs": end, "interval": "1m", "fidelity": 1},
        retries=1,
        timeout=6,
    ).get("history", [])


def parallel_persistence(wallet, activities):
    first = {}
    for row in sorted(activities, key=lambda item: scan.ts(item.get("timestamp")) or 0):
        if str(row.get("side") or "").upper() != "BUY":
            continue
        market = scan.cond(row)
        asset = str(row.get("asset") or "")
        timestamp = scan.ts(row.get("timestamp"))
        price = scan.num(row.get("price"))
        if market and asset and timestamp and price is not None:
            first.setdefault(market, row)
    samples = sorted(first.values(), key=lambda item: scan.ts(item.get("timestamp")) or 0, reverse=True)[: scan.PRICE_SAMPLES]

    def analyze(row):
        timestamp = scan.ts(row.get("timestamp"))
        entry = scan.num(row.get("price"))
        asset = str(row.get("asset"))
        error = None
        try:
            history = fast_price_history(asset, timestamp - 60, timestamp + 600)
        except Exception as exc:
            history = []
            error = repr(exc)
        price_1m = scan.price_after(history, timestamp + 60)
        price_5m = scan.price_after(history, timestamp + 300)
        adverse_1m = price_1m - entry if price_1m is not None else None
        adverse_5m = price_5m - entry if price_5m is not None else None
        return {
            "wallet": wallet,
            "timestamp": timestamp,
            "condition_id": row.get("conditionId"),
            "asset": asset,
            "title": row.get("title"),
            "outcome": row.get("outcome"),
            "entry_price": entry,
            "price_1m": price_1m,
            "price_5m": price_5m,
            "adverse_1m": adverse_1m,
            "adverse_5m": adverse_5m,
            "error": error,
            "source_url": f"{scan.CLOB}/prices-history?market={asset}&startTs={timestamp-60}&endTs={timestamp+600}&interval=1m&fidelity=1",
        }

    rows = []
    with ThreadPoolExecutor(max_workers=min(8, len(samples) or 1)) as executor:
        futures = [executor.submit(analyze, row) for row in samples]
        for future in as_completed(futures):
            rows.append(future.result())
    adverse_1m = [row["adverse_1m"] for row in rows if row["adverse_1m"] is not None]
    adverse_5m = [row["adverse_5m"] for row in rows if row["adverse_5m"] is not None]
    return rows, scan.med(adverse_1m), scan.med(adverse_5m), max(len(adverse_1m), len(adverse_5m))


scan.get_json = fast_get_json
scan.price_history = fast_price_history
scan.persistence = parallel_persistence

print("Starting bounded-time parallel Polymarket scan", flush=True)
scan.main()
