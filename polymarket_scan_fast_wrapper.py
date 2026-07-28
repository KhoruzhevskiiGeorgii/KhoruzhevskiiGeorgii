from __future__ import annotations

import json
import time
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
            request = Request(url, headers={"User-Agent": "polymarket-copytrade-scan/0.2"})
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


scan.get_json = fast_get_json
scan.price_history = fast_price_history

print("Starting bounded-time Polymarket scan", flush=True)
scan.main()
