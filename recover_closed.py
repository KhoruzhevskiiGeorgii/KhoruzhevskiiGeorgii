from __future__ import annotations

import csv
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = "https://data-api.polymarket.com"
OUT = Path("recovery_output")
OUT.mkdir(exist_ok=True)
WALLETS = json.loads(Path("recovery_wallets.json").read_text())
MAX_ROWS = 500
CONCURRENCY = 4
REQUEST_INTERVAL = 0.09
_lock = threading.Lock()
_last_request = 0.0


def throttle():
    global _last_request
    with _lock:
        now = time.monotonic()
        wait = REQUEST_INTERVAL - (now - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.monotonic()


def get_json(path, params, retries=8):
    url = BASE + path + "?" + urlencode(params)
    error = None
    for attempt in range(retries + 1):
        try:
            throttle()
            request = Request(url, headers={"User-Agent": "polymarket-copytrade-recovery/0.1"})
            with urlopen(request, timeout=20) as response:
                return json.loads(response.read().decode())
        except HTTPError as exc:
            error = exc
            if exc.code != 429 or attempt >= retries:
                break
            retry_after = exc.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else min(1.5 * (2 ** attempt), 20)
            time.sleep(delay)
        except Exception as exc:
            error = exc
            if attempt >= retries:
                break
            time.sleep(min(1.0 * (2 ** attempt), 10))
    raise RuntimeError(f"{url}: {error}")


def fetch_wallet(wallet):
    rows = []
    errors = []
    for offset in range(0, MAX_ROWS, 50):
        try:
            page = get_json(
                "/closed-positions",
                {
                    "user": wallet,
                    "limit": 50,
                    "offset": offset,
                    "sortBy": "TIMESTAMP",
                    "sortDirection": "DESC",
                },
            )
        except Exception as exc:
            errors.append({"wallet": wallet, "offset": offset, "error": repr(exc), "partial_rows": len(rows)})
            break
        rows.extend({"wallet": wallet, **row} for row in page)
        if len(page) < 50:
            break
    return wallet, rows, errors


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
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    all_rows = []
    all_errors = []
    completed = 0
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = {executor.submit(fetch_wallet, wallet): wallet for wallet in WALLETS}
        for future in as_completed(futures):
            wallet, rows, errors = future.result()
            all_rows.extend(rows)
            all_errors.extend(errors)
            completed += 1
            print(f"closed_recovery={completed}/{len(WALLETS)} wallet={wallet} rows={len(rows)} errors={len(errors)}", flush=True)
    (OUT / "recovered_closed_positions.json").write_text(json.dumps(all_rows, ensure_ascii=False))
    write_csv(OUT / "recovery_errors.csv", all_errors)
    summary = {
        "wallets_requested": len(WALLETS),
        "wallets_with_rows": len({row["wallet"] for row in all_rows}),
        "rows": len(all_rows),
        "errors": len(all_errors),
    }
    (OUT / "recovery_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
