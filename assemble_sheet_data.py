from __future__ import annotations

import csv
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import polymarket_scan_remote as scan

FULL = Path("downloaded/full")
RECOVERY = Path("downloaded/recovery")
PRICE = Path("downloaded/price")
OUT = Path("sheet_data")
OUT.mkdir(exist_ok=True)
NOW = datetime.now(timezone.utc)
scan.NOW = NOW


def locate(root: Path, name: str) -> Path:
    matches = list(root.rglob(name))
    if not matches:
        raise FileNotFoundError(f"{name} not found under {root}")
    return matches[0]


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path):
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(name: str, rows):
    rows = list(rows)
    path = OUT / name
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def coerce(value):
    if value is None or str(value).strip() == "":
        return None
    text = str(value).strip()
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    try:
        return float(text) if any(char in text.lower() for char in (".", "e")) else int(text)
    except ValueError:
        return value


def classify(reason: str) -> str:
    text = reason.lower()
    if "days since last trade" in text:
        return "Inactive"
    if "active days" in text:
        return "Too few active days"
    if "closed positions" in text:
        return "Insufficient closed sample"
    if "visible history" in text:
        return "Short visible history"
    if "roi" in text:
        return "Low/missing ROI"
    if "trades per" in text or "hft" in text:
        return "Too frequent / HFT"
    if "top-1" in text or "top-5" in text:
        return "Concentrated PnL"
    if "holding" in text:
        return "Short/missing holding period"
    if "hedged" in text:
        return "Hedged/complex positions"
    if "adverse" in text:
        return "Poor/missing price persistence"
    return "Other"


candidate_rows = read_csv(locate(FULL, "candidates.csv"))
leaderboards = load_json(locate(FULL, "raw_leaderboards.json"))
activity = load_json(locate(FULL, "raw_activity.json"))
base_closed = load_json(locate(FULL, "raw_closed_positions.json"))
current = load_json(locate(FULL, "raw_current_positions.json"))
recovered = load_json(locate(RECOVERY, "recovered_closed_positions.json"))
price_rows = [{key: coerce(value) for key, value in row.items()} for row in read_csv(locate(PRICE, "recovered_price_persistence.csv"))]
recovery_wallets = {str(wallet).lower() for wallet in load_json(Path("recovery_wallets.json"))}

activity_by = defaultdict(list)
for row in activity:
    activity_by[str(row.get("wallet", "")).lower()].append(row)
closed_by = defaultdict(list)
for row in base_closed:
    closed_by[str(row.get("wallet", "")).lower()].append(row)
recovered_by = defaultdict(list)
for row in recovered:
    recovered_by[str(row.get("wallet", "")).lower()].append(row)
for wallet in recovery_wallets:
    closed_by[wallet] = recovered_by.get(wallet, [])
current_by = defaultdict(list)
for row in current:
    current_by[str(row.get("wallet", "")).lower()].append(row)
price_by = defaultdict(list)
for row in price_rows:
    price_by[str(row.get("wallet", "")).lower()].append(row)

candidates = []
for old in candidate_rows:
    wallet = old["wallet"].lower()
    candidate_def = {
        "wallet": wallet,
        "username": old.get("username") or None,
        "x_username": old.get("x_username") or None,
        "memberships": [item.strip() for item in (old.get("leaderboard_memberships") or "").split(",") if item.strip()],
    }
    row = scan.metrics(candidate_def, activity_by.get(wallet, []), closed_by.get(wallet, []), current_by.get(wallet, []))
    wallet_prices = price_by.get(wallet, [])
    a1 = [float(item["adverse_1m"]) for item in wallet_prices if item.get("adverse_1m") is not None]
    a5 = [float(item["adverse_5m"]) for item in wallet_prices if item.get("adverse_5m") is not None]
    row["median_adverse_1m"] = statistics.median(a1) if a1 else None
    row["median_adverse_5m"] = statistics.median(a5) if a5 else None
    row["persistence_sample_size"] = max(len(a1), len(a5))
    if wallet_prices:
        failed = sum(bool(item.get("error")) for item in wallet_prices)
        empty = sum((item.get("history_points") in (None, 0)) and not item.get("error") for item in wallet_prices)
        notes = [row.get("data_limitations") or ""]
        if row["persistence_sample_size"] < 5:
            notes.append(f"Price-persistence sample small ({row['persistence_sample_size']})")
        if failed:
            notes.append(f"{failed} price-history request(s) failed")
        if empty:
            notes.append(f"{empty} price-history request(s) returned no points")
        row["data_limitations"] = "; ".join(item for item in notes if item)
    scan.decide(row)
    candidates.append(row)

candidates.sort(key=lambda row: (row["decision"] != "PASS", row["failed_rule_count"], -(row["copyability_score"] or 0)))
near = [row for row in candidates if row["decision"] != "PASS"][:50]

categories = {}
for row in candidates:
    key = row.get("primary_category") or "UNKNOWN"
    bucket = categories.setdefault(key, {"category": key, "candidates": 0, "passed": 0, "crypto_candidates": 0, "scores": []})
    bucket["candidates"] += 1
    bucket["passed"] += int(row["decision"] == "PASS")
    bucket["crypto_candidates"] += int(bool(row.get("is_crypto_candidate")))
    bucket["scores"].append(row.get("copyability_score") or 0)
category_rows = []
for bucket in categories.values():
    scores = bucket.pop("scores")
    bucket["avg_score"] = sum(scores) / len(scores) if scores else None
    category_rows.append(bucket)
category_rows.sort(key=lambda row: (-row["candidates"], row["category"]))

reason_counts = Counter()
for row in candidates:
    for reason in filter(None, (row.get("rejection_reasons") or "").split("; ")):
        reason_counts[classify(reason)] += 1
reason_rows = [{"reason": name, "failures": count} for name, count in reason_counts.most_common()]

manual = [
    {"candidate": "ИТОГ СКАНА", "wallet": "—", "verdict": "РЕАЛЬНЫЙ АВТОКОПИР НЕ ЗАПУСКАТЬ", "strengths": "430 уникальных кошельков; 600 строк лидербордов; 360 проверок задержки цены; закрытые позиции восстановлены после rate-limit.", "risks": "Ни один кошелёк не прошёл все заранее зафиксированные фильтры.", "recommendation": "Только paper-тест 30–50 сигналов; деньги не подключать до повторного прохождения фильтров.", "priority": 0},
    {"candidate": "0xf559…", "wallet": "0xf5592765133941c40c77e52bc56bbe7ec167f462", "verdict": "ЛУЧШИЙ АКТИВНЫЙ КАНДИДАТ ТОЛЬКО ДЛЯ PAPER-ТЕСТА", "strengths": "333 закрытые позиции; ROI ~45.7%; низкая концентрация прибыли; минутная цена не ухудшилась.", "risks": "55 дней истории; 10.6 сделки/день; удержание 0.60 суток; много Counter-Strike и спорта несмотря на тег POLITICS.", "recommendation": "Paper: только первый вход в новый не-live рынок, без спорта/киберспорта, лимит +2 цента, виртуальные $2, минимум 50 сигналов.", "priority": 1},
    {"candidate": "M2sx92kljs42", "wallet": "0xb4f2592e67c333e73c923547cfe05e768180e5fa", "verdict": "НАБЛЮДАТЬ КАК БОЛЕЕ МЕДЛЕННЫЙ ПОЛИТИЧЕСКИЙ СТИЛЬ", "strengths": "439 закрытых позиций; медианное удержание ~14 дней; активен; умеренная концентрация.", "risks": "ROI ~13.7% ниже порога; 61 день истории; 11.1 сделки/день и 5.1 сделки/рынок.", "recommendation": "Повторить оценку после 90 дней; не копировать дробные доливки, только первое существенное открытие рынка.", "priority": 2},
    {"candidate": "Contradict", "wallet": "0xe32b46a28b649031939a78ee47fca260758a626e", "verdict": "ИНТЕРЕСНЫЙ, НО СЛИШКОМ МАЛЕНЬКАЯ ВЫБОРКА", "strengths": "Медленный стиль; удержание 3.4 дня; ROI ~239%; минутное ухудшение 1.4 цента.", "risks": "22 закрытые позиции; 53 дня истории; последняя сделка 3.6 дня назад; топ-5 рынков дали ~66% положительной прибыли.", "recommendation": "Watchlist без копирования до 50 закрытых позиций и 90 дней истории.", "priority": 3},
    {"candidate": "Hugewinner", "wallet": "0xc8ec6d4cef5c5fe8409ef69303c37f05b678e8f1", "verdict": "НЕ ПОДХОДИТ СЕЙЧАС: НЕАКТИВЕН", "strengths": "311 закрытых позиций; ROI ~61.9%; низкая концентрация; долгие удержания.", "risks": "Не торговал 8.8 дня; activity обрезана на 500 строках; тег ECONOMICS скрывает спорт и короткую крипту.", "recommendation": "Только повторный скан после возобновления регулярной торговли.", "priority": 4},
    {"candidate": "SDTrading", "wallet": "0x16bb9951a36fce71e2ef57890b786145e0ba8492", "verdict": "НЕ КОПИРОВАТЬ: БЫСТРЫЙ СПОРТИВНЫЙ ПОРТФЕЛЬ", "strengths": "Активен; 358 закрытых позиций; ROI ~79%; распределённая прибыль.", "risks": "33 дня истории; 17.9 сделки/день; удержание 0.43 суток; сотни связанных спортивных позиций.", "recommendation": "Исключить из копитрейдинга.", "priority": 5},
    {"candidate": "kejsi", "wallet": "0xb64fe08cf7ebbf52cf2963bba89deb13b79ddc88", "verdict": "НЕ КОПИРОВАТЬ: СЛОЖНЫЙ СМЕШАННЫЙ ПОРТФЕЛЬ", "strengths": "335 закрытых позиций; ROI ~16.1%; активен.", "risks": "Много Dota 2, экспрессов и связанных TECH-порогов; обе стороны; удержание 0.12 суток; activity обрезана.", "recommendation": "Не копировать аккаунт целиком.", "priority": 6},
    {"candidate": "fred4332 — лучший CRYPTO", "wallet": "0x301e286fd88abb7cda0a19a24e4dc4823c4db752", "verdict": "КРИПТА ДЛЯ КОПИРОВАНИЯ НЕ ПОДХОДИТ", "strengths": "Активен; ROI ~29.3%; 500 закрытых позиций; BTC/ETH специализация.", "risks": "43 дня истории; 13.9 сделки/день; 8.9 сделки/рынок; удержание 0.52 суток; короткие up/down рынки.", "recommendation": "Не копировать; оставлен ради сравнения.", "priority": 7},
]

method = [
    {"section": "Universe", "metric": "Periods", "value": "WEEK, MONTH", "notes": "Official Data API leaderboard"},
    {"section": "Universe", "metric": "Categories", "value": "OVERALL, POLITICS, ECONOMICS, FINANCE, TECH, CRYPTO", "notes": "Crypto included and separately flagged"},
    {"section": "Formula", "metric": "Realized ROI", "value": "sum(realizedPnl) / sum(totalBought * avgPrice)", "notes": "Missing when denominator is unavailable"},
    {"section": "Formula", "metric": "Price persistence", "value": "price after 1m/5m - copied BUY price", "notes": "Positive is worse for delayed copier"},
    {"section": "Hard rule", "metric": "Max days since last trade", "value": 3, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Min active days in 30d", "value": 8, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Min closed positions", "value": 50, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Min visible history days", "value": 90, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Min realized ROI", "value": 0.15, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Max trades per active day", "value": 10, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Max trades per market", "value": 3, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Max top-1 positive PnL share", "value": 0.20, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Max top-5 positive PnL share", "value": 0.50, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Min median holding days", "value": 1, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Max hedged market share", "value": 0.05, "notes": "Fixed before scan"},
    {"section": "Hard rule", "metric": "Max median adverse 1m", "value": 0.02, "notes": "Fixed before scan"},
    {"section": "Run audit", "metric": "Leaderboard rows", "value": len(leaderboards), "notes": "12 pulls × 50"},
    {"section": "Run audit", "metric": "Unique candidates", "value": len(candidates), "notes": "Deduplicated by wallet"},
    {"section": "Run audit", "metric": "Recovered closed-position wallets", "value": len(recovery_wallets), "notes": "Recovery errors: 0"},
    {"section": "Run audit", "metric": "Price-persistence rows", "value": len(price_rows), "notes": "Top 30 preliminary candidates"},
    {"section": "Limitation", "metric": "Activity / closed caps", "value": "500 rows per wallet", "notes": "Very active histories may be truncated"},
    {"section": "Source", "metric": "Leaderboard", "value": "https://data-api.polymarket.com/v1/leaderboard", "notes": "Public read-only API"},
    {"section": "Source", "metric": "Activity", "value": "https://data-api.polymarket.com/activity", "notes": "Public read-only API"},
    {"section": "Source", "metric": "Closed positions", "value": "https://data-api.polymarket.com/closed-positions", "notes": "Public read-only API"},
    {"section": "Source", "metric": "Price history", "value": "https://clob.polymarket.com/prices-history", "notes": "Absolute start/end timestamps, 1-minute fidelity"},
]

summary = [
    {"metric": "Snapshot UTC", "value": NOW.isoformat()},
    {"metric": "Leaderboard rows", "value": len(leaderboards)},
    {"metric": "Unique candidates", "value": len(candidates)},
    {"metric": "Passed all hard rules", "value": sum(row["decision"] == "PASS" for row in candidates)},
    {"metric": "Crypto-tagged candidates", "value": sum(bool(row.get("is_crypto_candidate")) for row in candidates)},
    {"metric": "Price-persistence rows", "value": len(price_rows)},
    {"metric": "Usable 1-minute observations", "value": sum(row.get("adverse_1m") is not None for row in price_rows)},
    {"metric": "Price-history errors", "value": sum(bool(row.get("error")) for row in price_rows)},
    {"metric": "Conclusion", "value": "No candidate passes all fixed rules; no real auto-copy deployment"},
]

write_csv("summary.csv", summary)
write_csv("candidates.csv", candidates)
write_csv("near_misses.csv", near)
write_csv("price_persistence.csv", price_rows)
write_csv("category_mix.csv", category_rows)
write_csv("rejection_reasons.csv", reason_rows)
write_csv("manual_review.csv", manual)
write_csv("methodology.csv", method)
write_csv("raw_leaderboards.csv", leaderboards)
print(json.dumps({"candidates": len(candidates), "passed": sum(row["decision"] == "PASS" for row in candidates), "files": sorted(path.name for path in OUT.iterdir())}, indent=2))
