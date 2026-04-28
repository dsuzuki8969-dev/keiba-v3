"""三連単フォーメーション戦略（Phase 3）の過去成績集計。

既存 pred.json + results.json を読み込み、 monthly_backtest.py と同じロジックで
三連単フォーメーションを組んだ場合の成績を集計する。

マスター指示 2026-04-22:
  - 表示: 収支 / 三連単F回収率 / 三連単F的中率（レース単位）
        / 予想R数 / 的中R数 / 購入額 / 払戻額
  - skip 対象: 信頼度 SS / C / D（SANRENTAN_SKIP_CONFIDENCES）
  - 各点 100円固定
"""
from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Dict, Optional

from scripts.monthly_backtest import build_sanrentan_tickets, get_payout
from src.calculator.betting import SANRENTAN_SKIP_CONFIDENCES


# year → (last_ts, summary_dict) のシンプルキャッシュ
_CACHE: Dict[str, tuple] = {}
_CACHE_TTL = 1800  # 30分


def _year_match(date_str: str, year_filter: str) -> bool:
    """year_filter が 'all' / '2024' / '2025' / '2026' 等を受け取り、
    date_str='20260422' が該当するかを返す。
    """
    if year_filter in ("all", "", None):
        return True
    # '2025年' のような指定があってもOK
    y = year_filter.replace("年", "").strip()
    return date_str.startswith(y)


def _compute(year_filter: str = "all") -> dict:
    """三連単フォーメーション戦略の集計を実行する。"""
    pred_dir = Path("data/predictions")
    res_dir = Path("data/results")
    # 予想R数, 的中R数, 購入, 払戻, skip数, 最高配当 などを積む
    stats = {
        "period_days": 0,
        "races_total": 0,     # 対象レース総数（pred にある）
        "races_played": 0,    # 実際に買ったレース
        "races_skipped": 0,   # skip（信頼度SS/C/D or 候補不足）
        "races_hit": 0,       # 1点以上的中
        "points": 0,
        "hits": 0,
        "stake": 0,
        "payback": 0,
        "max_payout": 0,
        "date_from": "",
        "date_to": "",
    }
    # 高配当 TOP10 用: 的中したチケットを積む
    hit_log = []  # [{payout, date, venue, race_no, race_name, combo, conf}, ...]
    # 自信度別集計
    by_conf = {}  # conf -> {played, hit, stake, payback}
    # 月別集計: "YYYY-MM" -> {stake, payback, played, hit}
    by_month = {}

    # pred ファイル走査
    pred_files = sorted(pred_dir.glob("*_pred.json"))
    for fp in pred_files:
        if "_prev" in fp.name:
            continue
        date_str = fp.name.split("_")[0]
        if len(date_str) != 8 or not date_str.isdigit():
            continue
        if not _year_match(date_str, year_filter):
            continue

        res_fp = res_dir / f"{date_str}_results.json"
        if not res_fp.exists():
            continue

        try:
            with fp.open(encoding="utf-8") as f:
                pred = json.load(f)
            with res_fp.open(encoding="utf-8") as f:
                results = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        stats["period_days"] += 1
        if not stats["date_from"] or date_str < stats["date_from"]:
            stats["date_from"] = date_str
        if not stats["date_to"] or date_str > stats["date_to"]:
            stats["date_to"] = date_str

        for r in pred.get("races", []):
            stats["races_total"] += 1
            race_id = str(r.get("race_id", ""))
            conf = r.get("confidence", "C")
            n = r.get("field_count") or len(r.get("horses", []))
            is_jra = r.get("is_jra", True)
            horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
            if not horses:
                continue
            rdata = results.get(race_id)
            if rdata is None:
                continue
            payouts = rdata.get("payouts", {})
            if not payouts or "三連単" not in payouts:
                continue

            # skip 対象
            if conf in SANRENTAN_SKIP_CONFIDENCES:
                stats["races_skipped"] += 1
                continue

            try:
                tickets = build_sanrentan_tickets(horses, n, is_jra)
            except Exception:
                continue
            if not tickets:
                stats["races_skipped"] += 1
                continue

            stats["races_played"] += 1
            # 自信度別集計の初期化
            bc = by_conf.setdefault(conf, {"played": 0, "hit": 0, "stake": 0, "payback": 0})
            bc["played"] += 1
            # 月別集計の初期化（YYYYMM → YYYY-MM）
            month_key = f"{date_str[:4]}-{date_str[4:6]}"
            bm = by_month.setdefault(month_key, {"played": 0, "hit": 0, "stake": 0, "payback": 0})
            bm["played"] += 1

            race_hit = False
            race_payback = 0
            race_max_pp = 0
            for t in tickets:
                stake = t["stake"]
                pp = get_payout(payouts, t)  # 100円ベース
                payback = pp * (stake // 100)
                hit = 1 if payback > 0 else 0
                stats["points"] += 1
                stats["hits"] += hit
                stats["stake"] += stake
                stats["payback"] += payback
                bc["stake"] += stake
                bc["payback"] += payback
                bm["stake"] += stake
                bm["payback"] += payback
                if hit:
                    race_hit = True
                    race_payback += payback
                    if pp > race_max_pp:
                        race_max_pp = pp
                    if pp > stats["max_payout"]:
                        stats["max_payout"] = pp
            if race_hit:
                stats["races_hit"] += 1
                bc["hit"] += 1
                bm["hit"] += 1
                # 高配当ログ: 1レースの最高配当 1件を登録（10件を超えたら最小を捨てる）
                hit_log.append({
                    "payout": race_max_pp,
                    "date": date_str,
                    "venue": r.get("venue", ""),
                    "race_no": r.get("race_no", 0),
                    "race_name": (r.get("race_name", "") or "")[:40],
                    "conf": conf,
                    "race_payback": race_payback,
                })

    # 派生指標
    stake = stats["stake"]
    payback = stats["payback"]
    played = stats["races_played"]
    race_hit = stats["races_hit"]
    pts = stats["points"]
    hits = stats["hits"]

    stats["balance"] = payback - stake
    stats["roi_pct"] = round(payback / stake * 100, 1) if stake > 0 else 0.0
    stats["race_hit_rate_pct"] = round(race_hit / played * 100, 1) if played > 0 else 0.0
    stats["point_hit_rate_pct"] = round(hits / pts * 100, 2) if pts > 0 else 0.0

    # 自信度別（表示順: SS > S > A > B > C > D の配列で返す）
    conf_order = ["SS", "S", "A", "B", "C", "D"]
    stats["by_confidence"] = []
    for c in conf_order:
        v = by_conf.get(c)
        if not v or v["played"] == 0:
            continue
        r_hit = v["hit"] / v["played"] * 100 if v["played"] > 0 else 0
        c_roi = v["payback"] / v["stake"] * 100 if v["stake"] > 0 else 0
        stats["by_confidence"].append({
            "confidence": c,
            "played": v["played"],
            "hit": v["hit"],
            "hit_rate_pct": round(r_hit, 1),
            "stake": v["stake"],
            "payback": v["payback"],
            "roi_pct": round(c_roi, 1),
        })

    # 高配当 TOP10（1レース最高配当基準）
    hit_log.sort(key=lambda x: -x["payout"])
    stats["top10_payouts"] = hit_log[:10]

    # 月別（トレンドチャート用 / YYYY-MM 昇順）
    months_sorted = sorted(by_month.keys())
    stats["monthly"] = []
    # 累積 ROI も計算
    cum_stake, cum_payback = 0, 0
    for m in months_sorted:
        v = by_month[m]
        cum_stake += v["stake"]
        cum_payback += v["payback"]
        stats["monthly"].append({
            "month": m,
            "played": v["played"],
            "hit": v["hit"],
            "stake": v["stake"],
            "payback": v["payback"],
            "balance": v["payback"] - v["stake"],
            "roi_pct": round(v["payback"] / v["stake"] * 100, 1) if v["stake"] > 0 else 0.0,
            "cum_roi_pct": round(cum_payback / cum_stake * 100, 1) if cum_stake > 0 else 0.0,
        })

    return stats


def get_sanrentan_summary(year_filter: str = "all", force: bool = False) -> dict:
    """キャッシュ付き集計 API。force=True でキャッシュを無視して再計算。"""
    if not force:
        cached = _CACHE.get(year_filter)
        if cached and (time.time() - cached[0]) < _CACHE_TTL:
            return cached[1]
    result = _compute(year_filter)
    _CACHE[year_filter] = (time.time(), result)
    return result


def invalidate_cache() -> None:
    """結果取得 / pred 更新後に外部から呼んでキャッシュを破棄する。"""
    _CACHE.clear()
