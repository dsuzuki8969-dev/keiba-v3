"""3 券種ハイブリッド振り分け方式 案 C vs 案 A — 6 ケース比較バックテスト
(plan: wiggly-cake / 2026-04-30 / Step 1-4 実装)

案 C ハイブリッド:
  Layer 1: 各券種の独立 EV 判定
  Layer 2: 5 ルール組合せ判定 (排他/保険/絞り)
  Layer 3: 信頼度ガード G-S / G-SA / G-NONE

案 A 独立閾値 (Phase A v3-fix 相当):
  Layer 1: 同じ EV 条件
  Layer 2: OR 結合 (各発動候補を全部買う)
  Layer 3: 信頼度ガード 3 種

使い方:
  python scripts/dispatch_backtest.py \\
      --start-date 20231201 \\
      --end-date   20260428 \\
      --output-csv tmp/dispatch_backtest_results.csv \\
      --output-json tmp/dispatch_backtest_results.json
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from itertools import combinations
from pathlib import Path
from typing import Any, Optional

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, ".")

# ────────────────────────────────────────────────────────────────
# 定数
# ────────────────────────────────────────────────────────────────
STAKE_PER_TICKET = 100  # 各点 100 円固定

# 印グループ
HONMEI_MARKS  = {"◉", "◎"}
TAIKOU_MARKS  = {"○", "〇"}
RENKA_MARKS   = {"▲"}
WIDE_MARKS    = {"△", "★"}
OANA_MARKS    = {"☆"}

MARK_PRIORITY: dict[str, int] = {
    "◉": 0, "◎": 1, "○": 2, "〇": 2,
    "▲": 3, "△": 4, "★": 5, "☆": 6,
}

# ☆ 動的補完
HOSHI_DYNAMIC_MIN_ODDS = 10.0

# 信頼度順序
CONF_ORDER = ["SS", "S", "A", "B", "C", "D"]

# 信頼度ガード
GUARD_S    = {"B", "C", "D", "SS"}   # S 帯のみ運用 (SS/B/C/D skip)
GUARD_SA   = {"C", "D", "SS", "B"}   # S+A 帯運用 (SS/C/D/B skip) — 実質 B もskip
GUARD_SA_V2 = {"C", "D", "SS"}       # S+A 帯運用 (SS/C/D skip のみ)
GUARD_NONE = set()                    # 全帯


# ── 信頼度ガード実定義 ─────────────────────────────
# G-S   : A/B/C/D/SS をスキップ (S のみ通過)
# G-SA  : B/C/D/SS をスキップ (S+A 通過)
# G-NONE: スキップなし
GUARD_DEFS: dict[str, set[str]] = {
    "G-S":    {"A", "B", "C", "D", "SS"},
    "G-SA":   {"B", "C", "D", "SS"},
    "G-NONE": set(),
}

# ────────────────────────────────────────────────────────────────
# ロガー
# ────────────────────────────────────────────────────────────────
logger = logging.getLogger("dispatch_backtest")


def setup_logger(log_path: Optional[str] = None) -> None:
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    if log_path:
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# ────────────────────────────────────────────────────────────────
# 払戻ユーティリティ (phase_a_v3 と同じロジック)
# ────────────────────────────────────────────────────────────────

def lookup_payout(payouts: dict, ticket_type: str, combo_nos: list[int]) -> int:
    """払戻額を返す。
    combo_nos: 単勝=[馬番], 馬単=[1着,2着], 三連複=[昇順3頭]
    """
    bucket = payouts.get(ticket_type)
    if bucket is None:
        return 0
    nos_str = "-".join(str(x) for x in combo_nos)

    def _match(item: Any) -> int:
        if not isinstance(item, dict):
            return 0
        if str(item.get("combo", "")) == nos_str:
            return int(item.get("payout", 0) or 0)
        return 0

    if isinstance(bucket, dict):
        return _match(bucket)
    if isinstance(bucket, list):
        for it in bucket:
            v = _match(it)
            if v:
                return v
    return 0


# ────────────────────────────────────────────────────────────────
# 馬リストユーティリティ
# ────────────────────────────────────────────────────────────────

def get_odds(h: dict) -> float:
    v = h.get("odds") or h.get("predicted_tansho_odds") or 0.0
    return float(v)


def filter_active(horses: list[dict]) -> list[dict]:
    return [h for h in horses
            if not h.get("is_tokusen_kiken") and not h.get("is_scratched")]


def get_mark_horses(horses: list[dict], mark_set: set[str]) -> list[dict]:
    result = [h for h in horses if h.get("mark", "") in mark_set]
    result.sort(key=lambda h: (
        MARK_PRIORITY.get(h.get("mark", ""), 9),
        -(h.get("composite") or 0)
    ))
    return result


def get_pivot(horses: list[dict]) -> Optional[dict]:
    """軸馬 (◉◎ 最優先 1 頭)"""
    cands = get_mark_horses(horses, HONMEI_MARKS)
    return cands[0] if cands else None


def resolve_hoshi(horses: list[dict]) -> Optional[dict]:
    """☆ 印馬 / 動的補完"""
    hoshi_list = get_mark_horses(horses, OANA_MARKS)
    if hoshi_list:
        return hoshi_list[0]
    unmarked = [
        h for h in horses
        if h.get("mark", "") in ("", None, "－", "-")
        and get_odds(h) >= HOSHI_DYNAMIC_MIN_ODDS
    ]
    if not unmarked:
        return None
    unmarked.sort(key=lambda h: -(h.get("win_prob") or 0.0))
    return unmarked[0]


# ────────────────────────────────────────────────────────────────
# Layer 1: 独立 EV 判定
# ────────────────────────────────────────────────────────────────

def layer1_sanrenpuku(horses: list[dict]) -> Optional[str]:
    """三連複動的フォーメーション発動ケース判定。
    Returns: "絞り" / "中" / "広" / None (見送り)
    """
    pivot = get_pivot(horses)
    if pivot is None:
        return None

    p_ev          = float(pivot.get("ev") or 0.0)
    p_place3_prob = float(pivot.get("place3_prob") or 0.0)

    if p_ev < 1.0:
        return None

    taikou_list = get_mark_horses(horses, TAIKOU_MARKS)
    renka_list  = get_mark_horses(horses, RENKA_MARKS)
    taikou = taikou_list[0] if taikou_list else None
    renka  = renka_list[0]  if renka_list  else None

    o_place3 = float(taikou.get("place3_prob") or 0.0) if taikou else 0.0
    r_place3 = float(renka.get("place3_prob")  or 0.0) if renka  else 0.0

    if (p_ev >= 1.8 and p_place3_prob >= 0.65
            and o_place3 >= 0.50 and taikou is not None):
        return "絞り"

    if (p_ev >= 1.3 and p_place3_prob >= 0.55
            and (o_place3 >= 0.40 or r_place3 >= 0.40)):
        return "中"

    return "広"


def layer1_umatan(horses: list[dict]) -> bool:
    """馬単中拡張発動判定。
    EV≥1.3 AND ◉◎ win_prob≥0.35
    """
    pivot = get_pivot(horses)
    if pivot is None:
        return False
    p_ev       = float(pivot.get("ev") or 0.0)
    p_win_prob = float(pivot.get("win_prob") or 0.0)
    return p_ev >= 1.3 and p_win_prob >= 0.35


def layer1_tansho(horses: list[dict]) -> bool:
    """単勝 T-4 発動判定。
    ◉◎ EV≥1.0 または ○ EV≥1.0
    """
    # ◉◎ の EV チェック
    honmei = get_mark_horses(horses, HONMEI_MARKS)
    if honmei and float(honmei[0].get("ev") or 0.0) >= 1.0:
        return True
    # ○ の EV チェック
    taikou = get_mark_horses(horses, TAIKOU_MARKS)
    if taikou and float(taikou[0].get("ev") or 0.0) >= 1.0:
        return True
    return False


# ────────────────────────────────────────────────────────────────
# チケット生成ロジック
# ────────────────────────────────────────────────────────────────

def build_sanrenpuku_tickets(horses: list[dict], payouts: dict,
                              case: str) -> list[tuple]:
    """三連複チケット生成 (case: 絞り/中/広)。
    Returns: [(combo_tuple, payout), ...]
    """
    pivot = get_pivot(horses)
    if pivot is None:
        return []

    hoshi = resolve_hoshi(horses)
    pivot_no = pivot["horse_no"]

    if case == "絞り":
        taikou_list = get_mark_horses(horses, TAIKOU_MARKS)
        if not taikou_list:
            return []
        taikou_no = taikou_list[0]["horse_no"]
        third_marks = {"▲", "△", "★"}
        thirds = get_mark_horses(horses, third_marks)
        if hoshi:
            existing = {h["horse_no"] for h in thirds}
            if hoshi["horse_no"] not in existing:
                thirds.append(hoshi)
        third_nos = [h["horse_no"] for h in thirds]
        tickets = []
        for t in third_nos:
            if t in (pivot_no, taikou_no):
                continue
            combo = tuple(sorted([pivot_no, taikou_no, t]))
            pb = lookup_payout(payouts, "三連複", list(combo))
            tickets.append((combo, pb))
        return tickets

    elif case == "中":
        second_marks = {"○", "〇", "▲"}
        second_horses = get_mark_horses(horses, second_marks)
        third_marks = {"○", "〇", "▲", "△", "★"}
        third_horses = get_mark_horses(horses, third_marks)
        if hoshi:
            existing = {h["horse_no"] for h in third_horses}
            if hoshi["horse_no"] not in existing:
                third_horses.append(hoshi)
        second_nos = [h["horse_no"] for h in second_horses]
        all_third_nos = [h["horse_no"] for h in third_horses]
        seen: set[tuple] = set()
        tickets = []
        for s_no in second_nos:
            if s_no == pivot_no:
                continue
            for t_no in all_third_nos:
                if t_no == pivot_no or t_no == s_no:
                    continue
                combo = tuple(sorted([pivot_no, s_no, t_no]))
                if combo in seen:
                    continue
                seen.add(combo)
                pb = lookup_payout(payouts, "三連複", list(combo))
                tickets.append((combo, pb))
        return tickets

    else:  # 広
        sub_all_marks = {"○", "〇", "▲", "△", "★"}
        sub_all = get_mark_horses(horses, sub_all_marks)
        if hoshi:
            existing = {h["horse_no"] for h in sub_all}
            if hoshi["horse_no"] not in existing:
                sub_all.append(hoshi)
        sub_nos = [h["horse_no"] for h in sub_all]
        partners = [p for p in sub_nos if p != pivot_no]
        tickets = []
        for p1, p2 in combinations(partners, 2):
            combo = tuple(sorted([pivot_no, p1, p2]))
            pb = lookup_payout(payouts, "三連複", list(combo))
            tickets.append((combo, pb))
        return tickets


def build_umatan_tickets(horses: list[dict], payouts: dict) -> list[tuple]:
    """馬単中拡張チケット生成 (◉◎→5頭流し + 逆軸2点 = 計7点)。
    Returns: [(combo_tuple, payout), ...]
    """
    pivot = get_pivot(horses)
    if pivot is None:
        return []
    pivot_no = pivot["horse_no"]

    # 2着候補: ○,▲,△,★,☆ (5頭)
    second_marks = {"○", "〇", "▲", "△", "★", "☆"}
    seconds = get_mark_horses(horses, second_marks)
    second_nos = [h["horse_no"] for h in seconds if h["horse_no"] != pivot_no][:5]

    tickets = []
    # 正方向: pivot → second
    for s_no in second_nos:
        combo = (pivot_no, s_no)
        pb = lookup_payout(payouts, "馬単", [pivot_no, s_no])
        tickets.append((combo, pb))

    # 逆方向: 上位2頭 → pivot
    for s_no in second_nos[:2]:
        combo = (s_no, pivot_no)
        pb = lookup_payout(payouts, "馬単", [s_no, pivot_no])
        tickets.append((combo, pb))

    return tickets


def build_tansho_tickets(horses: list[dict], payouts: dict) -> list[tuple]:
    """単勝 T-4 チケット生成 (◉◎+○ 2点)。
    Returns: [(combo_tuple, payout), ...]
    """
    tickets = []
    honmei = get_mark_horses(horses, HONMEI_MARKS)
    if honmei:
        no = honmei[0]["horse_no"]
        pb = lookup_payout(payouts, "単勝", [no])
        tickets.append(((no,), pb))
    taikou = get_mark_horses(horses, TAIKOU_MARKS)
    if taikou:
        no = taikou[0]["horse_no"]
        pb = lookup_payout(payouts, "単勝", [no])
        tickets.append(((no,), pb))
    return tickets


# ────────────────────────────────────────────────────────────────
# Layer 2: 組合せルール (案 C)
# ────────────────────────────────────────────────────────────────

def apply_layer2_case_c(
    sanrenpuku_case: Optional[str],
    umatan_active: bool,
    tansho_active: bool,
    horses: list[dict],
    payouts: dict,
) -> dict[str, list[tuple]]:
    """案 C 5 ルール適用。
    Returns: {"sanrenpuku": [...], "umatan": [...], "tansho": [...]}
    """
    result: dict[str, list[tuple]] = {
        "sanrenpuku": [],
        "umatan": [],
        "tansho": [],
    }

    # 全発動候補が空 → Rule 5: 見送り
    if sanrenpuku_case is None and not umatan_active and not tansho_active:
        return result

    # Rule 1: 絞り単独優位
    if sanrenpuku_case == "絞り":
        result["sanrenpuku"] = build_sanrenpuku_tickets(horses, payouts, "絞り")
        # 馬単・単勝は除外
        return result

    # Rule 2: 本命確信ボーナス (◉win_prob≥0.5 AND 三連複「中」発動)
    pivot = get_pivot(horses)
    if (pivot is not None
            and float(pivot.get("win_prob") or 0.0) >= 0.5
            and sanrenpuku_case == "中"):
        result["sanrenpuku"] = build_sanrenpuku_tickets(horses, payouts, "中")
        if tansho_active:
            result["tansho"] = build_tansho_tickets(horses, payouts)
        # 馬単は除外
        return result

    # Rule 3: 上位固定強 (馬単中拡張 AND 三連複「中」発動)
    if umatan_active and sanrenpuku_case == "中":
        result["sanrenpuku"] = build_sanrenpuku_tickets(horses, payouts, "中")
        result["umatan"]     = build_umatan_tickets(horses, payouts)
        # 単勝カット (周辺カバー済)
        return result

    # Rule 4: デフォルト (OR 結合)
    if sanrenpuku_case is not None:
        result["sanrenpuku"] = build_sanrenpuku_tickets(
            horses, payouts, sanrenpuku_case
        )
    if umatan_active:
        result["umatan"] = build_umatan_tickets(horses, payouts)
    if tansho_active:
        result["tansho"] = build_tansho_tickets(horses, payouts)

    return result


# ────────────────────────────────────────────────────────────────
# Layer 2: 案 A (OR 結合 / Phase A v3-fix 相当)
# ────────────────────────────────────────────────────────────────

def apply_layer2_case_a(
    sanrenpuku_case: Optional[str],
    umatan_active: bool,
    tansho_active: bool,
    horses: list[dict],
    payouts: dict,
) -> dict[str, list[tuple]]:
    """案 A 独立閾値: 各発動候補を全部買う (OR 結合)。"""
    result: dict[str, list[tuple]] = {
        "sanrenpuku": [],
        "umatan": [],
        "tansho": [],
    }
    if sanrenpuku_case is not None:
        result["sanrenpuku"] = build_sanrenpuku_tickets(
            horses, payouts, sanrenpuku_case
        )
    if umatan_active:
        result["umatan"] = build_umatan_tickets(horses, payouts)
    if tansho_active:
        result["tansho"] = build_tansho_tickets(horses, payouts)
    return result


# ────────────────────────────────────────────────────────────────
# 集計ユーティリティ
# ────────────────────────────────────────────────────────────────

def empty_case_stat() -> dict:
    return {
        # 総合
        "races_played":      0,
        "races_hit":         0,
        "total_stake":       0,
        "total_payback":     0,
        "drawdown_running":  0,
        "max_drawdown":      0,
        # 券種別
        "sanrenpuku": {"races": 0, "hit": 0, "stake": 0, "payback": 0},
        "umatan":     {"races": 0, "hit": 0, "stake": 0, "payback": 0},
        "tansho":     {"races": 0, "hit": 0, "stake": 0, "payback": 0},
    }


def update_stat(stat: dict, tickets_by_type: dict[str, list[tuple]]) -> None:
    total_stake   = 0
    total_payback = 0
    any_hit       = False

    for ttype in ("sanrenpuku", "umatan", "tansho"):
        tickets = tickets_by_type.get(ttype, [])
        if not tickets:
            continue
        n     = len(tickets)
        stake = n * STAKE_PER_TICKET
        pb    = sum(p for _, p in tickets)
        hit   = any(p > 0 for _, p in tickets)

        stat[ttype]["races"]   += 1
        stat[ttype]["hit"]     += 1 if hit else 0
        stat[ttype]["stake"]   += stake
        stat[ttype]["payback"] += pb

        total_stake   += stake
        total_payback += pb
        if hit:
            any_hit = True

    if total_stake > 0:
        stat["races_played"] += 1
        stat["races_hit"]    += 1 if any_hit else 0
        stat["total_stake"]   += total_stake
        stat["total_payback"] += total_payback

        # ドローダウン計算 (当該レース単位の損益)
        net = total_payback - total_stake
        if net < 0:
            stat["drawdown_running"] -= net  # 損失累積
        else:
            stat["drawdown_running"] = 0
        if stat["drawdown_running"] > stat["max_drawdown"]:
            stat["max_drawdown"] = stat["drawdown_running"]


def finalize_stat(stat: dict) -> dict:
    """集計結果を最終フォーマットに変換"""
    rp   = stat["races_played"]
    rh   = stat["races_hit"]
    ts   = stat["total_stake"]
    tpb  = stat["total_payback"]
    roi  = tpb / ts * 100 if ts > 0 else 0.0
    net  = tpb - ts
    avgstake = ts / rp if rp > 0 else 0.0
    hit_rate = rh / rp * 100 if rp > 0 else 0.0

    result: dict[str, Any] = {
        "races_played":       rp,
        "races_hit":          rh,
        "hit_rate":           round(hit_rate, 2),
        "total_stake":        ts,
        "total_payback":      tpb,
        "roi_pct":            round(roi, 2),
        "net_profit":         net,
        "max_drawdown":       stat["max_drawdown"],
        "avg_stake_per_race": round(avgstake, 1),
    }

    for ttype in ("sanrenpuku", "umatan", "tansho"):
        s = stat[ttype]
        t_roi = s["payback"] / s["stake"] * 100 if s["stake"] > 0 else 0.0
        result[ttype] = {
            "races":     s["races"],
            "hit":       s["hit"],
            "stake":     s["stake"],
            "payback":   s["payback"],
            "roi_pct":   round(t_roi, 2),
            "net_profit": s["payback"] - s["stake"],
        }

    return result


# ────────────────────────────────────────────────────────────────
# 1 日処理
# ────────────────────────────────────────────────────────────────

def process_day(date_str: str, case_stats: dict[str, dict]) -> int:
    """1 日分を処理し、各ケースの stat を更新。処理レース数を返す。"""
    pred_fp = Path(f"data/predictions/{date_str}_pred.json")
    res_fp  = Path(f"data/results/{date_str}_results.json")
    if not pred_fp.exists() or not res_fp.exists():
        return 0

    try:
        with pred_fp.open("r", encoding="utf-8") as f:
            pred = json.load(f)
        with res_fp.open("r", encoding="utf-8") as f:
            results = json.load(f)
    except (json.JSONDecodeError, OSError) as ex:
        logger.warning("[%s] SKIP JSON error: %s", date_str, ex)
        return 0

    n_races = 0
    for r in pred.get("races", []):
        race_id = str(r.get("race_id", ""))
        conf    = r.get("confidence", "C") or "C"

        horses = filter_active(r.get("horses", []))
        if not horses:
            continue

        rdata = results.get(race_id)
        if rdata is None:
            continue
        payouts = rdata.get("payouts", {})
        if not payouts:
            continue

        n_races += 1

        # Layer 1: 独立 EV 判定 (全ケース共通)
        sanrenpuku_case = layer1_sanrenpuku(horses)
        umatan_active   = layer1_umatan(horses)
        tansho_active   = layer1_tansho(horses)

        # 各ケースを処理
        for case_name, case_def in CASE_DEFS.items():
            guard_set  = GUARD_DEFS[case_def["guard"]]
            layer2_fn  = case_def["layer2_fn"]

            # Layer 3: 信頼度ガード
            if conf in guard_set:
                continue

            # Layer 2: 組合せルール適用
            tickets_by_type = layer2_fn(
                sanrenpuku_case, umatan_active, tansho_active,
                horses, payouts
            )

            # stat 更新
            update_stat(case_stats[case_name], tickets_by_type)

    return n_races


# ────────────────────────────────────────────────────────────────
# ケース定義
# ────────────────────────────────────────────────────────────────

CASE_DEFS: dict[str, dict] = {
    "C-S":    {"guard": "G-S",    "layer2_fn": apply_layer2_case_c},
    "C-SA":   {"guard": "G-SA",   "layer2_fn": apply_layer2_case_c},
    "C-NONE": {"guard": "G-NONE", "layer2_fn": apply_layer2_case_c},
    "A-S":    {"guard": "G-S",    "layer2_fn": apply_layer2_case_a},
    "A-SA":   {"guard": "G-SA",   "layer2_fn": apply_layer2_case_a},
    "A-NONE": {"guard": "G-NONE", "layer2_fn": apply_layer2_case_a},
}


# ────────────────────────────────────────────────────────────────
# メイン
# ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="3 券種ハイブリッド振り分け方式 6 ケース比較バックテスト"
    )
    parser.add_argument("--start-date", default="20231201", help="開始日 YYYYMMDD")
    parser.add_argument("--end-date",   default="20260428", help="終了日 YYYYMMDD")
    parser.add_argument("--output-csv",  default="tmp/dispatch_backtest_results.csv")
    parser.add_argument("--output-json", default="tmp/dispatch_backtest_results.json")
    parser.add_argument("--output-log",  default="tmp/dispatch_backtest.log")
    args = parser.parse_args()

    Path(args.output_log).parent.mkdir(parents=True, exist_ok=True)
    setup_logger(args.output_log)

    s, e = args.start_date, args.end_date
    start = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    end   = date(int(e[:4]), int(e[4:6]), int(e[6:8]))
    total_days = (end - start).days + 1

    logger.info("=" * 70)
    logger.info("案 C vs 案 A バックテスト開始: %s〜%s (%d 日間)", s, e, total_days)
    logger.info("6 ケース: %s", ", ".join(CASE_DEFS.keys()))
    logger.info("=" * 70)

    # stat 初期化
    case_stats: dict[str, dict] = {cn: empty_case_stat() for cn in CASE_DEFS}

    d = start
    processed = 0
    total_races = 0

    while d <= end:
        ds = d.strftime("%Y%m%d")
        n = process_day(ds, case_stats)
        total_races += n
        processed   += 1

        # 進捗表示 (30 日ごと & 最終)
        if processed % 30 == 0 or d == end:
            pct    = processed / total_days * 100
            filled = int(40 * processed / total_days)
            bar    = "█" * filled + "░" * (40 - filled)
            logger.info("[%s] %5.1f%% | %s | 処理日: %d / 総R: %d",
                        bar, pct, ds, processed, total_races)

        d += timedelta(days=1)

    # ────────────────────────────────────
    # 最終集計
    # ────────────────────────────────────
    logger.info("=" * 70)
    logger.info("集計完了: 処理日 %d / 総R %d", processed, total_races)

    final_results: dict[str, dict] = {}
    for cn, stat in case_stats.items():
        final_results[cn] = finalize_stat(stat)

    # コンソール表形式出力
    import io as _io
    out = _io.StringIO()
    header = f"{'ケース':<8} {'発動R':>6} {'的中R':>6} {'的中率':>7} {'投資額':>12} {'払戻額':>12} {'ROI':>8} {'純利益':>12} {'最大DD':>10}"
    sep    = "-" * len(header)
    print(sep, file=out)
    print(header, file=out)
    print(sep, file=out)
    for cn, fr in final_results.items():
        print(
            f"{cn:<8} {fr['races_played']:>6,} {fr['races_hit']:>6,} "
            f"{fr['hit_rate']:>6.1f}% {fr['total_stake']:>12,} "
            f"{fr['total_payback']:>12,} {fr['roi_pct']:>7.1f}% "
            f"{fr['net_profit']:>12,} {fr['max_drawdown']:>10,}",
            file=out
        )
    print(sep, file=out)
    print("", file=out)
    print("券種別 ROI:", file=out)
    for cn, fr in final_results.items():
        sr = fr["sanrenpuku"]["roi_pct"]
        ur = fr["umatan"]["roi_pct"]
        tr = fr["tansho"]["roi_pct"]
        print(f"  {cn:<8} 三連複={sr:>7.1f}% 馬単={ur:>7.1f}% 単勝={tr:>7.1f}%", file=out)
    print(sep, file=out)

    table_str = out.getvalue()
    logger.info("\n%s", table_str)

    # ────────────────────────────────────
    # CSV 出力
    # ────────────────────────────────────
    csv_path = Path(args.output_csv)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "case", "races_played", "races_hit", "hit_rate",
        "total_stake", "total_payback", "roi_pct", "net_profit",
        "max_drawdown", "avg_stake_per_race",
        "sanrenpuku_races", "sanrenpuku_hit", "sanrenpuku_stake",
        "sanrenpuku_payback", "sanrenpuku_roi_pct", "sanrenpuku_net",
        "umatan_races", "umatan_hit", "umatan_stake",
        "umatan_payback", "umatan_roi_pct", "umatan_net",
        "tansho_races", "tansho_hit", "tansho_stake",
        "tansho_payback", "tansho_roi_pct", "tansho_net",
    ]
    with csv_path.open("w", newline="", encoding="utf-8-sig") as csvf:
        writer = csv.DictWriter(csvf, fieldnames=fieldnames)
        writer.writeheader()
        for cn, fr in final_results.items():
            row = {
                "case":              cn,
                "races_played":      fr["races_played"],
                "races_hit":         fr["races_hit"],
                "hit_rate":          fr["hit_rate"],
                "total_stake":       fr["total_stake"],
                "total_payback":     fr["total_payback"],
                "roi_pct":           fr["roi_pct"],
                "net_profit":        fr["net_profit"],
                "max_drawdown":      fr["max_drawdown"],
                "avg_stake_per_race": fr["avg_stake_per_race"],
            }
            for ttype in ("sanrenpuku", "umatan", "tansho"):
                t = fr[ttype]
                row[f"{ttype}_races"]   = t["races"]
                row[f"{ttype}_hit"]     = t["hit"]
                row[f"{ttype}_stake"]   = t["stake"]
                row[f"{ttype}_payback"] = t["payback"]
                row[f"{ttype}_roi_pct"] = t["roi_pct"]
                row[f"{ttype}_net"]     = t["net_profit"]
            writer.writerow(row)

    logger.info("CSV 保存: %s", csv_path)

    # ────────────────────────────────────
    # JSON 出力
    # ────────────────────────────────────
    json_path = Path(args.output_json)
    output_json = {
        "meta": {
            "start_date":  s,
            "end_date":    e,
            "total_days":  total_days,
            "total_races": total_races,
        },
        "cases": final_results,
        "table": table_str,
    }
    with json_path.open("w", encoding="utf-8") as jf:
        json.dump(output_json, jf, ensure_ascii=False, indent=2)

    logger.info("JSON 保存: %s", json_path)
    logger.info("=" * 70)
    logger.info("バックテスト完了")


if __name__ == "__main__":
    main()
