"""Phase A v2 — 単勝・馬連・三連複 軸固定フォーメーション ROI バックテスト
(マスター仕様 T-048 v2 / 2026-04-29 確定)

対象券種: 単勝 / 馬連 / 三連複 (3 種のみ)
戦略パターン: 軸固定 (1〜2 頭) フォーメーション 計 20 戦略
集計軸 (slice):
  - 信頼度: SS / S / A / B / C / D
  - 軸単勝オッズ帯: 1.0-1.5 / 1.5-2.5 / 2.5-5.0 / 5.0-10.0 / 10.0-30.0 / 30.0+
  - 戦略

各点 100 円固定。

使い方:
  python scripts/phase_a_v2_axis_formation.py \\
      --start-date 20240101 \\
      --end-date   20260428 \\
      --output-csv tmp/phase_a_v2_results.csv \\
      --output-json tmp/phase_a_v2_results.json

"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from itertools import combinations
from pathlib import Path
from typing import Any, Optional

# エンコーディング強制設定
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, ".")

# ────────────────────────────────────────────────────────────────
# 定数
# ────────────────────────────────────────────────────────────────
STAKE_PER_TICKET = 100  # 各点 100 円固定

# 印の優先順位 (tie-break 用)
MARK_PRIORITY: dict[str, int] = {
    "◉": 0, "◎": 1, "○": 2, "〇": 2,
    "▲": 3, "△": 4, "★": 5, "☆": 6,
}

# 印グループ定義
HONMEI_MARKS = {"◉", "◎"}       # 軸 (◉◎)
TAIKOU_MARKS = {"○", "〇"}       # 対抗
RENKA_MARKS  = {"▲"}             # 連下▲
WIDE_MARKS   = {"△", "★"}       # 広め印
OANA_MARKS   = {"☆"}             # 穴

# 信頼度順序
CONF_ORDER = ["SS", "S", "A", "B", "C", "D"]

# 軸オッズ帯 (軸馬の単勝オッズ)
ODDS_BANDS = [
    ("1.0-1.5",   1.0,   1.5),
    ("1.5-2.5",   1.5,   2.5),
    ("2.5-5.0",   2.5,   5.0),
    ("5.0-10.0",  5.0,  10.0),
    ("10.0-30.0", 10.0, 30.0),
    ("30.0+",     30.0, 9999.0),
]

# ────────────────────────────────────────────────────────────────
# 払戻 JSON パーサー (日本語キー / 英字キー 両対応)
# ────────────────────────────────────────────────────────────────
_TICKET_TYPE_ALIAS = {
    "単勝": "tansho",
    "馬連": "umaren",
    "三連複": "sanrenpuku",
}


def _get_payout_bucket(payouts: dict, ja_key: str):
    """日本語キー優先 → 英字 alias フォールバック"""
    if not isinstance(payouts, dict):
        return None
    if ja_key in payouts:
        return payouts[ja_key]
    en_key = _TICKET_TYPE_ALIAS.get(ja_key)
    if en_key and en_key in payouts:
        return payouts[en_key]
    return None


def lookup_payout(payouts: dict, ticket_type: str, combo_nos: list[int]) -> int:
    """払い戻し金額を返す。
    combo_nos: 単勝=[馬番], 馬連=[昇順2頭], 三連複=[昇順3頭]
    形式: dict {"combo": "6-8-10", "payout": 830}
          list [{"combo": ..., "payout": ...}, ...]
    """
    bucket = _get_payout_bucket(payouts, ticket_type)
    if bucket is None:
        return 0

    nos_str = "-".join(str(x) for x in combo_nos)

    def _match(item) -> int:
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
# 馬フィルタ / 印抽出ユーティリティ
# ────────────────────────────────────────────────────────────────

def get_odds(h: dict) -> float:
    """馬の単勝オッズ取得。なければ predicted_tansho_odds フォールバック。"""
    v = h.get("odds") or h.get("predicted_tansho_odds") or 0.0
    return float(v)


def filter_active(horses: list[dict]) -> list[dict]:
    """取消・危険馬を除外"""
    return [h for h in horses
            if not h.get("is_tokusen_kiken") and not h.get("is_scratched")]


def get_mark_horses(horses: list[dict], mark_set: set[str]) -> list[dict]:
    """mark が mark_set に含まれる馬を印優先度 + composite 降順で返す"""
    result = [h for h in horses if h.get("mark", "") in mark_set]
    result.sort(key=lambda h: (
        MARK_PRIORITY.get(h.get("mark", ""), 9),
        -(h.get("composite") or 0)
    ))
    return result


def get_pivot(horses: list[dict]) -> Optional[dict]:
    """軸馬 (◉◎ の最優先 1 頭) を返す"""
    cands = get_mark_horses(horses, HONMEI_MARKS)
    return cands[0] if cands else None


def odds_band_label(odds: float) -> str:
    for label, lo, hi in ODDS_BANDS:
        if lo <= odds < hi:
            return label
    return "30.0+"


# ────────────────────────────────────────────────────────────────
# 統計レコード管理
# ────────────────────────────────────────────────────────────────

def empty_stat() -> dict:
    return {
        "races":       0,  # 対象レース数
        "tickets":     0,  # 購入券数
        "stake":       0,  # 投資合計
        "payback":     0,  # 払戻合計
        "hit_races":   0,  # 的中レース数
        "hit_tickets": 0,  # 的中券数
    }


def add_stat(stat: dict, n_tickets: int, payback: int,
             hit_bool: bool, n_hit: int) -> None:
    stat["races"]       += 1
    stat["tickets"]     += n_tickets
    stat["stake"]       += n_tickets * STAKE_PER_TICKET
    stat["payback"]     += payback
    stat["hit_races"]   += 1 if hit_bool else 0
    stat["hit_tickets"] += n_hit


def calc_roi(stat: dict) -> float:
    return stat["payback"] / stat["stake"] * 100 if stat["stake"] else 0.0


def calc_race_hit_rate(stat: dict) -> float:
    return stat["hit_races"] / stat["races"] * 100 if stat["races"] else 0.0


def calc_ticket_hit_rate(stat: dict) -> float:
    return stat["hit_tickets"] / stat["tickets"] * 100 if stat["tickets"] else 0.0


# ────────────────────────────────────────────────────────────────
# 的中判定 共通ロジック
# ────────────────────────────────────────────────────────────────

def stat_tuple_from_tickets(tickets_paybacks: list[tuple]) -> tuple[int, int, bool, int]:
    """(n_tickets, total_payback, any_hit, hit_count)"""
    n    = len(tickets_paybacks)
    pb   = sum(p for _, p in tickets_paybacks)
    hits = sum(1 for _, p in tickets_paybacks if p > 0)
    return n, pb, hits > 0, hits


# ────────────────────────────────────────────────────────────────
# 単勝戦略 (T-1 〜 T-5)
# ────────────────────────────────────────────────────────────────

def _tansho_buy(horse_nos: list[int], payouts: dict) -> list[tuple]:
    """単勝: 馬番リストで買う → (馬番, 払戻) のリスト"""
    return [(no, lookup_payout(payouts, "単勝", [no])) for no in horse_nos]


def strat_T1(horses: list[dict], payouts: dict) -> Optional[tuple]:
    """T-1: ◉◎ 1頭軸 (最優先1頭)"""
    pivot = get_pivot(horses)
    if not pivot:
        return None
    return stat_tuple_from_tickets(_tansho_buy([pivot["horse_no"]], payouts))


def strat_T2(horses: list[dict], payouts: dict) -> Optional[tuple]:
    """T-2: ○ 1頭軸"""
    cands = get_mark_horses(horses, TAIKOU_MARKS)
    if not cands:
        return None
    return stat_tuple_from_tickets(_tansho_buy([cands[0]["horse_no"]], payouts))


def strat_T3(horses: list[dict], payouts: dict) -> Optional[tuple]:
    """T-3: ▲ 1頭軸 (比較用)"""
    cands = get_mark_horses(horses, {"▲"})
    if not cands:
        return None
    return stat_tuple_from_tickets(_tansho_buy([cands[0]["horse_no"]], payouts))


def strat_T4(horses: list[dict], payouts: dict) -> Optional[tuple]:
    """T-4: ◉◎ + ○ 2頭軸 (2点)"""
    pivot = get_pivot(horses)
    taikou = get_mark_horses(horses, TAIKOU_MARKS)
    if not pivot or not taikou:
        return None
    nos = list(dict.fromkeys([pivot["horse_no"], taikou[0]["horse_no"]]))
    if len(nos) < 2:
        return None
    return stat_tuple_from_tickets(_tansho_buy(nos, payouts))


def strat_T5(horses: list[dict], payouts: dict) -> Optional[tuple]:
    """T-5: ◉◎ + ○ + ▲ 3頭軸 (3点)"""
    pivot  = get_pivot(horses)
    taikou = get_mark_horses(horses, TAIKOU_MARKS)
    renka  = get_mark_horses(horses, {"▲"})
    if not pivot:
        return None
    nos = [pivot["horse_no"]]
    if taikou:
        nos.append(taikou[0]["horse_no"])
    if renka:
        nos.append(renka[0]["horse_no"])
    nos = list(dict.fromkeys(nos))
    if len(nos) < 2:
        return None
    return stat_tuple_from_tickets(_tansho_buy(nos, payouts))


# ────────────────────────────────────────────────────────────────
# 馬連戦略 (U-1a 〜 U-2c)
# ────────────────────────────────────────────────────────────────

def _umaren_pairs(pivot_no: int, partner_nos: list[int],
                   payouts: dict) -> list[tuple]:
    """軸1頭 + 相手リストで馬連を買う → (combo_tuple, 払戻) のリスト"""
    result = []
    for pno in partner_nos:
        if pno == pivot_no:
            continue
        combo = tuple(sorted([pivot_no, pno]))
        pb = lookup_payout(payouts, "馬連", list(combo))
        result.append((combo, pb))
    return result


def _umaren_box_pairs(nos_list: list[int], payouts: dict) -> list[tuple]:
    """馬番リストの全組合せ馬連を買う"""
    result = []
    for a, b in combinations(nos_list, 2):
        combo = tuple(sorted([a, b]))
        pb = lookup_payout(payouts, "馬連", list(combo))
        result.append((combo, pb))
    return result


def strat_U1a(horses, payouts):
    """U-1a: ◉◎ → ○ (1点)"""
    pivot  = get_pivot(horses)
    taikou = get_mark_horses(horses, TAIKOU_MARKS)
    if not pivot or not taikou:
        return None
    pairs = _umaren_pairs(pivot["horse_no"], [taikou[0]["horse_no"]], payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_U1b(horses, payouts):
    """U-1b: ◉◎ → {○,▲} (2点)"""
    pivot   = get_pivot(horses)
    partners = get_mark_horses(horses, {"○", "〇", "▲"})
    if not pivot or not partners:
        return None
    nos = [h["horse_no"] for h in partners]
    pairs = _umaren_pairs(pivot["horse_no"], nos, payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_U1c(horses, payouts):
    """U-1c: ◉◎ → {○,▲,△} (3点)"""
    pivot    = get_pivot(horses)
    partners = get_mark_horses(horses, {"○", "〇", "▲", "△"})
    if not pivot or not partners:
        return None
    nos = [h["horse_no"] for h in partners]
    pairs = _umaren_pairs(pivot["horse_no"], nos, payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_U1d(horses, payouts):
    """U-1d: ◉◎ → {○,▲,△,★} (4点)"""
    pivot    = get_pivot(horses)
    partners = get_mark_horses(horses, {"○", "〇", "▲", "△", "★"})
    if not pivot or not partners:
        return None
    nos = [h["horse_no"] for h in partners]
    pairs = _umaren_pairs(pivot["horse_no"], nos, payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_U2a(horses, payouts):
    """U-2a: ◉◎-○ (1点・組合せ確定)"""
    pivot  = get_pivot(horses)
    taikou = get_mark_horses(horses, TAIKOU_MARKS)
    if not pivot or not taikou:
        return None
    combo = tuple(sorted([pivot["horse_no"], taikou[0]["horse_no"]]))
    pb = lookup_payout(payouts, "馬連", list(combo))
    return stat_tuple_from_tickets([(combo, pb)])


def strat_U2b(horses, payouts):
    """U-2b: ◉◎-○ + ◉◎-▲ + ○-▲ (3点 / ◉◎○▲ 3頭ボックス相当)"""
    pivot  = get_pivot(horses)
    taikou = get_mark_horses(horses, TAIKOU_MARKS)
    renka  = get_mark_horses(horses, {"▲"})
    if not pivot or not taikou or not renka:
        return None
    nos = list(dict.fromkeys([pivot["horse_no"], taikou[0]["horse_no"], renka[0]["horse_no"]]))
    if len(nos) < 3:
        return None
    pairs = _umaren_box_pairs(nos, payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_U2c(horses, payouts):
    """U-2c: ◉◎○▲△ 4頭ボックス相当 (6点)"""
    pivot  = get_pivot(horses)
    taikou = get_mark_horses(horses, TAIKOU_MARKS)
    renka  = get_mark_horses(horses, {"▲"})
    wide   = get_mark_horses(horses, {"△"})
    if not pivot or not taikou or not renka or not wide:
        return None
    nos = list(dict.fromkeys([
        pivot["horse_no"],
        taikou[0]["horse_no"],
        renka[0]["horse_no"],
        wide[0]["horse_no"],
    ]))
    if len(nos) < 4:
        return None
    pairs = _umaren_box_pairs(nos, payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


# ────────────────────────────────────────────────────────────────
# 三連複戦略 (S-1a 〜 S-2d)
# ────────────────────────────────────────────────────────────────

def _sanrenpuku_1axis(pivot_no: int, partner_nos: list[int],
                       payouts: dict) -> list[tuple]:
    """1頭軸: pivot_no を固定 + partner_nos から 2 頭組合せ"""
    result = []
    partners = [p for p in partner_nos if p != pivot_no]
    for p1, p2 in combinations(partners, 2):
        combo = tuple(sorted([pivot_no, p1, p2]))
        pb = lookup_payout(payouts, "三連複", list(combo))
        result.append((combo, pb))
    return result


def _sanrenpuku_2axis(pivot1_no: int, pivot2_no: int,
                       third_nos: list[int], payouts: dict) -> list[tuple]:
    """2頭軸: pivot1 + pivot2 を固定 + third_nos から 1 頭"""
    result = []
    for t in third_nos:
        if t in (pivot1_no, pivot2_no):
            continue
        combo = tuple(sorted([pivot1_no, pivot2_no, t]))
        pb = lookup_payout(payouts, "三連複", list(combo))
        result.append((combo, pb))
    return result


# --- 1頭軸流し ---

def strat_S1a(horses, payouts):
    """S-1a: ◉◎ → {○,▲} (相手 2頭 = 1点 / ◉◎-○-▲)"""
    pivot    = get_pivot(horses)
    partners = get_mark_horses(horses, {"○", "〇", "▲"})
    if not pivot or len(partners) < 2:
        return None
    nos = [h["horse_no"] for h in partners]
    pairs = _sanrenpuku_1axis(pivot["horse_no"], nos[:2], payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_S1b(horses, payouts):
    """S-1b: ◉◎ → {○,▲,△} (相手 3頭 = 3点)"""
    pivot    = get_pivot(horses)
    partners = get_mark_horses(horses, {"○", "〇", "▲", "△"})
    if not pivot or len(partners) < 2:
        return None
    nos = [h["horse_no"] for h in partners]
    pairs = _sanrenpuku_1axis(pivot["horse_no"], nos, payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_S1c(horses, payouts):
    """S-1c: ◉◎ → {○,▲,△,★} (相手 4頭 = 6点)"""
    pivot    = get_pivot(horses)
    partners = get_mark_horses(horses, {"○", "〇", "▲", "△", "★"})
    if not pivot or len(partners) < 2:
        return None
    nos = [h["horse_no"] for h in partners]
    pairs = _sanrenpuku_1axis(pivot["horse_no"], nos, payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_S1d(horses, payouts):
    """S-1d: ◉◎ → {○,▲,△,★,☆} (相手 5頭 = 10点)"""
    pivot    = get_pivot(horses)
    partners = get_mark_horses(horses, {"○", "〇", "▲", "△", "★", "☆"})
    if not pivot or len(partners) < 2:
        return None
    nos = [h["horse_no"] for h in partners]
    pairs = _sanrenpuku_1axis(pivot["horse_no"], nos, payouts)
    return stat_tuple_from_tickets(pairs) if pairs else None


# --- 2頭軸流し ---

def strat_S2a(horses, payouts):
    """S-2a: ◉◎-○ → ▲ (1点 / ◉◎-○-▲)"""
    pivot  = get_pivot(horses)
    taikou = get_mark_horses(horses, TAIKOU_MARKS)
    renka  = get_mark_horses(horses, {"▲"})
    if not pivot or not taikou or not renka:
        return None
    pairs = _sanrenpuku_2axis(
        pivot["horse_no"], taikou[0]["horse_no"],
        [renka[0]["horse_no"]], payouts
    )
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_S2b(horses, payouts):
    """S-2b: ◉◎-○ → {▲,△} (2点)"""
    pivot   = get_pivot(horses)
    taikou  = get_mark_horses(horses, TAIKOU_MARKS)
    thirds  = get_mark_horses(horses, {"▲", "△"})
    if not pivot or not taikou or not thirds:
        return None
    third_nos = [h["horse_no"] for h in thirds]
    pairs = _sanrenpuku_2axis(
        pivot["horse_no"], taikou[0]["horse_no"],
        third_nos, payouts
    )
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_S2c(horses, payouts):
    """S-2c: ◉◎-○ → {▲,△,★} (3点)"""
    pivot   = get_pivot(horses)
    taikou  = get_mark_horses(horses, TAIKOU_MARKS)
    thirds  = get_mark_horses(horses, {"▲", "△", "★"})
    if not pivot or not taikou or not thirds:
        return None
    third_nos = [h["horse_no"] for h in thirds]
    pairs = _sanrenpuku_2axis(
        pivot["horse_no"], taikou[0]["horse_no"],
        third_nos, payouts
    )
    return stat_tuple_from_tickets(pairs) if pairs else None


def strat_S2d(horses, payouts):
    """S-2d: ◉◎-○ → {▲,△,★,☆} (4点)"""
    pivot   = get_pivot(horses)
    taikou  = get_mark_horses(horses, TAIKOU_MARKS)
    thirds  = get_mark_horses(horses, {"▲", "△", "★", "☆"})
    if not pivot or not taikou or not thirds:
        return None
    third_nos = [h["horse_no"] for h in thirds]
    pairs = _sanrenpuku_2axis(
        pivot["horse_no"], taikou[0]["horse_no"],
        third_nos, payouts
    )
    return stat_tuple_from_tickets(pairs) if pairs else None


# ────────────────────────────────────────────────────────────────
# 全戦略テーブル (ticket_type, strategy_id, description, func)
# ────────────────────────────────────────────────────────────────
STRATEGIES: list[tuple[str, str, str, Any]] = [
    # 単勝 5戦略
    ("単勝", "T-1", "◉◎1頭軸(1点)",          strat_T1),
    ("単勝", "T-2", "○1頭軸(1点)",             strat_T2),
    ("単勝", "T-3", "▲1頭軸(1点)比較用",       strat_T3),
    ("単勝", "T-4", "◉◎+○2頭(2点)",           strat_T4),
    ("単勝", "T-5", "◉◎+○+▲3頭(3点)",         strat_T5),
    # 馬連 1頭軸流し 4戦略
    ("馬連", "U-1a", "◉◎→○(1点)",             strat_U1a),
    ("馬連", "U-1b", "◉◎→○▲(2点)",            strat_U1b),
    ("馬連", "U-1c", "◉◎→○▲△(3点)",           strat_U1c),
    ("馬連", "U-1d", "◉◎→○▲△★(4点)",          strat_U1d),
    # 馬連 2頭軸流し 3戦略
    ("馬連", "U-2a", "◉◎-○確定(1点)",          strat_U2a),
    ("馬連", "U-2b", "◉◎○▲BOX(3点)",           strat_U2b),
    ("馬連", "U-2c", "◉◎○▲△BOX(6点)",          strat_U2c),
    # 三連複 1頭軸流し 4戦略
    ("三連複", "S-1a", "◉◎→○▲(1点)",          strat_S1a),
    ("三連複", "S-1b", "◉◎→○▲△(3点)",         strat_S1b),
    ("三連複", "S-1c", "◉◎→○▲△★(6点)",        strat_S1c),
    ("三連複", "S-1d", "◉◎→○▲△★☆(10点)",      strat_S1d),
    # 三連複 2頭軸流し 4戦略
    ("三連複", "S-2a", "◉◎-○→▲(1点)",         strat_S2a),
    ("三連複", "S-2b", "◉◎-○→▲△(2点)",        strat_S2b),
    ("三連複", "S-2c", "◉◎-○→▲△★(3点)",       strat_S2c),
    ("三連複", "S-2d", "◉◎-○→▲△★☆(4点)",      strat_S2d),
]

# 券種ごとのデータ存在確認キー
TICKET_KEY_CHECK = {
    "単勝":  ("単勝",  "tansho"),
    "馬連":  ("馬連",  "umaren"),
    "三連複": ("三連複", "sanrenpuku"),
}


# ────────────────────────────────────────────────────────────────
# 1 日処理
# ────────────────────────────────────────────────────────────────

def process_day(date_str: str) -> Optional[dict]:
    """1 日分を処理し、{(strat_id, conf, odds_band): stat} を返す"""
    pred_fp = Path(f"data/predictions/{date_str}_pred.json")
    res_fp  = Path(f"data/results/{date_str}_results.json")
    if not pred_fp.exists() or not res_fp.exists():
        return None

    try:
        with pred_fp.open("r", encoding="utf-8") as f:
            pred = json.load(f)
        with res_fp.open("r", encoding="utf-8") as f:
            results = json.load(f)
    except (json.JSONDecodeError, OSError) as ex:
        print(f"[{date_str}] SKIP JSON error: {ex}", file=sys.stderr)
        return None

    # key: (strat_id, conf, odds_band) -> stat dict
    stats: dict[tuple, dict] = defaultdict(empty_stat)
    n_races = 0

    for r in pred.get("races", []):
        n_races += 1
        race_id = str(r.get("race_id", ""))
        conf    = r.get("confidence", "C")
        conf_norm = conf if conf in CONF_ORDER else "C"

        # 取消馬を除外して有効馬リストを作成
        horses = filter_active(r.get("horses", []))
        if not horses:
            continue

        rdata = results.get(race_id)
        if rdata is None:
            continue
        payouts = rdata.get("payouts", {})
        if not payouts:
            continue

        # 軸馬 (◉◎) のオッズ帯を決定
        pivot = get_pivot(horses)
        if pivot:
            p_odds = get_odds(pivot)
            p_band = odds_band_label(p_odds) if p_odds > 0 else "30.0+"
        else:
            # 軸なし → 後段の各戦略が None を返すが、帯は仮割り当て
            p_band = "30.0+"

        for ticket_type, strat_id, _desc, func in STRATEGIES:
            # 当該券種の払戻データが存在するか確認
            ja_key, en_key = TICKET_KEY_CHECK[ticket_type]
            if ja_key not in payouts and en_key not in payouts:
                continue

            try:
                result = func(horses, payouts)
            except Exception:
                continue
            if result is None:
                continue

            n_tickets, payback, hit_bool, n_hit = result
            if n_tickets == 0:
                continue

            # 信頼度 × オッズ帯 の全 slice に追記
            # (conf_norm+band, conf_norm+ALL, ALL+band, ALL+ALL)
            for conf_key in (conf_norm, "ALL"):
                for band_key in (p_band, "ALL"):
                    key = (strat_id, conf_key, band_key)
                    add_stat(stats[key], n_tickets, payback, hit_bool, n_hit)

    return {"stats": dict(stats), "n_races": n_races}


# ────────────────────────────────────────────────────────────────
# メイン
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Phase A v2 — 単勝・馬連・三連複 軸固定フォーメーション ROI バックテスト"
    )
    parser.add_argument("--start-date", default="20240101", help="開始日 YYYYMMDD")
    parser.add_argument("--end-date",   default="20260428", help="終了日 YYYYMMDD")
    parser.add_argument("--output-csv",  default="tmp/phase_a_v2_results.csv")
    parser.add_argument("--output-json", default="tmp/phase_a_v2_results.json")
    args = parser.parse_args()

    s, e = args.start_date, args.end_date
    start = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    end   = date(int(e[:4]), int(e[4:6]), int(e[6:8]))

    grand: dict[tuple, dict] = defaultdict(empty_stat)
    total_races = 0
    n_days = 0

    total_days = (end - start).days + 1
    d = start
    processed = 0

    print(f"[Phase A v2 バックテスト開始] {s}〜{e} ({total_days} 日間)")
    print(f"券種: 単勝/馬連/三連複 / 戦略数: {len(STRATEGIES)} / "
          f"信頼度 {len(CONF_ORDER)+1} 段階 / オッズ帯 {len(ODDS_BANDS)+1} 帯 (ALL 含)")
    print()

    while d <= end:
        ds = d.strftime("%Y%m%d")
        result = process_day(ds)
        processed += 1

        if result:
            n_days += 1
            total_races += result["n_races"]
            for key, stat in result["stats"].items():
                target = grand[key]
                for k, v in stat.items():
                    target[k] += v

        # 進捗バー (30 日ごと & 最終)
        if processed % 30 == 0 or d == end:
            pct = processed / total_days * 100
            bar_len = 40
            filled = int(bar_len * processed / total_days)
            bar = "█" * filled + "░" * (bar_len - filled)
            print(
                f"[{bar}] {pct:5.1f}% | {ds} | "
                f"処理日: {n_days}日 / 総R: {total_races:,}",
                flush=True
            )

        d += timedelta(days=1)

    print()
    print("=" * 100)
    print(f"集計完了: 処理日 {n_days}日 / 総レース {total_races:,}R / スライス数 {len(grand):,}")
    print("=" * 100)

    # ────────────────────────────────────────────────
    # CSV / JSON 出力準備
    # ────────────────────────────────────────────────
    Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)

    strat_desc_map  = {sid: (ttype, desc) for ttype, sid, desc, _ in STRATEGIES}
    rows = []

    for (strat_id, conf, odds_band), stat in grand.items():
        if stat["races"] == 0:
            continue
        ticket_type, strategy_desc = strat_desc_map.get(strat_id, ("不明", "不明"))
        roi = calc_roi(stat)
        net = stat["payback"] - stat["stake"]
        rows.append({
            "ticket_type":     ticket_type,
            "strategy_id":     strat_id,
            "strategy_desc":   strategy_desc,
            "confidence":      conf,
            "odds_band":       odds_band,
            "races":           stat["races"],
            "tickets":         stat["tickets"],
            "stake":           stat["stake"],
            "payback":         stat["payback"],
            "hit_races":       stat["hit_races"],
            "hit_tickets":     stat["hit_tickets"],
            "race_hit_rate":   round(calc_race_hit_rate(stat), 2),
            "ticket_hit_rate": round(calc_ticket_hit_rate(stat), 2),
            "roi":             round(roi, 2),
            "net_profit":      net,
        })

    rows.sort(key=lambda r: -r["roi"])

    # ── CSV ──
    csv_path = Path(args.output_csv)
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)
    print(f"CSV 出力: {csv_path} ({csv_path.stat().st_size:,} bytes, {len(rows):,} rows)")

    # ── JSON ──
    json_data = {
        "meta": {
            "start_date":    s,
            "end_date":      e,
            "n_days":        n_days,
            "total_races":   total_races,
            "n_slices":      len(rows),
            "n_strategies":  len(STRATEGIES),
        },
        "rows": rows,
    }
    json_path = Path(args.output_json)
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    print(f"JSON 出力: {json_path} ({json_path.stat().st_size:,} bytes)")

    # ────────────────────────────────────────────────
    # サマリーレポート出力
    # ────────────────────────────────────────────────
    # ALL/ALL の行のみでサマリーを構築
    all_rows = [r for r in rows if r["confidence"] == "ALL" and r["odds_band"] == "ALL"]
    all_rows.sort(key=lambda r: -r["roi"])

    # 黒字化シナリオ (ROI ≥ 100% / ≥ 30R 以上)
    profitable = [r for r in all_rows if r["roi"] >= 100.0 and r["races"] >= 30]

    header = (f"{'順':>3} {'券種':>4} {'戦略ID':>5} {'説明':>18} "
              f"{'R':>6} {'投資':>12} {'払戻':>12} "
              f"{'R的中%':>7} {'券的中%':>7} {'ROI%':>8} {'純損益':>13}")

    def print_row(i, r):
        print(
            f"{i:>3} {r['ticket_type']:>4} {r['strategy_id']:>5} {r['strategy_desc']:>18} "
            f"{r['races']:>6,} {r['stake']:>12,} {r['payback']:>12,} "
            f"{r['race_hit_rate']:>6.1f}% {r['ticket_hit_rate']:>6.1f}% "
            f"{r['roi']:>7.1f}% {r['net_profit']:>+13,}"
        )

    # ── 黒字化シナリオ ──
    print()
    print("─" * 110)
    print(f"黒字化シナリオ (ROI ≥ 100% / ≥30R): {len(profitable)} 件")
    print("─" * 110)
    if profitable:
        print(header)
        for i, r in enumerate(profitable, 1):
            print_row(i, r)
    else:
        print("  黒字化シナリオなし (ROI < 100% または ≥30R 未満)")

    # ── 単勝 Top 5 ──
    print()
    print("─" * 110)
    print("単勝 Top 5 (ALL conf / ALL オッズ帯 / ≥30R)")
    print("─" * 110)
    tansho_rows = [r for r in all_rows if r["ticket_type"] == "単勝" and r["races"] >= 30]
    print(header)
    for i, r in enumerate(tansho_rows[:5], 1):
        print_row(i, r)

    # ── 馬連 Top 5 ──
    print()
    print("─" * 110)
    print("馬連 Top 5 (ALL conf / ALL オッズ帯 / ≥30R)")
    print("─" * 110)
    umaren_rows = [r for r in all_rows if r["ticket_type"] == "馬連" and r["races"] >= 30]
    print(header)
    for i, r in enumerate(umaren_rows[:5], 1):
        print_row(i, r)

    # ── 三連複 Top 5 ──
    print()
    print("─" * 110)
    print("三連複 Top 5 (ALL conf / ALL オッズ帯 / ≥30R)")
    print("─" * 110)
    san_rows = [r for r in all_rows if r["ticket_type"] == "三連複" and r["races"] >= 30]
    print(header)
    for i, r in enumerate(san_rows[:5], 1):
        print_row(i, r)

    # ── Bottom 5 (各券種) ──
    print()
    print("─" * 110)
    print("Bottom 5 (各券種 / 赤字深い順 / ALL conf / ALL オッズ帯 / ≥30R)")
    print("─" * 110)
    for label, rows_filtered in [
        ("単勝", sorted(tansho_rows, key=lambda r: r["roi"])),
        ("馬連", sorted(umaren_rows, key=lambda r: r["roi"])),
        ("三連複", sorted(san_rows,   key=lambda r: r["roi"])),
    ]:
        print(f"\n【{label} Bottom 5】")
        print(header)
        for i, r in enumerate(rows_filtered[:5], 1):
            print_row(i, r)

    # ── 券種ごとの最善戦略 ──
    print()
    print("─" * 110)
    print("券種ごとの最善戦略 (ROI 最高 / ≥30R)")
    print("─" * 110)
    print(header)
    for ticket_type_label in ("単勝", "馬連", "三連複"):
        t_rows = [r for r in all_rows
                  if r["ticket_type"] == ticket_type_label and r["races"] >= 30]
        if t_rows:
            print_row(1, t_rows[0])

    # ── 信頼度別 Top 3 ──
    print()
    print("─" * 110)
    print("信頼度別 Top 3 (ALL オッズ帯 / ≥20R)")
    print("─" * 110)
    for conf in CONF_ORDER:
        conf_rows = [r for r in rows
                     if r["confidence"] == conf and r["odds_band"] == "ALL"
                     and r["races"] >= 20]
        conf_rows.sort(key=lambda r: -r["roi"])
        if not conf_rows:
            continue
        print(f"\n  【{conf} 帯】 Top 3:")
        for i, r in enumerate(conf_rows[:3], 1):
            print(f"    {i}. {r['ticket_type']} {r['strategy_id']} {r['strategy_desc']:>18} "
                  f"R={r['races']:,} ROI={r['roi']:.1f}% 純益={r['net_profit']:+,}")

    # ── オッズ帯別 傾向 ──
    print()
    print("─" * 110)
    print("オッズ帯別 傾向 (ALL conf / ≥20R / 各帯の最高 ROI 戦略)")
    print("─" * 110)
    for band_label, _, _ in ODDS_BANDS:
        band_rows = [r for r in rows
                     if r["confidence"] == "ALL" and r["odds_band"] == band_label
                     and r["races"] >= 20]
        band_rows.sort(key=lambda r: -r["roi"])
        if band_rows:
            r = band_rows[0]
            print(f"  {band_label:>12}: {r['ticket_type']:>4} {r['strategy_id']:>5} "
                  f"ROI={r['roi']:.1f}% R={r['races']:,} 純益={r['net_profit']:+,}")

    # ── 副次発見 ──
    print()
    print("─" * 110)
    print("副次発見 (全信頼度/全オッズ帯 ≥ 30R)")
    print("─" * 110)
    # 全 slice で ROI ≥ 100 の件数
    total_profitable = sum(
        1 for r in rows
        if r["roi"] >= 100.0 and r["races"] >= 30
        and r["confidence"] != "ALL" and r["odds_band"] != "ALL"
    )
    print(f"  特定 slice で ROI ≥ 100%: {total_profitable} 件 (信頼度 × オッズ帯 個別 slice)")

    # 信頼度別全体 ROI
    print("\n  信頼度別 全体 ROI (全券種合算 / ALL オッズ帯):")
    for conf in CONF_ORDER:
        conf_all = [r for r in rows if r["confidence"] == conf and r["odds_band"] == "ALL"]
        total_st = sum(r["stake"] for r in conf_all)
        total_pb = sum(r["payback"] for r in conf_all)
        if total_st:
            print(f"    {conf}: 投資={total_st:,} 払戻={total_pb:,} ROI={total_pb/total_st*100:.1f}%")

    # オッズ帯別全体 ROI
    print("\n  オッズ帯別 全体 ROI (全券種合算 / ALL 信頼度):")
    for band_label, _, _ in ODDS_BANDS:
        band_all = [r for r in rows if r["odds_band"] == band_label and r["confidence"] == "ALL"]
        total_st = sum(r["stake"] for r in band_all)
        total_pb = sum(r["payback"] for r in band_all)
        if total_st:
            print(f"    {band_label:>12}: 投資={total_st:,} 払戻={total_pb:,} ROI={total_pb/total_st*100:.1f}%")

    print()
    print("[████████████████████████████████████████] 100% | Phase A v2 バックテスト完了")
    return 0


if __name__ == "__main__":
    sys.exit(main())
