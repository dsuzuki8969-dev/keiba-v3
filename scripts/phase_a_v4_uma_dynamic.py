"""Phase A v4 — 馬連・馬単 動的フォーメーション ROI バックテスト
(T-051 v2 / 2026-04-29)

概要:
  馬連・馬単の動的フォーメーション集計。
  EV フォールバック / ☆ 補完 / 取消馬除外は phase_a_v3_fix と完全同一。

馬連ケース:
  中 (umaren_mid):  ◉◎.ev_fb >= 1.3 AND ◉◎.place2_prob >= 0.55 → ◉◎-{○,▲,△} 3点
  広 (umaren_wide): ◉◎.ev_fb >= 1.0 (デフォルト)              → ◉◎-{○,▲,△,★,☆} 5点
  見送り: 上記未達

馬単ケース:
  戦略 A 中・基本 (umatan_mid_basic):    ◉◎.ev_fb >= 1.3 AND ◉◎.win_prob >= 0.35
                                          → ◉◎→{○,▲,△,★,☆} 5点 (1着固定)
  戦略 B 中・拡張 (umatan_mid_extended): 同条件
                                          → 基本5点 + ○→◉◎ (1点) + ▲→◉◎ (1点) = 7点

slice 軸: 券種・戦略 / 信頼度 / 軸単勝オッズ帯

使い方:
  python scripts/phase_a_v4_uma_dynamic.py \\
      --start-date 20240101 \\
      --end-date   20260428 \\
      --output-csv  tmp/phase_a_v4_results.csv \\
      --output-json tmp/phase_a_v4_results.json \\
      --output-log  tmp/phase_a_v4_execution.log
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
from pathlib import Path
from typing import Optional

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
HONMEI_MARKS = {"◉", "◎"}   # 軸 (◉◎)
TAIKOU_MARKS = {"○", "〇"}   # 対抗
RENKA_MARKS  = {"▲"}         # 連下▲
WIDE_MARKS   = {"△", "★"}   # 広め印
OANA_MARKS   = {"☆"}         # 穴

# ☆ 動的補完の条件
HOSHI_DYNAMIC_MIN_ODDS = 10.0  # 単勝オッズ 10 倍以上

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

# 戦略ラベル (スライス軸)
STRAT_UMAREN_MID      = "umaren_mid"       # 馬連 中 3点
STRAT_UMAREN_WIDE     = "umaren_wide"      # 馬連 広 5点
STRAT_UMATAN_BASIC    = "umatan_mid_basic"    # 馬単 中・基本 5点
STRAT_UMATAN_EXTENDED = "umatan_mid_extended" # 馬単 中・拡張 7点

STRAT_ORDER = [
    STRAT_UMAREN_MID, STRAT_UMAREN_WIDE,
    STRAT_UMATAN_BASIC, STRAT_UMATAN_EXTENDED,
]

# ────────────────────────────────────────────────────────────────
# ロガー設定
# ────────────────────────────────────────────────────────────────
logger = logging.getLogger("phase_a_v4")


def setup_logger(log_path: str) -> None:
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# ────────────────────────────────────────────────────────────────
# 払戻 JSON パーサー (日本語キー / 英字キー 両対応)
# ────────────────────────────────────────────────────────────────
_TICKET_TYPE_ALIAS = {
    "単勝":   "tansho",
    "馬連":   "umaren",
    "馬単":   "umatan",
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
    combo_nos: 単勝=[馬番], 馬連=[昇順2頭], 馬単=[順序固定2頭]
    形式: dict {"combo": "6-8", "payout": 830}
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
# EV フォールバック (phase_a_v3_fix と完全同一)
# ────────────────────────────────────────────────────────────────

# EV 統計グローバルカウンタ
_ev_stat = {
    "ev_positive": 0,  # ev > 0 (そのまま使用)
    "ev_fallback": 0,  # ev <= 0 → win_prob × odds で計算
    "ev_invalid":  0,  # ev <= 0 かつ win_prob/odds も無効
}


def get_ev_tracked(horse: dict) -> float:
    """EV フォールバック付き取得。統計カウント版。"""
    ev_raw = horse.get("ev") or 0
    if ev_raw > 0:
        _ev_stat["ev_positive"] += 1
        return float(ev_raw)
    win_prob = horse.get("win_prob") or 0
    odds = horse.get("odds") or horse.get("predicted_tansho_odds") or 0
    if win_prob > 0 and odds > 0:
        _ev_stat["ev_fallback"] += 1
        return float(win_prob) * float(odds)
    _ev_stat["ev_invalid"] += 1
    return 0.0


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
# ☆ 動的補完ロジック (phase_a_v3_fix と完全同一)
# ────────────────────────────────────────────────────────────────

def resolve_hoshi(horses: list[dict]) -> Optional[dict]:
    """☆印馬を返す。存在しない場合は動的補完を試みる。"""
    hoshi_list = get_mark_horses(horses, OANA_MARKS)
    if hoshi_list:
        return hoshi_list[0]

    # 動的補完: 無印馬かつ odds >= 10.0 の win_prob 最高位
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
# 馬連 フォーメーション判定
# ────────────────────────────────────────────────────────────────

def process_umaren(horses: list[dict], payouts: dict,
                   race_id: str) -> dict[str, list[tuple]]:
    """馬連 動的フォーメーション判定。
    馬連払戻データがなければ両ケースとも空。
    Returns: {strat_label: [(combo, payout), ...]}
    """
    result: dict[str, list[tuple]] = {
        STRAT_UMAREN_MID:  [],
        STRAT_UMAREN_WIDE: [],
    }

    # 馬連払戻データ存在確認
    if _get_payout_bucket(payouts, "馬連") is None:
        return result

    pivot = get_pivot(horses)
    if pivot is None:
        return result

    pivot_no = pivot["horse_no"]
    p_ev          = get_ev_tracked(pivot)
    p_place2_prob = float(pivot.get("place2_prob") or 0.0)

    # 対抗 (○)・連下 (▲)・広め印 (△★)・穴 (☆ 動的補完)
    taikou_list = get_mark_horses(horses, TAIKOU_MARKS)   # ○
    renka_list  = get_mark_horses(horses, RENKA_MARKS)    # ▲
    wide_list   = get_mark_horses(horses, WIDE_MARKS)     # △★
    hoshi       = resolve_hoshi(horses)                   # ☆

    def _build_umaren_tickets(partner_set: list[dict]) -> list[tuple]:
        tickets = []
        partner_nos = [h["horse_no"] for h in partner_set if h["horse_no"] != pivot_no]
        for p_no in partner_nos:
            combo = tuple(sorted([pivot_no, p_no]))  # 馬連は順不同 → sorted 正規化
            pb = lookup_payout(payouts, "馬連", list(combo))
            tickets.append((combo, pb))
        return tickets

    # ─── 中ケース: ev >= 1.3 AND place2_prob >= 0.55 → ◉◎-{○,▲,△} 3点 ───
    if p_ev >= 1.3 and p_place2_prob >= 0.55:
        mid_partners = taikou_list + renka_list + wide_list
        # △★のうち△のみに絞る: WIDE_MARKS = {"△", "★"} → 中ケースは △ のみ
        # 仕様: 中 = ◉◎ → {○,▲,△} 3点
        delta_list = [h for h in horses if h.get("mark", "") == "△"]
        mid_partners = taikou_list + renka_list + delta_list
        # 重複除去 (horse_no で管理)
        seen_nos: set[int] = set()
        dedup = []
        for h in mid_partners:
            if h["horse_no"] not in seen_nos:
                seen_nos.add(h["horse_no"])
                dedup.append(h)
        result[STRAT_UMAREN_MID] = _build_umaren_tickets(dedup)

    # ─── 広ケース: ev >= 1.0 → ◉◎-{○,▲,△,★,☆} 5点 ───
    if p_ev >= 1.0:
        wide_partners = taikou_list + renka_list + wide_list
        if hoshi:
            existing_nos = {h["horse_no"] for h in wide_partners}
            if hoshi["horse_no"] not in existing_nos:
                wide_partners.append(hoshi)
        result[STRAT_UMAREN_WIDE] = _build_umaren_tickets(wide_partners)

    return result


# ────────────────────────────────────────────────────────────────
# 馬単 フォーメーション判定
# ────────────────────────────────────────────────────────────────

def process_umatan(horses: list[dict], payouts: dict,
                   race_id: str) -> dict[str, list[tuple]]:
    """馬単 動的フォーメーション判定。
    発動条件: ◉◎.ev_fb >= 1.3 AND ◉◎.win_prob >= 0.35
    Returns: {strat_label: [(combo, payout), ...]}
    """
    result: dict[str, list[tuple]] = {
        STRAT_UMATAN_BASIC:    [],
        STRAT_UMATAN_EXTENDED: [],
    }

    # 馬単払戻データ存在確認
    if _get_payout_bucket(payouts, "馬単") is None:
        return result

    pivot = get_pivot(horses)
    if pivot is None:
        return result

    pivot_no  = pivot["horse_no"]
    p_ev      = get_ev_tracked(pivot)
    p_win_prob = float(pivot.get("win_prob") or 0.0)

    # 発動条件チェック
    if p_ev < 1.3 or p_win_prob < 0.35:
        return result

    # 対抗 (○)・連下 (▲)・広め印 (△★)・穴 (☆)
    taikou_list = get_mark_horses(horses, TAIKOU_MARKS)   # ○
    renka_list  = get_mark_horses(horses, RENKA_MARKS)    # ▲
    wide_list   = get_mark_horses(horses, WIDE_MARKS)     # △★
    hoshi       = resolve_hoshi(horses)                   # ☆ 動的補完

    # 2・3着候補 {○,▲,△,★,☆}
    sub_partners = taikou_list + renka_list + wide_list
    if hoshi:
        existing_nos = {h["horse_no"] for h in sub_partners}
        if hoshi["horse_no"] not in existing_nos:
            sub_partners.append(hoshi)

    # ─── 戦略 A: 基本 5点 ◉◎→{○,▲,△,★,☆} 1着固定 ───
    basic_tickets: list[tuple] = []
    for partner in sub_partners:
        p_no = partner["horse_no"]
        if p_no == pivot_no:
            continue
        combo = (pivot_no, p_no)   # 馬単は順序固定 (1着→2着)
        pb = lookup_payout(payouts, "馬単", list(combo))
        basic_tickets.append((combo, pb))
    result[STRAT_UMATAN_BASIC] = basic_tickets

    # ─── 戦略 B: 拡張 7点 = 基本5点 + ○→◉◎ (1点) + ▲→◉◎ (1点) ───
    extended_tickets = list(basic_tickets)  # 基本 5点をコピー

    # 拡張 2点: ○ → ◉◎ (2着)、▲ → ◉◎ (2着)
    seen_ext: set[tuple] = {t[0] for t in extended_tickets}
    for ext_source_list, label in [(taikou_list, "○"), (renka_list, "▲")]:
        if not ext_source_list:
            continue
        src = ext_source_list[0]  # ○ or ▲ の 1頭目
        if src["horse_no"] == pivot_no:
            continue
        ext_combo = (src["horse_no"], pivot_no)  # src → ◉◎ (2着)
        if ext_combo not in seen_ext:
            seen_ext.add(ext_combo)
            pb = lookup_payout(payouts, "馬単", list(ext_combo))
            extended_tickets.append((ext_combo, pb))

    result[STRAT_UMATAN_EXTENDED] = extended_tickets
    return result


# ────────────────────────────────────────────────────────────────
# 統計レコード管理
# ────────────────────────────────────────────────────────────────

def empty_stat() -> dict:
    return {
        "target_races":  0,  # 対象レース数 (有効馬&払戻データあり)
        "buy_races":     0,  # 購入レース数
        "tickets":       0,  # 購入券数
        "stake":         0,  # 投資合計
        "payback":       0,  # 払戻合計
        "hit_races":     0,  # 的中レース数
        "hit_tickets":   0,  # 的中券数
    }


def add_stat(stat: dict, n_tickets: int, payback: int,
             hit_bool: bool, n_hit: int) -> None:
    """購入があった 1 レース分を集計に追加"""
    stat["buy_races"]   += 1
    stat["tickets"]     += n_tickets
    stat["stake"]       += n_tickets * STAKE_PER_TICKET
    stat["payback"]     += payback
    stat["hit_races"]   += 1 if hit_bool else 0
    stat["hit_tickets"] += n_hit


def calc_roi(stat: dict) -> float:
    return stat["payback"] / stat["stake"] * 100 if stat["stake"] else 0.0


def calc_race_hit_rate(stat: dict) -> float:
    return stat["hit_races"] / stat["buy_races"] * 100 if stat["buy_races"] else 0.0


def calc_ticket_hit_rate(stat: dict) -> float:
    return stat["hit_tickets"] / stat["tickets"] * 100 if stat["tickets"] else 0.0


# ────────────────────────────────────────────────────────────────
# 1 日処理
# ────────────────────────────────────────────────────────────────

def process_day(date_str: str) -> Optional[dict]:
    """1 日分を処理し、集計結果を返す"""
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
        logger.warning("[%s] SKIP JSON error: %s", date_str, ex)
        return None

    # key: (strat_label, conf, odds_band) -> stat dict
    stats: dict[tuple, dict] = defaultdict(empty_stat)
    n_races = 0

    for r in pred.get("races", []):
        n_races += 1
        race_id   = str(r.get("race_id", ""))
        conf      = r.get("confidence", "C")
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
            p_band = "30.0+"

        # ─── 馬連 処理 ───
        try:
            umaren_result = process_umaren(horses, payouts, race_id)
        except Exception as ex:
            logger.warning("[%s] %s 馬連フォーメーション判定エラー: %s", date_str, race_id, ex)
            umaren_result = {STRAT_UMAREN_MID: [], STRAT_UMAREN_WIDE: []}

        for strat_label, tickets in umaren_result.items():
            if not tickets:
                continue
            n_tickets = len(tickets)
            payback   = sum(p for _, p in tickets)
            hit_count = sum(1 for _, p in tickets if p > 0)
            hit_bool  = hit_count > 0

            for conf_key in (conf_norm, "ALL"):
                for band_key in (p_band, "ALL"):
                    key = (strat_label, conf_key, band_key)
                    add_stat(stats[key], n_tickets, payback, hit_bool, hit_count)

        # ─── 馬単 処理 ───
        try:
            umatan_result = process_umatan(horses, payouts, race_id)
        except Exception as ex:
            logger.warning("[%s] %s 馬単フォーメーション判定エラー: %s", date_str, race_id, ex)
            umatan_result = {STRAT_UMATAN_BASIC: [], STRAT_UMATAN_EXTENDED: []}

        for strat_label, tickets in umatan_result.items():
            if not tickets:
                continue
            n_tickets = len(tickets)
            payback   = sum(p for _, p in tickets)
            hit_count = sum(1 for _, p in tickets if p > 0)
            hit_bool  = hit_count > 0

            for conf_key in (conf_norm, "ALL"):
                for band_key in (p_band, "ALL"):
                    key = (strat_label, conf_key, band_key)
                    add_stat(stats[key], n_tickets, payback, hit_bool, hit_count)

    return {
        "stats":   dict(stats),
        "n_races": n_races,
    }


# ────────────────────────────────────────────────────────────────
# メイン
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Phase A v4 — 馬連・馬単 動的フォーメーション ROI バックテスト"
    )
    parser.add_argument("--start-date", default="20240101", help="開始日 YYYYMMDD")
    parser.add_argument("--end-date",   default="20260428", help="終了日 YYYYMMDD")
    parser.add_argument("--output-csv",  default="tmp/phase_a_v4_results.csv")
    parser.add_argument("--output-json", default="tmp/phase_a_v4_results.json")
    parser.add_argument("--output-log",  default="tmp/phase_a_v4_execution.log")
    args = parser.parse_args()

    Path(args.output_log).parent.mkdir(parents=True, exist_ok=True)
    setup_logger(args.output_log)

    s, e = args.start_date, args.end_date
    start = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    end   = date(int(e[:4]), int(e[4:6]), int(e[6:8]))

    grand: dict[tuple, dict] = defaultdict(empty_stat)
    total_races = 0
    n_days      = 0
    total_days  = (end - start).days + 1

    logger.info("Phase A v4 馬連・馬単 動的フォーメーション バックテスト開始: %s〜%s (%d 日間)", s, e, total_days)
    logger.info("EV フォールバック: ev <= 0 の場合 win_prob × odds で代替計算")
    logger.info("戦略: umaren_mid(3点) / umaren_wide(5点) / umatan_mid_basic(5点) / umatan_mid_extended(7点)")

    d = start
    processed = 0

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
            pct    = processed / total_days * 100
            filled = int(40 * processed / total_days)
            bar    = "█" * filled + "░" * (40 - filled)
            logger.info(
                "[%s] %5.1f%% | %s | 処理日: %d日 / 総R: %d",
                bar, pct, ds, n_days, total_races
            )

        d += timedelta(days=1)

    # ────────────────────────────────────────────────
    # EV フォールバック統計
    # ────────────────────────────────────────────────
    ev_total = sum(_ev_stat.values())
    logger.info("=" * 80)
    logger.info("EV フォールバック統計 (軸馬評価 全件):")
    logger.info("  ev > 0 (既存値使用)      : %d (%.1f%%)",
                _ev_stat["ev_positive"],
                _ev_stat["ev_positive"] / ev_total * 100 if ev_total else 0)
    logger.info("  ev <= 0 → フォールバック  : %d (%.1f%%)",
                _ev_stat["ev_fallback"],
                _ev_stat["ev_fallback"] / ev_total * 100 if ev_total else 0)
    logger.info("  ev <= 0 かつ計算不能      : %d (%.1f%%)",
                _ev_stat["ev_invalid"],
                _ev_stat["ev_invalid"] / ev_total * 100 if ev_total else 0)
    logger.info("=" * 80)

    # ────────────────────────────────────────────────
    # CSV / JSON 出力準備
    # ────────────────────────────────────────────────
    Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for (strat_label, conf, odds_band), stat in grand.items():
        if stat["buy_races"] == 0:
            continue
        roi = calc_roi(stat)
        net = stat["payback"] - stat["stake"]
        rows.append({
            "strategy":        strat_label,
            "confidence":      conf,
            "odds_band":       odds_band,
            "buy_races":       stat["buy_races"],
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
    if rows:
        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        logger.info("CSV 出力: %s (%d bytes, %d rows)", csv_path, csv_path.stat().st_size, len(rows))
    else:
        logger.warning("集計行 0 件 — CSV は空です")

    # ── JSON ──
    ev_total_j = sum(_ev_stat.values())
    json_data = {
        "meta": {
            "start_date":   s,
            "end_date":     e,
            "n_days":       n_days,
            "total_races":  total_races,
            "n_slices":     len(rows),
            "version":      "v4",
        },
        "ev_fallback_stats": {
            "ev_positive":     _ev_stat["ev_positive"],
            "ev_fallback":     _ev_stat["ev_fallback"],
            "ev_invalid":      _ev_stat["ev_invalid"],
            "ev_positive_pct": round(_ev_stat["ev_positive"] / ev_total_j * 100, 1) if ev_total_j else 0,
            "ev_fallback_pct": round(_ev_stat["ev_fallback"]  / ev_total_j * 100, 1) if ev_total_j else 0,
            "ev_invalid_pct":  round(_ev_stat["ev_invalid"]   / ev_total_j * 100, 1) if ev_total_j else 0,
        },
        "rows": rows,
    }
    json_path = Path(args.output_json)
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    logger.info("JSON 出力: %s (%d bytes)", json_path, json_path.stat().st_size)

    # ────────────────────────────────────────────────
    # コンソール サマリーレポート
    # ────────────────────────────────────────────────
    all_rows = [r for r in rows if r["confidence"] == "ALL" and r["odds_band"] == "ALL"]
    all_rows.sort(key=lambda r: STRAT_ORDER.index(r["strategy"]) if r["strategy"] in STRAT_ORDER else 9)

    logger.info("")
    logger.info("=" * 80)
    logger.info("全体集計 (ALL conf / ALL band)")
    logger.info("  処理日: %d日 / 総レース: %d R", n_days, total_races)
    logger.info("=" * 80)

    # 馬連まとめ
    logger.info("")
    logger.info("【馬連】")
    umaren_rows = [r for r in all_rows if r["strategy"].startswith("umaren")]
    fmt = "  {:<30} {:>7} {:>14,} {:>14,} {:>8.1f}% {:>+14,}"
    header_fmt = "  {:<30} {:>7} {:>14} {:>14} {:>9} {:>14}"
    logger.info(header_fmt.format("戦略", "R数", "投資(円)", "払戻(円)", "ROI%", "純損益(円)"))
    for r in umaren_rows:
        logger.info(fmt.format(
            r["strategy"], r["buy_races"],
            r["stake"], r["payback"],
            r["roi"], r["net_profit"]
        ))
    # 馬連合算 (中+広 ≠ 単純加算: 異なる R セット)
    if len(umaren_rows) >= 2:
        total_stake_u   = sum(r["stake"]    for r in umaren_rows)
        total_payback_u = sum(r["payback"]  for r in umaren_rows)
        total_buy_r_u   = sum(r["buy_races"] for r in umaren_rows)
        roi_u = total_payback_u / total_stake_u * 100 if total_stake_u else 0.0
        net_u = total_payback_u - total_stake_u
        logger.info(fmt.format(
            "合算 (中+広)", total_buy_r_u, total_stake_u, total_payback_u, roi_u, net_u
        ))

    # 馬単まとめ
    logger.info("")
    logger.info("【馬単】")
    umatan_rows = [r for r in all_rows if r["strategy"].startswith("umatan")]
    logger.info(header_fmt.format("戦略", "R数", "投資(円)", "払戻(円)", "ROI%", "純損益(円)"))
    for r in umatan_rows:
        logger.info(fmt.format(
            r["strategy"], r["buy_races"],
            r["stake"], r["payback"],
            r["roi"], r["net_profit"]
        ))

    # 馬単 拡張 2 点の効果
    basic_row    = next((r for r in umatan_rows if r["strategy"] == STRAT_UMATAN_BASIC), None)
    extended_row = next((r for r in umatan_rows if r["strategy"] == STRAT_UMATAN_EXTENDED), None)
    if basic_row and extended_row:
        roi_delta = extended_row["roi"] - basic_row["roi"]
        net_delta = extended_row["net_profit"] - basic_row["net_profit"]
        logger.info("")
        logger.info("【馬単 拡張 2 点効果 (Δ ROI)】")
        logger.info("  基本 5点 → 拡張 7点 で ROI が %+.1f pt 変化 (純損益 %+d 円)",
                    roi_delta, net_delta)
        # 拡張 2 点単独 ROI 計算
        ext_stake    = extended_row["stake"]    - basic_row["stake"]
        ext_payback  = extended_row["payback"]  - basic_row["payback"]
        ext_roi      = ext_payback / ext_stake * 100 if ext_stake else 0.0
        logger.info("  拡張 2 点単独 ROI: %.1f%%  (投資 %d 円 / 払戻 %d 円)",
                    ext_roi, ext_stake, ext_payback)

    # 信頼度 × 戦略 ROI TOP 10
    logger.info("")
    logger.info("信頼度 × 戦略 ROI (ALL band / ≥10R / ROI 降順 TOP 10):")
    conf_rows = [r for r in rows
                 if r["confidence"] != "ALL"
                 and r["odds_band"] == "ALL"
                 and r["buy_races"] >= 10]
    conf_rows.sort(key=lambda x: -x["roi"])
    logger.info("  %-30s %5s %7s  %14s", "戦略+信頼度", "R数", "ROI%", "純損益(円)")
    for r in conf_rows[:10]:
        logger.info(
            "  %-30s %5d %7.1f%%  %+14d",
            f"{r['strategy']} [{r['confidence']}]",
            r["buy_races"], r["roi"], r["net_profit"]
        )

    # v3-fix (三連複) との比較セクション
    logger.info("")
    logger.info("【v3-fix (三連複) との比較】")
    logger.info("  戦略                             | 購入R  | ROI     | 純損益")
    logger.info("  ─────────────────────────────────┼────────┼─────────┼──────────────")
    logger.info("  三連複 v3-fix (中+広+絞り)        |  5,787 |  242.1%% | +6,620,000 円 (参考値)")
    for r in all_rows:
        logger.info(
            "  {:<33}| {:>6,} | {:>7.1f}%% | {:>+,} 円".format(
                r["strategy"], r["buy_races"], r["roi"], r["net_profit"]
            )
        )

    logger.info("")
    logger.info("[████████████████████████████████████████] 100%% | T-051 v2 Phase A v4 馬連・馬単 完了 | 累犯:14")
    logger.info("出力ファイル:")
    logger.info("  %s", Path(args.output_csv).resolve())
    logger.info("  %s", Path(args.output_json).resolve())
    logger.info("  %s", Path(args.output_log).resolve())

    return 0


if __name__ == "__main__":
    sys.exit(main())
