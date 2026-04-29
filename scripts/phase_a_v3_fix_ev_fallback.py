"""Phase A v3-fix — EV フォールバック付き 動的フォーメーション 三連複 ROI バックテスト
(T-049-fix / 2026-04-29)

修正内容 (phase_a_v3_dynamic_formation.py との差分):
  - get_ev_with_fallback(horse) を追加
    → horse.ev が 0 以下 (= 未設定) なら win_prob × odds で動的計算
  - process_race_dynamic 内の p_ev 取得を get_ev_with_fallback に置換
  - その他のロジック (動的フォーメーション 3 ケース判定 / ☆補完 / 三連複的中判定 / slice 集計) は完全同一

背景:
  2024-2025 の pred.json には ev フィールドが存在しないものが大多数で
  phase_a_v3 では 96.5% が「見送り」になっていた。
  win_prob × odds = 期待値の近似値としてフォールバック計算することで
  全期間 (846 日) での統計的有効性を確立する。

使い方:
  python scripts/phase_a_v3_fix_ev_fallback.py \\
      --start-date 20240101 \\
      --end-date   20260428 \\
      --output-csv tmp/phase_a_v3_fix_results.csv \\
      --output-json tmp/phase_a_v3_fix_results.json \\
      --output-log tmp/phase_a_v3_fix_execution.log
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

# ☆動的補完の条件
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

# 発動ケースラベル
CASE_STRICT = "絞り(S-strict)"   # 4点
CASE_MID    = "中(S-mid)"         # 7点
CASE_WIDE   = "広(S-wide)"        # 10点
CASE_WIDE4  = "広☆補完不可(6点)"  # 6点 (☆なし縮退)
CASE_SKIP   = "見送り"             # 0点
CASE_ORDER  = [CASE_STRICT, CASE_MID, CASE_WIDE, CASE_WIDE4, CASE_SKIP]

# ────────────────────────────────────────────────────────────────
# ロガー設定
# ────────────────────────────────────────────────────────────────
logger = logging.getLogger("phase_a_v3_fix")

def setup_logger(log_path: str) -> None:
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    # ファイルハンドラ
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    # コンソールハンドラ (INFO以上)
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
# EV フォールバック (T-049-fix の核心修正)
# ────────────────────────────────────────────────────────────────

def get_ev_with_fallback(horse: dict) -> float:
    """horse.ev が 0 以下 (= 未設定) なら win_prob × odds で動的計算。

    2024-2025 の pred.json では ev フィールドが空のものが多いため、
    win_prob × odds = 期待値の近似値でフォールバックする。
    両方揃っていない場合は 0.0 (= 見送り扱い) を返す。
    """
    ev = horse.get("ev") or 0
    if ev > 0:
        return float(ev)
    # フォールバック: win_prob × odds (両方揃っている場合のみ)
    win_prob = horse.get("win_prob") or 0
    odds = horse.get("odds") or horse.get("predicted_tansho_odds") or 0
    if win_prob > 0 and odds > 0:
        return float(win_prob) * float(odds)
    return 0.0  # 計算不能 → 見送り扱い


# EV フォールバック統計を収集するグローバルカウンタ (スレッド非対応だが単純ループなので問題なし)
_ev_stat = {
    "ev_positive":    0,  # ev > 0 (そのまま使用)
    "ev_fallback":    0,  # ev <= 0 → win_prob × odds で計算
    "ev_invalid":     0,  # ev <= 0 かつ win_prob/odds も無効
}


def get_ev_tracked(horse: dict) -> float:
    """get_ev_with_fallback + 統計カウント版。"""
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
# ☆ 動的補完ロジック
# ────────────────────────────────────────────────────────────────

def resolve_hoshi(horses: list[dict]) -> Optional[dict]:
    """☆印馬を返す。存在しない場合は動的補完を試みる。
    動的補完条件: 無印馬 (mark が空 or '－') かつ odds >= HOSHI_DYNAMIC_MIN_ODDS の中で
                  win_prob 最高位 1 頭を動的 ☆ 扱いとする。
    補完不可の場合は None を返す。
    """
    # 既存 ☆ 馬を探す
    hoshi_list = get_mark_horses(horses, OANA_MARKS)
    if hoshi_list:
        return hoshi_list[0]

    # 動的補完: 無印馬 (mark が None / "" / "－") かつ odds >= 10.0
    unmarked = [
        h for h in horses
        if h.get("mark", "") in ("", None, "－", "-")
        and get_odds(h) >= HOSHI_DYNAMIC_MIN_ODDS
    ]
    if not unmarked:
        return None

    # win_prob 最高位
    unmarked.sort(key=lambda h: -(h.get("win_prob") or 0.0))
    return unmarked[0]


def get_all_sub_marks(horses: list[dict], hoshi: Optional[dict]) -> list[dict]:
    """○▲△★☆ 5 印の馬リストを返す (☆ は動的補完含む)。
    重複を避けてソート済みで返す。
    """
    # 通常印 (☆ 以外)
    sub_set = {"○", "〇", "▲", "△", "★"}
    sub_horses = get_mark_horses(horses, sub_set)

    # ☆ 馬を末尾に追加 (重複チェック)
    if hoshi:
        existing_nos = {h["horse_no"] for h in sub_horses}
        if hoshi["horse_no"] not in existing_nos:
            sub_horses.append(hoshi)

    return sub_horses


# ────────────────────────────────────────────────────────────────
# 三連複 フォーメーションロジック
# ────────────────────────────────────────────────────────────────

def sanrenpuku_1axis(pivot_no: int, partner_nos: list[int],
                     payouts: dict) -> list[tuple]:
    """1頭軸: pivot_no を固定 + partner_nos から 2 頭組合せ"""
    result = []
    partners = [p for p in partner_nos if p != pivot_no]
    for p1, p2 in combinations(partners, 2):
        combo = tuple(sorted([pivot_no, p1, p2]))
        pb = lookup_payout(payouts, "三連複", list(combo))
        result.append((combo, pb))
    return result


def sanrenpuku_2axis(pivot1_no: int, pivot2_no: int,
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


# ────────────────────────────────────────────────────────────────
# 動的フォーメーション 1 レース処理 (EV フォールバック版)
# ────────────────────────────────────────────────────────────────

def process_race_dynamic(horses: list[dict],
                          payouts: dict,
                          race_id: str) -> tuple[str, list[tuple]]:
    """動的フォーメーションでケースを判定し、買い目リストと発動ケースを返す。

    phase_a_v3 との差分:
      p_ev の取得を get_ev_with_fallback (実際は get_ev_tracked) に変更。
      ev > 0 なら既存値、ev <= 0 なら win_prob × odds でフォールバック。

    Returns: (case_label, [(combo, payout), ...])
    """
    # 三連複払戻データ存在確認
    if _get_payout_bucket(payouts, "三連複") is None:
        return CASE_SKIP, []

    pivot = get_pivot(horses)
    if pivot is None:
        logger.debug("[%s] 軸馬なし (◉◎ 不存在)", race_id)
        return CASE_SKIP, []

    # ★ EV フォールバック適用 (修正点)
    p_ev          = get_ev_tracked(pivot)
    p_place3_prob = float(pivot.get("place3_prob") or 0.0)

    # 見送り判定
    if p_ev < 1.0:
        return CASE_SKIP, []

    # 対抗・連下取得
    taikou_list = get_mark_horses(horses, TAIKOU_MARKS)
    renka_list  = get_mark_horses(horses, RENKA_MARKS)

    taikou = taikou_list[0] if taikou_list else None
    renka  = renka_list[0]  if renka_list  else None

    o_place3 = float(taikou.get("place3_prob") or 0.0) if taikou else 0.0
    r_place3 = float(renka.get("place3_prob")  or 0.0) if renka  else 0.0

    # ☆ 解決 (動的補完込み)
    hoshi = resolve_hoshi(horses)

    # ケース判定 (絞り > 中 > 広)
    # ────── 絞り (S-strict) ──────
    if (p_ev >= 1.8
            and p_place3_prob >= 0.65
            and o_place3 >= 0.50
            and taikou is not None):
        # ◉◎-○ → {▲,△,★,☆} (3着流し 4点)
        third_marks = {"▲", "△", "★"}
        thirds = get_mark_horses(horses, third_marks)
        # ☆ 追加 (重複チェック)
        if hoshi:
            existing_nos = {h["horse_no"] for h in thirds}
            if hoshi["horse_no"] not in existing_nos:
                thirds.append(hoshi)
        third_nos = [h["horse_no"] for h in thirds]
        tickets = sanrenpuku_2axis(pivot["horse_no"], taikou["horse_no"],
                                   third_nos, payouts)
        if tickets:
            return CASE_STRICT, tickets
        # thirds が 0 頭なら 中 へフォールスルー

    # ────── 中 (S-mid) ──────
    if (p_ev >= 1.3
            and p_place3_prob >= 0.55
            and (o_place3 >= 0.40 or r_place3 >= 0.40)):
        # ◉◎ → {○,▲} → {○,▲,△,★,☆} (2頭軸 → 多頭、7点想定)
        second_marks = {"○", "〇", "▲"}
        second_horses = get_mark_horses(horses, second_marks)
        third_marks = {"○", "〇", "▲", "△", "★"}
        third_horses = get_mark_horses(horses, third_marks)
        if hoshi:
            existing_nos = {h["horse_no"] for h in third_horses}
            if hoshi["horse_no"] not in existing_nos:
                third_horses.append(hoshi)

        second_nos = [h["horse_no"] for h in second_horses]
        all_third_nos = [h["horse_no"] for h in third_horses]

        # フォーメーション: pivot × second × third (重複除外)
        pivot_no = pivot["horse_no"]
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
        if tickets:
            return CASE_MID, tickets
        # tickets が空なら 広 へフォールスルー

    # ────── 広 (S-wide) ──────
    # ◉◎ → {○,▲,△,★,☆} 5頭から2頭組合せ (10点 or 6点)
    sub_all = get_all_sub_marks(horses, hoshi)
    sub_nos = [h["horse_no"] for h in sub_all]
    pivot_no = pivot["horse_no"]

    if hoshi is None:
        # ☆ 補完不可 → 4頭流し (6点) 縮退
        base_nos = [h["horse_no"] for h in get_mark_horses(horses, {"○", "〇", "▲", "△", "★"})]
        tickets = sanrenpuku_1axis(pivot_no, base_nos, payouts)
        return CASE_WIDE4, tickets
    else:
        tickets = sanrenpuku_1axis(pivot_no, sub_nos, payouts)
        return CASE_WIDE, tickets


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

    # key: (case_label, conf, odds_band) -> stat dict
    stats: dict[tuple, dict] = defaultdict(empty_stat)
    # 発動ケース別カウント用
    case_counts: dict[str, int] = defaultdict(int)
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
            p_band = "30.0+"

        # 動的フォーメーション判定 (EV フォールバック版)
        try:
            case_label, tickets = process_race_dynamic(horses, payouts, race_id)
        except Exception as ex:
            logger.warning("[%s] %s フォーメーション判定エラー: %s", date_str, race_id, ex)
            continue

        # 発動ケース別カウント
        case_counts[case_label] += 1

        # 見送りは投資なし
        if case_label == CASE_SKIP or not tickets:
            continue

        # 集計
        n_tickets = len(tickets)
        payback   = sum(p for _, p in tickets)
        hit_count = sum(1 for _, p in tickets if p > 0)
        hit_bool  = hit_count > 0

        # 信頼度 × オッズ帯 の全 slice に追記
        # (conf_norm+band, conf_norm+ALL, ALL+band, ALL+ALL)
        for conf_key in (conf_norm, "ALL"):
            for band_key in (p_band, "ALL"):
                key = (case_label, conf_key, band_key)
                add_stat(stats[key], n_tickets, payback, hit_bool, hit_count)

    return {
        "stats": dict(stats),
        "case_counts": dict(case_counts),
        "n_races": n_races,
    }


# ────────────────────────────────────────────────────────────────
# メイン
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Phase A v3-fix — EV フォールバック付き 動的フォーメーション 三連複 ROI バックテスト"
    )
    parser.add_argument("--start-date", default="20240101", help="開始日 YYYYMMDD")
    parser.add_argument("--end-date",   default="20260428", help="終了日 YYYYMMDD")
    parser.add_argument("--output-csv",  default="tmp/phase_a_v3_fix_results.csv")
    parser.add_argument("--output-json", default="tmp/phase_a_v3_fix_results.json")
    parser.add_argument("--output-log",  default="tmp/phase_a_v3_fix_execution.log")
    args = parser.parse_args()

    Path(args.output_log).parent.mkdir(parents=True, exist_ok=True)
    setup_logger(args.output_log)

    s, e = args.start_date, args.end_date
    start = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    end   = date(int(e[:4]), int(e[4:6]), int(e[6:8]))

    grand: dict[tuple, dict] = defaultdict(empty_stat)
    grand_case_counts: dict[str, int] = defaultdict(int)
    total_races = 0
    n_days = 0
    total_days = (end - start).days + 1

    logger.info("Phase A v3-fix EV フォールバック バックテスト開始: %s〜%s (%d 日間)", s, e, total_days)
    logger.info("修正点: ev <= 0 の場合 win_prob × odds でフォールバック計算")
    logger.info("判定ケース: 絞り(4点) / 中(7点) / 広(10点) / 広☆補完不可(6点) / 見送り")

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
            for case_lbl, cnt in result["case_counts"].items():
                grand_case_counts[case_lbl] += cnt

        # 進捗バー (30 日ごと & 最終)
        if processed % 30 == 0 or d == end:
            pct = processed / total_days * 100
            bar_len = 40
            filled = int(bar_len * processed / total_days)
            bar = "█" * filled + "░" * (bar_len - filled)
            logger.info(
                "[%s] %s%% | %s | 処理日: %d日 / 総R: %d",
                bar, f"{pct:5.1f}", ds, n_days, total_races
            )

        d += timedelta(days=1)

    # ────────────────────────────────────────────────
    # EV フォールバック統計
    # ────────────────────────────────────────────────
    ev_total = sum(_ev_stat.values())
    logger.info("=" * 80)
    logger.info("EV フォールバック統計 (軸馬の ev 評価 全件):")
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
    # 発動ケース内訳
    # ────────────────────────────────────────────────
    total_r_counted = sum(grand_case_counts.values())
    logger.info("集計完了: 処理日 %d日 / 総レース %d R", n_days, total_races)
    logger.info("  発動ケース内訳 (total_r_counted=%d):", total_r_counted)
    for case_lbl in CASE_ORDER:
        cnt = grand_case_counts.get(case_lbl, 0)
        pct = cnt / total_r_counted * 100 if total_r_counted else 0.0
        logger.info("    %s: %d R (%.1f%%)", case_lbl, cnt, pct)
    logger.info("=" * 80)

    # ────────────────────────────────────────────────
    # CSV / JSON 出力準備
    # ────────────────────────────────────────────────
    Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for (case_label, conf, odds_band), stat in grand.items():
        if stat["races"] == 0:
            continue
        roi = calc_roi(stat)
        net = stat["payback"] - stat["stake"]
        rows.append({
            "case_label":      case_label,
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
    logger.info("CSV 出力: %s (%d bytes, %d rows)", csv_path, csv_path.stat().st_size, len(rows))

    # ── JSON ──
    # 発動ケース件数を case_counts セクションとして保存
    case_counts_summary = {}
    for case_lbl in CASE_ORDER:
        cnt = grand_case_counts.get(case_lbl, 0)
        pct = cnt / total_r_counted * 100 if total_r_counted else 0.0
        case_counts_summary[case_lbl] = {"count": cnt, "pct": round(pct, 2)}

    # EV フォールバック統計も JSON に含める
    ev_total_for_json = sum(_ev_stat.values())
    ev_stat_pct = {
        "ev_positive":    round(_ev_stat["ev_positive"] / ev_total_for_json * 100, 1) if ev_total_for_json else 0,
        "ev_fallback":    round(_ev_stat["ev_fallback"]  / ev_total_for_json * 100, 1) if ev_total_for_json else 0,
        "ev_invalid":     round(_ev_stat["ev_invalid"]   / ev_total_for_json * 100, 1) if ev_total_for_json else 0,
    }

    json_data = {
        "meta": {
            "start_date":          s,
            "end_date":            e,
            "n_days":              n_days,
            "total_races":         total_races,
            "n_slices":            len(rows),
            "ev_fallback_version": "v3-fix",
        },
        "ev_fallback_stats": {
            "ev_positive":    _ev_stat["ev_positive"],
            "ev_fallback":    _ev_stat["ev_fallback"],
            "ev_invalid":     _ev_stat["ev_invalid"],
            "ev_positive_pct": ev_stat_pct["ev_positive"],
            "ev_fallback_pct": ev_stat_pct["ev_fallback"],
            "ev_invalid_pct":  ev_stat_pct["ev_invalid"],
        },
        "case_counts": case_counts_summary,
        "rows": rows,
    }
    json_path = Path(args.output_json)
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    logger.info("JSON 出力: %s (%d bytes)", json_path, json_path.stat().st_size)

    # ────────────────────────────────────────────────
    # コンソール / ログ サマリーレポート
    # ────────────────────────────────────────────────
    all_rows = [r for r in rows if r["confidence"] == "ALL" and r["odds_band"] == "ALL"]
    all_rows.sort(key=lambda r: -r["roi"])

    # 全体集計 (全ケース合算)
    total_stake   = sum(r["stake"]   for r in all_rows)
    total_payback = sum(r["payback"] for r in all_rows)
    total_buy_r   = sum(r["races"]   for r in all_rows)
    total_hit_r   = sum(r["hit_races"] for r in all_rows)
    overall_roi   = total_payback / total_stake * 100 if total_stake else 0.0
    overall_net   = total_payback - total_stake
    overall_hit   = total_hit_r / total_buy_r * 100 if total_buy_r else 0.0

    logger.info("")
    logger.info("=" * 80)
    logger.info("全体集計 (全ケース合算 / ALL conf / ALL band)")
    logger.info("  購入 R 数  : %d", total_buy_r)
    logger.info("  投資合計   : %d 円", total_stake)
    logger.info("  払戻合計   : %d 円", total_payback)
    logger.info("  ROI        : %.1f%%", overall_roi)
    logger.info("  純損益     : %+d 円", overall_net)
    logger.info("  レース的中率: %.1f%%", overall_hit)
    logger.info("=" * 80)

    # ケース別 ROI
    logger.info("")
    logger.info("ケース別 ROI (ALL conf / ALL band):")
    header_fmt = (
        "  {:<20} {:>7} {:>14} {:>14} {:>8} {:>14}"
    )
    logger.info(header_fmt.format("ケース", "R数", "投資(円)", "払戻(円)", "ROI%", "純損益(円)"))
    for r in all_rows:
        logger.info(
            "  {:<20} {:>7,} {:>14,} {:>14,} {:>7.1f}% {:>+14,}".format(
                r["case_label"], r["races"],
                r["stake"], r["payback"],
                r["roi"], r["net_profit"]
            )
        )

    # 信頼度 × ケース別 ROI ヒートマップ (SS/S/A/B のみ)
    logger.info("")
    logger.info("信頼度 × ケース別 ROI ヒートマップ (ALL band / ≥10R):")
    for conf in ["SS", "S", "A", "B"]:
        conf_rows = [r for r in rows
                     if r["confidence"] == conf
                     and r["odds_band"] == "ALL"
                     and r["races"] >= 10]
        if not conf_rows:
            continue
        logger.info("  [%s]", conf)
        for r in sorted(conf_rows, key=lambda x: -x["roi"]):
            logger.info(
                "    {:<20} R={:>6,} ROI={:>7.1f}%% 純益={:>+12,}".format(
                    r["case_label"], r["races"], r["roi"], r["net_profit"]
                )
            )

    # 軸オッズ帯別 ROI
    logger.info("")
    logger.info("軸オッズ帯別 ROI (ALL conf / ≥10R):")
    for band_label, _, _ in ODDS_BANDS:
        band_rows = [r for r in rows
                     if r["confidence"] == "ALL"
                     and r["odds_band"] == band_label
                     and r["races"] >= 10]
        if not band_rows:
            continue
        logger.info("  [%s]", band_label)
        for r in sorted(band_rows, key=lambda x: -x["roi"])[:3]:
            logger.info(
                "    {:<20} R={:>5,} ROI={:>7.1f}%% 純益={:>+12,}".format(
                    r["case_label"], r["races"], r["roi"], r["net_profit"]
                )
            )

    # 副次発見
    logger.info("")
    logger.info("副次発見:")
    best_slice = [r for r in rows
                  if r["roi"] >= 100.0
                  and r["races"] >= 30
                  and r["confidence"] != "ALL"
                  and r["odds_band"] != "ALL"]
    logger.info("  特定 slice (conf×band) で ROI ≥ 100%%: %d 件", len(best_slice))
    if best_slice:
        best_slice.sort(key=lambda x: -x["roi"])
        for r in best_slice[:5]:
            logger.info(
                "    {:<20} conf={} band={} R={:,} ROI={:.1f}%%".format(
                    r["case_label"], r["confidence"],
                    r["odds_band"], r["races"], r["roi"]
                )
            )

    logger.info("")
    logger.info("[████████████████████████████████████████] 100%% | T-049-fix Phase A v3-fix EV フォールバック完了 | 累犯:14")
    logger.info("出力ファイル:")
    logger.info("  %s", Path(args.output_csv).resolve())
    logger.info("  %s", Path(args.output_json).resolve())
    logger.info("  %s", Path(args.output_log).resolve())

    return 0


if __name__ == "__main__":
    sys.exit(main())
