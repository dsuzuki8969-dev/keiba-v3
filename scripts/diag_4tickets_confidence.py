# -*- coding: utf-8 -*-
"""4 点合算馬券 × 自信度別 ROI 集計 (diag_4tickets_confidence.py)

買い目定義 (1 race あたり 4 点・400 円投資):
  #1 馬連 ◎/◉ - ○  100 円
  #2 馬連 ◎/◉ - ▲  100 円
  #3 ワイド ◎/◉ - ○  100 円
  #4 ワイド ◎/◉ - ▲  100 円

データソース:
  - data/predictions/*_pred.json (本番運用 pred.json)
  - data/results_fixed/*.json 優先 → data/results/*.json fallback (ワイド払戻バグ修正済)

集計軸:
  - overall_confidence 8 段階: SS / S / A / B / C / D / E / F

除外ルール:
  - ばんえい (venue_code="65") 除外
  - ◎/◉ / ○ / ▲ が揃わない race はスキップ
  - is_scratched=True の馬は印から除外

Usage:
  # フル実行 (2024-04 〜 2026-04)
  python scripts/diag_4tickets_confidence.py

  # デバッグ (最初の 10 ファイルのみ)
  python scripts/diag_4tickets_confidence.py --debug

  # 日付範囲指定
  python scripts/diag_4tickets_confidence.py --start 2025-01-01 --end 2025-12-31

git commit 禁止 / 既存ファイル変更禁止。
"""

import argparse
import csv
import glob
import json
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

# プロジェクトルートをパスに追加
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

from src.utils.payout_normalizer import (
    get_payout_for_combo,
    normalize_payouts,
)

# ============================================================
# ロガー設定
# ============================================================
def _setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("diag_4tickets")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

    # ファイルハンドラ
    fh = logging.FileHandler(log_path, encoding="utf-8", mode="w")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # コンソールハンドラ
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ============================================================
# 定数
# ============================================================
DIAG_DIR        = os.path.join(ROOT, "data", "_diag")
PRED_DIR        = os.path.join(ROOT, "data", "predictions")
RESULTS_DIR     = os.path.join(ROOT, "data", "results")
RESULTS_FIXED_DIR = os.path.join(ROOT, "data", "results_fixed")

# 集計対象 confidence 順序
CONFIDENCE_ORDER = ["SS", "S", "A", "B", "C", "D", "E", "F"]

# 印マーク定義 (Unicode コードポイント確認済)
MARK_HONMEI   = "◎"   # ord=9678  本命
MARK_TEKIPAN  = "◉"   # ord=9673  テキパン (◉ と ◎ は同枠・どちらか 1 頭が軸)
MARK_TAIKOU   = "○"   # ord=9675  対抗
MARK_TANNUKE  = "▲"   # ord=9650  単穴

# 軸馬となれるmark
JIKU_MARKS = {MARK_HONMEI, MARK_TEKIPAN}

# ばんえい venue_code
BANEI_VENUE_CODE = "65"

# デフォルト日付範囲
DEFAULT_START = "2024-04-01"
DEFAULT_END   = "2026-05-01"  # exclusive (2026-04-30 まで)

# マスター基準
MASTER_HIT_PCT   = 25.0   # hit% (race ベース) >= 25%
MASTER_ROI_PCT   = 110.0  # ROI >= 110%

# 1 race あたり投資額
INVEST_PER_RACE = 400  # 円 (4 点 × 100 円)

# サブチケット定義
SUB_TICKETS = [
    {"key": "umaren_a", "type": "umaren", "partner_mark": MARK_TAIKOU,  "label": "馬連 ◎-○"},
    {"key": "umaren_b", "type": "umaren", "partner_mark": MARK_TANNUKE, "label": "馬連 ◎-▲"},
    {"key": "wide_a",   "type": "wide",   "partner_mark": MARK_TAIKOU,  "label": "ワイド ◎-○"},
    {"key": "wide_b",   "type": "wide",   "partner_mark": MARK_TANNUKE, "label": "ワイド ◎-▲"},
]


# ============================================================
# results.json 読み込み
# ============================================================
def _load_results_payouts(
    date_start: str,
    date_end: str,
    logger: logging.Logger,
) -> Dict[str, Dict]:
    """results.json から race_id → 正規化 payouts をロードする

    Args:
        date_start: 'YYYY-MM-DD' (inclusive)
        date_end:   'YYYY-MM-DD' (exclusive)
        logger:     ロガー
    Returns:
        {race_id_str: {tansho: [...], umaren: [...], wide: [...], ...}}
    """
    result_map: Dict[str, Dict] = {}

    start_yyyymmdd = date_start.replace("-", "")
    end_yyyymmdd   = date_end.replace("-", "")

    pattern = os.path.join(RESULTS_DIR, "*_results.json")
    files = sorted(glob.glob(pattern))

    # results_fixed/ に同名ファイルがあれば優先
    fixed_pattern = os.path.join(RESULTS_FIXED_DIR, "*_results.json")
    fixed_files_map = {os.path.basename(f): f for f in glob.glob(fixed_pattern)}
    logger.info(f"results_fixed/ ファイル数: {len(fixed_files_map)} (ワイド払戻バグ修正版)")

    loaded_files = 0
    fixed_used = 0
    for fp in files:
        fn = os.path.basename(fp)
        date_str = fn[:8]
        if not date_str.isdigit() or len(date_str) != 8:
            continue
        if not (start_yyyymmdd <= date_str < end_yyyymmdd):
            continue

        # results_fixed/ に同名ファイルがあれば優先
        actual_fp = fixed_files_map.get(fn, fp)
        if actual_fp != fp:
            fixed_used += 1

        try:
            with open(actual_fp, encoding="utf-8") as f:
                day_data = json.load(f)
        except Exception as e:
            logger.warning(f"results.json 読み込みエラー: {fp} — {e}")
            continue

        for race_id_str, rdata in day_data.items():
            raw_p = rdata.get("payouts", {})
            norm_p = normalize_payouts(raw_p)
            result_map[str(race_id_str)] = norm_p

        loaded_files += 1

    logger.info(
        f"results.json ロード: {loaded_files} ファイル / {len(result_map)} レース "
        f"({date_start}〜{date_end}) / results_fixed 優先使用: {fixed_used} ファイル"
    )
    return result_map


# ============================================================
# pred.json 収集
# ============================================================
def _collect_pred_files(
    date_start: str,
    date_end: str,
    debug: bool,
    logger: logging.Logger,
) -> List[str]:
    """指定期間の pred.json ファイルリストを返す"""
    start_yyyymmdd = date_start.replace("-", "")
    end_yyyymmdd   = date_end.replace("-", "")

    pattern = os.path.join(PRED_DIR, "*_pred.json")
    files = sorted(glob.glob(pattern))

    filtered = []
    for fp in files:
        fn = os.path.basename(fp)
        if fn.endswith(".bak"):
            continue
        date_str = fn[:8]
        if not date_str.isdigit() or len(date_str) != 8:
            continue
        if not (start_yyyymmdd <= date_str < end_yyyymmdd):
            continue
        filtered.append(fp)

    if debug:
        filtered = filtered[:10]
        logger.info(f"[デバッグモード] 最初の {len(filtered)} ファイルのみ処理")

    logger.info(f"pred.json 対象ファイル数: {len(filtered)} ({date_start}〜{date_end})")
    return filtered


# ============================================================
# 単一 race の集計
# ============================================================
def _extract_marks(
    horses: List[Dict],
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """horses リストから (軸馬 horse_no, ○ horse_no, ▲ horse_no) を取得

    Returns:
        (jiku, taikou, tannuke) の horse_no タプル。
        見つからない場合は None。
        is_scratched=True の馬は除外。
    """
    jiku    = None  # ◎ or ◉
    taikou  = None  # ○
    tannuke = None  # ▲

    for h in horses:
        # 取消馬は除外
        if h.get("is_scratched") is True:
            continue
        mark = h.get("mark", "")
        hno = h.get("horse_no")
        if hno is None:
            continue

        if mark in JIKU_MARKS:
            jiku = hno
        elif mark == MARK_TAIKOU:
            taikou = hno
        elif mark == MARK_TANNUKE:
            tannuke = hno

    return jiku, taikou, tannuke


def _calc_race_result(
    jiku: int,
    taikou: int,
    tannuke: int,
    norm_payouts: Dict,
) -> Dict[str, Any]:
    """4 点の払戻計算

    Returns:
        {
          'umaren_a':  (hit: bool, payout: int),
          'umaren_b':  (hit: bool, payout: int),
          'wide_a':    (hit: bool, payout: int),
          'wide_b':    (hit: bool, payout: int),
          'total_payout': int,
          'any_hit': bool,
        }
    """
    result = {}
    total = 0

    for sub in SUB_TICKETS:
        key      = sub["key"]
        ttype    = sub["type"]
        partner  = taikou if sub["partner_mark"] == MARK_TAIKOU else tannuke

        payout = get_payout_for_combo(norm_payouts, ttype, [jiku, partner])
        result[key] = {"hit": payout > 0, "payout": payout}
        total += payout

    result["total_payout"] = total
    result["any_hit"] = total > 0
    return result


# ============================================================
# 集計構造
# ============================================================
def _new_conf_agg() -> Dict:
    """confidence 別集計の初期構造"""
    agg: Dict = {
        "played":       0,    # 購入した race 数
        "hit_races":    0,    # 1 点以上的中した race 数
        "total_payout": 0,    # 総払戻金 (円)
    }
    for sub in SUB_TICKETS:
        agg[sub["key"] + "_played"] = 0
        agg[sub["key"] + "_hit"]    = 0
        agg[sub["key"] + "_payout"] = 0
    return agg


# ============================================================
# メイン集計ループ
# ============================================================
def run(
    date_start: str,
    date_end: str,
    debug: bool,
    logger: logging.Logger,
) -> Tuple[Dict, Dict]:
    """集計実行

    Returns:
        (conf_agg, monthly_agg):
          conf_agg    = {confidence: _new_conf_agg()}
          monthly_agg = {(yyyymm, confidence): _new_conf_agg()}
    """
    # 集計構造の初期化
    conf_agg: Dict[str, Dict] = {c: _new_conf_agg() for c in CONFIDENCE_ORDER}
    monthly_agg: Dict[Tuple[str, str], Dict] = defaultdict(lambda: {
        **_new_conf_agg()
    })

    # results.json 全件ロード
    logger.info("=== results.json ロード開始 ===")
    results_map = _load_results_payouts(date_start, date_end, logger)
    logger.info(f"results_map: {len(results_map)} races")

    # pred.json ファイルリスト
    pred_files = _collect_pred_files(date_start, date_end, debug, logger)
    total_files = len(pred_files)

    # カウンタ
    cnt_races_total   = 0
    cnt_banei_skip    = 0
    cnt_no_marks_skip = 0
    cnt_no_results    = 0
    cnt_played        = 0

    logger.info("=== race 集計ループ開始 ===")
    start_time = time.time()

    for file_idx, fp in enumerate(pred_files):
        fn = os.path.basename(fp)
        date_str = fn[:8]  # YYYYMMDD
        yyyymm   = date_str[:6]  # YYYYMM

        # 進捗バー表示 (50 ファイル毎)
        if file_idx % 50 == 0:
            elapsed = time.time() - start_time
            pct = (file_idx / total_files * 100) if total_files > 0 else 0
            bar_filled = int(pct / 5)
            bar = "█" * bar_filled + "░" * (20 - bar_filled)
            eta = (elapsed / max(file_idx, 1)) * (total_files - file_idx)
            logger.info(
                f"[{bar}] {pct:.1f}% "
                f"({file_idx}/{total_files} files) "
                f"played={cnt_played} "
                f"経過{elapsed:.0f}s 残{eta:.0f}s"
            )

        try:
            with open(fp, encoding="utf-8") as f:
                pred_data = json.load(f)
        except Exception as e:
            logger.warning(f"pred.json 読み込みエラー: {fp} — {e}")
            continue

        races = pred_data.get("races", [])

        for race in races:
            cnt_races_total += 1

            # ばんえい除外
            # 新形式: venue_code="65" / 旧形式: race_id[4:6]="65"
            venue_code = str(race.get("venue_code") or "")
            if not venue_code:
                race_id_str = str(race.get("race_id", ""))
                if len(race_id_str) >= 6:
                    venue_code = race_id_str[4:6]
            if venue_code == BANEI_VENUE_CODE:
                cnt_banei_skip += 1
                continue

            # overall_confidence 取得
            # 旧形式 (2024-2025) は overall_confidence=None → confidence フィールドにフォールバック
            confidence = race.get("overall_confidence") or race.get("confidence", "")
            if confidence not in CONFIDENCE_ORDER:
                # 不明 confidence はスキップ
                cnt_no_marks_skip += 1
                continue

            # 馬リスト
            horses = race.get("horses", [])

            # 印取得
            jiku, taikou, tannuke = _extract_marks(horses)

            # ◎/◉ / ○ / ▲ が揃わない場合はスキップ
            if jiku is None or taikou is None or tannuke is None:
                cnt_no_marks_skip += 1
                continue

            # race_id から results を取得
            race_id = str(race.get("race_id", ""))
            norm_payouts = results_map.get(race_id, {})

            if not norm_payouts:
                cnt_no_results += 1
                # results なくても played に加算 (投資はした扱い)
                # → 実際の運用でも results がないとわからないのでスキップで統一
                # マスター指示: results がない race は集計除外
                continue

            # 4 点計算
            res = _calc_race_result(jiku, taikou, tannuke, norm_payouts)
            cnt_played += 1

            # confidence 別集計に加算
            agg = conf_agg[confidence]
            agg["played"]       += 1
            agg["hit_races"]    += 1 if res["any_hit"] else 0
            agg["total_payout"] += res["total_payout"]

            for sub in SUB_TICKETS:
                key = sub["key"]
                agg[key + "_played"] += 1
                agg[key + "_hit"]    += 1 if res[key]["hit"] else 0
                agg[key + "_payout"] += res[key]["payout"]

            # 月別集計
            mkey = (yyyymm, confidence)
            magg = monthly_agg[mkey]
            magg["played"]       += 1
            magg["hit_races"]    += 1 if res["any_hit"] else 0
            magg["total_payout"] += res["total_payout"]
            for sub in SUB_TICKETS:
                key = sub["key"]
                magg[key + "_played"] += 1
                magg[key + "_hit"]    += 1 if res[key]["hit"] else 0
                magg[key + "_payout"] += res[key]["payout"]

    # 最終進捗
    elapsed = time.time() - start_time
    logger.info(
        f"[████████████████████] 100.0% "
        f"({total_files}/{total_files} files) 完了 経過{elapsed:.1f}s"
    )
    logger.info(f"=== 集計完了 ===")
    logger.info(f"  総 race 数:         {cnt_races_total:,}")
    logger.info(f"  ばんえい除外:        {cnt_banei_skip:,}")
    logger.info(f"  ◎/○/▲ 揃わずスキップ: {cnt_no_marks_skip:,}")
    logger.info(f"  results なしスキップ: {cnt_no_results:,}")
    logger.info(f"  集計対象 race 数:    {cnt_played:,}")

    return conf_agg, dict(monthly_agg), {
        "total_races":    cnt_races_total,
        "banei_skip":     cnt_banei_skip,
        "no_marks_skip":  cnt_no_marks_skip,
        "no_results":     cnt_no_results,
        "played":         cnt_played,
    }


# ============================================================
# CSV 出力
# ============================================================
def _roi(payout: int, played: int, invest_per: int) -> float:
    """ROI (%) 計算"""
    invest = played * invest_per
    if invest == 0:
        return 0.0
    return payout / invest * 100.0


def _hit_pct(hit: int, played: int) -> float:
    if played == 0:
        return 0.0
    return hit / played * 100.0


def write_summary_csv(
    conf_agg: Dict[str, Dict],
    output_path: str,
    logger: logging.Logger,
) -> None:
    """confidence 別サマリ CSV 出力

    列:
      confidence, played, hit_races, hit_pct_races, total_invest, total_payout, roi_pct,
      umaren_a_hit, umaren_a_hit_pct, umaren_a_roi,
      umaren_b_hit, umaren_b_hit_pct, umaren_b_roi,
      wide_a_hit,   wide_a_hit_pct,   wide_a_roi,
      wide_b_hit,   wide_b_hit_pct,   wide_b_roi,
      master_achieved (hit% >= 25% AND ROI >= 110%)
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    header = [
        "confidence", "played", "hit_races", "hit_pct_races", "total_invest", "total_payout", "roi_pct",
        "umaren_a_hit", "umaren_a_hit_pct", "umaren_a_roi",
        "umaren_b_hit", "umaren_b_hit_pct", "umaren_b_roi",
        "wide_a_hit",   "wide_a_hit_pct",   "wide_a_roi",
        "wide_b_hit",   "wide_b_hit_pct",   "wide_b_roi",
        "master_achieved",
    ]

    rows = []
    master_achieved = 0

    for conf in CONFIDENCE_ORDER:
        agg = conf_agg.get(conf, _new_conf_agg())
        played     = agg["played"]
        hit_races  = agg["hit_races"]
        total_pay  = agg["total_payout"]
        total_inv  = played * INVEST_PER_RACE

        hp   = _hit_pct(hit_races, played)
        roi  = _roi(total_pay, played, INVEST_PER_RACE)
        ach  = (hp >= MASTER_HIT_PCT) and (roi >= MASTER_ROI_PCT)
        if ach:
            master_achieved += 1

        row: Dict[str, Any] = {
            "confidence":    conf,
            "played":        played,
            "hit_races":     hit_races,
            "hit_pct_races": f"{hp:.2f}",
            "total_invest":  total_inv,
            "total_payout":  total_pay,
            "roi_pct":       f"{roi:.2f}",
            "master_achieved": "YES" if ach else "",
        }

        for sub in SUB_TICKETS:
            key = sub["key"]
            sp    = agg[key + "_played"]
            sh    = agg[key + "_hit"]
            spay  = agg[key + "_payout"]
            s_hp  = _hit_pct(sh, sp)
            s_roi = _roi(spay, sp, 100)  # サブチケットは 100 円投資
            row[key + "_hit"]     = sh
            row[key + "_hit_pct"] = f"{s_hp:.2f}"
            row[key + "_roi"]     = f"{s_roi:.2f}"

        rows.append(row)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"サマリ CSV 出力: {output_path} ({len(rows)} 行) マスター基準達成: {master_achieved} セル")
    return master_achieved


def write_monthly_csv(
    monthly_agg: Dict,
    output_path: str,
    logger: logging.Logger,
) -> None:
    """月別 × confidence CSV 出力

    列:
      yyyymm, confidence, played, hit_races, hit_pct_races, total_invest, total_payout, roi_pct,
      umaren_a_hit, umaren_a_hit_pct, umaren_a_roi,
      umaren_b_hit, umaren_b_hit_pct, umaren_b_roi,
      wide_a_hit,   wide_a_hit_pct,   wide_a_roi,
      wide_b_hit,   wide_b_hit_pct,   wide_b_roi,
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # 月リスト (YYYYMM) を生成 (2024-04 〜 2026-04)
    months_order = []
    for year in range(2024, 2027):
        for month in range(1, 13):
            yyyymm = f"{year:04d}{month:02d}"
            if yyyymm >= "202404" and yyyymm <= "202604":
                months_order.append(yyyymm)

    header = [
        "yyyymm", "confidence", "played", "hit_races", "hit_pct_races",
        "total_invest", "total_payout", "roi_pct",
        "umaren_a_hit", "umaren_a_hit_pct", "umaren_a_roi",
        "umaren_b_hit", "umaren_b_hit_pct", "umaren_b_roi",
        "wide_a_hit",   "wide_a_hit_pct",   "wide_a_roi",
        "wide_b_hit",   "wide_b_hit_pct",   "wide_b_roi",
    ]

    rows = []
    for yyyymm in months_order:
        for conf in CONFIDENCE_ORDER:
            key = (yyyymm, conf)
            agg = monthly_agg.get(key)
            if agg is None:
                continue
            played    = agg["played"]
            hit_races = agg["hit_races"]
            total_pay = agg["total_payout"]
            total_inv = played * INVEST_PER_RACE
            hp  = _hit_pct(hit_races, played)
            roi = _roi(total_pay, played, INVEST_PER_RACE)

            row: Dict[str, Any] = {
                "yyyymm":        yyyymm,
                "confidence":    conf,
                "played":        played,
                "hit_races":     hit_races,
                "hit_pct_races": f"{hp:.2f}",
                "total_invest":  total_inv,
                "total_payout":  total_pay,
                "roi_pct":       f"{roi:.2f}",
            }
            for sub in SUB_TICKETS:
                k = sub["key"]
                sp   = agg[k + "_played"]
                sh   = agg[k + "_hit"]
                spay = agg[k + "_payout"]
                s_hp  = _hit_pct(sh, sp)
                s_roi = _roi(spay, sp, 100)
                row[k + "_hit"]     = sh
                row[k + "_hit_pct"] = f"{s_hp:.2f}"
                row[k + "_roi"]     = f"{s_roi:.2f}"

            rows.append(row)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"月別 CSV 出力: {output_path} ({len(rows)} 行)")


# ============================================================
# 結果表示
# ============================================================
def print_report(
    conf_agg: Dict[str, Dict],
    counters: Dict,
    master_achieved: int,
    logger: logging.Logger,
) -> None:
    """最終結果をログ出力"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("【4 点合算 自信度別集計 結果】")
    logger.info("=" * 60)
    logger.info(f"処理 race 数:          {counters['total_races']:,}")
    logger.info(f"ばんえい除外:           {counters['banei_skip']:,}")
    logger.info(f"◎/○/▲ 揃わずスキップ:  {counters['no_marks_skip']:,}")
    logger.info(f"results なしスキップ:   {counters['no_results']:,}")
    logger.info(f"集計対象 race 数:       {counters['played']:,}")
    logger.info("")
    logger.info(f"{'conf':4s} {'played':>7s} {'hit_race%':>10s} {'ROI%':>8s}  サブ tickets")
    logger.info("-" * 80)

    for conf in CONFIDENCE_ORDER:
        agg = conf_agg.get(conf, _new_conf_agg())
        played    = agg["played"]
        hit_races = agg["hit_races"]
        total_pay = agg["total_payout"]
        hp  = _hit_pct(hit_races, played)
        roi = _roi(total_pay, played, INVEST_PER_RACE)
        ach = (hp >= MASTER_HIT_PCT) and (roi >= MASTER_ROI_PCT)
        marker = " ★達成" if ach else ""

        sub_info = []
        for sub in SUB_TICKETS:
            k = sub["key"]
            sp   = agg[k + "_played"]
            sh   = agg[k + "_hit"]
            spay = agg[k + "_payout"]
            s_roi = _roi(spay, sp, 100)
            sub_info.append(f"{sub['label']} hit={sh}/{sp}({_hit_pct(sh, sp):.1f}%) ROI={s_roi:.1f}%")

        logger.info(
            f"  {conf:2s}: played={played:>5,} hit_race%={hp:5.1f}% "
            f"ROI={roi:7.2f}%{marker}"
        )
        for info in sub_info:
            logger.info(f"       {info}")
        logger.info("")

    logger.info(f"マスター基準達成 confidence セル: {master_achieved} 個 (hit% >={MASTER_HIT_PCT}% AND ROI >={MASTER_ROI_PCT}%)")


# ============================================================
# CLI エントリポイント
# ============================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="4 点合算馬券 (馬連×2 + ワイド×2) × 自信度別 ROI 集計"
    )
    parser.add_argument("--start",  default=DEFAULT_START, help=f"開始日 (YYYY-MM-DD, デフォルト: {DEFAULT_START})")
    parser.add_argument("--end",    default=DEFAULT_END,   help=f"終了日 exclusive (YYYY-MM-DD, デフォルト: {DEFAULT_END})")
    parser.add_argument("--debug",  action="store_true",   help="デバッグモード (最初の 10 ファイルのみ)")
    args = parser.parse_args()

    # ログファイル
    log_path = os.path.join(DIAG_DIR, "4tickets_confidence_run.log")
    os.makedirs(DIAG_DIR, exist_ok=True)
    logger = _setup_logger(log_path)

    logger.info(f"=== diag_4tickets_confidence.py 開始 ===")
    logger.info(f"  期間: {args.start} 〜 {args.end} (exclusive)")
    logger.info(f"  デバッグ: {args.debug}")
    logger.info(f"  ログ: {log_path}")

    # 集計実行
    conf_agg, monthly_agg, counters = run(args.start, args.end, args.debug, logger)

    # CSV 出力
    summary_path  = os.path.join(DIAG_DIR, "4tickets_confidence_summary.csv")
    monthly_path  = os.path.join(DIAG_DIR, "4tickets_confidence_monthly.csv")

    master_achieved = write_summary_csv(conf_agg, summary_path, logger)
    write_monthly_csv(monthly_agg, monthly_path, logger)

    # 最終レポート
    print_report(conf_agg, counters, master_achieved, logger)
    logger.info(f"=== 完了 ===")
    logger.info(f"  summary CSV: {summary_path}")
    logger.info(f"  monthly CSV: {monthly_path}")
    logger.info(f"  log:         {log_path}")


if __name__ == "__main__":
    main()
