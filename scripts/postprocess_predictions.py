"""
過去予想JSONの軽量ポストプロセス

既存 pred.json を読み込み、以下を反映して上書き保存:
  1. 印の再割り当て（composite 降順、☆/× 含む全再割り当て）
  2. tokusen_score 計算 & is_tokusen フラグ付与
  3. 自信度(confidence)再計算（6信号一致方式・JRA/NAR分離）
  4. 特選危険馬(×)再計算（JRA:OR方式 / NAR:AND方式）

ML推論は行わない。composite / course_record 等の値はそのまま。

Usage:
  python scripts/postprocess_predictions.py
  python scripts/postprocess_predictions.py --dry-run   # 変更せず統計のみ
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PRED_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "predictions")

from config.settings import (
    TEKIPAN_GAP_JRA, TEKIPAN_GAP_NAR,
    TEKIPAN_WIN_PROB_JRA, TEKIPAN_WIN_PROB_NAR,
    TEKIPAN_PLACE3_PROB_JRA, TEKIPAN_PLACE3_PROB_NAR,
    CONFIDENCE_GAP_DIVISOR_JRA, CONFIDENCE_GAP_DIVISOR_NAR,
    CONFIDENCE_THRESHOLDS_JRA, CONFIDENCE_THRESHOLDS_NAR,
    CONFIDENCE_WP_GATE_SS_JRA, CONFIDENCE_WP_GATE_SS_NAR,
    CONFIDENCE_GAP_GATE_SS_JRA, CONFIDENCE_GAP_GATE_SS_NAR,
    CONFIDENCE_WP_GATE_S_JRA, CONFIDENCE_WP_GATE_S_NAR,
    CONFIDENCE_GAP_GATE_S_JRA, CONFIDENCE_GAP_GATE_S_NAR,
    TOKUSEN_KIKEN_POP_LIMIT_JRA, TOKUSEN_KIKEN_POP_LIMIT_NAR,
    TOKUSEN_KIKEN_ODDS_LIMIT_JRA, TOKUSEN_KIKEN_ODDS_LIMIT_NAR,
    TOKUSEN_KIKEN_ML_RANK_PCT_JRA, TOKUSEN_KIKEN_ML_RANK_PCT_NAR,
    TOKUSEN_KIKEN_COMP_RANK_PCT_JRA, TOKUSEN_KIKEN_COMP_RANK_PCT_NAR,
    TOKUSEN_KIKEN_SCORE_THRESHOLD, TOKUSEN_KIKEN_MAX_PER_RACE,
)

# ============================================================
# 印の再割り当て（composite順、☆/×含む・JRA/NAR分離閾値）
# ============================================================

MARK_SEQ = ["○", "▲", "△", "★"]


def reassign_all_marks(horses, is_jra=True):
    """composite 順で印を全再割り当て（☆/×含む・JRA/NAR分離）

    formatter.py の assign_marks() と同一ロジックの dict 版。
    """
    if not horses:
        return

    tekipan_gap = TEKIPAN_GAP_JRA if is_jra else TEKIPAN_GAP_NAR
    tekipan_wp = TEKIPAN_WIN_PROB_JRA if is_jra else TEKIPAN_WIN_PROB_NAR
    tekipan_p3 = TEKIPAN_PLACE3_PROB_JRA if is_jra else TEKIPAN_PLACE3_PROB_NAR

    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)

    # 全印クリア
    for h in horses:
        h["mark"] = "－"

    # ◉/◎: composite 1位 — JRA/NAR別閾値（gap, win_prob, place3_prob）
    top = sorted_h[0]
    c1 = top.get("composite", 0)
    c2 = sorted_h[1].get("composite", 0) if len(sorted_h) > 1 else 0
    gap = c1 - c2
    wp = top.get("win_prob", 0) or 0
    p3 = top.get("place3_prob", 0) or 0
    is_tekipan = gap >= tekipan_gap and wp >= tekipan_wp and p3 >= tekipan_p3
    top["mark"] = "◉" if is_tekipan else "◎"

    # ○▲△★: composite 2-5位
    mark_idx = 0
    for h in sorted_h[1:]:
        if mark_idx >= len(MARK_SEQ):
            break
        h["mark"] = MARK_SEQ[mark_idx]
        mark_idx += 1

    # ☆: 特選穴馬（is_tokusen の未印馬、最大2頭、tokusen_score降順）
    tokusen_candidates = [
        h for h in sorted_h
        if h["mark"] == "－" and h.get("is_tokusen", False)
    ]
    tokusen_candidates.sort(key=lambda h: h.get("tokusen_score", 0), reverse=True)
    for h in tokusen_candidates[:2]:
        h["mark"] = "☆"

    # ×: is_tokusen_kiken の馬（最大2頭、他の印を上書き）
    kiken_candidates = [
        h for h in sorted_h
        if h.get("is_tokusen_kiken", False)
    ]
    kiken_candidates.sort(key=lambda h: h.get("tokusen_kiken_score", 0), reverse=True)
    for h in kiken_candidates[:2]:
        h["mark"] = "×"


# ============================================================
# tokusen_score 計算（Cohen's d ベース重み付き）
# ============================================================

TOKUSEN_ODDS_THRESHOLD = 15.0
TOKUSEN_SCORE_THRESHOLD = 3.0
TOKUSEN_MAX_PER_RACE = 2


def calc_tokusen_scores(horses):
    """dict版 tokusen_score 計算

    jockey_trainer.py の calc_tokusen_score() と同一ロジック。
    """
    field_count = len(horses) or 1

    for h in horses:
        h["tokusen_score"] = 0.0
        h["is_tokusen"] = False

        odds = h.get("odds") or h.get("predicted_tansho_odds")
        if not odds or odds < TOKUSEN_ODDS_THRESHOLD:
            continue

        # ML win_prob が穴馬として有望か（top5外でwp≥0.06はlift 2.5x）
        wp = h.get("win_prob", 0) or 0
        if wp < 0.04:
            continue

        score = 0.0

        # 1. win_prob (主軸) — ML予測勝率
        if wp >= 0.08:
            score += 3.5
        elif wp >= 0.06:
            score += 2.5
        elif wp >= 0.04:
            score += 1.5

        # 2. course_record — コース実績偏差値
        cr = h.get("course_record", 0) or 0
        if cr >= 52:
            score += 2.0
        elif cr >= 45:
            score += 1.0

        # 3. course_total — コース総合偏差値
        ct = h.get("course_total", 0) or 0
        if ct >= 52:
            score += 1.5

        # 4. place3_prob — 複勝率推定
        base_p3 = 3.0 / field_count
        p3 = h.get("place3_prob", 0) or 0
        if p3 >= base_p3 * 1.5:
            score += 1.5
        elif p3 >= base_p3 * 1.2:
            score += 0.5

        # 5. ability_trend — 近走トレンド
        trend = h.get("ability_trend", "") or ""
        if trend in ("急上昇", "上昇"):
            score += 1.0

        h["tokusen_score"] = round(score, 2)

    # composite上位5頭を除外（既に◉◎○▲△★が付くため☆不要）
    sorted_by_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    top5_nos = {h.get("horse_no") for h in sorted_by_comp[:5]}
    candidates = sorted(
        [h for h in horses
         if h["tokusen_score"] >= TOKUSEN_SCORE_THRESHOLD
         and h.get("horse_no") not in top5_nos],
        key=lambda h: h["tokusen_score"], reverse=True,
    )
    for h in candidates[:TOKUSEN_MAX_PER_RACE]:
        h["is_tokusen"] = True


# ============================================================
# 自信度(confidence)再計算 — betting.py の dict 版
# ============================================================


def recalc_confidence(horses, is_jra=True):
    """自信度の再計算 — Phase 12: JRA/NAR統一7信号スコア方式

    JRA/NAR共通で7信号加重合成スコアを算出し、閾値で自信度を判定。
    SS: スコア≥0.65 + 硬性条件(2/3) + オッズゲート
    """
    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    if len(sorted_h) < 3:
        if len(sorted_h) >= 1:
            return "SS", 1.0
        return "E", 0.0

    top = sorted_h[0]
    top_no = top.get("horse_no")
    wp = top.get("win_prob", 0) or 0

    # ---- 7信号スコア算出（JRA/NAR共通） ----
    # 1. composite差 (15%)
    gap = sorted_h[0].get("composite", 0) - sorted_h[1].get("composite", 0)
    gap_divisor = CONFIDENCE_GAP_DIVISOR_JRA if is_jra else CONFIDENCE_GAP_DIVISOR_NAR
    gap_norm = min(gap / gap_divisor, 1.0)

    # 2. ML一致度 (20%)
    sorted_wp = sorted(horses, key=lambda h: h.get("win_prob", 0) or 0, reverse=True)
    wp_top_no = sorted_wp[0].get("horse_no")
    if wp_top_no == top_no:
        ml_agreement = 1.0
    elif len(sorted_wp) >= 2 and sorted_wp[1].get("horse_no") == top_no:
        ml_agreement = 0.5
    else:
        ml_agreement = 0.0

    # 3. 2-3位差 (10%)
    gap23 = sorted_h[1].get("composite", 0) - sorted_h[2].get("composite", 0)
    gap23_norm = min(gap23 / 4.0, 1.0)

    # v5: value_score（市場信号）を除去。市場評価は自信度に不要

    # 4. 因子間合意 (20%)
    top_ability_no = max(horses, key=lambda h: h.get("ability_total", 0) or 0).get("horse_no")
    top_pace_no = max(horses, key=lambda h: h.get("pace_total", 0) or 0).get("horse_no")
    top_course_no = max(horses, key=lambda h: h.get("course_total", 0) or 0).get("horse_no")
    factor_match = sum(1 for fno in [top_ability_no, top_pace_no, top_course_no] if fno == top_no)
    if factor_match == 3:
        multi_factor = 1.0
    elif factor_match == 2:
        multi_factor = 0.6
    else:
        multi_factor = 0.0

    # 5. 信頼度 (10%)
    top3_reliable = sum(1 for h in sorted_h[:3] if h.get("ability_reliability") == "A")
    reliability_norm = top3_reliable / 3.0

    # 6. ML確信度 (15%) — raw_lgbm_probの1位-2位gap
    raw_ml_top = top.get("raw_lgbm_prob")
    raw_ml_2nd = sorted_h[1].get("raw_lgbm_prob") if len(sorted_h) >= 2 else None
    if raw_ml_top is not None and raw_ml_2nd is not None:
        ml_raw_gap = raw_ml_top - raw_ml_2nd
        ml_confidence = min(ml_raw_gap / 0.10, 1.0) if ml_raw_gap > 0 else 0.0
    else:
        ml_confidence = 0.5  # 不明時は中立

    # v5: 6信号加重合成（市場フリー）
    score = (
        gap_norm * 0.20
        + ml_agreement * 0.25
        + gap23_norm * 0.10
        + multi_factor * 0.20
        + reliability_norm * 0.10
        + ml_confidence * 0.15
    )

    # ---- v5 自信度判定: パーセンタイル閾値 + win_prob/gapゲート ----
    thresholds = CONFIDENCE_THRESHOLDS_JRA if is_jra else CONFIDENCE_THRESHOLDS_NAR

    if score >= thresholds["SS"]:
        level = "SS"
    elif score >= thresholds["S"]:
        level = "S"
    elif score >= thresholds["A"]:
        level = "A"
    elif score >= thresholds["B"]:
        level = "B"
    elif score >= thresholds["C"]:
        level = "C"
    else:
        level = "D"

    # v5 win_prob/gapゲート（市場フリー）
    wp_gate_ss = CONFIDENCE_WP_GATE_SS_JRA if is_jra else CONFIDENCE_WP_GATE_SS_NAR
    gap_gate_ss = CONFIDENCE_GAP_GATE_SS_JRA if is_jra else CONFIDENCE_GAP_GATE_SS_NAR
    wp_gate_s = CONFIDENCE_WP_GATE_S_JRA if is_jra else CONFIDENCE_WP_GATE_S_NAR
    gap_gate_s = CONFIDENCE_GAP_GATE_S_JRA if is_jra else CONFIDENCE_GAP_GATE_S_NAR

    if level == "SS" and (wp < wp_gate_ss or gap < gap_gate_ss):
        level = "S"
    if level == "S" and (wp < wp_gate_s or gap < gap_gate_s):
        level = "A"

    return level, round(score, 3)


# ============================================================
# 特選危険馬(×)再計算 — jockey_trainer.py の dict 版
# ============================================================


def recalc_kiken(horses, race, is_jra=True):
    """特選危険馬の再計算（ML×composite二重否定方式・JRA/NAR分離）

    jockey_trainer.py の calc_tokusen_kiken_score() と同一ロジック。
    JRA: 必須条件②③はOR（大頭数対応）
    NAR: 必須条件②③はAND（現行維持）
    """
    pop_limit = TOKUSEN_KIKEN_POP_LIMIT_JRA if is_jra else TOKUSEN_KIKEN_POP_LIMIT_NAR
    odds_limit = TOKUSEN_KIKEN_ODDS_LIMIT_JRA if is_jra else TOKUSEN_KIKEN_ODDS_LIMIT_NAR
    ml_pct = TOKUSEN_KIKEN_ML_RANK_PCT_JRA if is_jra else TOKUSEN_KIKEN_ML_RANK_PCT_NAR
    comp_pct = TOKUSEN_KIKEN_COMP_RANK_PCT_JRA if is_jra else TOKUSEN_KIKEN_COMP_RANK_PCT_NAR

    n = len(horses)
    if n < 4:
        for h in horses:
            h["tokusen_kiken_score"] = 0.0
            h["is_tokusen_kiken"] = False
        return

    # ランキング事前計算
    sorted_wp = sorted(horses, key=lambda h: h.get("win_prob", 0) or 0, reverse=True)
    sorted_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    wp_rank = {h.get("horse_no"): i + 1 for i, h in enumerate(sorted_wp)}
    comp_rank = {h.get("horse_no"): i + 1 for i, h in enumerate(sorted_comp)}

    # オッズ順ソート（推定人気算出用）
    sorted_odds = sorted(
        [h for h in horses if (h.get("odds") or h.get("predicted_tansho_odds"))],
        key=lambda h: h.get("odds") or h.get("predicted_tansho_odds")
    )
    odds_rank = {h.get("horse_no"): i + 1 for i, h in enumerate(sorted_odds)}

    # レースの馬場（同馬場複勝率の計算用）
    race_surface = race.get("surface", "")

    wp_threshold = max(3, int(n * ml_pct))
    comp_threshold = max(3, int(n * comp_pct))

    for h in horses:
        h["tokusen_kiken_score"] = 0.0
        h["is_tokusen_kiken"] = False
        hno = h.get("horse_no")

        # ---- 必須条件①: 人気馬である ----
        eff_odds = h.get("odds") or h.get("predicted_tansho_odds")
        if not eff_odds or eff_odds >= odds_limit:
            continue

        real_pop = h.get("popularity")
        if real_pop is not None:
            if real_pop > pop_limit:
                continue
        else:
            est_pop = odds_rank.get(hno, 99)
            if est_pop > pop_limit:
                continue

        # ---- 必須条件②③: ML低評価 / composite低評価 ----
        rank_wp = wp_rank.get(hno, n)
        rank_comp = comp_rank.get(hno, n)
        ml_low = rank_wp >= wp_threshold
        comp_low = rank_comp >= comp_threshold

        if is_jra:
            # JRA: OR条件（どちらか一方で通過）
            if not (ml_low or comp_low):
                continue
        else:
            # NAR: AND条件（両方必要）
            if not (ml_low and comp_low):
                continue

        # ---- 必須条件通過 → 追加スコアリング ----
        score = 0.0
        past_runs = h.get("past_3_runs", []) or []

        # 1. 前走大敗（8着以下）: +2pt
        if past_runs:
            prev_fp = past_runs[0].get("finish_pos")
            if prev_fp is not None and prev_fp >= 8:
                score += 2.0

        # 2. 連続凡走（直近3走中2走以上が4着以下）: +2pt
        if past_runs:
            recent = past_runs[:3]
            poor_count = sum(
                1 for r in recent
                if r.get("finish_pos") and r["finish_pos"] >= 4
            )
            if len(recent) >= 2 and poor_count >= 2:
                score += 2.0

        # 3. 同馬場複勝率が低い（20%未満）: +2pt
        # ※past_3_runsは最大3走のため、同馬場2走以上の場合のみ判定
        if past_runs and race_surface:
            same_runs = [
                r for r in past_runs
                if r.get("surface") == race_surface and r.get("finish_pos")
            ]
            if len(same_runs) >= 2:
                same_rate = sum(1 for r in same_runs if r["finish_pos"] <= 3) / len(same_runs)
                if same_rate < 0.20:
                    score += 2.0

        # 4. 騎手グレードD以下: +1pt
        jockey_grade = h.get("jockey_grade", "")
        if jockey_grade in ("D", "E"):
            score += 1.0

        # 5. 過去勝率5%未満: +1pt
        # ※past_3_runsは最大3走のため、このチェックはスキップ
        # （3走で勝率<5%はサンプル不足で偽陽性が多い）

        # 6. 長期休み明け（120日以上）: +1pt
        if past_runs:
            last_date_str = past_runs[0].get("date")
            race_date_str = race.get("_race_date")  # process_file で設定
            if last_date_str and race_date_str:
                try:
                    d1 = datetime.strptime(race_date_str, "%Y-%m-%d")
                    d0 = datetime.strptime(last_date_str, "%Y-%m-%d")
                    if (d1 - d0).days >= 120:
                        score += 1.0
                except Exception:
                    pass

        h["tokusen_kiken_score"] = round(score, 2)

    # 閾値判定（上位N頭）
    candidates = sorted(
        [h for h in horses if h["tokusen_kiken_score"] >= TOKUSEN_KIKEN_SCORE_THRESHOLD],
        key=lambda h: h["tokusen_kiken_score"],
        reverse=True,
    )
    for h in candidates[:TOKUSEN_KIKEN_MAX_PER_RACE]:
        h["is_tokusen_kiken"] = True


# ============================================================
# メイン処理
# ============================================================

def process_file(filepath, dry_run=False):
    """1ファイルをポストプロセス。変更統計を返す。"""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    # ファイル名から日付を取得（kiken休み明け計算用）
    fname = os.path.basename(filepath)
    date_str = fname.replace("_pred.json", "")
    try:
        race_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        race_date = None

    races = data.get("races", [])
    if isinstance(races, dict):
        races = list(races.values())

    mark_changes = 0
    tokusen_total = 0
    kiken_total = 0
    conf_changes = 0
    total_horses = 0

    for race in races:
        horses = race.get("horses", [])
        if not horses:
            continue

        total_horses += len(horses)
        is_jra = race.get("is_jra", True)

        # 日付をraceに一時設定（kiken計算用）
        race["_race_date"] = race_date

        # 旧値を記録
        old_marks = {h.get("horse_no"): h.get("mark", "－") for h in horses}
        old_conf = race.get("confidence", "")

        # 1. tokusen_score 計算
        calc_tokusen_scores(horses)

        # 2. 危険馬(×)再計算
        recalc_kiken(horses, race, is_jra=is_jra)

        # 3. 印再割り当て（tokusen/kiken計算後に実行）
        reassign_all_marks(horses, is_jra=is_jra)

        # 4. 自信度再計算
        new_conf, new_score = recalc_confidence(horses, is_jra=is_jra)
        race["confidence"] = new_conf
        race["confidence_score"] = new_score

        # 一時キーを削除
        race.pop("_race_date", None)

        # 変更カウント
        for h in horses:
            hno = h.get("horse_no")
            if old_marks.get(hno) != h.get("mark"):
                mark_changes += 1
            if h.get("is_tokusen"):
                tokusen_total += 1
            if h.get("is_tokusen_kiken"):
                kiken_total += 1
        if old_conf != new_conf:
            conf_changes += 1

    if not dry_run:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    return len(races), total_horses, mark_changes, tokusen_total, kiken_total, conf_changes


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="過去予想JSONの軽量ポストプロセス")
    parser.add_argument("--dry-run", action="store_true", help="変更せず統計のみ表示")
    args = parser.parse_args()

    # pred.json ファイル一覧
    files = sorted([
        f for f in os.listdir(PRED_DIR)
        if f.endswith("_pred.json")
    ])

    if not files:
        print("pred.json ファイルが見つかりません")
        return

    print(f"{'='*60}")
    print(f"  ポストプロセス{'（dry-run）' if args.dry_run else ''}")
    print(f"  対象: {len(files)} ファイル")
    print(f"  ディレクトリ: {PRED_DIR}")
    print(f"{'='*60}\n")

    t0 = time.time()
    total_files = 0
    total_races = 0
    total_horses = 0
    total_mark_changes = 0
    total_tokusen = 0
    total_kiken = 0
    total_conf_changes = 0

    for i, fname in enumerate(files, 1):
        filepath = os.path.join(PRED_DIR, fname)
        try:
            n_races, n_horses, n_mark_changes, n_tokusen, n_kiken, n_conf = process_file(
                filepath, dry_run=args.dry_run
            )
            total_files += 1
            total_races += n_races
            total_horses += n_horses
            total_mark_changes += n_mark_changes
            total_tokusen += n_tokusen
            total_kiken += n_kiken
            total_conf_changes += n_conf

            if i % 50 == 0 or i == len(files):
                elapsed = time.time() - t0
                eta = elapsed / i * (len(files) - i)
                print(
                    f"  [{i}/{len(files)}] {fname}  "
                    f"印変更={n_mark_changes}  特選={n_tokusen}  危険={n_kiken}  "
                    f"自信度変更={n_conf}  経過={elapsed:.0f}s  残={eta:.0f}s"
                )
        except Exception as e:
            print(f"  [ERROR] {fname}: {e}")

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  完了{'（dry-run）' if args.dry_run else ''}")
    print(f"  ファイル: {total_files}")
    print(f"  レース:   {total_races:,}")
    print(f"  馬:       {total_horses:,}")
    print(f"  印変更:   {total_mark_changes:,}")
    print(f"  特選穴馬: {total_tokusen:,}")
    print(f"  危険馬:   {total_kiken:,}")
    print(f"  自信度変更: {total_conf_changes:,}")
    print(f"  処理時間: {elapsed:.1f}秒 ({elapsed/60:.1f}分)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
