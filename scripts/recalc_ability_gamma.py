# -*- coding: utf-8 -*-
"""
recalc_ability_gamma.py
=======================
γ案修正後の ability.py で過去 pred.json の ability_wa / ability_total / composite を
再計算して上書きするスクリプト。

ML モデルは再ロードしない（重いため）。
ability + composite のみ再計算する。

使用例:
  python scripts/recalc_ability_gamma.py                          # 全件
  python scripts/recalc_ability_gamma.py --date 20260101          # 単日
  python scripts/recalc_ability_gamma.py --from 20260101 --to 20260131  # 期間指定
"""

import argparse
import json
import os
import shutil
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# プロジェクトルートを sys.path に追加
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.log import get_logger

logger = get_logger(__name__)


# ============================================================
# 定数・パス
# ============================================================

PREDICTIONS_DIR = os.path.join(_ROOT, "data", "predictions")


# ============================================================
# 対象 pred.json ファイル収集
# ============================================================

def _collect_pred_files(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    single_date: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """
    対象の pred.json ファイルパスを (date_str, path) のリストで返す。
    _prev.json / _backup / _old / _before / _nomarket は除外する。
    """
    results: List[Tuple[str, str]] = []

    for fname in sorted(os.listdir(PREDICTIONS_DIR)):
        # 通常形式: YYYYMMDD_pred.json のみ対象
        if not fname.endswith("_pred.json"):
            continue
        # 除外キーワード
        if any(kw in fname for kw in ("_prev", "_backup", "_old", "_before", "_nomarket")):
            continue

        date_str = fname[:8]  # 先頭8文字が YYYYMMDD
        if len(date_str) != 8 or not date_str.isdigit():
            continue

        if single_date:
            if date_str != single_date:
                continue
        else:
            if date_from and date_str < date_from:
                continue
            if date_to and date_str > date_to:
                continue

        path = os.path.join(PREDICTIONS_DIR, fname)
        results.append((date_str, path))

    return results


# ============================================================
# StandardTimeCalculator / TrackCorrector の初期化
# ============================================================

def _build_std_calc():
    """
    StandardTimeCalculator を構築する。
    course_db は SQLite + preload ファイルから合成。
    """
    from config.settings import COURSE_DB_PRELOAD_PATH
    from src.scraper.course_db_collector import load_preload_course_db
    from src.scraper.race_results import StandardTimeDBBuilder

    std_builder = StandardTimeDBBuilder()
    course_db = std_builder.get_course_db()

    # SQLite course_db テーブルから追加（ローリングウィンドウ: 全件）
    try:
        from src.database import get_course_db as _get_sqlite_course_db
        from src.scraper.course_db_collector import _dict_to_past_run

        sqlite_db = _get_sqlite_course_db()
        sqlite_count = 0
        for cid, recs in sqlite_db.items():
            for r in recs:
                try:
                    course_db.setdefault(cid, []).append(_dict_to_past_run(r))
                    sqlite_count += 1
                except Exception:
                    pass
        print(f"[初期化] SQLite course_db: {sqlite_count:,} 走読み込み", flush=True)
    except Exception as e:
        print(f"[警告] SQLite course_db 読み込み失敗: {e}", flush=True)

    # preload ファイルから追加
    try:
        preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
        for cid, runs in preload.items():
            course_db.setdefault(cid, []).extend(runs)
        print(f"[初期化] preload course_db: {sum(len(v) for v in preload.values()):,} 走追加", flush=True)
    except Exception as e:
        print(f"[警告] preload course_db 読み込み失敗: {e}", flush=True)

    from src.calculator.ability import StandardTimeCalculator, TrackCorrector

    std_calc = StandardTimeCalculator(course_db)
    track_corr = TrackCorrector()
    print(f"[初期化] StandardTimeCalculator / TrackCorrector 構築完了", flush=True)
    return std_calc, track_corr


# ============================================================
# race_log から過去走 PastRun リストを取得
# ============================================================

def _get_past_runs_from_db(horse_id: str, before_date: str, max_n: int = 5):
    """
    race_log テーブルから horse_id の過去走 (before_date より前) を PastRun リストで返す。
    get_past_runs_from_race_log() は max_n 件取得し finish_pos < 90 のみ返す。
    ここで追加絞り込みとして before_date 条件を適用する。
    """
    from src.scraper.horse_db_builder import get_past_runs_from_race_log

    runs = get_past_runs_from_race_log(horse_id, max_runs=max_n + 10)  # 余分に取得してフィルタ

    # before_date (YYYY-MM-DD) より前のレースのみ
    filtered = [r for r in runs if r.race_date < before_date]
    return filtered[:max_n]


# ============================================================
# ability_deviation の再計算
# ============================================================

def _recalc_ability(
    h: dict,
    race: dict,
    past_runs,
    std_calc,
    track_corr,
    race_date_str: str,
) -> Optional[object]:
    """
    ability.calc_ability_deviation() を呼び出して AbilityDeviation を返す。
    失敗時は None を返す。
    """
    from src.calculator.ability import calc_ability_deviation
    from src.models import Horse, PastRun

    horse_id = h.get("horse_id", "")
    horse_name = h.get("horse_name", "")
    sex = h.get("sex", "牡")
    age = h.get("age", 4)
    weight_kg = h.get("weight_kg", 55.0)
    horse_weight = h.get("horse_weight")
    weight_change = h.get("weight_change")

    race_date = f"{race_date_str[:4]}-{race_date_str[4:6]}-{race_date_str[6:]}"
    race_surface = race.get("surface", "芝")
    course_id = race.get("venue_code", "")
    # course_id を venue_code + surface + distance で組み立て
    if course_id and race_surface and race.get("distance"):
        course_id_full = f"{course_id}_{race_surface}_{race.get('distance')}"
    else:
        course_id_full = course_id or ""

    current_condition = race.get("condition", "良")
    is_jra = race.get("is_jra", True)
    race_grade = race.get("grade", "")
    race_distance = race.get("distance", 1600)

    # Horse オブジェクトを組み立て（past_runs を注入）
    horse_obj = Horse(
        horse_no=h.get("horse_no", 1),
        horse_name=horse_name,
        horse_id=horse_id,
        sex=sex,
        age=age,
        color=h.get("color", ""),
        breeder=h.get("breeder", ""),
        weight_kg=float(weight_kg or 55.0),
        horse_weight=horse_weight,
        weight_change=weight_change,
        gate_no=h.get("gate_no", 1),
        jockey=h.get("jockey", ""),
        jockey_id=h.get("jockey_id", ""),
        trainer=h.get("trainer", ""),
        trainer_id=h.get("trainer_id", ""),
        trainer_affiliation=h.get("trainer_affiliation", ""),
        sire=h.get("sire", ""),
        dam=h.get("dam", ""),
        maternal_grandsire=h.get("maternal_grandsire", ""),
        owner=h.get("owner", ""),
        owner_id=h.get("owner_id", ""),
        past_runs=past_runs,
    )

    try:
        ability = calc_ability_deviation(
            horse=horse_obj,
            race_date=race_date,
            race_surface=race_surface,
            course_id=course_id_full,
            std_calc=std_calc,
            track_corr=track_corr,
            current_condition=current_condition,
            current_cv=None,
            current_moisture=None,
            is_jra=bool(is_jra),
            race_grade=race_grade,
            race_distance=race_distance,
            bloodline_db=None,
            pace_db=None,
            pace_type=None,
            surface_switch_context=None,
        )
        return ability
    except Exception as e:
        logger.debug("ability 再計算失敗 horse=%s: %s", horse_name, e, exc_info=True)
        return None


# ============================================================
# composite の再計算 (stand-alone)
# ============================================================

def _recalc_composite(
    h: dict,
    race: dict,
    new_ability_total: float,
) -> float:
    """
    pred.json から各因子を取得して composite を再計算する。
    ability_total のみ新値を使用し、他因子は pred.json の既存値を使用する。
    """
    from config.settings import DEVIATION, get_composite_weights
    from src.scraper.improvement_dbs import calc_weight_change_adjustment

    venue_name = race.get("venue", "")
    surface = race.get("surface")
    field_size = race.get("field_count")
    distance = race.get("distance")

    w = get_composite_weights(
        venue_name,
        surface=surface,
        field_size=field_size,
        distance=distance,
    )

    pace_total = h.get("pace_total") or 50.0
    course_total = h.get("course_total") or 50.0
    jockey_dev = h.get("jockey_dev")
    trainer_dev = h.get("trainer_dev")
    bloodline_dev = h.get("bloodline_dev")

    # 調教好調ボーナス (models.py composite プロパティと同一ロジック)
    training_dev = h.get("training_dev")
    _TRAINING_ALPHA = 0.006
    training_multiplier = 1.0
    if training_dev is not None and training_dev > 50:
        training_multiplier = 1.0 + (training_dev - 50) * _TRAINING_ALPHA

    v = (
        new_ability_total * w["ability"] * training_multiplier
        + pace_total * w["pace"] * training_multiplier
        + course_total * w["course"]
        + (jockey_dev if jockey_dev is not None else 50.0) * w.get("jockey", 0.10)
        + (trainer_dev if trainer_dev is not None else 50.0) * w.get("trainer", 0.05)
        + (bloodline_dev if bloodline_dev is not None else 50.0) * w.get("bloodline", 0.05)
    )

    # 馬体重変動補正
    weight_change = h.get("weight_change")
    horse_weight = h.get("horse_weight")
    try:
        v += calc_weight_change_adjustment(weight_change, horse_weight)
    except Exception:
        pass

    # 各種補正値
    odds_consistency_adj = h.get("odds_consistency_adj") or 0.0
    ml_composite_adj = h.get("ml_composite_adj") or 0.0
    market_anchor_adj = h.get("market_anchor_adj") or 0.0

    v += odds_consistency_adj
    v += ml_composite_adj
    v += market_anchor_adj

    comp_min = DEVIATION["composite"]["min"]
    comp_max = DEVIATION["composite"]["max"]
    return max(comp_min, min(comp_max, v))


# ============================================================
# 順位変動チェック (◎/○ 入れ替え判定)
# ============================================================

def _check_mark_change(horses_before: list, horses_after: list) -> bool:
    """
    ◎ と ○ の馬が入れ替わったかをチェックする。
    composite 順位の変動を簡易判定。
    """
    def _get_mark_horse(horses, mark):
        for h in horses:
            if h.get("mark") == mark:
                return h.get("horse_no")
        return None

    for mark in ("◎", "○"):
        if _get_mark_horse(horses_before, mark) != _get_mark_horse(horses_after, mark):
            return True
    return False


# ============================================================
# メイン処理: 1 pred.json を再計算
# ============================================================

def _process_pred_file(
    date_str: str,
    path: str,
    std_calc,
    track_corr,
) -> dict:
    """
    1 つの pred.json を再計算して上書き保存する。
    戻り値: {"backup_created": bool, "races": N, "horses": N,
             "ability_wa_improved": N, "ability_total_improved": N, "mark_changed": N}
    """
    # race_date: YYYY-MM-DD 形式に変換
    race_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    # バックアップ: {date}_pred_prev.json が未存在の場合のみ作成
    base = os.path.basename(path)          # YYYYMMDD_pred.json
    backup_name = base.replace("_pred.json", "_pred_prev.json")
    backup_path = os.path.join(PREDICTIONS_DIR, backup_name)
    backup_created = False
    if not os.path.exists(backup_path):
        shutil.copy2(path, backup_path)
        backup_created = True

    # pred.json 読み込み
    with open(path, encoding="utf-8") as f:
        pred = json.load(f)

    stats = {
        "backup_created": backup_created,
        "races": 0,
        "horses": 0,
        "ability_wa_improved": 0,
        "ability_total_improved": 0,
        "mark_changed": 0,
    }

    races = pred.get("races", [])
    stats["races"] = len(races)

    for race in races:
        horses_before_snapshot = [
            {"horse_no": h.get("horse_no"), "mark": h.get("mark")}
            for h in race.get("horses", [])
        ]

        horses_modified = False
        for h in race.get("horses", []):
            horse_id = h.get("horse_id", "")
            if not horse_id:
                stats["horses"] += 1
                continue

            # race_log から過去走を取得 (race_date より前、最大 5 走)
            past_runs = _get_past_runs_from_db(horse_id, race_date, max_n=5)

            # ability 再計算
            ability = _recalc_ability(h, race, past_runs, std_calc, track_corr, date_str)
            if ability is None:
                stats["horses"] += 1
                continue

            # 変更前の値を記録
            old_wa = h.get("ability_wa", 50.0) or 50.0
            old_total = h.get("ability_total", 50.0) or 50.0

            # ability フィールドを更新
            h["ability_wa"] = round(ability.wa_dev, 2)
            h["ability_max"] = round(ability.max_dev, 2)
            h["ability_alpha"] = round(ability.alpha, 3)
            new_total = round(ability.total, 2)
            h["ability_total"] = new_total

            # composite 再計算
            new_composite = _recalc_composite(h, race, new_total)
            h["composite"] = round(new_composite, 2)

            # 統計
            if ability.wa_dev - old_wa > 10.0:
                stats["ability_wa_improved"] += 1
            if new_total - old_total > 5.0:
                stats["ability_total_improved"] += 1

            stats["horses"] += 1
            horses_modified = True

        # ◎/○ 入れ替え判定
        if horses_modified:
            horses_after_snapshot = [
                {"horse_no": h.get("horse_no"), "mark": h.get("mark")}
                for h in race.get("horses", [])
            ]
            if _check_mark_change(horses_before_snapshot, horses_after_snapshot):
                stats["mark_changed"] += 1

    # 上書き保存 (BOM なし UTF-8)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(pred, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp_path, path)

    return stats


# ============================================================
# エントリーポイント
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="γ案修正後の ability.py で過去 pred.json を再計算する"
    )
    parser.add_argument("--date", help="単日指定 (YYYYMMDD)")
    parser.add_argument("--from", dest="date_from", help="期間開始 (YYYYMMDD)")
    parser.add_argument("--to", dest="date_to", help="期間終了 (YYYYMMDD)")
    args = parser.parse_args()

    # 対象ファイルを収集
    files = _collect_pred_files(
        date_from=args.date_from,
        date_to=args.date_to,
        single_date=args.date,
    )

    if not files:
        print("[エラー] 対象 pred.json が見つかりません", flush=True)
        sys.exit(1)

    print(f"[開始] 対象 pred.json: {len(files)} 日分", flush=True)

    # StandardTimeCalculator / TrackCorrector を初期化 (1回のみ)
    std_calc, track_corr = _build_std_calc()

    # 集計変数
    total_backup_created = 0
    total_backup_skipped = 0
    total_races = 0
    total_horses = 0
    total_wa_improved = 0
    total_total_improved = 0
    total_mark_changed = 0

    t_start = time.time()

    for i, (date_str, path) in enumerate(files, 1):
        try:
            result = _process_pred_file(date_str, path, std_calc, track_corr)
        except Exception as e:
            print(f"[スキップ] {date_str}: {e}", flush=True)
            logger.warning("pred.json 処理失敗 date=%s path=%s", date_str, path, exc_info=True)
            continue

        # 集計
        if result["backup_created"]:
            total_backup_created += 1
        else:
            total_backup_skipped += 1
        total_races += result["races"]
        total_horses += result["horses"]
        total_wa_improved += result["ability_wa_improved"]
        total_total_improved += result["ability_total_improved"]
        total_mark_changed += result["mark_changed"]

        # 100 ファイルごとに進捗表示
        if i % 100 == 0 or i == len(files):
            elapsed = time.time() - t_start
            print(
                f"[進捗] {i}/{len(files)} ({date_str}) "
                f"elapsed={elapsed:.0f}s / 再計算馬数={total_horses:,}",
                flush=True,
            )

    elapsed_total = time.time() - t_start

    # 完了レポート
    print("", flush=True)
    print("=" * 60, flush=True)
    print("[完了レポート]", flush=True)
    print(f"  集計対象 pred 日数     : {len(files):>6}", flush=True)
    print(f"  バックアップ 作成/スキップ: {total_backup_created:>4} / {total_backup_skipped:>4}", flush=True)
    print(f"  処理対象 race / horse  : {total_races:>6,} / {total_horses:>6,}", flush=True)
    print(f"  ability_wa 改善 (>10pt増): {total_wa_improved:>5,} 頭", flush=True)
    print(f"  ability_total 改善(>5pt増): {total_total_improved:>5,} 頭", flush=True)
    print(f"  composite 順位変動 race : {total_mark_changed:>5,} (◎/○ 入れ替えあり)", flush=True)
    print(f"  Total elapsed          : {elapsed_total:.0f}s", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()
