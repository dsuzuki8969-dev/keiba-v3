"""
結果照合・成績集計モジュール

フロー:
  1. 分析実行時 → save_prediction() で予想JSONを保存
  2. レース後   → fetch_actual_results() でネットケイバから実際の着順・オッズを取得
  3. 集計時     → compare_and_aggregate() で的中率・収支・回収率を計算
"""

import json
import os
import re
import time
from typing import Dict, List, Optional, Tuple

from config.settings import PREDICTIONS_DIR, RESULTS_DIR
from data.masters.venue_master import is_banei

# SQLite DB（利用可能な場合のみ使用）
try:
    from src import database as _db
    _DB_AVAILABLE = True
except Exception:
    _db = None
    _DB_AVAILABLE = False


# ============================================================
# 予想データの保存（分析完了時に呼ぶ）
# ============================================================


def save_prediction(date: str, analyses_by_venue: dict, *, lightweight: bool = False) -> str:
    """
    分析結果から予想JSONを生成して保存する。
    analyses_by_venue = {"東京": {1: RaceAnalysis, ...}, ...}
    lightweight=True: 段階的保存用。コメント・LLM生成をスキップして高速保存。
    Returns: 保存先ファイルパス

    Phase 0: 全偏差値内訳・確率推定・騎手スコア・調教データをフル保存。
    将来の ML 学習データとして使えるよう、全特徴量を網羅する。
    """
    os.makedirs(PREDICTIONS_DIR, exist_ok=True)
    payload = {"date": date, "version": 2, "races": []}

    for venue, race_map in analyses_by_venue.items():
        for race_no, analysis in sorted(race_map.items()):
            race_info = analysis.race
            course = race_info.course if race_info.course else None
            race_data = {
                "race_id": race_info.race_id,
                "venue": venue,
                "race_no": race_no,
                "race_name": race_info.race_name,
                "surface": course.surface if course else "",
                "distance": course.distance if course else 0,
                "direction": course.direction if course else "",
                "condition": getattr(race_info, "track_condition_turf", "")
                or getattr(race_info, "track_condition_dirt", ""),
                "is_jra": getattr(race_info, "is_jra", True),
                "is_banei": is_banei(course.venue_code if course else ""),
                "venue_code": course.venue_code if course else "",
                "water_content": getattr(race_info, "moisture_dirt", None),
                "field_count": getattr(race_info, "field_count", 0),
                "grade": getattr(race_info, "grade", ""),
                # コース形態（展開見解用）
                "straight_m": course.straight_m if course else 0,
                "corner_count": course.corner_count if course else 0,
                "corner_type": course.corner_type if course else "",
                "slope_type": course.slope_type if course else "",
                "inside_outside": course.inside_outside if course else "",
                "confidence": analysis.overall_confidence.value
                if analysis.overall_confidence
                else "B",
                "confidence_score": round(getattr(analysis, "confidence_score", 0.0), 3),
                "pace_predicted": analysis.pace_type_predicted.value
                if analysis.pace_type_predicted
                else "",
                "post_time": getattr(race_info, "post_time", ""),
                "estimated_front_3f": _round_or_none(analysis.estimated_front_3f),
                "estimated_last_3f": _round_or_none(analysis.estimated_last_3f),
                "pace_comment": analysis.pace_comment or "",
                "favorable_style": analysis.favorable_style or "",
                "favorable_style_reason": analysis.favorable_style_reason or "",
                "leading_horses": list(analysis.leading_horses or []),
                "front_horses": list(analysis.front_horses or []),
                "mid_horses": list(analysis.mid_horses or []),
                "rear_horses": list(analysis.rear_horses or []),
                "predicted_race_time": _round_or_none(getattr(analysis, "predicted_race_time", None)),
                "final_formation": getattr(analysis, "final_formation", None),
                "pace_reliability_label": getattr(analysis, "pace_reliability_label", ""),
                "llm_pace_comment": getattr(analysis, "llm_pace_comment", ""),
                "llm_mark_comment": getattr(analysis, "llm_mark_comment", ""),
                "horses": [],
                "tickets": [],
                "formation_tickets": [],
            }

            # leading_horses に含まれる馬番セット（running_style整合用）
            _leading_set = set(analysis.leading_horses or [])

            for ev in analysis.evaluations:
                h = ev.horse
                horse_data = {
                    # 基本情報
                    "horse_no": h.horse_no,
                    "horse_name": h.horse_name,
                    "horse_id": getattr(h, "horse_id", ""),
                    "sex": getattr(h, "sex", ""),
                    "age": getattr(h, "age", None),
                    "gate_no": getattr(h, "gate_no", None),
                    "weight_kg": getattr(h, "weight_kg", None),
                    "jockey": getattr(h, "jockey", ""),
                    "jockey_id": getattr(h, "jockey_id", ""),
                    "trainer": getattr(h, "trainer", ""),
                    "trainer_id": getattr(h, "trainer_id", ""),
                    "sire": getattr(h, "sire", ""),
                    "dam": getattr(h, "dam", ""),
                    "maternal_grandsire": getattr(h, "maternal_grandsire", ""),
                    "owner": getattr(h, "owner", ""),
                    "horse_weight": getattr(h, "horse_weight", None),
                    "weight_change": getattr(h, "weight_change", None),
                    "weight_confirmed": False,  # オッズ取得時に公式データで上書きされたら True
                    "odds": h.odds,
                    "popularity": h.popularity,
                    # 総合
                    "mark": ev.mark.value if ev.mark else "-",
                    # assign_marks でスナップショットされた値を優先（印との整合性保証）— 30-70クランプ
                    "composite": round(max(30.0, min(70.0, getattr(ev, "_composite_snapshot", ev.composite))), 2),
                    # 能力偏差値 (A-E章) — 30-70クランプ
                    "ability_total": round(max(30.0, min(70.0, ev.ability.total)), 2),
                    "ability_max": round(ev.ability.max_dev, 2),
                    "ability_wa": round(ev.ability.wa_dev, 2),
                    "ability_alpha": round(ev.ability.alpha, 3),
                    "ability_trend": ev.ability.trend.value
                    if ev.ability.trend
                    else "stable",
                    "ability_reliability": ev.ability.reliability.value
                    if ev.ability.reliability
                    else "B",
                    "ability_class_adj": round(ev.ability.class_adjustment, 2),
                    "ability_bloodline_adj": round(ev.ability.bloodline_adj, 2),
                    "ability_chakusa_pattern": ev.ability.chakusa_pattern.value
                    if ev.ability.chakusa_pattern
                    else "",
                    # 展開偏差値 (F章)
                    "pace_total": round(ev.pace.total, 2),
                    "pace_base": round(ev.pace.base_score, 2),
                    "pace_last3f_eval": round(ev.pace.last3f_eval, 2),
                    "pace_position_balance": round(ev.pace.position_balance, 2),
                    "pace_gate_bias": round(ev.pace.gate_bias, 2),
                    "pace_course_style_bias": round(ev.pace.course_style_bias, 2),
                    "pace_jockey": round(ev.pace.jockey_pace, 2),
                    "pace_estimated_pos4c": round(ev.pace.estimated_position_4c * len(analysis.evaluations) + 1, 1) if ev.pace.estimated_position_4c is not None else None,
                    "pace_estimated_last3f": _round_or_none(ev.pace.estimated_last3f),
                    "position_initial": round(getattr(ev, "_normalized_position", 0.5), 3),
                    "running_style": ""
                    if race_data.get("is_banei")
                    else (
                        "逃げ"
                        if h.horse_no in _leading_set
                        else (
                            ev.pace.running_style.value
                            if ev.pace.running_style
                            else ""
                        )
                    ),
                    # コース適性 (G章)
                    "course_total": round(ev.course.total, 2),
                    "course_record": round(ev.course.course_record, 2),
                    "course_venue_apt": round(ev.course.venue_aptitude, 2),
                    "course_venue_level": ev.course.venue_contrib_level,
                    "course_jockey": round(ev.course.jockey_course, 2),
                    # 確率推定
                    "win_prob": round(ev.win_prob, 4),
                    "place2_prob": round(ev.place2_prob, 4),
                    "place3_prob": round(ev.place3_prob, 4),
                    # 騎手・調教
                    "jockey_change_score": round(ev.jockey_change_score, 2),
                    "shobu_score": round(ev.shobu_score, 2),
                    "odds_consistency_adj": round(ev.odds_consistency_adj, 2),
                    "ml_composite_adj": round(ev.ml_composite_adj, 2),
                    # 穴馬・危険馬
                    "ana_score": round(ev.ana_score, 2),
                    "ana_type": ev.ana_type.value if ev.ana_type else "none",
                    "tokusen_score": round(ev.tokusen_score, 2),
                    "is_tokusen": ev.is_tokusen,
                    "tokusen_kiken_score": round(ev.tokusen_kiken_score, 2),
                    "is_tokusen_kiken": ev.is_tokusen_kiken,
                    "kiken_score": round(ev.kiken_score, 2),
                    "kiken_type": ev.kiken_type.value if ev.kiken_type else "none",
                    # ML三連率
                    "ml_win_prob": _round_or_none(ev.ml_win_prob, 4),
                    "ml_top2_prob": _round_or_none(ev.ml_top2_prob, 4),
                    "ml_place_prob": _round_or_none(ev.ml_place_prob, 4),
                    # パイプライン診断用中間値
                    "raw_lgbm_prob": _round_or_none(getattr(ev, "_raw_lgbm_prob", None), 4),
                    "ensemble_prob": _round_or_none(getattr(ev, "_ensemble_prob", None), 4),
                    "ml_rule_prob": _round_or_none(getattr(ev, "_ml_rule_prob", None), 4),
                    "pre_pop_prob": _round_or_none(getattr(ev, "_pre_pop_prob", None), 4),
                    "model_level": getattr(ev, "_model_level", None),
                    # 予想オッズ・乖離
                    "predicted_tansho_odds": _round_or_none(ev.predicted_tansho_odds),
                    "odds_divergence": _round_or_none(ev.odds_divergence),
                    "divergence_signal": ev.divergence_signal or "",
                    # 調教データ (J-4)
                    "training_intensity": _extract_training_summary(ev.training_records),
                    "training_records": _extract_training_records(ev.training_records),
                    # 前三走（走破偏差値付き）
                    "past_3_runs": _extract_past_runs(h, 3, ev.ability.run_records),
                    # ── 全頭診断用グレード ──
                    # プロフィール用
                    "jockey_grade": getattr(ev, "_jockey_grade", "—"),
                    "trainer_grade": getattr(ev, "_trainer_grade", "—"),
                    "sire_grade": getattr(ev, "_sire_grade", "—"),
                    "mgs_grade": getattr(ev, "_mgs_grade", "—"),
                    "owner_grade": "—",
                    # 偏差値（数値）— 30-70クランプ
                    "jockey_dev": round(max(30.0, min(70.0, v)), 1) if (v := getattr(ev, "_jockey_dev", None)) is not None else None,
                    "trainer_dev": round(max(30.0, min(70.0, v)), 1) if (v := getattr(ev, "_trainer_dev", None)) is not None else None,
                    "bloodline_dev": round(max(30.0, min(70.0, v)), 1) if (v := getattr(ev, "_bloodline_dev", None)) is not None else None,
                    # 確率追加
                    "predicted_rank": getattr(ev, "_predicted_rank", None),
                    # 能力追加
                    "popularity_trend": getattr(ev, "_popularity_trend", "—"),
                    "condition_signal": getattr(ev, "_condition_signal", "—"),
                    # 展開追加
                    "gate_neighbors": getattr(ev, "_gate_neighbors", "—"),
                    "estimated_pos_1c": _round_or_none(getattr(ev, "_estimated_pos_1c", None)),
                    "estimated_last3f_rank": getattr(ev, "_estimated_last3f_rank", None),
                    "last3f_grade": getattr(ev, "_last3f_grade", "—"),
                    # 詳細グレード
                    "course_detail_grades": getattr(ev, "_course_detail_grades", {}),
                    "jockey_detail_grades": getattr(ev, "_jockey_detail_grades", {}),
                    "trainer_detail_grades": getattr(ev, "_trainer_detail_grades", {}),
                    "bloodline_detail_grades": getattr(ev, "_bloodline_detail_grades", {}),
                }
                race_data["horses"].append(horse_data)

            # 馬個別見解・印見解を自動生成（lightweight時はスキップ）
            if not lightweight:
                try:
                    from src.calculator.calibration import generate_horse_comment, generate_horse_diagnosis, generate_mark_comment_rich
                    all_composites = [hd["composite"] for hd in race_data["horses"]]
                    _rc = {
                        "field_count": race_data.get("field_count", 0),
                        "straight_m": race_data.get("straight_m", 0),
                        "slope_type": race_data.get("slope_type", ""),
                        "surface": race_data.get("surface", ""),
                        "pace_predicted": race_data.get("pace_predicted", "MM"),
                        "leading_horses": race_data.get("leading_horses", []),
                        "front_horses": race_data.get("front_horses", []),
                        "mid_horses": race_data.get("mid_horses", []),
                        "rear_horses": race_data.get("rear_horses", []),
                        "estimated_front_3f": race_data.get("estimated_front_3f"),
                        "all_composites": all_composites,
                        "is_banei": race_data.get("is_banei", False),
                        "water_content": race_data.get("water_content"),
                        "all_horses": race_data.get("horses", []),
                    }
                    mark_order = {"◉", "◎", "○", "▲"}
                    for hd in race_data["horses"]:
                        m = hd.get("mark", "-")
                        if m in mark_order:
                            lvl = "full"
                        elif m in ("△", "★", "☆"):
                            lvl = "normal"
                        else:
                            lvl = "short"
                        hd["horse_comment"] = generate_horse_comment(hd, _rc, lvl)
                        # 全頭診断用短評
                        hd["horse_diagnosis"] = generate_horse_diagnosis(hd, _rc)

                    sorted_h = sorted(race_data["horses"], key=lambda x: x.get("composite", 0), reverse=True)
                    race_data["mark_comment_rich"] = generate_mark_comment_rich(sorted_h, _rc)
                except Exception:
                    pass

            # 三連複フォーメーション買い目を formation_tickets に保存
            formation = analysis.formation or {}
            for t in formation.get("sanrenpuku", []):
                if isinstance(t, dict):
                    race_data["formation_tickets"].append({
                        "type": "三連複",
                        "combo": [t.get("a"), t.get("b"), t.get("c")],
                        "mark_a": t.get("mark_a", ""),
                        "mark_b": t.get("mark_b", ""),
                        "mark_c": t.get("mark_c", ""),
                        "ev": round(t.get("ev", 0), 1),
                        "stake": t.get("stake", 0),
                        "signal": t.get("signal", ""),
                        "prob": round(t.get("prob", 0), 6),
                        "odds": round(t.get("odds", 0), 1),
                    })

            # フォーメーション列情報を保存（フロントエンド表示用）
            _fm_cols = {}
            for _ck in ("col1", "col2", "col3"):
                _col_evals = formation.get(_ck, [])
                if _col_evals:
                    _fm_cols[_ck] = [e.horse.horse_no for e in _col_evals if hasattr(e, "horse")]
            if _fm_cols:
                race_data["formation_columns"] = _fm_cols

            # バリューベット
            race_data["value_bets"] = []
            for vb in (analysis.value_bets or []):
                race_data["value_bets"].append({
                    "type": vb.get("type", ""),
                    "combo": vb.get("combo", ""),
                    "name": vb.get("name", ""),
                    "predicted_odds": vb.get("predicted_odds", 0),
                    "actual_odds": vb.get("actual_odds", 0),
                    "divergence": vb.get("divergence", 0),
                    "ev": vb.get("ev", 0),
                    "signal": vb.get("signal", ""),
                    "prob": vb.get("prob", 0),
                })

            # LLM見解生成（API設定時のみ、lightweight時はスキップ）
            if not lightweight:
                try:
                    from src.output.llm_narrative import generate_pace_narrative, generate_mark_narrative
                    if not race_data.get("llm_pace_comment"):
                        race_data["llm_pace_comment"] = generate_pace_narrative(race_data)
                    if not race_data.get("llm_mark_comment"):
                        race_data["llm_mark_comment"] = generate_mark_narrative(race_data)
                except Exception:
                    pass  # フォールバック: 既存テンプレート見解を使用

            payload["races"].append(race_data)

    fpath = os.path.join(PREDICTIONS_DIR, f"{date.replace('-', '')}_pred.json")

    # 既存の pred.json とマージ（別分析セッションのレースを保持する）
    if os.path.isfile(fpath):
        try:
            with open(fpath, "r", encoding="utf-8") as ef:
                existing = json.load(ef)
            # 今回の分析に含まれる (venue, race_no) のセット
            new_keys = {(r["venue"], r["race_no"]) for r in payload["races"]}
            # 既存レースのうち、今回の分析に含まれないものを保持
            for old_race in existing.get("races", []):
                key = (old_race.get("venue", ""), old_race.get("race_no", 0))
                if key not in new_keys:
                    payload["races"].append(old_race)
            # odds_updated_at 等のメタ情報を引き継ぎ
            for meta_key in ("odds_updated_at",):
                if meta_key in existing and meta_key not in payload:
                    payload[meta_key] = existing[meta_key]
        except Exception:
            pass  # マージ失敗時は新規データのみで上書き

    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # SQLite にも保存（デュアルライト）
    if _DB_AVAILABLE:
        try:
            _db.save_prediction(date, payload)
        except Exception:
            pass

    return fpath


def _round_or_none(v, n=2):
    return round(v, n) if v is not None else None


def _extract_training_summary(records) -> Optional[dict]:
    """調教レコードから ML 用サマリーを抽出"""
    if not records:
        return None
    best = records[0]
    return {
        "course": getattr(best, "course", ""),
        "intensity": getattr(best, "intensity_label", ""),
        "sigma": _round_or_none(getattr(best, "sigma_from_mean", None), 2),
    }


def _extract_training_records(records) -> list:
    """調教レコード全体をフロントエンド用に変換"""
    if not records:
        return []
    result = []
    for rec in records:
        result.append({
            "date": getattr(rec, "date", ""),
            "venue": getattr(rec, "venue", ""),
            "course": getattr(rec, "course", ""),
            "splits": dict(getattr(rec, "splits", {}) or {}),
            "partner": getattr(rec, "partner", ""),
            "position": getattr(rec, "position", ""),
            "rider": getattr(rec, "rider", ""),
            "track_condition": getattr(rec, "track_condition", ""),
            "lap_count": getattr(rec, "lap_count", ""),
            "intensity_label": getattr(rec, "intensity_label", "通常"),
            "sigma_from_mean": _round_or_none(getattr(rec, "sigma_from_mean", None), 2),
            "comment": getattr(rec, "comment", ""),
        })
    return result


def _lookup_corners_from_cache(race_id: str, horse_no: int) -> list:
    """result.htmlキャッシュからコーナー通過順を取得"""
    import lz4.frame
    import os
    import re as _re
    from bs4 import BeautifulSoup

    cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "cache")
    # NAR優先 → JRA
    keys = [
        f"nar.netkeiba.com_race_result.html_race_id={race_id}",
        f"race.netkeiba.com_race_result.html_race_id={race_id}",
    ]
    html = None
    for key in keys:
        lz4_path = os.path.join(cache_dir, f"{key}.html.lz4")
        txt_path = os.path.join(cache_dir, f"{key}.html")
        if os.path.exists(lz4_path):
            try:
                with open(lz4_path, "rb") as f:
                    html = lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
                break
            except Exception:
                pass
        elif os.path.exists(txt_path):
            try:
                with open(txt_path, "r", encoding="utf-8", errors="replace") as f:
                    html = f.read()
                break
            except Exception:
                pass
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.Corner_Num")
    if not table:
        return []

    # 各コーナーの馬番順を抽出
    corner_orders = {}
    for tr in table.select("tr"):
        th = tr.select_one("th")
        td = tr.select_one("td")
        if not th or not td:
            continue
        m = _re.search(r"(\d)", th.get_text(strip=True))
        if not m:
            continue
        ci = int(m.group(1))
        nos = [int(x) for x in _re.findall(r"\d+", td.get_text())]
        corner_orders[ci] = nos

    if not corner_orders:
        return []

    # 指定馬番の各コーナー順位
    positions = []
    for ci in sorted(corner_orders.keys()):
        order = corner_orders[ci]
        try:
            pos = order.index(horse_no) + 1
        except ValueError:
            pos = 0
        positions.append(pos)
    return positions


# コーナー補完用キャッシュ（同一会話内で同じrace_idを再パースしない）
_corners_cache: dict = {}


def _get_corners_for_run(run) -> list:
    """PastRunのコーナー通過順を取得（DBからrace_id特定→キャッシュ読み込み）"""
    import sqlite3
    import os

    rd = getattr(run, "race_date", "")
    venue = getattr(run, "venue", "")
    horse_no = getattr(run, "horse_no", 0)
    distance = getattr(run, "distance", 0)
    finish_pos = getattr(run, "finish_pos", 0)
    if not rd or not venue or not horse_no:
        return []

    from data.masters.venue_master import VENUE_NAME_TO_CODE
    vc = VENUE_NAME_TO_CODE.get(venue, "")
    if not vc:
        return []

    # distance込みでキャッシュ（同日同場でも距離が違えば別レース）
    cache_key = (rd, vc, horse_no, distance)
    if cache_key in _corners_cache:
        return _corners_cache[cache_key]

    # race_log から race_id を特定（distance + finish_pos で絞り込み）
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "keiba.db")
    if not os.path.exists(db_path):
        return []

    try:
        conn = sqlite3.connect(db_path)
        # distance + finish_pos で正確に特定
        row = None
        if distance and finish_pos:
            row = conn.execute(
                "SELECT race_id FROM race_log "
                "WHERE race_date = ? AND venue_code = ? AND horse_no = ? "
                "AND distance = ? AND finish_pos = ? LIMIT 1",
                (rd, vc, horse_no, distance, finish_pos),
            ).fetchone()
        # フォールバック: distanceのみで絞り込み
        if not row and distance:
            row = conn.execute(
                "SELECT race_id FROM race_log "
                "WHERE race_date = ? AND venue_code = ? AND horse_no = ? "
                "AND distance = ? LIMIT 1",
                (rd, vc, horse_no, distance),
            ).fetchone()
        # 最終フォールバック: distance不明の場合
        if not row:
            row = conn.execute(
                "SELECT race_id FROM race_log "
                "WHERE race_date = ? AND venue_code = ? AND horse_no = ? LIMIT 1",
                (rd, vc, horse_no),
            ).fetchone()
        conn.close()
    except Exception:
        return []

    if not row:
        return []

    race_id = row[0]
    corners = _lookup_corners_from_cache(race_id, horse_no)
    _corners_cache[cache_key] = corners
    return corners


def _extract_past_runs(horse, count: int = 3, run_records=None) -> list:
    """馬の過去走データからフロントエンド用に前N走を抽出（走破偏差値付き）"""
    from src.calculator.grades import dev_to_grade

    runs = getattr(horse, "past_runs", None)
    if not runs:
        return []

    # run_records から走破偏差値をrace_dateでマッピング
    # run_records は (PastRun, dev, std_time) or (PastRun, dev, std_time, l3f_rank) のタプル
    dev_by_date = {}
    if run_records:
        for rec in run_records:
            pr = rec[0]
            dev = rec[1]
            rd = getattr(pr, "race_date", "")
            if rd and dev is not None:
                dev_by_date[rd] = round(dev, 1)

    result = []
    for run in runs[:count]:
        rd = getattr(run, "race_date", "")
        sd = dev_by_date.get(rd)
        # 通過順 (positions_corners)
        corners = getattr(run, "positions_corners", None)
        if corners:
            if isinstance(corners, (list, tuple)):
                corners_str = "-".join(str(c) for c in corners)
            else:
                corners_str = str(corners)
        else:
            # result.htmlキャッシュからコーナー通過順を補完
            try:
                cached_corners = _get_corners_for_run(run)
                if cached_corners:
                    corners_str = "-".join(str(c) for c in cached_corners)
                else:
                    corners_str = ""
            except Exception:
                corners_str = ""
        # ペース
        pace = getattr(run, "pace", None)
        if pace:
            pace_str = pace.value if hasattr(pace, "value") else str(pace)
        else:
            pace_str = ""
        # レースレベル (race_level_dev)
        race_level = getattr(run, "race_level_dev", None)
        race_level_grade = dev_to_grade(race_level) if race_level is not None else "—"
        # 走破レベル (speed_dev)
        speed_dev_grade = dev_to_grade(sd) if sd is not None else "—"

        result.append({
            "date": rd,
            "venue": getattr(run, "venue", ""),
            "surface": getattr(run, "surface", ""),
            "distance": getattr(run, "distance", 0),
            "condition": getattr(run, "condition", ""),
            "class": getattr(run, "class_name", "") or getattr(run, "grade", ""),
            "field_count": getattr(run, "field_count", 0),
            "horse_no": getattr(run, "horse_no", 0),
            "jockey": getattr(run, "jockey", ""),
            "weight_kg": getattr(run, "weight_kg", 0),
            "position_4c": getattr(run, "position_4c", 0),
            "finish_pos": getattr(run, "finish_pos", 0),
            "finish_time": _round_or_none(getattr(run, "finish_time_sec", None), 1),
            "last_3f": _round_or_none(getattr(run, "last_3f_sec", None), 1),
            "margin": _round_or_none(
                getattr(run, "margin_ahead", None) or getattr(run, "margin_behind", None),
                1,
            ),
            "speed_dev": sd,
            # 新規フィールド
            "positions_corners": corners_str,
            "pace": pace_str,
            "race_level_grade": race_level_grade,
            "speed_dev_grade": speed_dev_grade,
            "race_id": getattr(run, "race_id", ""),
            "result_cname": getattr(run, "result_cname", ""),
        })
    return result


def load_prediction(date: str) -> Optional[dict]:
    """予想データを読み込む（JSON優先、フォールバックでDB）
    JSONファイルが正規データソース。DBはバックフィル時に不正データが混入する場合があるため
    フォールバックとしてのみ使用。
    """
    # JSON優先
    fpath = os.path.join(PREDICTIONS_DIR, f"{date.replace('-', '')}_pred.json")
    if os.path.exists(fpath):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # DBフォールバック
    if _DB_AVAILABLE:
        try:
            data = _db.load_prediction(date)
            if data:
                return data
        except Exception:
            pass
    return None


def list_prediction_dates() -> List[str]:
    """予想済み日付一覧（新しい順）。JSONファイル + DB日付の和集合。"""
    dates_set: set = set()
    # JSONファイルから日付取得（正規ソース）
    if os.path.exists(PREDICTIONS_DIR):
        for f in os.listdir(PREDICTIONS_DIR):
            if f.endswith("_pred.json"):
                raw = f.replace("_pred.json", "")
                if len(raw) == 8:
                    dates_set.add(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
    # DBからも日付取得（JSONにない古い日付をカバー）
    if _DB_AVAILABLE:
        try:
            for d in _db.list_prediction_dates():
                dates_set.add(d)
        except Exception:
            pass
    return sorted(dates_set, reverse=True)


# ============================================================
# 実際の着順・オッズをネットケイバから取得
# ============================================================


def _is_nar_race(race_id: str) -> bool:
    """NAR（地方競馬）のレースかどうか判定"""
    try:
        from data.masters.venue_master import JRA_CODES
        return race_id[4:6] not in JRA_CODES
    except Exception:
        return race_id[4:6] not in {"01","02","03","04","05","06","07","08","09","10"}


def _fetch_from_official(race_id: str, official_scraper, date: str) -> Optional[dict]:
    """JRA/NAR公式から結果を取得（1段目フォールバック）"""
    try:
        if not _is_nar_race(race_id):
            # JRA公式（OfficialOddsScraper.get_jra_result）
            if hasattr(official_scraper, "get_jra_result"):
                result = official_scraper.get_jra_result(race_id)
                if result and result.get("order"):
                    return result
        else:
            # NAR公式（OfficialNARScraper.get_result）
            try:
                from src.scraper.official_nar import OfficialNARScraper
                nar = OfficialNARScraper()
                result = nar.get_result(race_id, date)
                if result and result.get("order"):
                    return result
            except ImportError:
                pass
    except Exception:
        pass
    return None


def _fetch_from_keibabook(race_id: str, kb_client, date: str) -> Optional[dict]:
    """競馬ブックから結果を取得（3段目フォールバック）"""
    try:
        from src.scraper.keibabook_training import KeibabookResultScraper
        scraper = KeibabookResultScraper(kb_client)
        result = scraper.fetch_result(race_id, race_date=date)
        if result and result.get("order"):
            return result
    except Exception:
        pass
    return None


def _fetch_from_rakuten(race_id: str, rakuten_client, date: str) -> Optional[dict]:
    """楽天競馬から結果を取得（4段目フォールバック・NAR限定）"""
    try:
        # 楽天競馬のrace_idはnetkeiba形式と異なるため、find_race_idで変換
        from data.masters.venue_master import VENUE_CODE_TO_NAME
        vc = race_id[4:6]
        venue_name = VENUE_CODE_TO_NAME.get(vc, "")
        race_no = int(race_id[10:12])
        rakuten_race_id = rakuten_client.find_race_id(date, venue_name, race_no)
        if not rakuten_race_id:
            return None
        result = rakuten_client.get_result(rakuten_race_id, date)
        if result and result.get("order"):
            return result
    except Exception:
        pass
    return None


def fetch_actual_results(
    date: str,
    client,
    *,
    official_scraper=None,
    kb_client=None,
    rakuten_client=None,
) -> dict:
    """
    指定日の全レース結果（着順・確定オッズ）を取得して保存。
    4段フォールバック: 公式 → netkeiba → 競馬ブック → 楽天競馬(NAR)

    Args:
        date:              YYYY-MM-DD
        client:            NetkeibaClient（2段目フォールバック）
        official_scraper:  JRA/NAR公式スクレイパー（1段目、省略可）
        kb_client:         KeibabookClient（3段目、省略可）
        rakuten_client:    RakutenKeibaScraper（4段目・NAR限定、省略可）

    Returns: {race_id: {"order": [...], "payouts": {...}, "source": "...", "lap_times": {...}}}
    """
    os.makedirs(RESULTS_DIR, exist_ok=True)
    fpath = os.path.join(RESULTS_DIR, f"{date.replace('-', '')}_results.json")

    # キャッシュがあればそれを使う
    if os.path.exists(fpath):
        with open(fpath, "r", encoding="utf-8") as f:
            cached = json.load(f)
        # 結果が空（全レースの order が空）の場合は再取得する
        if cached:
            has_any_order = any(
                v.get("order") for v in cached.values()
                if isinstance(v, dict)
            )
            if has_any_order:
                return cached
            os.remove(fpath)
        else:
            return cached

    pred = load_prediction(date)
    if not pred:
        return {}

    from data.masters.venue_master import JRA_CODES

    results = {}
    source_stats = {"official": 0, "netkeiba": 0, "keibabook": 0, "rakuten": 0, "failed": 0}

    for race in pred["races"]:
        race_id = race.get("race_id", "")
        if not race_id:
            continue

        order, payouts, lap_times, source = None, None, None, ""

        # 1st: JRA/NAR公式
        if official_scraper and not order:
            result = _fetch_from_official(race_id, official_scraper, date)
            if result and result.get("order"):
                order = result["order"]
                payouts = result.get("payouts", {})
                lap_times = result.get("lap_times")
                source = "official"

        # 2nd: netkeiba（従来のロジック）
        if not order:
            vc = race_id[4:6]
            base_url = "https://race.netkeiba.com" if vc in JRA_CODES else "https://nar.netkeiba.com"
            url = f"{base_url}/race/result.html"
            before_fetch = getattr(client, "_stats_fetch", 0)
            soup = client.get(url, params={"race_id": race_id})
            was_fetched = getattr(client, "_stats_fetch", 0) > before_fetch
            if soup:
                order = _parse_finish_order(soup)
                payouts = _parse_payouts(soup)
                if order:
                    source = "netkeiba"
            if was_fetched:
                time.sleep(1.5)

        # 3rd: 競馬ブック
        if not order and kb_client:
            result = _fetch_from_keibabook(race_id, kb_client, date)
            if result and result.get("order"):
                order = result["order"]
                payouts = result.get("payouts", {})
                source = "keibabook"

        # 4th: 楽天競馬（NAR限定）
        if not order and rakuten_client and _is_nar_race(race_id):
            result = _fetch_from_rakuten(race_id, rakuten_client, date)
            if result and result.get("order"):
                order = result["order"]
                payouts = result.get("payouts", {})
                source = "rakuten"

        # 払戻金の補完（公式/ブック/楽天で着順取得できたが払戻なし → netkeibaで補完）
        if order and not payouts and source != "netkeiba":
            vc = race_id[4:6]
            base_url = "https://race.netkeiba.com" if vc in JRA_CODES else "https://nar.netkeiba.com"
            url = f"{base_url}/race/result.html"
            soup = client.get(url, params={"race_id": race_id})
            if soup:
                payouts = _parse_payouts(soup)
                time.sleep(1.5)

        if order:
            source_stats[source] = source_stats.get(source, 0) + 1
        else:
            source_stats["failed"] += 1

        results[race_id] = {
            "order": order or [],
            "payouts": payouts or {},
            "source": source,
        }
        if lap_times:
            results[race_id]["lap_times"] = lap_times

    # ソース別統計をログ出力
    total = sum(source_stats.values())
    if total > 0:
        import logging
        _logger = logging.getLogger(__name__)
        parts = []
        for src, cnt in source_stats.items():
            if cnt > 0:
                parts.append(f"{src}={cnt}")
        _logger.info(f"結果取得ソース内訳 ({date}): {', '.join(parts)}")

    # 全レースの結果が空（まだ開催前/開催中）の場合はファイルを保存しない
    has_any_order = any(v.get("order") for v in results.values() if isinstance(v, dict))
    if not has_any_order and results:
        return results

    # 結果取得成功率が50%未満の場合は保存をスキップ（部分取得保護）
    success_count = sum(1 for v in results.values() if isinstance(v, dict) and v.get("order"))
    if results and success_count < len(results) * 0.5:
        import logging
        _logger = logging.getLogger(__name__)
        _logger.warning(
            f"結果取得成功率が低いため保存をスキップ ({date}): "
            f"{success_count}/{len(results)} ({success_count/len(results)*100:.0f}%)"
        )
        return results

    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # SQLite にも保存（デュアルライト）
    if _DB_AVAILABLE and results:
        try:
            _db.save_results(date, results)
        except Exception:
            pass

    return results


def _parse_finish_order(soup) -> List[dict]:
    """結果ページから着順・馬番・単勝オッズを抽出"""
    rows = []
    table = soup.select_one(".ResultTableWrap table")
    if not table:
        return rows
    for tr in table.select("tbody tr"):
        cells = tr.select("td")
        if len(cells) < 3:
            continue
        finish_text = cells[0].get_text(strip=True)
        if not finish_text.isdigit():
            continue
        finish = int(finish_text)
        horse_no_text = cells[2].get_text(strip=True)
        if not horse_no_text.isdigit():
            continue
        horse_no = int(horse_no_text)
        # 単勝オッズ列を探す（一般的に12列目前後）
        odds = None
        for c in cells[8:]:
            t = c.get_text(strip=True).replace(",", "")
            try:
                v = float(t)
                if 1.0 <= v <= 9999:
                    odds = v
                    break
            except ValueError:
                pass
        rows.append({"horse_no": horse_no, "finish": finish, "odds": odds})
    return rows


def _parse_payouts(soup) -> dict:
    """払戻テーブルから全券種の払戻金を抽出。
    netkeiba は Payout_Detail_Table が2つ（単勝〜馬連 / ワイド〜三連単）あるため
    select() で全テーブルをループする。

    netkeiba HTML構造:
      全券種で <br> 区切りの複数エントリが存在しうる（同着時）。
        <td class="Payout"><span>110円<br/>110円<br/>140円</span></td>
      必ず decode_contents().split("<br") で分割してパースする。

    複勝/ワイド: 常にリスト形式
    他の券種: 通常はdict形式、同着時のみリスト形式
    """
    payouts = {}
    payout_tables = soup.select(".Payout_Detail_Table, table.payout")
    if not payout_tables:
        return payouts
    # ラベル正規化マップ（半角3 → 全角三）
    LABEL_NORM = {"3連複": "三連複", "3連単": "三連単"}
    TARGETS = {"馬連", "馬単", "ワイド", "三連複", "三連単", "複勝", "単勝",
               "3連複", "3連単", "枠連"}
    # 常にリスト形式で格納する券種
    LIST_TYPES = {"複勝", "ワイド"}

    for payout_table in payout_tables:
        for tr in payout_table.select("tr"):
            cells = tr.select("td, th")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            if label not in TARGETS:
                continue
            label = LABEL_NORM.get(label, label)

            result_cell = cells[1] if len(cells) > 1 else None
            payout_cell = cells[2] if len(cells) > 2 else None

            # 全券種共通: <br> 区切りで払戻額を分割
            payout_vals = []
            if payout_cell:
                payout_texts = [t.strip().replace(",", "")
                                for t in payout_cell.decode_contents().split("<br")
                                if t.strip()]
                for pt in payout_texts:
                    cleaned = re.sub(r"[^\d]", "", pt)
                    payout_vals.append(int(cleaned) if cleaned else 0)

            # コンボ抽出
            if label == "複勝" and result_cell:
                # 複勝: <div><span>馬番</span></div> から非空spanを取得
                combos = [s.get_text(strip=True)
                          for s in result_cell.select("div > span")
                          if s.get_text(strip=True)]
                entries = []
                for j, combo in enumerate(combos):
                    pv = payout_vals[j] if j < len(payout_vals) else 0
                    entries.append({"combo": combo, "payout": pv})
                payouts["複勝"] = entries

            elif label == "単勝" and result_cell:
                # 単勝: <div><span>馬番</span></div> — 同着時は複数
                combos = [s.get_text(strip=True)
                          for s in result_cell.select("div > span")
                          if s.get_text(strip=True)]
                if len(combos) > 1 and len(payout_vals) > 1:
                    # 同着: リスト形式
                    entries = []
                    for j, combo in enumerate(combos):
                        pv = payout_vals[j] if j < len(payout_vals) else 0
                        entries.append({"combo": combo, "payout": pv})
                    payouts["単勝"] = entries[0]  # 最初の1つをdict形式で保存
                else:
                    combo = combos[0] if combos else ""
                    pv = payout_vals[0] if payout_vals else 0
                    payouts["単勝"] = {"combo": combo, "payout": pv}

            elif label in ("ワイド",) and result_cell:
                # ワイド: 常にリスト形式、<ul>グループ単位
                uls = result_cell.select("ul")
                entries = []
                for j, ul in enumerate(uls):
                    nums = [li.get_text(strip=True) for li in ul.select("li")
                            if li.get_text(strip=True)]
                    combo_str = "-".join(nums)
                    pv = payout_vals[j] if j < len(payout_vals) else 0
                    entries.append({"combo": combo_str, "payout": pv})
                payouts["ワイド"] = entries

            else:
                # 馬連/馬単/枠連/三連複/三連単: <ul>グループ単位でコンボ分割
                uls = result_cell.select("ul") if result_cell else []
                if uls:
                    entries = []
                    for j, ul in enumerate(uls):
                        nums = [li.get_text(strip=True) for li in ul.select("li")
                                if li.get_text(strip=True)]
                        combo_str = "-".join(nums)
                        pv = payout_vals[j] if j < len(payout_vals) else 0
                        entries.append({"combo": combo_str, "payout": pv})
                    if len(entries) == 1:
                        # 通常: dict形式
                        if label not in payouts:
                            payouts[label] = entries[0]
                    else:
                        # 同着: 最初のエントリをdict形式で保存（互換性維持）
                        if label not in payouts:
                            payouts[label] = entries[0]
                else:
                    # <ul>なし: テキストからコンボ取得
                    combo_text = result_cell.get_text(strip=True) if result_cell else ""
                    pv = payout_vals[0] if payout_vals else 0
                    if label not in payouts:
                        payouts[label] = {"combo": combo_text, "payout": pv}
    return payouts


# ============================================================
# 予想 vs 実際の照合・集計
# ============================================================


def _safe_tansho_payout(payouts: dict, winner_hno: int = 0) -> int:
    """単勝payoutを安全に取得。同着結合データは0を返す。

    winner_hno: 勝ち馬の馬番。指定時はcomboとの一致を検証する。
    """
    tp = payouts.get("単勝", {})
    if not isinstance(tp, dict):
        return 0
    combo = str(tp.get("combo", ""))
    try:
        combo_int = int(combo)
        if combo_int < 1 or combo_int > 18:
            return 0  # 同着で馬番が結合されたデータ
    except (ValueError, TypeError):
        return 0
    # winner_hno指定時: comboが勝ち馬の馬番と一致するか検証
    if winner_hno > 0 and combo_int != winner_hno:
        return 0  # 同着で別の馬番が結合されている
    payout = tp.get("payout", 0) or 0
    # 単勝payoutの上限キャップ（JRA/NAR最高記録を大幅に超える値を排除）
    if payout > 500000:
        return 0
    return payout


def compare_and_aggregate(date: str, *, _skip_disk_cache: bool = False) -> Optional[dict]:
    """
    予想JSONと結果JSONを照合し、的中・収支を集計して返す。
    Returns: 集計辞書 or None（データなし）
    """
    # ディスクキャッシュチェック（高速パス）
    if not _skip_disk_cache:
        cached = _load_daily_cache(date)
        if cached is not None:
            return cached

    pred = load_prediction(date)
    if not pred:
        return None

    # 結果データを results.json ファイルから読み込む（正規データソース）
    actual = None
    fpath = os.path.join(RESULTS_DIR, f"{date.replace('-', '')}_results.json")
    if os.path.exists(fpath):
        with open(fpath, "r", encoding="utf-8") as f:
            actual = json.load(f)
    if not actual and _DB_AVAILABLE:
        try:
            actual = _db.load_results(date)
        except Exception:
            pass
    if not actual:
        return None

    total_races = 0
    total_tickets = 0
    hit_tickets = 0
    total_stake = 0
    total_return = 0
    honmei_placed = 0  # ◎3着以内
    honmei_place2 = 0  # ◎2着以内（連対）
    honmei_win = 0  # ◎1着
    honmei_total = 0
    # ◎単勝100円シミュレーション
    honmei_tansho_stake = 0
    honmei_tansho_ret = 0
    # ◎複勝100円シミュレーション
    honmei_fukusho_stake = 0
    honmei_fukusho_ret = 0
    conf_stats: Dict[str, dict] = {}  # 自信度別
    ticket_stats: Dict[str, dict] = {}  # 券種別
    conf_ticket_stats: Dict[str, dict] = {}  # "{confidence}_{ticket_type}" 別
    mark_stats: Dict[str, dict] = {}  # 印別（◎◉○▲△☆の成績）
    ana_stats = {
        "total": 0, "win": 0, "place2": 0, "placed": 0,
        "tansho_stake": 0, "tansho_ret": 0,
        "fukusho_stake": 0, "fukusho_ret": 0,
    }
    kiken_stats = {"total": 0, "fell_through": 0}

    for race in pred["races"]:
        race_id = race.get("race_id", "")
        if not race_id:
            continue
        confidence = race.get("confidence", "B")
        result = actual.get(race_id)
        if not result:
            continue

        finish_map = {r["horse_no"]: r["finish"] for r in result["order"]}
        if not finish_map:
            continue  # 着順データなし（中止等）→スキップ
        payouts = result.get("payouts", {})
        total_races += 1

        if confidence not in conf_stats:
            conf_stats[confidence] = {"races": 0, "hits": 0, "stake": 0, "ret": 0}
        conf_stats[confidence]["races"] += 1

        # 印別の着順集計 + 穴馬・危険馬集計
        honmei_hno = None  # ◉◎馬の馬番（単勝券種用）
        for h in race["horses"]:
            mk = h.get("mark", "")
            pos = finish_map.get(h["horse_no"], 99)

            if mk in ("◉", "◎", "○", "▲", "△", "★", "☆", "×"):
                if mk not in mark_stats:
                    mark_stats[mk] = {
                        "total": 0, "win": 0, "place2": 0, "placed": 0,
                        "tansho_stake": 0, "tansho_ret": 0,
                    }
                mark_stats[mk]["total"] += 1
                if pos == 1:
                    mark_stats[mk]["win"] += 1
                if pos <= 2:
                    mark_stats[mk]["place2"] += 1
                if pos <= 3:
                    mark_stats[mk]["placed"] += 1
                # 各印の単勝100円シミュレーション（実際の払戻金額を使用）
                mark_stats[mk]["tansho_stake"] += 100
                if pos == 1:
                    _tpay = _safe_tansho_payout(payouts, h["horse_no"])
                    mark_stats[mk]["tansho_ret"] += _tpay

                # ◉と◎はどちらも本命として通算カウント
                if mk in ("◉", "◎"):
                    honmei_hno = h["horse_no"]
                    honmei_total += 1
                    if pos == 1:
                        honmei_win += 1
                    if pos <= 2:
                        honmei_place2 += 1
                    if pos <= 3:
                        honmei_placed += 1
                    honmei_tansho_stake += 100
                    if pos == 1:
                        _tpay2 = _safe_tansho_payout(payouts, h["horse_no"])
                        honmei_tansho_ret += _tpay2
                    # ◉◎複勝シミュレーション
                    honmei_fukusho_stake += 100
                    if pos <= 3:
                        fuku_pay = _get_fukusho_payout(h["horse_no"], payouts)
                        if fuku_pay and fuku_pay > 0:
                            honmei_fukusho_ret += fuku_pay

            # 穴馬検知（ana_type が「穴」を示す値）
            ana_t = h.get("ana_type", "")
            if ana_t and ana_t not in ("none", "該当なし", "なし", "-", ""):
                ana_stats["total"] += 1
                hno = h["horse_no"]
                ana_stats["tansho_stake"] += 100
                ana_stats["fukusho_stake"] += 100
                if pos == 1:
                    ana_stats["win"] += 1
                    _ana_pay = _safe_tansho_payout(payouts, hno)
                    ana_stats["tansho_ret"] += _ana_pay
                if pos <= 2:
                    ana_stats["place2"] += 1
                if pos <= 3:
                    ana_stats["placed"] += 1
                    # 複勝払戻: 確定払戻データから取得
                    fuku_pay = _get_fukusho_payout(hno, payouts)
                    if fuku_pay and fuku_pay > 0:
                        ana_stats["fukusho_ret"] += fuku_pay

            # 危険馬検知（kiken_type が「危険」を示す値）→ 4着以下なら予測成功
            kiken_t = h.get("kiken_type", "")
            if kiken_t and kiken_t not in ("none", "該当なし", "なし", "-", ""):
                kiken_stats["total"] += 1
                if pos >= 4:
                    kiken_stats["fell_through"] += 1

        # 通常買い目をレース単位（券種グループ）で集計
        # 馬連4点=1R, 三連複6点=1R として扱う
        race_by_type: Dict[str, dict] = {}  # ticket_type → {stake:int, hit:bool, ret:int}

        def _tally_ticket(ticket_type, combo, stake):
            """1点を処理して race_by_type に累積する"""
            if ticket_type not in race_by_type:
                race_by_type[ticket_type] = {"stake": 0, "hit": False, "ret": 0}
            race_by_type[ticket_type]["stake"] += stake
            hit, payout_per_100 = _check_ticket_hit(ticket_type, combo, finish_map, payouts)
            if hit:
                # 払戻 = 100円あたり払戻 × (実際の賭け金 / 100)
                actual_ret = int(payout_per_100 * stake / 100) if payout_per_100 > 0 else 0
                race_by_type[ticket_type]["hit"] = True
                race_by_type[ticket_type]["ret"] += actual_ret

        # 通常買い目（馬連・三連複）— stake=0のチケットはスキップ
        all_tickets = list(race.get("tickets", []))
        # フォーメーション買い目（stake>0のもの）
        all_tickets += [t for t in race.get("formation_tickets", []) if (t.get("stake") or 0) > 0]

        for t in all_tickets:
            stake = t.get("stake", 0)
            if stake <= 0:
                continue  # 買わないチケットは集計しない
            ticket_type = t.get("type", "")
            combo = tuple(int(x) for x in t.get("combo", []))
            _tally_ticket(ticket_type, combo, stake)

        # 単勝（◉◎馬がいれば1点100円を券種として追加）
        if honmei_hno is not None:
            tansho_pos = finish_map.get(honmei_hno, 99)
            tansho_hit = tansho_pos == 1
            tansho_ret_val = 0
            if tansho_hit:
                tansho_ret_val = _safe_tansho_payout(payouts, honmei_hno)
            race_by_type["単勝"] = {"stake": 100, "hit": tansho_hit, "ret": tansho_ret_val}

        # レース単位で集計（全券種を合算）
        race_total_stake = sum(rg["stake"] for rg in race_by_type.values())
        race_total_ret = sum(rg["ret"] for rg in race_by_type.values() if rg["hit"])
        race_any_hit = any(rg["hit"] for rg in race_by_type.values())
        if race_total_stake > 0:
            total_tickets += 1
            total_stake += race_total_stake
            total_return += race_total_ret
            conf_stats[confidence]["stake"] += race_total_stake
            conf_stats[confidence]["ret"]   += race_total_ret
            if race_any_hit:
                hit_tickets += 1
                conf_stats[confidence]["hits"] += 1

        # 券種別・自信度×券種別（全券種）
        for ticket_type, rg in race_by_type.items():
            stake_r = rg["stake"]
            ret_r = rg["ret"] if rg["hit"] else 0

            if ticket_type not in ticket_stats:
                ticket_stats[ticket_type] = {"total": 0, "hits": 0, "stake": 0, "ret": 0, "payouts": []}
            ticket_stats[ticket_type]["total"] += 1
            ticket_stats[ticket_type]["stake"] += stake_r
            if rg["hit"]:
                ticket_stats[ticket_type]["hits"] += 1
                ticket_stats[ticket_type]["ret"] += ret_r
                ticket_stats[ticket_type]["payouts"].append(ret_r)

            ct_key = f"{confidence}_{ticket_type}"
            if ct_key not in conf_ticket_stats:
                conf_ticket_stats[ct_key] = {"total": 0, "hits": 0, "stake": 0, "ret": 0, "payouts": []}
            conf_ticket_stats[ct_key]["total"] += 1
            conf_ticket_stats[ct_key]["stake"] += stake_r
            if rg["hit"]:
                conf_ticket_stats[ct_key]["hits"] += 1
                conf_ticket_stats[ct_key]["ret"] += ret_r
                conf_ticket_stats[ct_key]["payouts"].append(ret_r)

    roi = round(total_return / total_stake * 100, 1) if total_stake > 0 else 0.0
    hit_rate = round(hit_tickets / total_tickets * 100, 1) if total_tickets > 0 else 0.0
    honmei_rate = round(honmei_placed / honmei_total * 100, 1) if honmei_total > 0 else 0.0
    honmei_place2_rate = round(honmei_place2 / honmei_total * 100, 1) if honmei_total > 0 else 0.0
    honmei_win_rate = round(honmei_win / honmei_total * 100, 1) if honmei_total > 0 else 0.0
    honmei_tansho_roi = round(honmei_tansho_ret / honmei_tansho_stake * 100, 1) if honmei_tansho_stake > 0 else 0.0
    honmei_fukusho_roi = round(honmei_fukusho_ret / honmei_fukusho_stake * 100, 1) if honmei_fukusho_stake > 0 else 0.0

    # 穴馬・危険馬の率計算
    _at = ana_stats["total"]
    _ts = ana_stats["tansho_stake"]
    _fs = ana_stats["fukusho_stake"]
    ana_stats["win_rate"]    = round(ana_stats["win"]    / _at * 100, 1) if _at else 0.0
    ana_stats["place2_rate"] = round(ana_stats["place2"] / _at * 100, 1) if _at else 0.0
    ana_stats["place_rate"]  = round(ana_stats["placed"] / _at * 100, 1) if _at else 0.0
    ana_stats["tansho_roi"]  = round(ana_stats["tansho_ret"] / _ts * 100, 1) if _ts else 0.0
    ana_stats["fukusho_roi"] = round(ana_stats["fukusho_ret"] / _fs * 100, 1) if _fs else 0.0
    _kt = kiken_stats["total"]
    kiken_stats["fell_rate"] = round(kiken_stats["fell_through"] / _kt * 100, 1) if _kt else 0.0

    _result = {
        "date": date,
        "total_races": total_races,
        "total_tickets": total_tickets,
        "hit_tickets": hit_tickets,
        "hit_rate": hit_rate,
        "total_stake": total_stake,
        "total_return": total_return,
        "profit": total_return - total_stake,
        "roi": roi,
        "honmei_placed": honmei_placed,
        "honmei_place2": honmei_place2,
        "honmei_win": honmei_win,
        "honmei_total": honmei_total,
        "honmei_rate": honmei_rate,
        "honmei_place2_rate": honmei_place2_rate,
        "honmei_win_rate": honmei_win_rate,
        "honmei_tansho_stake": honmei_tansho_stake,
        "honmei_tansho_ret": honmei_tansho_ret,
        "honmei_tansho_roi": honmei_tansho_roi,
        "honmei_fukusho_stake": honmei_fukusho_stake,
        "honmei_fukusho_ret": honmei_fukusho_ret,
        "honmei_fukusho_roi": honmei_fukusho_roi,
        "by_confidence": conf_stats,
        "by_ticket_type": ticket_stats,
        "by_mark": mark_stats,
        "by_ana": ana_stats,
        "by_kiken": kiken_stats,
        "by_conf_ticket": conf_ticket_stats,
    }
    # ディスクキャッシュに保存（次回以降は瞬時に読み込み）
    _save_daily_cache(date, _result)
    return _result


def _try_split_combo(s: str, remaining: int, current: list, results: list):
    """連結された馬番文字列をバックトラッキングで分割する補助関数。"""
    if results:  # 最初の有効な分割が見つかれば終了
        return
    if remaining == 0:
        if not s:
            results.append(list(current))
        return
    for length in (1, 2):
        if length <= len(s):
            part = s[:length]
            if part[0] == '0':  # "01" のような先頭ゼロは無効
                continue
            n = int(part)
            if 1 <= n <= 18:  # 通常の最大頭数は18
                _try_split_combo(s[length:], remaining - 1, current + [n], results)


def _parse_fukusho_combo(combo_str: str) -> Optional[List[int]]:
    """連結された3頭分の馬番文字列をパースする (例: "10411" -> [10, 4, 11])"""
    digits = re.sub(r"[^0-9]", "", combo_str)
    if not digits:
        return None
    results: List[List[int]] = []
    _try_split_combo(digits, 3, [], results)
    return results[0] if results else None


def _get_fukusho_payout(horse_no: int, payouts: dict) -> Optional[int]:
    """
    指定馬番の複勝払戻を取得する。
    新形式: payouts["複勝"] = [{"combo": "4", "payout": 120}, {"combo": "10", "payout": 260}, ...]
    旧形式(ML): payouts["複勝"] = {"combo": "10411", "payout": 110240170}（後方互換）
    """
    fukusho = payouts.get("複勝")
    if not fukusho:
        return None
    # リスト形式（複数組）対応
    entries = fukusho if isinstance(fukusho, list) else [fukusho]
    for entry in entries:
        combo_str = str(entry.get("combo", ""))
        payout_val = entry.get("payout", 0)
        # 新形式チェック: combo が1〜2桁の単独馬番か判定
        digits_only = re.sub(r"[^0-9]", "", combo_str)
        if len(digits_only) <= 2 and digits_only:
            n_val = int(digits_only)
            if 1 <= n_val <= 28:
                # 単独エントリ（新形式）
                if horse_no == n_val:
                    return int(payout_val)
                continue
        # 旧形式（連結）: 3頭分をバックトラッキングで解析
        splits = _parse_fukusho_combo(combo_str)
        if not splits:
            continue
        # payout も等分割
        payout_str = str(payout_val)
        n = len(splits)
        chunk = len(payout_str) // n
        payout_parts = []
        for i in range(n):
            start = i * chunk
            end = start + chunk if i < n - 1 else len(payout_str)
            try:
                payout_parts.append(int(payout_str[start:end]))
            except ValueError:
                payout_parts.append(0)
        for num, pay in zip(splits, payout_parts):
            if num == horse_no:
                return pay
    return None


def _check_ticket_hit(
    ticket_type: str,
    combo: tuple,
    finish_map: dict,
    payouts: dict,
) -> Tuple[bool, float]:
    """
    買い目が的中しているか判定し、100円あたりの払戻額を返す。
    払戻テーブルがある場合はそこから、なければ推定値(0)を返す。
    ワイドは複数組の払戻リストから対応コンボを検索する。
    """
    if not combo:
        return False, 0.0

    if ticket_type == "馬連" or ticket_type == "馬連(F)":
        top2 = {h for h, f in finish_map.items() if f <= 2}
        hit = set(combo) <= top2
        payout = payouts.get("馬連", {}).get("payout", 0)
        return hit, payout

    elif ticket_type == "ワイド":
        top3 = {h for h, f in finish_map.items() if f <= 3}
        hit = set(combo) <= top3
        if not hit:
            return False, 0.0
        wide_data = payouts.get("ワイド", [])
        # 旧形式（dict）との互換性
        if isinstance(wide_data, dict):
            wide_data = [wide_data]
        if not wide_data:
            return True, 0.0
        # コンボ番号でマッチング
        a, b = sorted(int(x) for x in combo)
        for wp in wide_data:
            parts = re.split(r"[-\s→ー]", wp.get("combo", ""))
            nums = sorted(int(p) for p in parts if p.strip().isdigit())
            if len(nums) >= 2 and nums[:2] == [a, b]:
                return True, wp["payout"]
        # マッチなし → 最低払戻
        payout = min(w.get("payout", 0) for w in wide_data)
        return True, payout

    elif ticket_type == "三連複":
        top3 = {h for h, f in finish_map.items() if f <= 3}
        hit = set(int(x) for x in combo) == top3
        # 複数キー形式に対応: 三連複(netkeiba) / 3連複 / sanrenpuku(公式/keibabook)
        payout = 0
        for key in ("三連複", "3連複"):
            p = payouts.get(key, {})
            if isinstance(p, dict) and p.get("payout", 0) > 0:
                payout = p["payout"]
                break
        if not payout:
            san = payouts.get("sanrenpuku", [])
            if isinstance(san, list) and san:
                payout = san[0].get("payout", 0)
            elif isinstance(san, dict):
                payout = san.get("payout", 0)
        return hit, payout

    elif ticket_type == "三連単":
        order = [h for h, f in sorted(finish_map.items(), key=lambda x: x[1]) if f <= 3]
        hit = list(int(x) for x in combo) == order
        payout = payouts.get("三連単", {}).get("payout", 0)
        return hit, payout

    return False, 0.0


# ============================================================
# 集計キャッシュ（API高速化用）
# ============================================================
_AGG_CACHE: Dict[str, Tuple[float, dict]] = {}   # key → (timestamp, result)
_AGG_CACHE_TTL = 300.0  # 5分

def _cache_key(func_name: str, year_filter: str) -> str:
    return f"{func_name}:{year_filter}"

def _get_cached(func_name: str, year_filter: str) -> Optional[dict]:
    key = _cache_key(func_name, year_filter)
    # メモリキャッシュ
    if key in _AGG_CACHE:
        ts, result = _AGG_CACHE[key]
        if time.time() - ts < _AGG_CACHE_TTL:
            return result
    # ディスクキャッシュ（ダッシュボード再起動後でも高速）
    disk_path = os.path.join(_DAILY_CACHE_DIR, f"_agg_{func_name}_{year_filter}.json")
    if os.path.exists(disk_path):
        cache_mt = _file_mtime(disk_path)
        # 結果JSONが更新された場合のみ無効化
        # pred.jsonの変更（オッズ・印更新）は成績集計に影響しない
        import datetime
        today_str = datetime.date.today().strftime("%Y%m%d")
        today_res = os.path.join(RESULTS_DIR, f"{today_str}_results.json")
        if _file_mtime(today_res) > cache_mt:
            return None
        try:
            with open(disk_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # 日付数チェック（新しい予測ファイル追加を検出）
            cached_count = data.pop("_date_count", -1)
            if cached_count >= 0:
                dates = list_prediction_dates()
                if year_filter and year_filter != "all":
                    dates = [d for d in dates if d.startswith(year_filter)]
                if cached_count != len(dates):
                    return None
            _AGG_CACHE[key] = (time.time(), data)
            return data
        except Exception:
            pass
    return None

def _set_cached(func_name: str, year_filter: str, result: dict) -> None:
    key = _cache_key(func_name, year_filter)
    _AGG_CACHE[key] = (time.time(), result)
    # ディスクにも保存（日付数をメタデータとして付与）
    os.makedirs(_DAILY_CACHE_DIR, exist_ok=True)
    disk_path = os.path.join(_DAILY_CACHE_DIR, f"_agg_{func_name}_{year_filter}.json")
    dates = list_prediction_dates()
    if year_filter and year_filter != "all":
        dates = [d for d in dates if d.startswith(year_filter)]
    try:
        with open(disk_path, "w", encoding="utf-8") as f:
            json.dump({**result, "_date_count": len(dates)}, f, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        pass

def invalidate_aggregate_cache() -> None:
    """外部から呼び出して集計キャッシュをクリアする"""
    _AGG_CACHE.clear()
    _DAILY_AGG_MEM.clear()
    _DETAIL_MEM.clear()
    # ディスクキャッシュを全削除（_agg_* と日付別キャッシュの両方）
    if os.path.exists(_DAILY_CACHE_DIR):
        for f in os.listdir(_DAILY_CACHE_DIR):
            fp = os.path.join(_DAILY_CACHE_DIR, f)
            if os.path.isfile(fp):
                try:
                    os.remove(fp)
                except OSError:
                    pass


# ============================================================
# 日付単位ディスクキャッシュ（初回集計を劇的に高速化）
# ============================================================
_DAILY_CACHE_DIR = os.path.join(os.path.dirname(PREDICTIONS_DIR), "cache", "agg_daily")
_DAILY_AGG_MEM: Dict[str, dict] = {}  # メモリキャッシュ（プロセス内再利用）

def _daily_cache_path(date: str) -> str:
    return os.path.join(_DAILY_CACHE_DIR, f"{date.replace('-', '')}.json")

def _file_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0

def _daily_cache_valid(date: str) -> bool:
    """pred/resultsのmtimeがキャッシュより古ければ有効"""
    cp = _daily_cache_path(date)
    if not os.path.exists(cp):
        return False
    cache_mt = _file_mtime(cp)
    raw = date.replace("-", "")
    pred_p = os.path.join(PREDICTIONS_DIR, f"{raw}_pred.json")
    res_p = os.path.join(RESULTS_DIR, f"{raw}_results.json")
    if _file_mtime(pred_p) > cache_mt:
        return False
    if _file_mtime(res_p) > cache_mt:
        return False
    return True

def _load_daily_cache(date: str) -> Optional[dict]:
    """ディスクキャッシュから日付集計を読む"""
    if date in _DAILY_AGG_MEM:
        return _DAILY_AGG_MEM[date]
    if not _daily_cache_valid(date):
        return None
    try:
        with open(_daily_cache_path(date), "r", encoding="utf-8") as f:
            data = json.load(f)
        _DAILY_AGG_MEM[date] = data
        return data
    except Exception:
        return None

def _save_daily_cache(date: str, data: dict) -> None:
    """日付集計結果をディスクキャッシュに書く"""
    os.makedirs(_DAILY_CACHE_DIR, exist_ok=True)
    try:
        with open(_daily_cache_path(date), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        _DAILY_AGG_MEM[date] = data
    except Exception:
        pass


# ============================================================
# 日次詳細キャッシュ（aggregate_detailed の高速化）
# pred.json (2.5MB) の代わりに ~15KB の抽出済みデータを読む
# ============================================================
_DETAIL_CACHE_DIR = os.path.join(_DAILY_CACHE_DIR, "detail")
_DETAIL_CACHE_VERSION = 2  # v2: horses_marked に tansho_ret/fukusho_ret/ana_type/is_tokusen_kiken 追加
_DETAIL_MEM: Dict[str, list] = {}


def _detail_cache_path(date: str) -> str:
    return os.path.join(_DETAIL_CACHE_DIR, f"{date.replace('-', '')}.json")


def _load_detail_cache(date: str) -> Optional[list]:
    """日次詳細キャッシュを読む（aggregate_detailed用）"""
    if date in _DETAIL_MEM:
        return _DETAIL_MEM[date]
    cp = _detail_cache_path(date)
    if not os.path.exists(cp):
        return None
    cache_mt = _file_mtime(cp)
    raw = date.replace("-", "")
    if _file_mtime(os.path.join(PREDICTIONS_DIR, f"{raw}_pred.json")) > cache_mt:
        return None
    if _file_mtime(os.path.join(RESULTS_DIR, f"{raw}_results.json")) > cache_mt:
        return None
    try:
        with open(cp, "r", encoding="utf-8") as f:
            payload = json.load(f)
        # バージョン付きキャッシュ: {"version": N, "races": [...]}
        if isinstance(payload, dict) and "version" in payload:
            if payload["version"] != _DETAIL_CACHE_VERSION:
                return None  # バージョン不一致 → 再構築
            data = payload["races"]
        else:
            # 旧形式（バージョンなし list）→ 再構築
            return None
        _DETAIL_MEM[date] = data
        return data
    except Exception:
        return None


def _save_detail_cache(date: str, data: list) -> None:
    """日次詳細キャッシュを書く（バージョン付き）"""
    os.makedirs(_DETAIL_CACHE_DIR, exist_ok=True)
    payload = {"version": _DETAIL_CACHE_VERSION, "races": data}
    try:
        with open(_detail_cache_path(date), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        _DETAIL_MEM[date] = data
    except Exception:
        pass


def _extract_detail_races(date: str, JRA_CODES, get_venue_name) -> Optional[list]:
    """1日分の pred.json + results.json から詳細レースデータを抽出してキャッシュ保存"""
    pred = load_prediction(date)
    if not pred:
        return None

    actual = None
    if _DB_AVAILABLE:
        try:
            actual = _db.load_results(date)
        except Exception:
            pass
    if not actual:
        fpath = os.path.join(RESULTS_DIR, f"{date.replace('-', '')}_results.json")
        if os.path.exists(fpath):
            with open(fpath, "r", encoding="utf-8") as f:
                actual = json.load(f)
    if not actual:
        return None

    races_detail = []
    for race in pred["races"]:
        race_id = race.get("race_id", "")
        if not race_id:
            continue
        result = actual.get(race_id)
        if not result:
            continue

        finish_map = {r["horse_no"]: r["finish"] for r in result["order"]}
        if not finish_map:
            continue
        payouts = result.get("payouts", {})

        vc = race_id[4:6] if len(race_id) >= 6 else ""
        is_jra = vc in JRA_CODES
        venue = race.get("venue", "") or ""
        if not venue:
            venue = get_venue_name(vc) or "不明"
        surface = race.get("surface", "") or ""
        dist = int(race.get("distance", 0) or 0)
        race_no = race.get("race_no") or 0
        if not race_no and len(race_id) >= 2:
            try:
                race_no = int(race_id[-2:])
            except ValueError:
                pass
        race_name = race.get("race_name", "") or ""

        # 買い目集計
        race_by_type: Dict[str, dict] = {}
        all_tickets = list(race.get("tickets", []))
        all_tickets += [t for t in race.get("formation_tickets", []) if (t.get("stake") or 0) > 0]

        for t in all_tickets:
            stake = t.get("stake", 100) or 100
            ticket_type = t.get("type", "")
            combo = tuple(int(x) for x in t.get("combo", []))
            if ticket_type not in race_by_type:
                race_by_type[ticket_type] = {"stake": 0, "hit": False, "ret": 0}
            race_by_type[ticket_type]["stake"] += stake
            hit, payout_per_100 = _check_ticket_hit(ticket_type, combo, finish_map, payouts)
            if hit and not race_by_type[ticket_type]["hit"]:
                race_by_type[ticket_type]["hit"] = True
                race_by_type[ticket_type]["ret"] = int(payout_per_100)

        # 単勝（◉◎馬がいれば1点100円を券種として追加）
        honmei_hno = None
        honmei_name = ""
        honmei_mark = ""
        for h in race.get("horses", []):
            if h.get("mark", "") in ("◉", "◎"):
                honmei_hno = h["horse_no"]
                honmei_name = h.get("horse_name", "")
                honmei_mark = h.get("mark", "")
                break
        if honmei_hno is not None:
            tansho_pos = finish_map.get(honmei_hno, 99)
            tansho_hit = tansho_pos == 1
            tansho_ret_val = _safe_tansho_payout(payouts, honmei_hno) if tansho_hit else 0
            race_by_type["単勝"] = {"stake": 100, "hit": tansho_hit, "ret": tansho_ret_val}

        if not race_by_type:
            continue

        # 印付き馬のみ抽出（_add_to_detail_stats で使用する印のみ）
        # 各馬のtansho/fukusho払戻・穴馬/危険馬フラグも含める
        horses_marked = []
        for h in race.get("horses", []):
            mk = h.get("mark", "")
            if mk not in ("◉", "◎", "○", "▲", "△", "★", "☆", "×"):
                continue
            hno = h["horse_no"]
            pos = finish_map.get(hno, 99)
            tansho_ret_h = _safe_tansho_payout(payouts, hno) if pos == 1 else 0
            fukusho_ret_h = 0
            if pos <= 3 and mk in ("◉", "◎"):
                fp = _get_fukusho_payout(hno, payouts)
                fukusho_ret_h = fp if fp and fp > 0 else 0
            horses_marked.append({
                "horse_no": hno,
                "mark": mk,
                "tansho_ret": tansho_ret_h,
                "fukusho_ret": fukusho_ret_h,
                "ana_type": h.get("ana_type", ""),
                "is_tokusen_kiken": h.get("is_tokusen_kiken", False),
            })

        tansho_data = race_by_type.get("単勝")
        rd = {
            "race_id": race_id,
            "venue": venue,
            "vc": vc,
            "is_jra": is_jra,
            "surface": surface,
            "dist": dist,
            "dzone": _dist_zone(dist),
            "race_no": race_no,
            "race_name": race_name,
            "confidence": race.get("confidence", "B"),
            "tansho": {"stake": tansho_data["stake"], "hit": tansho_data["hit"],
                       "ret": tansho_data["ret"]} if tansho_data else None,
            "horses": horses_marked,
            "finish_map": {str(k): v for k, v in finish_map.items()},
            "honmei_hno": honmei_hno,
            "honmei_name": honmei_name,
            "honmei_mark": honmei_mark,
        }
        races_detail.append(rd)

    _save_detail_cache(date, races_detail)
    return races_detail


# ============================================================
# 全日付の通算成績
# ============================================================


def aggregate_all(year_filter: str = "all") -> dict:
    """全予想日（または指定年）の通算集計。Walk-Forward方式のため全年含む。"""
    cached = _get_cached("aggregate_all", year_filter)
    if cached is not None:
        return cached
    dates = list_prediction_dates()
    if year_filter and year_filter != "all":
        dates = [d for d in dates if d.startswith(year_filter)]

    summary = {
        "total_races": 0,
        "total_tickets": 0,
        "hit_tickets": 0,
        "total_stake": 0,
        "total_return": 0,
        "honmei_placed": 0,
        "honmei_place2": 0,
        "honmei_win": 0,
        "honmei_total": 0,
        "honmei_tansho_stake": 0,
        "honmei_tansho_ret": 0,
        "honmei_fukusho_stake": 0,
        "honmei_fukusho_ret": 0,
        "by_date": [],
        "by_confidence": {},
        "by_ticket_type": {},
        "by_mark": {},
        "by_ana": {"total": 0, "win": 0, "place2": 0, "placed": 0, "tansho_stake": 0, "tansho_ret": 0, "fukusho_stake": 0, "fukusho_ret": 0},
        "by_kiken": {"total": 0, "fell_through": 0},
        "by_conf_ticket": {},
    }
    for date in dates:
        r = compare_and_aggregate(date)
        if not r:
            continue
        summary["total_races"] += r["total_races"]
        summary["total_tickets"] += r["total_tickets"]
        summary["hit_tickets"] += r["hit_tickets"]
        summary["total_stake"] += r["total_stake"]
        summary["total_return"] += r["total_return"]
        summary["honmei_placed"] += r["honmei_placed"]
        summary["honmei_place2"] += r.get("honmei_place2", 0)
        summary["honmei_win"] += r.get("honmei_win", 0)
        summary["honmei_total"] += r["honmei_total"]
        summary["honmei_tansho_stake"] += r.get("honmei_tansho_stake", r.get("honmei_total", 0) * 100)
        summary["honmei_tansho_ret"] += r.get("honmei_tansho_ret", 0)
        summary["honmei_fukusho_stake"] += r.get("honmei_fukusho_stake", 0)
        summary["honmei_fukusho_ret"] += r.get("honmei_fukusho_ret", 0)
        summary["by_date"].append(r)
        for conf, st in r.get("by_confidence", {}).items():
            if conf not in summary["by_confidence"]:
                summary["by_confidence"][conf] = {"races": 0, "hits": 0, "stake": 0, "ret": 0}
            for k in ("races", "hits", "stake", "ret"):
                summary["by_confidence"][conf][k] += st.get(k, 0)
        for tt, st in r.get("by_ticket_type", {}).items():
            if tt not in summary["by_ticket_type"]:
                summary["by_ticket_type"][tt] = {"total": 0, "hits": 0, "stake": 0, "ret": 0, "payouts": []}
            for k in ("total", "hits", "stake", "ret"):
                summary["by_ticket_type"][tt][k] += st.get(k, 0)
            summary["by_ticket_type"][tt]["payouts"].extend(st.get("payouts", []))
        for mk, st in r.get("by_mark", {}).items():
            if mk not in summary["by_mark"]:
                summary["by_mark"][mk] = {
                    "total": 0, "win": 0, "place2": 0, "placed": 0,
                    "tansho_stake": 0, "tansho_ret": 0,
                }
            for k in ("total", "win", "place2", "placed", "tansho_stake", "tansho_ret"):
                summary["by_mark"][mk][k] += st.get(k, 0)
        for k in ("total", "win", "place2", "placed", "tansho_stake", "tansho_ret", "fukusho_stake", "fukusho_ret"):
            summary["by_ana"][k] += r.get("by_ana", {}).get(k, 0)
        for k in ("total", "fell_through"):
            summary["by_kiken"][k] += r.get("by_kiken", {}).get(k, 0)
        for ct_key, st in r.get("by_conf_ticket", {}).items():
            if ct_key not in summary["by_conf_ticket"]:
                summary["by_conf_ticket"][ct_key] = {"total": 0, "hits": 0, "stake": 0, "ret": 0, "payouts": []}
            for k in ("total", "hits", "stake", "ret"):
                summary["by_conf_ticket"][ct_key][k] += st.get(k, 0)
            summary["by_conf_ticket"][ct_key]["payouts"].extend(st.get("payouts", []))

    # 通算の率計算
    _at = summary["by_ana"]["total"]
    _ts_ana = summary["by_ana"]["tansho_stake"]
    _fs_ana = summary["by_ana"]["fukusho_stake"]
    summary["by_ana"]["win_rate"]    = round(summary["by_ana"]["win"]    / _at * 100, 1) if _at else 0.0
    summary["by_ana"]["place2_rate"] = round(summary["by_ana"]["place2"] / _at * 100, 1) if _at else 0.0
    summary["by_ana"]["place_rate"]  = round(summary["by_ana"]["placed"] / _at * 100, 1) if _at else 0.0
    summary["by_ana"]["tansho_roi"]  = round(summary["by_ana"]["tansho_ret"] / _ts_ana * 100, 1) if _ts_ana else 0.0
    summary["by_ana"]["fukusho_roi"] = round(summary["by_ana"]["fukusho_ret"] / _fs_ana * 100, 1) if _fs_ana else 0.0
    _kt = summary["by_kiken"]["total"]
    summary["by_kiken"]["fell_rate"] = round(summary["by_kiken"]["fell_through"] / _kt * 100, 1) if _kt else 0.0

    ts = summary["total_stake"]
    tr = summary["total_return"]
    ht = summary["hit_tickets"]
    tt = summary["total_tickets"]
    hm = summary["honmei_total"]
    summary["roi"] = round(tr / ts * 100, 1) if ts > 0 else 0.0
    summary["hit_rate"] = round(ht / tt * 100, 1) if tt > 0 else 0.0
    summary["honmei_rate"]       = round(summary["honmei_placed"] / hm * 100, 1) if hm > 0 else 0.0
    summary["honmei_place2_rate"] = round(summary["honmei_place2"] / hm * 100, 1) if hm > 0 else 0.0
    summary["honmei_win_rate"]    = round(summary["honmei_win"]    / hm * 100, 1) if hm > 0 else 0.0
    _hmts = summary["honmei_tansho_stake"]
    _hmtr = summary["honmei_tansho_ret"]
    summary["honmei_tansho_roi"] = round(_hmtr / _hmts * 100, 1) if _hmts > 0 else 0.0
    _hmfs = summary["honmei_fukusho_stake"]
    _hmfr = summary["honmei_fukusho_ret"]
    summary["honmei_fukusho_roi"] = round(_hmfr / _hmfs * 100, 1) if _hmfs > 0 else 0.0
    summary["profit"] = tr - ts

    # 券種別・自信度×券種別の min/max/avg 配当計算
    for _stats_dict in (summary["by_ticket_type"], summary["by_conf_ticket"]):
        for _entry in _stats_dict.values():
            plist = [p for p in _entry.get("payouts", []) if p > 0]
            if plist:
                _entry["min_payout"] = min(plist)
                _entry["max_payout"] = max(plist)
                _entry["avg_payout"] = round(sum(plist) / len(plist))
            else:
                _entry["min_payout"] = 0
                _entry["max_payout"] = 0
                _entry["avg_payout"] = 0
            # 生リストは返さない（JSONサイズ削減）
            if "payouts" in _entry:
                del _entry["payouts"]

    # 期間情報（結果取得済み日付ベースで統一）
    from config.settings import RESULTS_DIR
    fetched = [d for d in dates if os.path.exists(os.path.join(RESULTS_DIR, f"{d.replace('-','')}_results.json"))]
    if fetched:
        summary["period_oldest"] = fetched[-1]
        summary["period_newest"] = fetched[0]
        summary["period_days"] = len(fetched)
        summary["fetched_oldest"] = fetched[-1]
        summary["fetched_newest"] = fetched[0]
        summary["fetched_count"] = len(fetched)
    elif dates:
        summary["period_oldest"] = dates[-1]
        summary["period_newest"] = dates[0]
        summary["period_days"] = len(dates)

    _set_cached("aggregate_all", year_filter, summary)
    return summary


# ============================================================
# 詳細集計: 競馬場別・コース別・距離区分別・高額配当TOP10
# ============================================================


def _dist_zone(dist: int) -> str:
    """距離をSMILEゾーンに分類
    SS: 0-1000m, S: 1001-1400m, M: 1401-1800m,
    I: 1801-2200m, L: 2201-2600m, E: 2601+m
    """
    if dist <= 1000:
        return "SS"
    if dist <= 1400:
        return "S"
    if dist <= 1800:
        return "M"
    if dist <= 2200:
        return "I"
    if dist <= 2600:
        return "L"
    return "E"


def _new_detail_stats() -> dict:
    return {
        "total_races": 0,
        "total_tickets": 0,
        "hit_tickets": 0,
        "total_stake": 0,
        "total_return": 0,
        "roi": 0.0,
        "hit_rate": 0.0,
        "tansho":     {"total": 0, "hits": 0, "stake": 0, "ret": 0, "roi": 0.0, "hit_rate": 0.0, "payouts": []},
        "by_mark": {},       # {mark: {total, win, place2, placed, tansho_stake, tansho_ret}}
        "by_conf": {},       # {conf: {total, hits, stake, ret, payouts:[]}}
        # 本命（◉◎）統計
        "honmei_total": 0, "honmei_win": 0, "honmei_place2": 0, "honmei_placed": 0,
        "honmei_tansho_stake": 0, "honmei_tansho_ret": 0,
        "honmei_fukusho_stake": 0, "honmei_fukusho_ret": 0,
        # 穴馬・危険馬
        "by_ana": {"total": 0, "win": 0, "place2": 0, "placed": 0,
                   "tansho_stake": 0, "tansho_ret": 0},
        "by_kiken": {"total": 0, "fell_through": 0},
    }


def _add_to_detail_stats(stats: dict, race_by_type: dict,
                         race: dict = None, finish_map: dict = None,
                         result_order: list = None) -> None:
    """race_by_type を stats に加算する（単勝のみ集計 + 印別・自信度別）"""
    tansho = race_by_type.get("単勝")
    race_total_stake = tansho["stake"] if tansho else 0
    race_total_ret   = tansho["ret"] if (tansho and tansho["hit"]) else 0
    stats["total_races"] += 1
    if tansho:
        stats["total_tickets"] += 1
        stats["total_stake"]   += race_total_stake
        stats["total_return"]  += race_total_ret
        if tansho["hit"]:
            stats["hit_tickets"] += 1
    for tt, key in (("単勝", "tansho"),):
        rg = race_by_type.get(tt)
        if rg is None:
            continue
        s = stats[key]
        s["total"] += 1
        s["stake"] += rg["stake"]
        if rg["hit"]:
            s["hits"] += 1
            s["ret"]  += rg["ret"]
            s["payouts"].append(rg["ret"])

    # 印別成績 + 本命・穴馬・危険馬集計
    if race and finish_map:
        bm = stats["by_mark"]
        for h in race.get("horses", []):
            mk = h.get("mark", "")
            if mk not in ("◉", "◎", "○", "▲", "△", "★", "☆", "×"):
                continue
            pos = finish_map.get(h["horse_no"], 99)
            if mk not in bm:
                bm[mk] = {"total": 0, "win": 0, "place2": 0, "placed": 0,
                           "tansho_stake": 0, "tansho_ret": 0}
            bm[mk]["total"] += 1
            if pos == 1:
                bm[mk]["win"] += 1
            if pos <= 2:
                bm[mk]["place2"] += 1
            if pos <= 3:
                bm[mk]["placed"] += 1
            # 印別 単勝100円シミュレーション
            bm[mk]["tansho_stake"] += 100
            if pos == 1:
                bm[mk]["tansho_ret"] += h.get("tansho_ret", 0)

            # 本命（◉◎）統計
            if mk in ("◉", "◎"):
                stats["honmei_total"] += 1
                if pos == 1:
                    stats["honmei_win"] += 1
                if pos <= 2:
                    stats["honmei_place2"] += 1
                if pos <= 3:
                    stats["honmei_placed"] += 1
                stats["honmei_tansho_stake"] += 100
                if pos == 1:
                    stats["honmei_tansho_ret"] += h.get("tansho_ret", 0)
                stats["honmei_fukusho_stake"] += 100
                if pos <= 3:
                    stats["honmei_fukusho_ret"] += h.get("fukusho_ret", 0)

            # 穴馬（ana_type が有効な値）
            ana_t = h.get("ana_type", "")
            if ana_t and ana_t not in ("none", "該当なし", "なし", "-", ""):
                stats["by_ana"]["total"] += 1
                stats["by_ana"]["tansho_stake"] += 100
                if pos == 1:
                    stats["by_ana"]["win"] += 1
                    stats["by_ana"]["tansho_ret"] += h.get("tansho_ret", 0)
                if pos <= 2:
                    stats["by_ana"]["place2"] += 1
                if pos <= 3:
                    stats["by_ana"]["placed"] += 1

            # 危険馬（is_tokusen_kiken）→ 4着以下なら予測成功
            if h.get("is_tokusen_kiken", False):
                stats["by_kiken"]["total"] += 1
                if pos >= 4:
                    stats["by_kiken"]["fell_through"] += 1

    # 自信度別成績（単勝ベース）
    if race and tansho:
        conf = race.get("confidence", "B")
        bc = stats["by_conf"]
        if conf not in bc:
            bc[conf] = {"total": 0, "hits": 0, "stake": 0, "ret": 0, "payouts": []}
        bc[conf]["total"] += 1
        bc[conf]["stake"] += tansho["stake"]
        if tansho["hit"]:
            bc[conf]["hits"] += 1
            bc[conf]["ret"] += tansho["ret"]
            bc[conf]["payouts"].append(tansho["ret"])


def _finalize_detail_stats(stats: dict) -> None:
    """率・回収率を計算して stats に追記"""
    ts = stats["total_stake"]
    tr = stats["total_return"]
    tt = stats["total_tickets"]
    ht = stats["hit_tickets"]
    stats["roi"]      = round(tr / ts * 100, 1) if ts > 0 else 0.0
    stats["hit_rate"] = round(ht / tt * 100, 1) if tt > 0 else 0.0
    stats["profit"]   = tr - ts
    for key in ("tansho",):
        s = stats[key]
        s["roi"]      = round(s["ret"] / s["stake"] * 100, 1) if s["stake"] > 0 else 0.0
        s["hit_rate"] = round(s["hits"] / s["total"] * 100, 1) if s["total"] > 0 else 0.0
        plist = [p for p in s.get("payouts", []) if p > 0]
        if plist:
            s["min_payout"] = min(plist)
            s["max_payout"] = max(plist)
            s["avg_payout"] = round(sum(plist) / len(plist))
        else:
            s["min_payout"] = 0
            s["max_payout"] = 0
            s["avg_payout"] = 0
        if "payouts" in s:
            del s["payouts"]

    # 印別: 勝率/連対率/複勝率/単勝ROI
    for mk, ms in stats.get("by_mark", {}).items():
        t = ms["total"]
        ms["win_rate"]    = round(ms["win"]    / t * 100, 1) if t else 0.0
        ms["place2_rate"] = round(ms["place2"] / t * 100, 1) if t else 0.0
        ms["place_rate"]  = round(ms["placed"] / t * 100, 1) if t else 0.0
        ts_mk = ms.get("tansho_stake", 0)
        ms["tansho_roi"]  = round(ms.get("tansho_ret", 0) / ts_mk * 100, 1) if ts_mk > 0 else 0.0

    # 自信度別: 的中率/回収率/最高/平均配当
    for conf, cs in stats.get("by_conf", {}).items():
        cs["hit_rate"] = round(cs["hits"] / cs["total"] * 100, 1) if cs["total"] else 0.0
        cs["roi"]      = round(cs["ret"] / cs["stake"] * 100, 1) if cs["stake"] else 0.0
        plist = [p for p in cs.get("payouts", []) if p > 0]
        cs["max_payout"] = max(plist) if plist else 0
        cs["avg_payout"] = round(sum(plist) / len(plist)) if plist else 0
        if "payouts" in cs:
            del cs["payouts"]

    # 本命（◉◎）率
    hm = stats.get("honmei_total", 0)
    if hm > 0:
        stats["honmei_win_rate"]    = round(stats["honmei_win"]    / hm * 100, 1)
        stats["honmei_place2_rate"] = round(stats["honmei_place2"] / hm * 100, 1)
        stats["honmei_rate"]        = round(stats["honmei_placed"] / hm * 100, 1)
    else:
        stats["honmei_win_rate"] = stats["honmei_place2_rate"] = stats["honmei_rate"] = 0.0
    hmts = stats.get("honmei_tansho_stake", 0)
    stats["honmei_tansho_roi"] = round(stats["honmei_tansho_ret"] / hmts * 100, 1) if hmts > 0 else 0.0
    hmfs = stats.get("honmei_fukusho_stake", 0)
    stats["honmei_fukusho_roi"] = round(stats["honmei_fukusho_ret"] / hmfs * 100, 1) if hmfs > 0 else 0.0

    # 穴馬率
    _at = stats["by_ana"]["total"]
    if _at > 0:
        stats["by_ana"]["win_rate"]    = round(stats["by_ana"]["win"]    / _at * 100, 1)
        stats["by_ana"]["place2_rate"] = round(stats["by_ana"]["place2"] / _at * 100, 1)
        stats["by_ana"]["place_rate"]  = round(stats["by_ana"]["placed"] / _at * 100, 1)
        _ts_ana = stats["by_ana"]["tansho_stake"]
        stats["by_ana"]["tansho_roi"]  = round(stats["by_ana"]["tansho_ret"] / _ts_ana * 100, 1) if _ts_ana > 0 else 0.0

    # 危険馬率
    _kt = stats["by_kiken"]["total"]
    if _kt > 0:
        stats["by_kiken"]["fell_rate"] = round(stats["by_kiken"]["fell_through"] / _kt * 100, 1)


def aggregate_detailed(year_filter: str = "all", after_filter: str = "",
                       exclude_venues: set = None) -> dict:
    """
    詳細集計: 全体/JRA/NAR ごとに 競馬場別・コース別・距離区分別 + 高額配当TOP10。
    Walk-Forward方式のため全年含む。

    Args:
        year_filter: 年フィルタ（"2026" 等）
        after_filter: この日付以降のみ集計（"YYYY-MM-DD" 形式）
        exclude_venues: 除外する競馬場名のセット（例: {"帯広"}）
    """
    cache_key = f"{year_filter}_{after_filter}_{sorted(exclude_venues) if exclude_venues else ''}"
    cached = _get_cached("aggregate_detailed", cache_key)
    if cached is not None:
        return cached
    from data.masters.venue_master import JRA_CODES, get_venue_name

    dates = list_prediction_dates()
    if year_filter and year_filter != "all":
        dates = [d for d in dates if d.startswith(year_filter)]
    if after_filter:
        # after_filter: "YYYY-MM-DD" → dates は "YYYY-MM-DD" 形式
        dates = [d for d in dates if d >= after_filter]

    # 集計コンテナ (全体/JRA/NAR)
    cats = {
        "all": {
            "stats": _new_detail_stats(),
            "by_venue": {},
            "by_surface": {},
            "by_dist_zone": {},
        },
        "jra": {
            "stats": _new_detail_stats(),
            "by_venue": {},
            "by_surface": {},
            "by_dist_zone": {},
        },
        "nar": {
            "stats": _new_detail_stats(),
            "by_venue": {},
            "by_surface": {},
            "by_dist_zone": {},
        },
    }

    top_tansho: List[dict] = []
    daily_stats: dict = {}  # key: (date, cat_key) → 日次集計

    def _ensure(d: dict, k: str) -> dict:
        if k not in d:
            d[k] = _new_detail_stats()
        return d[k]

    def _ensure_venue(d: dict, k: str) -> dict:
        """venue バケットは by_surface / by_dist_zone / top10 をネストする"""
        if k not in d:
            d[k] = {**_new_detail_stats(), "by_surface": {}, "by_dist_zone": {}, "top10_tansho": []}
        return d[k]

    for date in dates:
        # 日次詳細キャッシュ優先（~15KB vs pred.json ~2.5MB → 170倍高速）
        races_data = _load_detail_cache(date)
        if races_data is None:
            races_data = _extract_detail_races(date, JRA_CODES, get_venue_name)
            if races_data is None:
                continue

        for rd in races_data:
            tansho = rd.get("tansho")
            if tansho is None:
                continue
            # 除外競馬場フィルタ（ばんえい等）
            if exclude_venues and rd.get("venue") in exclude_venues:
                continue
            race_by_type = {"単勝": tansho}

            # _add_to_detail_stats 用のrace/finish_map
            race_dict = {"horses": rd["horses"], "confidence": rd["confidence"]}
            finish_map = {int(k): v for k, v in rd["finish_map"].items()}
            extra = dict(race=race_dict, finish_map=finish_map)

            is_jra = rd["is_jra"]
            venue = rd["venue"]
            surface = rd["surface"]
            dzone = rd["dzone"]

            cat_key = "jra" if is_jra else "nar"
            for ckey in ("all", cat_key):
                c = cats[ckey]
                _add_to_detail_stats(c["stats"], race_by_type, **extra)
                vs = _ensure_venue(c["by_venue"], venue)
                _add_to_detail_stats(vs, race_by_type, **extra)
                if surface:
                    _add_to_detail_stats(_ensure(vs["by_surface"], surface), race_by_type, **extra)
                _add_to_detail_stats(_ensure(vs["by_dist_zone"], dzone), race_by_type, **extra)
                if surface:
                    _add_to_detail_stats(_ensure(c["by_surface"], surface), race_by_type, **extra)
                _add_to_detail_stats(_ensure(c["by_dist_zone"], dzone), race_by_type, **extra)

            # 高額配当 TOP10 用（単勝のみ）
            if tansho["hit"] and tansho["ret"] > 0:
                entry = {
                    "date":       date,
                    "venue":      venue,
                    "race_no":    rd["race_no"],
                    "race_name":  rd["race_name"],
                    "race_id":    rd["race_id"],
                    "marks":      rd.get("honmei_mark", ""),
                    "combo":      str(rd["honmei_hno"]) if rd.get("honmei_hno") else "",
                    "horse_name": rd.get("honmei_name", ""),
                    "payout":     tansho["ret"],
                    "is_jra":     is_jra,
                }
                top_tansho.append(entry)
                for ckey in ("all", cat_key):
                    _ensure_venue(cats[ckey]["by_venue"], venue)["top10_tansho"].append(entry)

            # --- 日次集計（的中率 TOP10 用） ---
            for ckey in ("all", cat_key):
                # カテゴリ全体の日次集計
                ds_key = (date, ckey)
                if ds_key not in daily_stats:
                    daily_stats[ds_key] = {
                        "date": date, "total_races": 0, "hit_races": 0,
                        "total_stake": 0, "total_return": 0,
                        "honmei_total": 0, "honmei_win": 0,
                        "honmei_place2": 0, "honmei_placed": 0,
                    }
                ds = daily_stats[ds_key]
                ds["total_races"] += 1
                if tansho["hit"]:
                    ds["hit_races"] += 1
                ds["total_stake"] += tansho["stake"]
                ds["total_return"] += tansho["ret"] if tansho["hit"] else 0

                # 競馬場別の日次集計
                dsv_key = (date, ckey, venue)
                if dsv_key not in daily_stats:
                    daily_stats[dsv_key] = {
                        "date": date, "venue": venue, "total_races": 0, "hit_races": 0,
                        "total_stake": 0, "total_return": 0,
                        "honmei_total": 0, "honmei_win": 0,
                        "honmei_place2": 0, "honmei_placed": 0,
                    }
                dsv = daily_stats[dsv_key]
                dsv["total_races"] += 1
                if tansho["hit"]:
                    dsv["hit_races"] += 1
                dsv["total_stake"] += tansho["stake"]
                dsv["total_return"] += tansho["ret"] if tansho["hit"] else 0

                # 軸馬成績（全体・競馬場別共通）
                honmei_hno = rd.get("honmei_hno")
                fm = rd.get("finish_map", {})
                if honmei_hno is not None and fm:
                    for _ds in (ds, dsv):
                        _ds["honmei_total"] += 1
                    fin = fm.get(str(honmei_hno), fm.get(honmei_hno, 99))
                    if isinstance(fin, str):
                        fin = int(fin) if fin.isdigit() else 99
                    if fin == 1:
                        ds["honmei_win"] += 1
                        dsv["honmei_win"] += 1
                    if fin <= 2:
                        ds["honmei_place2"] += 1
                        dsv["honmei_place2"] += 1
                    if fin <= 3:
                        ds["honmei_placed"] += 1
                        dsv["honmei_placed"] += 1

    # 率の確定
    for cat in cats.values():
        _finalize_detail_stats(cat["stats"])
        for vd in cat["by_venue"].values():
            _finalize_detail_stats(vd)
            for sd in vd.get("by_surface", {}).values():
                _finalize_detail_stats(sd)
            for dd in vd.get("by_dist_zone", {}).values():
                _finalize_detail_stats(dd)
            # venue 内 top10 をソート・トリム
            vd["top10_tansho"] = sorted(vd.get("top10_tansho", []), key=lambda x: -x["payout"])[:10]
        for d in cat["by_surface"].values():
            _finalize_detail_stats(d)
        for d in cat["by_dist_zone"].values():
            _finalize_detail_stats(d)

    top_tansho.sort(key=lambda x: -x["payout"])

    # 的中率 TOP10 生成（日次集計）
    def _finalize_honmei_items(items):
        """軸馬率の率計算"""
        for item in items:
            n = item["total_races"]
            item["hit_rate"] = round(item["hit_races"] / n * 100, 1) if n else 0
            item["profit"] = item["total_return"] - item["total_stake"]
            ht = item["honmei_total"]
            item["honmei_win_rate"] = round(item["honmei_win"] / ht * 100, 1) if ht else 0
            item["honmei_place2_rate"] = round(item["honmei_place2"] / ht * 100, 1) if ht else 0
            item["honmei_placed_rate"] = round(item["honmei_placed"] / ht * 100, 1) if ht else 0
        items.sort(key=lambda x: (-x["honmei_win_rate"], -x["profit"]))
        return items[:10]

    for cat_key in ("all", "jra", "nar"):
        # カテゴリ全体の軸馬率 TOP10（キーが2要素のもの = 全体集計）
        items = [v for k, v in daily_stats.items()
                 if len(k) == 2 and k[1] == cat_key and v["total_races"] >= 3]
        cats[cat_key]["top10_honmei"] = _finalize_honmei_items(items)

        # 競馬場別の軸馬率 TOP10（キーが3要素のもの = 競馬場別集計）
        venue_items: dict = {}  # venue → list
        for k, v in daily_stats.items():
            if len(k) == 3 and k[1] == cat_key and v["total_races"] >= 3:
                venue_name = k[2]
                venue_items.setdefault(venue_name, []).append(v)
        for venue_name, vitems in venue_items.items():
            vd = cats[cat_key]["by_venue"].get(venue_name)
            if vd is not None:
                vd["top10_honmei"] = _finalize_honmei_items(vitems)

    result = {
        **cats,
        "top10_tansho":     top_tansho[:10],
    }
    _set_cached("aggregate_detailed", cache_key, result)
    return result


# ============================================================
# 配布用（簡易）HTML 生成
# ============================================================

_SIMPLE_CSS = """
*{box-sizing:border-box}
body{font-family:'Noto Sans JP',sans-serif;font-size:13px;margin:16px;background:#fff;color:#1f2937;line-height:1.5}
.date-header{font-size:1.3em;font-weight:700;padding:8px 14px;background:#166534;color:#fff;border-radius:6px;margin-bottom:14px}
.venue-section{margin-bottom:20px}
.venue-hdr{font-size:1.05em;font-weight:700;padding:5px 10px;background:#dcfce7;border-left:4px solid #16a34a;margin-bottom:8px}
.race-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:8px}
.race-card{border:1px solid #d1d5db;border-radius:6px;padding:9px 10px;background:#fafafa}
.race-hd{font-weight:600;font-size:0.93em;margin-bottom:3px}
.race-info{font-size:0.78em;color:#6b7280;margin-bottom:5px;display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.badge{display:inline-block;padding:0 5px;border-radius:3px;font-size:0.75em;font-weight:600}
.HH{background:#fee2e2;color:#991b1b}.HM{background:#fef3c7;color:#92400e}
.MM{background:#f0fdf4;color:#166534}.MS{background:#eff6ff;color:#1e40af}.SS{background:#f5f3ff;color:#5b21b6}
.conf{background:#f1f5f9;color:#374151}
table.mk{width:100%;border-collapse:collapse;font-size:0.82em;margin-bottom:5px}
table.mk th{background:#f8fafc;padding:2px 5px;border:1px solid #e2e8f0;font-weight:600}
table.mk td{padding:2px 5px;border:1px solid #e2e8f0}
.m-honmei{color:#dc2626;font-weight:700;font-size:1.05em}
.m-taikou{color:#2563eb;font-weight:700}.m-tannuke{color:#059669;font-weight:700}
.m-other{color:#7c3aed;font-weight:700}
.ana-b{color:#dc2626;font-size:0.75em;font-weight:700} .kiken-b{color:#9ca3af;font-size:0.75em}
.tickets{font-size:0.8em;color:#374151}
.t-row{display:flex;gap:5px;padding:1px 0;align-items:baseline;flex-wrap:wrap}
.t-type{font-weight:600;min-width:40px}.t-combo{min-width:55px}
.t-ev{color:#6b7280;font-size:0.9em}.t-sig{font-weight:600}
.t-sig.strong{color:#dc2626}.t-sig.buy{color:#2563eb}.t-sig.ok{color:#059669}
.t-stake{color:#374151;font-weight:600}
.no-buy{color:#9ca3af;font-size:0.85em;font-style:italic}
.vb-row{background:#fef9c3;padding:2px 6px;border-radius:3px;font-size:0.77em;margin-top:3px;color:#713f12}
@media print{.race-grid{grid-template-columns:repeat(3,1fr)} body{margin:8px;font-size:12px}}
"""


def generate_simple_html(date: str, output_dir: str) -> Optional[str]:
    """
    pred.json から印・買い目のみの配布用HTMLを生成して保存する。
    Returns: 保存先ファイルパス or None（予想データなし）
    """
    pred = load_prediction(date)
    if not pred:
        return None

    venues: dict = {}
    for race in pred.get("races", []):
        v = race.get("venue", "?")
        venues.setdefault(v, []).append(race)

    html = _render_simple_html(date, venues)
    os.makedirs(output_dir, exist_ok=True)
    date_key = date.replace("-", "")
    fname = f"{date_key}_配布用.html"
    fpath = os.path.join(output_dir, fname)
    with open(fpath, "w", encoding="utf-8") as f:
        f.write(html)
    return fpath


def _sig_cls(sig: str) -> str:
    if "勝負" in sig:
        return "strong"
    if "◎" in sig or "○" in sig:
        return "buy"
    return "ok"


def _render_simple_html(date: str, venues: dict) -> str:
    mark_cls = {"◎": "m-honmei", "◉": "m-honmei", "○": "m-taikou", "▲": "m-tannuke"}
    mark_order = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "★": 5, "☆": 6, "×": 7}

    sections = []
    for venue, races in venues.items():
        cards = []
        for race in sorted(races, key=lambda r: r.get("race_no", 0)):
            race_no = race.get("race_no", "?")
            race_name = race.get("race_name", "")
            surface = race.get("surface", "")
            distance = race.get("distance", 0)
            grade = race.get("grade", "")
            conf = race.get("confidence", "B")
            pace = race.get("pace_predicted", "MM")
            field = race.get("field_count", 0)

            # 印付き馬テーブル
            marked = [h for h in race.get("horses", []) if h.get("mark") in mark_cls or h.get("mark") in ("△", "★", "☆", "×")]
            marked.sort(key=lambda h: (mark_order.get(h.get("mark", "—"), 9), -h.get("composite", 0)))

            if marked:
                rows_html = ""
                for h in marked:
                    mk = h.get("mark", "—")
                    cls = mark_cls.get(mk, "m-other")
                    odds = h.get("odds")
                    odds_s = f"{odds:.1f}" if odds else "—"
                    p3 = h.get("place3_prob", 0) or 0
                    p3_s = f"{p3*100:.0f}%" if p3 else "—"
                    ana = h.get("ana_type", "none")
                    kiken = h.get("kiken_type", "none")
                    badge = ""
                    if ana and ana != "none":
                        badge = '<span class="ana-b">☆</span>'
                    elif kiken and kiken != "none":
                        badge = '<span class="kiken-b">×</span>'
                    rows_html += (
                        f"<tr><td class='{cls}'>{mk}</td>"
                        f"<td>{h.get('horse_name','')}{badge}</td>"
                        f"<td style='text-align:right'>{odds_s}</td>"
                        f"<td style='text-align:right'>{p3_s}</td></tr>"
                    )
                mk_html = (
                    "<table class='mk'><thead><tr>"
                    "<th>印</th><th>馬名</th><th>単勝</th><th>複勝率</th>"
                    "</tr></thead><tbody>" + rows_html + "</tbody></table>"
                )
            else:
                mk_html = "<p class='no-buy'>印なし</p>"

            # 買い目（通常 + フォーメーション）
            ticket_rows = []
            for t in list(race.get("tickets", [])) + list(race.get("formation_tickets", [])):
                stake = t.get("stake", 0) or 0
                if stake == 0:
                    continue
                tt = t.get("type", "")
                combo = t.get("combo", [])
                combo_s = "-".join(str(x) for x in combo)
                ev = t.get("ev", 0) or 0
                sig = t.get("signal", "")
                sc = _sig_cls(sig)
                ticket_rows.append(
                    f"<div class='t-row'>"
                    f"<span class='t-type'>{tt}</span>"
                    f"<span class='t-combo'>{combo_s}</span>"
                    f"<span class='t-ev'>EV{ev:.0f}%</span>"
                    f"<span class='t-sig {sc}'>{sig}</span>"
                    f"<span class='t-stake'>{stake:,}円</span>"
                    f"</div>"
                )
            if ticket_rows:
                tickets_html = "<div class='tickets'>" + "".join(ticket_rows) + "</div>"
            else:
                tickets_html = "<p class='no-buy'>見送り</p>"

            # バリューベット（S/A のみ）
            vb_rows = []
            for vb in race.get("value_bets", []):
                if vb.get("signal") not in ("S", "A"):
                    continue
                sig = vb.get("signal", "")
                tt = vb.get("type", "")
                name = vb.get("name", "")
                div = vb.get("divergence", 0) or 0
                pred_o = vb.get("predicted_odds", 0) or 0
                act_o = vb.get("actual_odds", 0) or 0
                vb_rows.append(
                    f"<div class='vb-row'>⚡{sig} {tt} {name} "
                    f"予想{pred_o:.1f}→実{act_o:.1f}倍 (乖離{div:.1f}x)</div>"
                )

            grade_s = f" [{grade}]" if grade and grade not in ("未勝利", "新馬", "1勝", "2勝", "3勝") else ""
            cards.append(
                f"<div class='race-card'>"
                f"<div class='race-hd'>{race_no}R {race_name}{grade_s}</div>"
                f"<div class='race-info'>"
                f"<span>{surface}{distance}m</span>"
                f"<span>{field}頭</span>"
                f"<span class='badge {pace}'>{pace}</span>"
                f"<span class='badge conf'>{conf}</span>"
                f"</div>"
                f"{mk_html}{tickets_html}{''.join(vb_rows)}"
                f"</div>"
            )

        sections.append(
            f"<div class='venue-section'>"
            f"<div class='venue-hdr'>{venue}</div>"
            f"<div class='race-grid'>{''.join(cards)}</div>"
            f"</div>"
        )

    return (
        f"<!DOCTYPE html><html lang='ja'><head>"
        f"<meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"<title>D-AI 予想 {date} 配布用</title>"
        f"<style>{_SIMPLE_CSS}</style></head><body>"
        f"<div class='date-header'>D-AI 競馬予想　{date}（印・買い目）</div>"
        f"{''.join(sections)}</body></html>"
    )
