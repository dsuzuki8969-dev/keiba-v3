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
                    "running_style": "逃げ"
                    if h.horse_no in _leading_set
                    else (
                        ev.pace.running_style.value
                        if ev.pace.running_style
                        else ""
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
                    # 穴馬・危険馬
                    "ana_score": round(ev.ana_score, 2),
                    "ana_type": ev.ana_type.value if ev.ana_type else "none",
                    "tokusen_score": round(ev.tokusen_score, 2),
                    "is_tokusen": ev.is_tokusen,
                    "kiken_score": round(ev.kiken_score, 2),
                    "kiken_type": ev.kiken_type.value if ev.kiken_type else "none",
                    # ML三連率
                    "ml_win_prob": _round_or_none(ev.ml_win_prob, 4),
                    "ml_top2_prob": _round_or_none(ev.ml_top2_prob, 4),
                    "ml_place_prob": _round_or_none(ev.ml_place_prob, 4),
                    # 予想オッズ・乖離
                    "predicted_tansho_odds": _round_or_none(ev.predicted_tansho_odds),
                    "odds_divergence": _round_or_none(ev.odds_divergence),
                    "divergence_signal": ev.divergence_signal or "",
                    # 調教データ (J-4)
                    "training_intensity": _extract_training_summary(ev.training_records),
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

            for t in analysis.tickets:
                race_data["tickets"].append(
                    {
                        "type": t.get("type", ""),
                        "combo": list(t.get("combo", [])),
                        "ev": t.get("ev", 0),
                        "stake": t.get("stake", 0),
                        "signal": t.get("signal", ""),
                        "prob": round(t.get("prob", 0), 6),
                        "odds": round(t.get("odds", 0), 1),
                    }
                )

            # フォーメーション買い目（三連複・馬連）を保存
            formation = analysis.formation or {}
            for t in formation.get("sanrenpuku", []):
                if isinstance(t, dict):
                    race_data["formation_tickets"].append({
                        "type": "三連複",
                        "combo": [t.get("a"), t.get("b"), t.get("c")],
                        "ev": round(t.get("ev", 0), 1),
                        "stake": t.get("stake", 0),
                        "signal": t.get("signal", ""),
                        "prob": round(t.get("prob", 0), 6),
                        "odds": round(t.get("odds", 0), 1),
                    })
            # フォーメーション馬連（重複排除のため通常tickets優先、なければ追加）
            existing_umaren = {
                tuple(sorted(t.get("combo", [])))
                for t in race_data["tickets"]
                if t.get("type") == "馬連"
            }
            for t in formation.get("umaren", []):
                if isinstance(t, dict):
                    key = tuple(sorted([t.get("a", 0), t.get("b", 0)]))
                    if key not in existing_umaren:
                        race_data["formation_tickets"].append({
                            "type": "馬連(F)",
                            "combo": [t.get("a"), t.get("b")],
                            "ev": round(t.get("ev", 0), 1),
                            "stake": t.get("stake", 0),
                            "signal": t.get("signal", ""),
                            "prob": round(t.get("prob", 0), 6),
                            "odds": round(t.get("odds", 0), 1),
                        })

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
    if not rd or not venue or not horse_no:
        return []

    from data.masters.venue_master import VENUE_NAME_TO_CODE
    vc = VENUE_NAME_TO_CODE.get(venue, "")
    if not vc:
        return []

    cache_key = (rd, vc, horse_no)
    if cache_key in _corners_cache:
        return _corners_cache[cache_key]

    # race_log から race_id を特定
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "keiba.db")
    if not os.path.exists(db_path):
        return []

    try:
        conn = sqlite3.connect(db_path)
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
            if rd:
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
        })
    return result


def load_prediction(date: str) -> Optional[dict]:
    """予想データを読み込む（DB優先、フォールバックでJSON）"""
    if _DB_AVAILABLE:
        try:
            data = _db.load_prediction(date)
            if data:
                return data
        except Exception:
            pass
    # JSON フォールバック
    fpath = os.path.join(PREDICTIONS_DIR, f"{date.replace('-', '')}_pred.json")
    if not os.path.exists(fpath):
        return None
    with open(fpath, "r", encoding="utf-8") as f:
        return json.load(f)


def list_prediction_dates() -> List[str]:
    """予想済み日付一覧（新しい順）"""
    if _DB_AVAILABLE:
        try:
            return _db.list_prediction_dates()
        except Exception:
            pass
    # JSON フォールバック
    if not os.path.exists(PREDICTIONS_DIR):
        return []
    files = sorted(
        [f for f in os.listdir(PREDICTIONS_DIR) if f.endswith("_pred.json")], reverse=True
    )
    dates = []
    for f in files:
        raw = f.replace("_pred.json", "")
        if len(raw) == 8:
            dates.append(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
    return dates


# ============================================================
# 実際の着順・オッズをネットケイバから取得
# ============================================================


def fetch_actual_results(date: str, client) -> dict:
    """
    指定日の全レース結果（着順・確定オッズ）をnetkeiba から取得して保存。
    Returns: {race_id: {"order": [{"horse_no":1,"finish":1,"odds":2.5},...], "payouts": {...}}}
    """
    os.makedirs(RESULTS_DIR, exist_ok=True)
    fpath = os.path.join(RESULTS_DIR, f"{date.replace('-', '')}_results.json")

    # キャッシュがあればそれを使う
    if os.path.exists(fpath):
        with open(fpath, "r", encoding="utf-8") as f:
            cached = json.load(f)
        # 結果が空（全レースの order が空）の場合は再取得する
        # レース開始前に取得してしまった場合の救済
        if cached:
            has_any_order = any(
                v.get("order") for v in cached.values()
                if isinstance(v, dict)
            )
            if has_any_order:
                return cached
            # 空結果 → キャッシュ破棄して再取得
            os.remove(fpath)
        else:
            return cached

    pred = load_prediction(date)
    if not pred:
        return {}

    from data.masters.venue_master import JRA_CODES

    results = {}

    for race in pred["races"]:
        race_id = race["race_id"]
        vc = race_id[4:6]
        base_url = "https://race.netkeiba.com" if vc in JRA_CODES else "https://nar.netkeiba.com"
        url = f"{base_url}/race/result.html"
        before_fetch = getattr(client, "_stats_fetch", 0)
        soup = client.get(url, params={"race_id": race_id})
        was_fetched = getattr(client, "_stats_fetch", 0) > before_fetch
        if not soup:
            if was_fetched:
                time.sleep(1.5)
            continue

        order = _parse_finish_order(soup)
        payouts = _parse_payouts(soup)
        results[race_id] = {"order": order, "payouts": payouts}
        # HTTP取得時のみスリープ（キャッシュヒット時はスキップ）
        if was_fetched:
            time.sleep(1.5)

    # 全レースの結果が空（まだ開催前/開催中）の場合はファイルを保存しない
    has_any_order = any(v.get("order") for v in results.values() if isinstance(v, dict))
    if not has_any_order and results:
        # 空結果をファイル保存しない → 次回再取得可能にする
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
    """払戻テーブルから馬連・ワイド(複数組)などの払戻金を抽出。
    ワイドは最大3組あるためリスト形式 payouts["ワイド"] = [{"combo":..,"payout":..},..]
    他の券種は dict 形式 payouts["馬連"] = {"combo":..,"payout":..}
    """
    payouts = {}
    payout_table = soup.select_one(".Payout_Detail_Table, table.payout")
    if not payout_table:
        return payouts
    for tr in payout_table.select("tr"):
        cells = tr.select("td, th")
        if len(cells) < 2:
            continue
        label = cells[0].get_text(strip=True)
        if label in ("馬連", "馬単", "ワイド", "三連複", "三連単", "複勝", "単勝"):
            combo_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            payout_cell = cells[2].get_text(strip=True).replace(",", "") if len(cells) > 2 else ""
            try:
                payout_val = int(re.sub(r"[^\d]", "", payout_cell)) if payout_cell else 0
            except ValueError:
                payout_val = 0
            entry = {"combo": combo_cell, "payout": payout_val}
            if label == "ワイド":
                # ワイドは最大3組 → リストで保持
                payouts.setdefault("ワイド", [])
                payouts["ワイド"].append(entry)
            elif label not in payouts:
                # 重複行は最初のエントリのみ使用
                payouts[label] = entry
    return payouts


# ============================================================
# 予想 vs 実際の照合・集計
# ============================================================


def compare_and_aggregate(date: str) -> Optional[dict]:
    """
    予想JSONと結果JSONを照合し、的中・収支を集計して返す。
    Returns: 集計辞書 or None（データなし）
    """
    pred = load_prediction(date)
    if not pred:
        return None

    # 結果データを DB 優先で読み込む
    actual = None
    if _DB_AVAILABLE:
        try:
            actual = _db.load_results(date)
        except Exception:
            pass
    if not actual:
        fpath = os.path.join(RESULTS_DIR, f"{date.replace('-', '')}_results.json")
        if not os.path.exists(fpath):
            return None
        with open(fpath, "r", encoding="utf-8") as f:
            actual = json.load(f)
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
        race_id = race["race_id"]
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

            if mk in ("◉", "◎", "○", "▲", "△", "☆"):
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
                    # 実払戻優先、フォールバックでオッズ×100
                    _tp = payouts.get("単勝", {})
                    _tpay = _tp.get("payout", 0) if isinstance(_tp, dict) else 0
                    if not _tpay:
                        _ao = next(
                            (r["odds"] for r in result["order"]
                             if r["horse_no"] == h["horse_no"] and r.get("odds")),
                            None,
                        )
                        _tpay = int(_ao * 100) if _ao else 0
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
                        # 実払戻優先、フォールバックでオッズ×100
                        _tp2 = payouts.get("単勝", {})
                        _tpay2 = _tp2.get("payout", 0) if isinstance(_tp2, dict) else 0
                        if not _tpay2:
                            _ao2 = next(
                                (r["odds"] for r in result["order"]
                                 if r["horse_no"] == h["horse_no"] and r.get("odds")),
                                None,
                            )
                            _tpay2 = int(_ao2 * 100) if _ao2 else 0
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
                    # 単勝回収率: 実際の払戻を優先、フォールバックでオッズ×100
                    _ana_tp = payouts.get("単勝", {})
                    _ana_pay = _ana_tp.get("payout", 0) if isinstance(_ana_tp, dict) else 0
                    if not _ana_pay:
                        _ana_odds = next(
                            (r["odds"] for r in result["order"] if r["horse_no"] == hno and r.get("odds")),
                            None,
                        )
                        _ana_pay = int(_ana_odds * 100) if _ana_odds else 0
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
            if hit and not race_by_type[ticket_type]["hit"]:
                # 最初の的中点の払戻を使用（馬連/三連複は同一組み合わせにつき1払戻）
                race_by_type[ticket_type]["hit"] = True
                race_by_type[ticket_type]["ret"] = int(payout_per_100)  # 100円あたり払戻

        # 通常買い目（馬連・三連複）
        all_tickets = list(race.get("tickets", []))
        # フォーメーション買い目（stake>0のもの）
        all_tickets += [t for t in race.get("formation_tickets", []) if (t.get("stake") or 0) > 0]

        for t in all_tickets:
            stake = t.get("stake", 100) or 100
            ticket_type = t.get("type", "")
            combo = tuple(int(x) for x in t.get("combo", []))
            _tally_ticket(ticket_type, combo, stake)

        # 単勝（◉◎馬がいれば1点100円を券種として追加）
        if honmei_hno is not None:
            tansho_pos = finish_map.get(honmei_hno, 99)
            tansho_hit = tansho_pos == 1
            tansho_ret_val = 0
            if tansho_hit:
                tp = payouts.get("単勝", {})
                if isinstance(tp, dict):
                    tansho_ret_val = tp.get("payout", 0)
                if not tansho_ret_val:
                    # フォールバック: オッズ × 100
                    odds_fb = next(
                        (r["odds"] for r in result["order"]
                         if r["horse_no"] == honmei_hno and r.get("odds")),
                        None,
                    )
                    if odds_fb:
                        tansho_ret_val = int(odds_fb * 100)
            race_by_type["単勝"] = {"stake": 100, "hit": tansho_hit, "ret": tansho_ret_val}

        # レース単位で集計（単勝のみ）
        tansho_rg = race_by_type.get("単勝")
        race_total_stake = tansho_rg["stake"] if tansho_rg else 0
        race_total_ret   = tansho_rg["ret"] if (tansho_rg and tansho_rg["hit"]) else 0
        race_any_hit = tansho_rg["hit"] if tansho_rg else False
        if tansho_rg:
            total_tickets += 1
            total_stake += race_total_stake
            total_return += race_total_ret
            conf_stats[confidence]["stake"] += race_total_stake
            conf_stats[confidence]["ret"]   += race_total_ret
            if race_any_hit:
                hit_tickets += 1
                conf_stats[confidence]["hits"] += 1

        # 券種別・自信度×券種別（単勝のみ）
        for ticket_type, rg in ((k, v) for k, v in race_by_type.items() if k == "単勝"):
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

    return {
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
        payout = payouts.get("三連複", {}).get("payout", 0)
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
    if key in _AGG_CACHE:
        ts, result = _AGG_CACHE[key]
        if time.time() - ts < _AGG_CACHE_TTL:
            return result
    return None

def _set_cached(func_name: str, year_filter: str, result: dict) -> None:
    key = _cache_key(func_name, year_filter)
    _AGG_CACHE[key] = (time.time(), result)

def invalidate_aggregate_cache() -> None:
    """外部から呼び出して集計キャッシュをクリアする"""
    _AGG_CACHE.clear()


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
        "by_mark": {},       # {mark: {total, win, place2, placed}}
        "by_conf": {},       # {conf: {total, hits, stake, ret, payouts:[]}}
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

    # 印別成績
    if race and finish_map:
        bm = stats["by_mark"]
        for h in race.get("horses", []):
            mk = h.get("mark", "")
            if mk not in ("◉", "◎", "○", "▲", "△", "☆"):
                continue
            pos = finish_map.get(h["horse_no"], 99)
            if mk not in bm:
                bm[mk] = {"total": 0, "win": 0, "place2": 0, "placed": 0}
            bm[mk]["total"] += 1
            if pos == 1:
                bm[mk]["win"] += 1
            if pos <= 2:
                bm[mk]["place2"] += 1
            if pos <= 3:
                bm[mk]["placed"] += 1

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

    # 印別: 勝率/連対率/複勝率
    for mk, ms in stats.get("by_mark", {}).items():
        t = ms["total"]
        ms["win_rate"]    = round(ms["win"]    / t * 100, 1) if t else 0.0
        ms["place2_rate"] = round(ms["place2"] / t * 100, 1) if t else 0.0
        ms["place_rate"]  = round(ms["placed"] / t * 100, 1) if t else 0.0

    # 自信度別: 的中率/回収率/最高/平均配当
    for conf, cs in stats.get("by_conf", {}).items():
        cs["hit_rate"] = round(cs["hits"] / cs["total"] * 100, 1) if cs["total"] else 0.0
        cs["roi"]      = round(cs["ret"] / cs["stake"] * 100, 1) if cs["stake"] else 0.0
        plist = [p for p in cs.get("payouts", []) if p > 0]
        cs["max_payout"] = max(plist) if plist else 0
        cs["avg_payout"] = round(sum(plist) / len(plist)) if plist else 0
        if "payouts" in cs:
            del cs["payouts"]


def aggregate_detailed(year_filter: str = "all") -> dict:
    """
    詳細集計: 全体/JRA/NAR ごとに 競馬場別・コース別・距離区分別 + 高額配当TOP10。
    Walk-Forward方式のため全年含む。
    """
    cached = _get_cached("aggregate_detailed", year_filter)
    if cached is not None:
        return cached
    from data.masters.venue_master import JRA_CODES, get_venue_name

    dates = list_prediction_dates()
    if year_filter and year_filter != "all":
        dates = [d for d in dates if d.startswith(year_filter)]

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
        pred = load_prediction(date)
        if not pred:
            continue

        # 結果データを DB 優先で読み込む
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
            continue

        for race in pred["races"]:
            race_id  = race["race_id"]
            result   = actual.get(race_id)
            if not result:
                continue

            finish_map = {r["horse_no"]: r["finish"] for r in result["order"]}
            if not finish_map:
                continue  # 着順データなし（中止等）→スキップ
            payouts    = result.get("payouts", {})

            vc       = race_id[4:6] if len(race_id) >= 6 else ""
            is_jra   = vc in JRA_CODES
            venue    = race.get("venue", "") or ""
            if not venue:
                venue = get_venue_name(vc) or "不明"
            surface  = race.get("surface", "") or ""
            dist     = int(race.get("distance", 0) or 0)
            dzone    = _dist_zone(dist)
            race_no  = race.get("race_no") or 0
            if not race_no and len(race_id) >= 2:
                try:
                    race_no = int(race_id[-2:])
                except ValueError:
                    pass
            race_name = race.get("race_name", "") or ""

            horse_mark_map = {h["horse_no"]: h.get("mark", "") for h in race.get("horses", [])}

            # 買い目集計
            race_by_type: Dict[str, dict] = {}
            all_tickets = list(race.get("tickets", []))
            all_tickets += [t for t in race.get("formation_tickets", []) if (t.get("stake") or 0) > 0]

            for t in all_tickets:
                stake       = t.get("stake", 100) or 100
                ticket_type = t.get("type", "")
                combo       = tuple(int(x) for x in t.get("combo", []))
                if ticket_type not in race_by_type:
                    race_by_type[ticket_type] = {"stake": 0, "hit": False, "ret": 0, "winning_ticket": None}
                race_by_type[ticket_type]["stake"] += stake
                hit, payout_per_100 = _check_ticket_hit(ticket_type, combo, finish_map, payouts)
                if hit and not race_by_type[ticket_type]["hit"]:
                    race_by_type[ticket_type]["hit"]   = True
                    race_by_type[ticket_type]["ret"]   = int(payout_per_100)
                    race_by_type[ticket_type]["winning_ticket"] = t

            # 単勝（◉◎馬がいれば1点100円を券種として追加）
            honmei_hno = None
            for h in race.get("horses", []):
                if h.get("mark", "") in ("◉", "◎"):
                    honmei_hno = h["horse_no"]
                    break
            if honmei_hno is not None:
                tansho_pos = finish_map.get(honmei_hno, 99)
                tansho_hit = tansho_pos == 1
                tansho_ret_val = 0
                if tansho_hit:
                    tp = payouts.get("単勝", {})
                    if isinstance(tp, dict):
                        tansho_ret_val = tp.get("payout", 0)
                    if not tansho_ret_val:
                        odds_fb = next(
                            (r["odds"] for r in result["order"]
                             if r["horse_no"] == honmei_hno and r.get("odds")),
                            None,
                        )
                        if odds_fb:
                            tansho_ret_val = int(odds_fb * 100)
                race_by_type["単勝"] = {"stake": 100, "hit": tansho_hit, "ret": tansho_ret_val, "winning_ticket": None}

            if not race_by_type:
                continue

            # 共通引数
            extra = dict(race=race, finish_map=finish_map)

            cat_key = "jra" if is_jra else "nar"
            for ckey in ("all", cat_key):
                c = cats[ckey]
                _add_to_detail_stats(c["stats"], race_by_type, **extra)
                # venue (ネスト構造)
                vs = _ensure_venue(c["by_venue"], venue)
                _add_to_detail_stats(vs, race_by_type, **extra)
                if surface:
                    _add_to_detail_stats(_ensure(vs["by_surface"], surface), race_by_type, **extra)
                _add_to_detail_stats(_ensure(vs["by_dist_zone"], dzone), race_by_type, **extra)
                # カテゴリ全体の surface / dist_zone
                if surface:
                    _add_to_detail_stats(_ensure(c["by_surface"], surface), race_by_type, **extra)
                _add_to_detail_stats(_ensure(c["by_dist_zone"], dzone), race_by_type, **extra)

            # 高額配当 TOP10 用（単勝のみ）
            rg_tansho = race_by_type.get("単勝")
            if rg_tansho and rg_tansho["hit"] and rg_tansho["ret"] > 0:
                combo_s = str(honmei_hno) if honmei_hno else ""
                mk_honmei = horse_mark_map.get(honmei_hno, "") if honmei_hno else ""
                # 馬名を取得
                honmei_name = ""
                if honmei_hno is not None:
                    for h in race.get("horses", []):
                        if h["horse_no"] == honmei_hno:
                            honmei_name = h.get("horse_name", "")
                            break
                entry = {
                    "date":       date,
                    "venue":      venue,
                    "race_no":    race_no,
                    "race_name":  race_name,
                    "race_id":    race_id,
                    "marks":      mk_honmei,
                    "combo":      combo_s,
                    "horse_name": honmei_name,
                    "payout":     rg_tansho["ret"],
                    "is_jra":     is_jra,
                }
                top_tansho.append(entry)
                # venue ネスト内にも追加
                for ckey in ("all", cat_key):
                    _ensure_venue(cats[ckey]["by_venue"], venue)["top10_tansho"].append(entry)

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

    result = {
        **cats,
        "top10_tansho":     top_tansho[:10],
    }
    _set_cached("aggregate_detailed", year_filter, result)
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
