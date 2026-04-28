"""
2025-01-01～2026-03-01 の全MLデータファイルをスキャンし、
LightGBM簡易予想(印・自信度)を生成してDBに保存する。
finish_pos + odds はMLデータから取得（スクレイピング不要）。
→ ダッシュボード③結果分析が自動的に照合・集計を行う。

Usage:
  python scripts/bulk_backfill_predictions.py
  python scripts/bulk_backfill_predictions.py --start 2025-01-01 --end 2025-06-30
  python scripts/bulk_backfill_predictions.py --force   # 既存データを上書き
"""
import sys, os, json, argparse, time
from datetime import date, timedelta
from itertools import combinations
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ml.lgbm_model import LGBMPredictor, _load_horse_sire_map
from src import database as db
from src.scraper.kaisai_calendar_util import validate_race_against_calendar
from data.masters.venue_master import VENUE_CODE_TO_NAME, JRA_VENUE_CODES as _JRA_VC

ML_DATA_DIR = "data/ml"


# ============================================================
# 印・自信度・穴危険ロジック
# ============================================================

def _assign_marks(probs: Dict[str, float]) -> Dict[str, str]:
    """P(top3)スコアで馬に印を付ける（各印1頭ずつ）
    norm_gap >= 0.5 の圧倒的本命は◉、それ以外は◎
    """
    sorted_list = sorted(probs.items(), key=lambda x: -x[1])
    vals = [p for _, p in sorted_list]
    total = sum(vals) or 1.0
    n = len(vals)
    norm_gap = (vals[0] - vals[1]) * n / total if len(vals) > 1 else 0.0

    marks = {}
    for i, (hid, p) in enumerate(sorted_list):
        if i == 0:
            marks[hid] = "◉" if norm_gap >= 0.5 else "◎"
        elif i == 1:
            marks[hid] = "○"
        elif i == 2:
            marks[hid] = "▲"
        elif i == 3:
            marks[hid] = "△"   # 4番手1頭のみ
        elif i == 4:
            marks[hid] = "☆"   # 5番手1頭のみ
        else:
            marks[hid] = "-"
    return marks


def _judge_confidence(probs: Dict[str, float]) -> str:
    """自信度判定（正規化ベース）
    LightGBMのraw probは全馬0.3〜0.5なので絶対値は使えない。
    合計で割って各馬の「相対シェア」で判定する。
    """
    if not probs:
        return "C"
    vals = sorted(probs.values(), reverse=True)
    total = sum(vals) or 1.0
    n = len(vals)
    # 1位馬の正規化スコア: 平均の何倍か (avg = total/n)
    norm_top  = vals[0] * n / total          # 1.0 = 平均, 2.0 = 平均の2倍
    norm_gap  = (vals[0] - vals[1]) * n / total if len(vals) > 1 else norm_top

    if norm_top >= 2.0 and norm_gap >= 0.4:
        return "SS"
    if norm_top >= 1.7 and norm_gap >= 0.3:
        return "S"
    if norm_top >= 1.5 and norm_gap >= 0.2:
        return "A"
    if norm_top >= 1.3:
        return "B"
    return "C"   # 非常に接戦（どの馬もほぼ同スコア）


def _get_ana_kiken(rank_among_unmarked: int, mk: str, h: dict, field_count: int):
    """穴馬・危険馬フラグ → (ana_type, kiken_type)
    定義:
      穴馬: ①odds>=10 ②pop>=5 ③印なしの中でLGBMランク上位2頭 ④無印(mark="-")
      危険馬: ①odds<10 ②pop<=4 ③LGBMランク下位1/3 ④無印(mark="-")
    簡易バックフィルでは③を LGBMランク位置で近似する
    """
    # ④ 無印のみ対象
    if mk != "-":
        return ("none", "none")

    odds = h.get("odds") or 0
    pop  = h.get("popularity") or 99

    # 穴馬: odds>=10 AND pop>=5 AND 印なしの中でLGBMランク上位2頭(0-indexed: 0,1)
    if odds >= 10.0 and pop >= 5 and rank_among_unmarked <= 1:
        if odds >= 30:
            return ("穴厚切り", "none")
        return ("穴", "none")

    # 危険馬: odds<10 AND pop<=4 AND LGBMランク下位1/3（全体ランクで判定）
    lower_threshold = max(5, field_count * 2 // 3)
    if odds < 10.0 and pop <= 4 and rank_among_unmarked > lower_threshold:
        if pop <= 2 and odds <= 3.0:
            return ("none", "人気危険")
        return ("none", "危険")

    return ("none", "none")


# ============================================================
# 1日分の処理
# ============================================================

def process_date(
    date_str: str,
    predictor: LGBMPredictor,
    sire_map: dict,
    force: bool = False,
) -> tuple:
    """
    Returns: (pred_races_saved, result_races_saved, skipped)
    """
    # 既存チェック（--force なしなら skip）
    if not force:
        existing = db.load_prediction(date_str)
        if existing and existing.get("races"):
            return 0, 0, True

    fname = date_str.replace("-", "") + ".json"
    fpath = os.path.join(ML_DATA_DIR, fname)
    if not os.path.exists(fpath):
        return 0, 0, False

    with open(fpath, encoding="utf-8") as f:
        day_data = json.load(f)

    races = day_data.get("races", [])
    if not races:
        return 0, 0, False

    pred_payload = {"date": date_str, "version": 2, "races": []}
    results_data = {}  # race_id -> {order, payouts}

    # ── [T-038] カレンダー突合 skip 集計 ──
    calendar_skip_count = 0

    for race in races:
        race_id = race.get("race_id", "")
        if not race_id:
            continue

        # ── [T-038] カレンダー突合検証 ──────────────────────────────
        # race_id の場コードから会場名・JRA/NAR を判定してカレンダーと照合する。
        # 不整合なら警告ログ + skip (data/ml への書き込み汚染を防止)。
        _vc = race_id[4:6] if len(race_id) >= 6 else ""
        _venue_for_cal = VENUE_CODE_TO_NAME.get(_vc, "")
        _is_jra_for_cal = _vc in _JRA_VC
        if _venue_for_cal:
            _cal_ok, _cal_reason = validate_race_against_calendar(
                race_id, date_str, _venue_for_cal, _is_jra_for_cal
            )
            if not _cal_ok:
                print(f"  [T-038][WARN] カレンダー不整合 → skip: {_cal_reason}")
                calendar_skip_count += 1
                continue
        # ─────────────────────────────────────────────────────────────

        horses = race.get("horses", [])
        if len(horses) < 3:
            continue

        # sire_id 付与
        horse_dicts = []
        for h in horses:
            hid = h.get("horse_id", "")
            sid, bid = sire_map.get(hid, ("", ""))
            horse_dicts.append(dict(h, sire_id=sid, bms_id=bid))

        # LightGBM 推論
        probs = predictor.predict_race(race, horse_dicts)
        if not probs:
            continue

        marks = _assign_marks(probs)
        confidence = _judge_confidence(probs)

        # ランク辞書 (horse_id → rank 0始まり)
        sorted_ids = [hid for hid, _ in sorted(probs.items(), key=lambda x: -x[1])]
        rank_map   = {hid: i for i, hid in enumerate(sorted_ids)}
        field_count = len(horse_dicts)

        # 印なしの馬を LGBMランク順で並べ rank_among_unmarked を計算
        unmarked_sorted = [hid for hid in sorted_ids if marks.get(hid, "-") == "-"]
        rank_among_unmarked_map = {hid: i for i, hid in enumerate(unmarked_sorted)}

        # 馬データ組み立て
        pred_horses = []
        for h in horse_dicts:
            hid = h.get("horse_id", "")
            p = probs.get(hid, 0.0)
            mk = marks.get(hid, "-")
            rank = rank_map.get(hid, field_count)
            rank_u = rank_among_unmarked_map.get(hid, field_count)
            ana_type, kiken_type = _get_ana_kiken(rank_u, mk, h, field_count)
            pred_horses.append({
                "horse_no":     h.get("horse_no"),
                "horse_name":   h.get("horse_name", ""),
                "horse_id":     hid,
                "sex":          h.get("sex", ""),
                "age":          h.get("age"),
                "gate_no":      h.get("gate_no"),
                "jockey":       h.get("jockey", ""),
                "jockey_id":    h.get("jockey_id", ""),
                "trainer":      h.get("trainer", ""),
                "horse_weight": h.get("horse_weight"),
                "weight_change": h.get("weight_change"),
                "odds":         h.get("odds"),
                "popularity":   h.get("popularity"),
                "mark":         mk,
                "composite":    round(p * 100, 2),
                "ml_place_prob": round(p, 4),
                "win_prob":      round(p * 0.40, 4),   # 概算
                "place2_prob":   round(p * 0.70, 4),
                "place3_prob":   round(p, 4),
                "ana_type":      ana_type,
                "kiken_type":    kiken_type,
                # 簡易モードではゼロ埋め
                "ability_total": 0.0, "ability_max": 0.0, "ability_wa": 0.0,
                "ability_alpha": 0.0, "ability_trend": "stable",
                "ability_reliability": "B",
                "ability_class_adj": 0.0, "ability_bloodline_adj": 0.0,
                "ability_chakusa_pattern": "",
                "pace_total": 0.0, "pace_base": 0.0, "pace_last3f_eval": 0.0,
                "pace_position_balance": 0.0, "pace_gate_bias": 0.0,
                "pace_course_style_bias": 0.0, "pace_jockey": 0.0,
                "pace_estimated_pos4c": None, "pace_estimated_last3f": None,
                "running_style": "",
                "course_total": 0.0, "course_record": 0.0,
                "course_venue_apt": 0.0, "course_venue_level": "",
                "course_jockey": 0.0,
                "ml_win_prob": round(p * 0.40, 4),
                "ml_top2_prob": round(p * 0.70, 4),
                "jockey_change_score": 0.0, "shobu_score": 0.0,
                "odds_consistency_adj": 0.0,
                "ana_score": 0.0, "kiken_score": 0.0,
                "predicted_tansho_odds": None, "odds_divergence": None,
                "divergence_signal": "", "training_intensity": None,
            })

        # 簡易チケット: ◎or◉軸で馬連4点 + 三連複6点 = 計10点×100円
        honmei_no = next((h["horse_no"] for h in pred_horses if h["mark"] in ("◉", "◎")), None)
        others = [h["horse_no"] for h in pred_horses if h["mark"] in ("○", "▲", "△", "☆")]
        tickets = []
        if honmei_no and others:
            # 馬連4点: 軸→○▲△☆
            for o in others[:4]:
                tickets.append({"type": "馬連", "combo": [honmei_no, o],
                                "ev": 0, "stake": 100, "signal": "簡易"})
            # 三連複6点: 軸+相手2頭 C(4,2)=6通り
            for b, c in combinations(others[:4], 2):
                tickets.append({"type": "三連複", "combo": [honmei_no, b, c],
                                "ev": 0, "stake": 100, "signal": "簡易"})

        pred_race = {
            "race_id":        race_id,
            "venue":          race.get("venue", ""),
            "race_no":        race.get("race_no", 0),
            "race_name":      race.get("race_name", ""),
            "surface":        race.get("surface", ""),
            "distance":       race.get("distance", 0),
            "direction":      race.get("direction", ""),
            "is_jra":         race.get("is_jra", True),
            "field_count":    len(horses),
            "grade":          race.get("grade", ""),
            "confidence":     confidence,
            "pace_predicted": "",
            "horses":         pred_horses,
            "tickets":        tickets,
            "formation_tickets": [],
            "value_bets":     [],
        }
        pred_payload["races"].append(pred_race)

        # レース結果: finish_pos + tansho_odds をMLデータから取得
        order = []
        for h in horses:
            fp = h.get("finish_pos")
            if fp is not None:
                order.append({
                    "horse_no": h.get("horse_no"),
                    "finish":   fp,
                    "odds":     h.get("odds"),  # 単勝オッズ（◎ROI計算に使用）
                })
        if order:
            # payoutsは空で保存 → backfill_payouts_from_html.py で確定払戻を上書きする
            results_data[race_id] = {"order": order, "payouts": {}}

    # DB 保存
    preds_saved = 0
    if pred_payload["races"]:
        try:
            db.save_prediction(date_str, pred_payload)
            preds_saved = len(pred_payload["races"])
        except Exception as e:
            print(f"  [WARN] predictions save failed {date_str}: {e}")

    results_saved = 0
    if results_data:
        try:
            db.save_results(date_str, results_data)
            results_saved = len(results_data)
        except Exception as e:
            print(f"  [WARN] results save failed {date_str}: {e}")

    # [T-038] カレンダー不整合 skip 集計をログ表示
    if calendar_skip_count > 0:
        print(f"  [T-038] {date_str}: カレンダー不整合 skip={calendar_skip_count}件")

    return preds_saved, results_saved, False


# ============================================================
# メイン
# ============================================================

def _date_range(start: str, end: str):
    d = date.fromisoformat(start)
    e = date.fromisoformat(end)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end",   default="2026-03-01")
    parser.add_argument("--force", action="store_true", help="既存データも上書き")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  簡易予想バックフィル  {args.start} ～ {args.end}")
    print(f"  force={args.force}")
    print(f"{'='*60}\n")

    # DB 初期化
    db.init_schema()

    # LightGBM ロード
    print("LightGBM モデル読み込み...")
    t0 = time.time()
    predictor = LGBMPredictor()
    if not predictor.load():
        print("  [ERROR] モデル読み込み失敗。retrain_all.py を先に実行してください。")
        sys.exit(1)
    print(f"  完了 ({time.time()-t0:.1f}秒)")

    # Sire マップ読み込み
    print("血統マップ読み込み...")
    t0 = time.time()
    sire_map = _load_horse_sire_map()
    print(f"  {len(sire_map):,}頭 ({time.time()-t0:.1f}秒)")

    # 日付ループ
    dates = list(_date_range(args.start, args.end))
    print(f"\n{len(dates)}日分を処理します...\n")

    total_preds = 0
    total_results = 0
    total_skipped = 0
    t_all = time.time()

    for i, date_str in enumerate(dates, 1):
        p, r, skipped = process_date(date_str, predictor, sire_map, force=args.force)
        if skipped:
            total_skipped += 1
            continue
        if p == 0 and r == 0:
            continue  # MLデータなし
        total_preds   += p
        total_results += r
        elapsed = time.time() - t_all
        eta = elapsed / i * (len(dates) - i)
        print(f"  [{i:3d}/{len(dates)}] {date_str}  予想:{p:3d}R  結果:{r:3d}R  "
              f"(経過{elapsed/60:.1f}分 残{eta/60:.1f}分)")

    elapsed_total = time.time() - t_all
    print(f"\n{'='*60}")
    print(f"  完了!  予想:{total_preds:,}レース  結果:{total_results:,}レース")
    print(f"  スキップ(既存):{total_skipped}日  合計:{elapsed_total/60:.1f}分")
    print(f"{'='*60}\n")
    print("ダッシュボード③結果分析タブで確認してください。")


if __name__ == "__main__":
    main()
