"""ペース予測精度の集計スクリプト

予想JSON (data/predictions/*_pred.json) と
実績 (data/keiba.db race_log) を突合し、以下の精度を集計:
1. ペース種別 (H/M/S) 正解率
2. 予想走破タイム vs 実績タイム (MAE, 誤差分布)
3. 前半3F 精度
4. 後半3F 精度
5. 道中タイム 精度
6. 展開自信度別の精度
"""
import sys, os, json, sqlite3, glob
from collections import defaultdict
from datetime import datetime
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PRED_DIR = "data/predictions"
DB_PATH = "data/keiba.db"

# 直近N日分（多すぎると重いので制限）
MAX_DAYS = 90

def load_predictions(date_start=None):
    """予想JSONを読み込み"""
    files = sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
    # バックアップ除外
    files = [f for f in files if "_backup" not in f and "_prev" not in f]

    if date_start:
        files = [f for f in files if os.path.basename(f)[:8] >= date_start]

    # 最新MAX_DAYS分
    files = files[-MAX_DAYS:]

    all_races = []
    for fpath in files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            races = data.get("races", []) if isinstance(data, dict) else data
            pred_date = os.path.basename(fpath)[:8]
            for race in races:
                race["_pred_date"] = pred_date
            all_races.extend(races)
        except Exception:
            continue
    return all_races, len(files)

def load_actual_results():
    """SQLite race_logから実績データを取得"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # race_logから実績取得
    cur.execute("""
        SELECT race_id, horse_no, finish_time_sec, last_3f_sec, first_3f_sec,
               finish_pos, race_pace, position_4c, field_count, race_first_3f
        FROM race_log
        WHERE finish_time_sec IS NOT NULL AND finish_time_sec > 0
    """)

    results = {}
    for row in cur.fetchall():
        rid = row["race_id"]
        hno = row["horse_no"]
        if rid not in results:
            results[rid] = {"horses": {}, "pace": None}
        results[rid]["horses"][hno] = {
            "finish_time": row["finish_time_sec"],
            "last_3f": row["last_3f_sec"],
            "first_3f": row["first_3f_sec"],
            "finish_pos": row["finish_pos"],
            "race_pace": row["race_pace"],
            "pos_4c": row["position_4c"],
            "field_count": row["field_count"],
            "race_first_3f": row["race_first_3f"],
        }

    # レースペースは勝ち馬のrace_paceを使用
    for rid, rdata in results.items():
        winner = [h for h in rdata["horses"].values() if h["finish_pos"] == 1]
        if winner:
            rdata["pace"] = winner[0].get("race_pace")
            rdata["winner_time"] = winner[0]["finish_time"]
            rdata["winner_l3f"] = winner[0]["last_3f"]
            rdata["winner_f3f"] = winner[0]["first_3f"]
            rdata["race_first_3f"] = winner[0].get("race_first_3f")

    conn.close()
    return results

def normalize_pace(p):
    """ペース表記を正規化"""
    if not p:
        return None
    p = str(p).strip().upper()
    if p in ("H", "ハイ"):
        return "H"
    if p in ("M", "ミドル"):
        return "M"
    if p in ("S", "スロー"):
        return "S"
    # 部分一致
    if "ハイ" in str(p) or "H" in str(p):
        return "H"
    if "スロー" in str(p) or "S" in str(p):
        return "S"
    return "M"

def main():
    print("=" * 60)
    print("  ペース予測精度 集計レポート")
    print("=" * 60)

    # データ読み込み
    print("\nデータ読み込み中...")
    races, n_files = load_predictions()
    actual = load_actual_results()
    print(f"  予想ファイル: {n_files}日分, {len(races)}レース")
    print(f"  実績データ: {len(actual)}レース")

    # マッチング
    pace_matches = []       # (predicted, actual) ペース種別
    time_errors = []        # (pred_time, actual_time, error) 走破タイム
    f3f_errors = []         # 前半3F
    l3f_errors = []         # 後半3F
    mid_errors = []         # 道中
    confidence_data = defaultdict(list)  # 自信度別

    matched_races = 0

    for race in races:
        rid = race.get("race_id")
        if not rid or rid not in actual:
            continue

        act = actual[rid]
        matched_races += 1

        # ---- 1. ペース種別 ----
        pred_pace = race.get("pace_predicted")
        actual_pace = act.get("pace")
        if pred_pace and actual_pace:
            pp = normalize_pace(pred_pace)
            ap = normalize_pace(actual_pace)
            if pp and ap:
                pace_matches.append((pp, ap))

        # ---- 2. 予想走破タイム ----
        pred_time = race.get("predicted_race_time")
        actual_time = act.get("winner_time")
        if pred_time and actual_time and pred_time > 0 and actual_time > 0:
            err = pred_time - actual_time
            time_errors.append((pred_time, actual_time, err))

            # 自信度別に蓄積
            conf = race.get("pace_reliability_label", "?")
            confidence_data[conf].append(abs(err))

        # ---- 3. 前半3F ----
        pred_f3f = race.get("estimated_front_3f")
        actual_f3f = act.get("race_first_3f") or act.get("winner_f3f")
        if pred_f3f and actual_f3f and pred_f3f > 0 and actual_f3f > 0:
            f3f_errors.append((pred_f3f, actual_f3f, pred_f3f - actual_f3f))

        # ---- 4. 後半3F ----
        pred_l3f = race.get("estimated_last_3f")
        actual_l3f = act.get("winner_l3f")
        if pred_l3f and actual_l3f and pred_l3f > 0 and actual_l3f > 0:
            l3f_errors.append((pred_l3f, actual_l3f, pred_l3f - actual_l3f))

        # ---- 5. 道中タイム ----
        pred_mid = race.get("estimated_mid_time")
        actual_f3f_for_mid = act.get("race_first_3f")
        actual_l3f_for_mid = act.get("winner_l3f")
        if pred_mid and pred_mid > 0 and actual_time and actual_f3f_for_mid and actual_l3f_for_mid:
            if actual_f3f_for_mid > 0 and actual_l3f_for_mid > 0:
                actual_mid = actual_time - actual_f3f_for_mid - actual_l3f_for_mid
                if actual_mid > 0:
                    mid_errors.append((pred_mid, actual_mid, pred_mid - actual_mid))

    print(f"  マッチレース: {matched_races}")

    # ================================================================
    # 1. ペース種別
    # ================================================================
    print(f"\n{'='*60}")
    print("  1. ペース種別 (H/M/S) 正解率")
    print(f"{'='*60}")
    if pace_matches:
        correct = sum(1 for p, a in pace_matches if p == a)
        total = len(pace_matches)
        print(f"  正解: {correct}/{total} = {correct/total*100:.1f}%")

        # 混同行列
        labels = ["H", "M", "S"]
        print(f"\n  {'':>10} 実績→")
        print(f"  {'予想↓':>10} {'H':>8} {'M':>8} {'S':>8}")
        print(f"  {'-'*36}")
        for pl in labels:
            counts = [sum(1 for p, a in pace_matches if p == pl and a == al) for al in labels]
            total_row = sum(counts)
            print(f"  {pl:>10} {counts[0]:>8} {counts[1]:>8} {counts[2]:>8}  (n={total_row})")

        # 隣接正解(HをMと予測 etc.)
        adjacent_correct = sum(
            1 for p, a in pace_matches
            if p == a or abs(labels.index(p) - labels.index(a)) <= 1
        )
        print(f"\n  隣接含む正解: {adjacent_correct}/{total} = {adjacent_correct/total*100:.1f}%")
    else:
        print("  データなし")

    # ================================================================
    # 2. 走破タイム
    # ================================================================
    print(f"\n{'='*60}")
    print("  2. 予想走破タイム精度")
    print(f"{'='*60}")
    if time_errors:
        errs = [e for _, _, e in time_errors]
        abs_errs = [abs(e) for e in errs]
        print(f"  サンプル数: {len(errs)}")
        print(f"  平均誤差 (バイアス): {np.mean(errs):+.2f}秒")
        print(f"  MAE (平均絶対誤差): {np.mean(abs_errs):.2f}秒")
        print(f"  RMSE: {np.sqrt(np.mean(np.array(errs)**2)):.2f}秒")
        print(f"  中央値絶対誤差: {np.median(abs_errs):.2f}秒")
        print(f"\n  誤差分布:")
        for threshold in [0.5, 1.0, 1.5, 2.0, 3.0]:
            pct = sum(1 for e in abs_errs if e <= threshold) / len(abs_errs) * 100
            bar = "#" * int(pct / 2)
            print(f"    ±{threshold:.1f}秒以内: {pct:5.1f}%  {bar}")
    else:
        print("  データなし")

    # ================================================================
    # 3. 前半3F
    # ================================================================
    print(f"\n{'='*60}")
    print("  3. 前半3F精度")
    print(f"{'='*60}")
    if f3f_errors:
        errs = [e for _, _, e in f3f_errors]
        abs_errs = [abs(e) for e in errs]
        print(f"  サンプル数: {len(errs)}")
        print(f"  平均誤差: {np.mean(errs):+.2f}秒")
        print(f"  MAE: {np.mean(abs_errs):.2f}秒")
        print(f"  RMSE: {np.sqrt(np.mean(np.array(errs)**2)):.2f}秒")
        for threshold in [0.3, 0.5, 1.0, 1.5]:
            pct = sum(1 for e in abs_errs if e <= threshold) / len(abs_errs) * 100
            print(f"    ±{threshold:.1f}秒以内: {pct:5.1f}%")
    else:
        print("  データなし")

    # ================================================================
    # 4. 後半3F
    # ================================================================
    print(f"\n{'='*60}")
    print("  4. 後半3F精度")
    print(f"{'='*60}")
    if l3f_errors:
        errs = [e for _, _, e in l3f_errors]
        abs_errs = [abs(e) for e in errs]
        print(f"  サンプル数: {len(errs)}")
        print(f"  平均誤差: {np.mean(errs):+.2f}秒")
        print(f"  MAE: {np.mean(abs_errs):.2f}秒")
        print(f"  RMSE: {np.sqrt(np.mean(np.array(errs)**2)):.2f}秒")
        for threshold in [0.3, 0.5, 1.0, 1.5]:
            pct = sum(1 for e in abs_errs if e <= threshold) / len(abs_errs) * 100
            print(f"    ±{threshold:.1f}秒以内: {pct:5.1f}%")
    else:
        print("  データなし")

    # ================================================================
    # 5. 道中タイム
    # ================================================================
    print(f"\n{'='*60}")
    print("  5. 道中タイム精度")
    print(f"{'='*60}")
    if mid_errors:
        errs = [e for _, _, e in mid_errors]
        abs_errs = [abs(e) for e in errs]
        print(f"  サンプル数: {len(errs)}")
        print(f"  平均誤差: {np.mean(errs):+.2f}秒")
        print(f"  MAE: {np.mean(abs_errs):.2f}秒")
        print(f"  RMSE: {np.sqrt(np.mean(np.array(errs)**2)):.2f}秒")
        for threshold in [0.5, 1.0, 1.5, 2.0]:
            pct = sum(1 for e in abs_errs if e <= threshold) / len(abs_errs) * 100
            print(f"    ±{threshold:.1f}秒以内: {pct:5.1f}%")
    else:
        print("  データなし")

    # ================================================================
    # 6. 展開自信度別
    # ================================================================
    print(f"\n{'='*60}")
    print("  6. 展開自信度別の走破タイムMAE")
    print(f"{'='*60}")
    if confidence_data:
        print(f"  {'自信度':>8} {'MAE':>8} {'サンプル':>8}")
        print(f"  {'-'*30}")
        for conf in sorted(confidence_data.keys()):
            vals = confidence_data[conf]
            if len(vals) >= 5:
                mae = np.mean(vals)
                print(f"  {conf:>8} {mae:>7.2f}秒 {len(vals):>8}")
    else:
        print("  データなし")

    # ================================================================
    # 7. JRA vs NAR 別
    # ================================================================
    print(f"\n{'='*60}")
    print("  7. JRA/NAR 別 走破タイム精度")
    print(f"{'='*60}")
    jra_errs = []
    nar_errs = []
    for race in races:
        rid = race.get("race_id")
        if not rid or rid not in actual:
            continue
        pred_time = race.get("predicted_race_time")
        actual_time = actual[rid].get("winner_time")
        if pred_time and actual_time and pred_time > 0 and actual_time > 0:
            err = abs(pred_time - actual_time)
            is_jra = race.get("is_jra", False)
            if is_jra:
                jra_errs.append(err)
            else:
                nar_errs.append(err)

    if jra_errs:
        print(f"  JRA: MAE={np.mean(jra_errs):.2f}秒, ±1.0秒={sum(1 for e in jra_errs if e<=1.0)/len(jra_errs)*100:.1f}%, n={len(jra_errs)}")
    if nar_errs:
        print(f"  NAR: MAE={np.mean(nar_errs):.2f}秒, ±1.0秒={sum(1 for e in nar_errs if e<=1.0)/len(nar_errs)*100:.1f}%, n={len(nar_errs)}")

    # ================================================================
    # 8. 距離帯別
    # ================================================================
    print(f"\n{'='*60}")
    print("  8. 距離帯別 走破タイム精度")
    print(f"{'='*60}")
    dist_errs = defaultdict(list)
    for race in races:
        rid = race.get("race_id")
        if not rid or rid not in actual:
            continue
        pred_time = race.get("predicted_race_time")
        actual_time = actual[rid].get("winner_time")
        dist = race.get("distance", 0)
        if pred_time and actual_time and pred_time > 0 and actual_time > 0 and dist > 0:
            err = abs(pred_time - actual_time)
            if dist <= 1200:
                band = "短距離(~1200)"
            elif dist <= 1600:
                band = "マイル(~1600)"
            elif dist <= 2000:
                band = "中距離(~2000)"
            else:
                band = "長距離(2001~)"
            dist_errs[band].append(err)

    for band in ["短距離(~1200)", "マイル(~1600)", "中距離(~2000)", "長距離(2001~)"]:
        vals = dist_errs.get(band, [])
        if vals:
            print(f"  {band}: MAE={np.mean(vals):.2f}秒, ±1.0秒={sum(1 for e in vals if e<=1.0)/len(vals)*100:.1f}%, n={len(vals)}")

    print(f"\n{'='*60}")
    print("  集計完了")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
