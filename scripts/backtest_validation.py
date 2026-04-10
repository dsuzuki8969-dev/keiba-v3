"""新旧モデル比較バックテスト（2026Q1）

新モデル: LightGBMのplace_prob（3着以内確率）で順位付け
旧モデル: predictionsテーブルのwin_prob（engine.pyフルパイプライン）で順位付け

比較軸:
  A. Top-N（place_prob順位）別の勝率・連対率・複勝率・単回収率
  B. 旧モデルの印別成績（ベースライン）
  C. 自信度別
  D. JRA/NAR別
"""
import sqlite3, json, sys, os, time, pickle
import numpy as np
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding='utf-8')

from src.ml.lgbm_model import (
    LGBMPredictor, FEATURE_COLUMNS, FEATURE_COLUMNS_BANEI,
    _load_ml_races, _extract_features, _add_race_relative_features,
    _load_horse_sire_map, _smile_key_ml,
    RollingStatsTracker, RollingSireTracker,
    SURFACE_MAP,
)

DB_PATH = "data/keiba.db"

# ====================================================================
# 設定
# ====================================================================
VALID_FROM = "2026-01-01"
VALID_TO   = "2026-03-31"  # MLデータの最終日までカバー

print("=" * 90)
print(f" 新旧モデル比較バックテスト ({VALID_FROM} ～)")
print("=" * 90)

# ====================================================================
# 1. 全レース走査 → 検証期間の特徴量構築
# ====================================================================
print("\n[1/4] 全レース走査・特徴量構築中（ローリング統計蓄積）...")
t0 = time.time()

races = _load_ml_races()
all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
print(f"  MLデータ: {all_dates[0]} ～ {all_dates[-1]} ({len(all_dates)}日, {len(races)}レース)")
print(f"  検証対象: {VALID_FROM} ～ {all_dates[-1]}")

sire_map = _load_horse_sire_map()
tracker = RollingStatsTracker()
sire_tracker = RollingSireTracker()

# 調教特徴量
training_extractor = None
try:
    from src.ml.training_features import TrainingFeatureExtractor
    training_extractor = TrainingFeatureExtractor()
    training_extractor.load_all()
    print("  調教特徴量: ロード成功")
except Exception as e:
    print(f"  調教特徴量: スキップ ({e})")

valid_races = []
n_train_races = 0

for ri, race in enumerate(races):
    date_str = race.get("date", "")
    is_valid = VALID_FROM <= date_str <= VALID_TO
    race_id = race.get("race_id", "")

    # 調教特徴量
    train_feats_map = {}
    if training_extractor and is_valid:
        horse_names = [h.get("horse_name", "") for h in race.get("horses", [])
                       if h.get("horse_name")]
        if horse_names:
            train_feats_map = training_extractor.get_race_training_features(
                race_id, horse_names, date_str)

    race_feats = []
    race_horses = []
    for h in race.get("horses", []):
        fp = h.get("finish_pos")
        if fp is None:
            continue
        hid = h.get("horse_id", "")
        sid, bid = sire_map.get(hid, ("", ""))
        feat = _extract_features(dict(h, sire_id=sid, bms_id=bid),
                                 race, tracker, sire_tracker)
        hname = h.get("horse_name", "")
        if hname in train_feats_map:
            feat.update(train_feats_map[hname])
        race_feats.append(feat)
        race_horses.append({
            "fp": fp,
            "hno": h.get("horse_no"),
            "odds": h.get("odds", 0) or 0,
            "popularity": h.get("popularity"),
            "horse_name": hname,
        })

    if race_feats:
        _add_race_relative_features(race_feats)
        if is_valid:
            valid_races.append({
                "race_id": race_id,
                "date": date_str,
                "surface": SURFACE_MAP.get(race.get("surface", ""), -1),
                "is_jra": int(bool(race.get("is_jra", True))),
                "venue_code": str(race.get("venue_code", "") or "").zfill(2),
                "distance": int(race.get("distance") or 0),
                "feats": race_feats,
                "horses": race_horses,
                "field_count": race.get("field_count", len(race_horses)),
            })
        else:
            n_train_races += 1

    tracker.update_race(race)
    sire_tracker.update_race(race, sire_map)

    if (ri + 1) % 5000 == 0:
        print(f"  {ri+1}/{len(races)}レース処理済み ({time.time()-t0:.0f}秒)")

elapsed1 = time.time() - t0
print(f"  完了: 検証={len(valid_races)}レース, 学習={n_train_races}レース ({elapsed1:.0f}秒)")

# ====================================================================
# 2. 新モデルで推論（place_prob）
# ====================================================================
print("\n[2/4] 新モデルで推論中...")
t1 = time.time()

predictor = LGBMPredictor()
if not predictor._loaded:
    predictor.load()

from data.masters.venue_master import is_banei as _is_banei_check

for vr in valid_races:
    vc = vr["venue_code"]
    is_banei = _is_banei_check(vc)
    feat_cols = FEATURE_COLUMNS_BANEI if is_banei else FEATURE_COLUMNS

    surface_val = vr["surface"]
    is_jra = bool(vr["is_jra"])
    distance = vr["distance"]
    smile_cat = _smile_key_ml(distance) if distance else ""

    model, level = predictor._select_model(surface_val, is_jra, vc, smile_cat)
    if model is None:
        for h in vr["horses"]:
            h["new_prob"] = 0
        continue

    if hasattr(model, "num_feature"):
        n = model.num_feature()
        if n < len(feat_cols):
            feat_cols = feat_cols[:n]

    X = np.array(
        [[float(f.get(c, None) or float("nan")) if f.get(c) is not None else float("nan")
          for c in feat_cols]
         for f in vr["feats"]],
        dtype=np.float32,
    )
    probs = model.predict(X)
    for i, h in enumerate(vr["horses"]):
        h["new_prob"] = float(probs[i])

elapsed2 = time.time() - t1
print(f"  完了 ({elapsed2:.1f}秒)")

# ====================================================================
# 3. 旧モデル予想を取得（predictionsテーブル）
# ====================================================================
print("\n[3/4] 旧モデル予想取得中...")

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row

old_preds = {}
rows = db.execute('''
    SELECT race_id, confidence, horses_json
    FROM predictions
    WHERE date >= ? AND date <= ?
''', (VALID_FROM, VALID_TO)).fetchall()

for row in rows:
    rid = row['race_id']
    conf = row['confidence'] or 'C'
    try:
        horses = json.loads(row['horses_json']) if row['horses_json'] else []
    except:
        continue
    for h in horses:
        hno = h.get('horse_no')
        if hno is None:
            continue
        old_preds[(rid, hno)] = {
            "mark": h.get('mark', ''),
            "wp": float(h.get('win_prob', 0) or 0),
            "confidence": conf,
        }

print(f"  旧モデル予測: {len(old_preds):,}件")
db.close()

# ====================================================================
# 4. 統合・集計
# ====================================================================
print("\n[4/4] 集計中...")

all_records = []
for vr in valid_races:
    rid = vr["race_id"]
    fc = vr["field_count"]
    for h in vr["horses"]:
        hno = h.get("hno")
        fp = h["fp"]
        odds = h["odds"]
        new_prob = h.get("new_prob", 0)
        old_info = old_preds.get((rid, hno), {})

        all_records.append({
            "rid": rid,
            "date": vr["date"],
            "hno": hno,
            "fp": fp,
            "odds": odds,
            "fc": fc,
            "is_jra": vr["is_jra"],
            "new_prob": new_prob,
            "old_mark": old_info.get("mark", ""),
            "old_wp": old_info.get("wp", 0),
            "old_conf": old_info.get("confidence", ""),
        })

matched = [r for r in all_records if r["old_mark"]]
print(f"  全レコード: {len(all_records):,}")
print(f"  旧モデル照合: {len(matched):,}")

# レースごとにグルーピング
races_dict = defaultdict(list)
for r in all_records:
    races_dict[r["rid"]].append(r)
n_races = len(races_dict)


# ====================================================================
# ヘルパー
# ====================================================================
def s(recs):
    n = len(recs)
    if n == 0:
        return None
    w = sum(1 for r in recs if r["fp"] == 1)
    p2 = sum(1 for r in recs if r["fp"] <= 2)
    p3 = sum(1 for r in recs if r["fp"] <= 3)
    tp = sum(int(r["odds"] * 100) for r in recs if r["fp"] == 1 and r["odds"])
    wr = w / n * 100
    p2r = p2 / n * 100
    p3r = p3 / n * 100
    roi = tp / (n * 100) * 100
    return {"n": n, "w": w, "wr": wr, "p2r": p2r, "p3r": p3r, "roi": roi}


def fmt(st, with_p2=True):
    if st is None:
        return "       -       -       -       -"
    if with_p2:
        return f"{st['n']:>6,}件 {st['wr']:>6.1f}% {st['p2r']:>6.1f}% {st['p3r']:>6.1f}% {st['roi']:>6.1f}%"
    return f"{st['n']:>6,}件 {st['wr']:>6.1f}% {st['p3r']:>6.1f}% {st['roi']:>6.1f}%"


def diff_arrow(old_val, new_val, threshold=0.5):
    d = new_val - old_val
    a = "↑" if d > threshold else ("↓" if d < -threshold else "→")
    return f"{d:>+6.1f}%{a}"


# ====================================================================
# A. Top-N 成績比較（メイン・最も公正な比較）
# ====================================================================
print(f"\n{'='*95}")
print(f"  A. Top-N 成績比較 (place_prob/win_prob順位)")
print(f"     期間: {VALID_FROM} ～ {all_dates[-1]}  ({n_races:,}レース)")
print(f"     旧モデル = predictions.win_prob順  / 新モデル = LightGBM place_prob順")
print(f"{'='*95}")

rank_old = defaultdict(list)
rank_new = defaultdict(list)

for rid, horses in races_dict.items():
    # 旧モデル: old_wp > 0 のもののみ
    with_old = [h for h in horses if h['old_wp'] > 0]
    sorted_old = sorted(with_old, key=lambda x: x['old_wp'], reverse=True)
    for i, h in enumerate(sorted_old[:5]):
        rank_old[i + 1].append(h)

    # 新モデル: new_prob順
    sorted_new = sorted(horses, key=lambda x: x['new_prob'], reverse=True)
    for i, h in enumerate(sorted_new[:5]):
        rank_new[i + 1].append(h)

print(f"\n  {'順位':>4} {'モデル':>8} {'件数':>7} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単回収':>7}")
print("  " + "-" * 73)

for rank in range(1, 6):
    so = s(rank_old[rank])
    sn = s(rank_new[rank])
    if so:
        print(f"  {rank}位  旧モデル {fmt(so)}")
    if sn:
        print(f"  {rank}位  新モデル {fmt(sn)}")
    if so and sn:
        print(f"       {'差分':>8}        "
              f"{diff_arrow(so['wr'], sn['wr'])}         "
              f"{diff_arrow(so['p3r'], sn['p3r'])} "
              f"{diff_arrow(so['roi'], sn['roi'], 1.0)}")
    print()


# ====================================================================
# B. 旧モデル印別成績（ベースライン）+ 新モデルの同じ馬の成績
# ====================================================================
print(f"\n{'='*95}")
print(f"  B. 旧モデルの印別成績（ベースライン）")
print(f"     ※旧モデルのフルパイプライン（ML+能力値+ペース分析+composite）での印付け")
print(f"{'='*95}")

print(f"\n  {'印':>3} {'件数':>7} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単回収':>7}")
print("  " + "-" * 55)

for mark in ['◉', '◎', '○', '▲', '△', '★', '☆', '×']:
    sub = [r for r in matched if r['old_mark'] == mark]
    st = s(sub)
    if st:
        print(f"  {mark}   {fmt(st)}")


# ====================================================================
# C. 新モデル Top-1 の詳細分析
# ====================================================================
print(f"\n{'='*95}")
print(f"  C. 新モデル Top-1 の詳細プロファイル")
print(f"{'='*95}")

top1_new = rank_new[1]

# 旧モデルでの印分布
mark_dist = defaultdict(int)
for h in top1_new:
    mark_dist[h['old_mark']] += 1

print(f"\n  新モデルTop-1が旧モデルで何印だったか:")
for mark in ['◉', '◎', '○', '▲', '△', '★', '☆', '×', '']:
    cnt = mark_dist.get(mark, 0)
    if cnt > 0:
        label = mark if mark else '(印なし)'
        pct = cnt / len(top1_new) * 100
        sub = [h for h in top1_new if h['old_mark'] == mark]
        st = s(sub)
        print(f"    {label:>4}: {cnt:>5}件 ({pct:>4.1f}%)  勝{st['wr']:>5.1f}% 複{st['p3r']:>5.1f}% 回{st['roi']:>5.1f}%")

# 旧Top-1と新Top-1が一致/不一致
top1_old_set = set()
for rid, horses in races_dict.items():
    with_old = [h for h in horses if h['old_wp'] > 0]
    if with_old:
        best = max(with_old, key=lambda x: x['old_wp'])
        top1_old_set.add((rid, best['hno']))

top1_new_set = set()
for rid, horses in races_dict.items():
    if horses:
        best = max(horses, key=lambda x: x['new_prob'])
        top1_new_set.add((rid, best['hno']))

agree = top1_old_set & top1_new_set
disagree_old = top1_old_set - top1_new_set
disagree_new = top1_new_set - top1_old_set

# 一致/不一致の成績
agree_records = [r for r in all_records if (r['rid'], r['hno']) in agree]
disagree_old_records = [r for r in all_records if (r['rid'], r['hno']) in disagree_old]
disagree_new_records = [r for r in all_records if (r['rid'], r['hno']) in disagree_new]

print(f"\n  新旧Top-1の一致/不一致:")
agree_st = s(agree_records)
dis_old_st = s(disagree_old_records)
dis_new_st = s(disagree_new_records)
if agree_st:
    print(f"    一致  ({agree_st['n']:>5,}件): 勝{agree_st['wr']:>5.1f}% 複{agree_st['p3r']:>5.1f}% 回{agree_st['roi']:>5.1f}%")
if dis_old_st:
    print(f"    旧のみ({dis_old_st['n']:>5,}件): 勝{dis_old_st['wr']:>5.1f}% 複{dis_old_st['p3r']:>5.1f}% 回{dis_old_st['roi']:>5.1f}%")
if dis_new_st:
    print(f"    新のみ({dis_new_st['n']:>5,}件): 勝{dis_new_st['wr']:>5.1f}% 複{dis_new_st['p3r']:>5.1f}% 回{dis_new_st['roi']:>5.1f}%")


# ====================================================================
# D. 自信度別 Top-1 成績
# ====================================================================
print(f"\n{'='*95}")
print(f"  D. 自信度別 Top-1/Top-3 成績比較")
print(f"{'='*95}")

for topn, label in [(1, "Top-1"), (3, "Top-3")]:
    print(f"\n  [{label}]")
    print(f"  {'自信度':>5} {'モデル':>8} {'件数':>6} {'勝率':>7} {'複勝率':>7} {'単回収':>7}")
    print("  " + "-" * 55)

    for conf in ['SS', 'S', 'A', 'B', 'C']:
        # 旧モデル
        old_sub = []
        new_sub = []
        for rid, horses in races_dict.items():
            # このレースの自信度（旧モデル予想にconfidence付き）
            race_conf = ''
            for h in horses:
                if h['old_conf']:
                    race_conf = h['old_conf']
                    break
            if race_conf != conf:
                continue

            with_old = [h for h in horses if h['old_wp'] > 0]
            sorted_old = sorted(with_old, key=lambda x: x['old_wp'], reverse=True)
            for h in sorted_old[:topn]:
                old_sub.append(h)

            sorted_new = sorted(horses, key=lambda x: x['new_prob'], reverse=True)
            for h in sorted_new[:topn]:
                new_sub.append(h)

        so = s(old_sub)
        sn = s(new_sub)
        if so and so['n'] >= 20:
            print(f"  {conf:>5}  旧モデル {fmt(so, False)}")
        if sn and sn['n'] >= 20:
            print(f"  {conf:>5}  新モデル {fmt(sn, False)}")
        if so and sn and so['n'] >= 20 and sn['n'] >= 20:
            print(f"         {'差分':>8}        "
                  f"{diff_arrow(so['wr'], sn['wr'])} "
                  f"{diff_arrow(so['p3r'], sn['p3r'])} "
                  f"{diff_arrow(so['roi'], sn['roi'], 1.0)}")
        print()


# ====================================================================
# E. JRA / NAR 別
# ====================================================================
print(f"\n{'='*95}")
print(f"  E. JRA / NAR 別 Top-1/Top-3 成績比較")
print(f"{'='*95}")

for scope, jra_val in [("JRA", 1), ("NAR", 0)]:
    print(f"\n  [{scope}]")
    for topn, label in [(1, "Top-1"), (3, "Top-3")]:
        old_sub = []
        new_sub = []
        for rid, horses in races_dict.items():
            if not horses or horses[0]['is_jra'] != jra_val:
                continue
            with_old = [h for h in horses if h['old_wp'] > 0]
            sorted_old = sorted(with_old, key=lambda x: x['old_wp'], reverse=True)
            for h in sorted_old[:topn]:
                old_sub.append(h)
            sorted_new = sorted(horses, key=lambda x: x['new_prob'], reverse=True)
            for h in sorted_new[:topn]:
                new_sub.append(h)

        so = s(old_sub)
        sn = s(new_sub)
        if so and sn:
            print(f"    {label} 旧モデル {fmt(so, False)}")
            print(f"    {label} 新モデル {fmt(sn, False)}")
            print(f"           差分        "
                  f"{diff_arrow(so['wr'], sn['wr'])} "
                  f"{diff_arrow(so['p3r'], sn['p3r'])} "
                  f"{diff_arrow(so['roi'], sn['roi'], 1.0)}")
            print()


# ====================================================================
# F. 月別推移
# ====================================================================
print(f"\n{'='*95}")
print(f"  F. 月別 Top-1 成績推移")
print(f"{'='*95}")

monthly_old = defaultdict(list)
monthly_new = defaultdict(list)

for rid, horses in races_dict.items():
    if not horses:
        continue
    month = horses[0]['date'][:7]

    with_old = [h for h in horses if h['old_wp'] > 0]
    if with_old:
        best_old = max(with_old, key=lambda x: x['old_wp'])
        monthly_old[month].append(best_old)

    best_new = max(horses, key=lambda x: x['new_prob'])
    monthly_new[month].append(best_new)

print(f"\n  {'月':>8}  {'旧件数':>5} {'旧勝率':>6} {'旧複勝':>6} {'旧単回':>6}  |  {'新件数':>5} {'新勝率':>6} {'新複勝':>6} {'新単回':>6}")
print("  " + "-" * 80)

for month in sorted(set(list(monthly_old.keys()) + list(monthly_new.keys()))):
    so = s(monthly_old.get(month, []))
    sn = s(monthly_new.get(month, []))
    if so and sn:
        print(f"  {month}  {so['n']:>5} {so['wr']:>5.1f}% {so['p3r']:>5.1f}% {so['roi']:>5.1f}%  |  "
              f"{sn['n']:>5} {sn['wr']:>5.1f}% {sn['p3r']:>5.1f}% {sn['roi']:>5.1f}%")
    elif sn:
        print(f"  {month}      -      -      -      -  |  "
              f"{sn['n']:>5} {sn['wr']:>5.1f}% {sn['p3r']:>5.1f}% {sn['roi']:>5.1f}%")

total_time = time.time() - t0
print(f"\n総実行時間: {total_time:.0f}秒")
