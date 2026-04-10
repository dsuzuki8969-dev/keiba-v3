"""新モデルでML学習データを再推論してバックテスト

ML学習データ（data/ml/*.json）には各馬の実際の着順が入っている。
これに対して新モデルで推論→印付けして旧モデルと比較する。
"""
import sqlite3, json, sys, os, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding='utf-8')

from src.ml.lgbm_model import LGBMPredictor, FEATURE_COLUMNS

DB_PATH = "data/keiba.db"
ML_DIR = "data/ml"

# 新モデルで推論
predictor = LGBMPredictor()

# ML学習データの読み込み
files = sorted([f for f in os.listdir(ML_DIR)
                if f[:8].isdigit() and f.endswith('.json')])

# 直近6ヶ月のデータを使う（学習データと重複するが、モデル比較には十分）
cutoff = "20251001"  # 2025年10月以降
target_files = [f for f in files if f[:8] >= cutoff]
print(f"対象ファイル: {len(target_files)}日分 ({target_files[0][:8]}～{target_files[-1][:8]})")

# 旧モデルの予測を取得（predictionsテーブル）
db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row

old_preds = {}  # {(race_id, horse_no): {"mark": ..., "wp": ..., "confidence": ...}}
rows = db.execute('''
    SELECT race_id, confidence, horses_json
    FROM predictions
    WHERE date >= '2025-10-01' AND date <= '2026-03-24'
''').fetchall()

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
            "wp": h.get('win_prob', 0),
            "confidence": conf,
        }

print(f"旧モデル予測: {len(old_preds):,}件")

# 新モデルで推論
print("新モデルで推論中...")
start = time.time()

new_results = []  # [{race_id, horse_no, finish_pos, tansho_odds, new_wp, old_mark, old_wp, confidence, field_count}, ...]
n_races = 0
n_horses = 0

for fi, fname in enumerate(target_files):
    date_str = fname[:8]
    date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    with open(os.path.join(ML_DIR, fname), 'r', encoding='utf-8') as f:
        data = json.load(f)

    for race in data.get('races', []):
        rid = race.get('race_id', '')
        horses = race.get('horses', [])
        fc = race.get('field_count', len(horses))

        # LGBMPredictorで予測
        try:
            predictions = predictor.predict_race(race)
        except Exception as e:
            continue

        if not predictions:
            continue

        n_races += 1

        # predictions = [(horse_no, win_prob, place_prob), ...]
        new_wp_map = {}
        for hno, wp, pp in predictions:
            new_wp_map[hno] = wp

        for h in horses:
            hno = h.get('horse_no')
            fp = h.get('finish_pos')
            if hno is None or not fp or fp <= 0:
                continue

            to = h.get('odds', 0) or 0
            old_info = old_preds.get((rid, hno), {})
            new_wp = new_wp_map.get(hno, 0)

            n_horses += 1
            new_results.append({
                "rid": rid,
                "date": date_fmt,
                "hno": hno,
                "fp": fp,
                "to": to,
                "fc": fc,
                "new_wp": new_wp,
                "old_mark": old_info.get("mark", ""),
                "old_wp": old_info.get("wp", 0),
                "confidence": old_info.get("confidence", ""),
            })

    if (fi + 1) % 30 == 0:
        elapsed = time.time() - start
        print(f"  {fi+1}/{len(target_files)}日 ({n_races:,}R, {n_horses:,}馬) {elapsed:.0f}秒")

elapsed = time.time() - start
print(f"推論完了: {n_races:,}レース, {n_horses:,}馬 ({elapsed:.1f}秒)")

db.close()


# ====================================================================
# 新モデルで印を再付与
# ====================================================================
def assign_marks(results):
    """レースごとにwin_probで印を再付与"""
    races = defaultdict(list)
    for r in results:
        races[r['rid']].append(r)

    for rid, horses in races.items():
        # win_prob降順でソート
        horses.sort(key=lambda x: x['new_wp'], reverse=True)
        fc = horses[0]['fc'] if horses else 0

        # 印付けロジック（簡易版：engine.pyの完全版ではないが近似）
        # ◉: wp >= 0.30 かつ 1位
        # ◎: 1位（◉でなければ）
        # ○: 2位
        # ▲: 3位
        # △: 4位
        # ★: 5位
        # ☆/×: 省略（正確にはcomposite等を考慮するため）
        for i, h in enumerate(horses):
            if i == 0:
                if h['new_wp'] >= 0.30:
                    h['new_mark'] = '◉'
                else:
                    h['new_mark'] = '◎'
            elif i == 1:
                if horses[0].get('new_mark') == '◉':
                    h['new_mark'] = '◎'
                else:
                    h['new_mark'] = '○'
            elif i == 2:
                if horses[0].get('new_mark') == '◉':
                    h['new_mark'] = '○'
                else:
                    h['new_mark'] = '▲'
            elif i == 3:
                if horses[0].get('new_mark') == '◉':
                    h['new_mark'] = '▲'
                else:
                    h['new_mark'] = '△'
            elif i == 4:
                if horses[0].get('new_mark') == '◉':
                    h['new_mark'] = '△'
                else:
                    h['new_mark'] = '★'
            elif i == 5:
                if horses[0].get('new_mark') == '◉':
                    h['new_mark'] = '★'
                else:
                    h['new_mark'] = '-'
            else:
                h['new_mark'] = '-'

    return results

new_results = assign_marks(new_results)


# ====================================================================
# 集計と比較
# ====================================================================
def calc_stats(records, mark_key='old_mark'):
    """印別の成績集計"""
    stats = {}
    for m in ['◉', '◎', '○', '▲', '△', '★', '☆', '×', '-']:
        stats[m] = {'n': 0, 'w': 0, 'p2': 0, 'p3': 0, 'tp': 0}

    for r in records:
        mark = r.get(mark_key, '')
        if mark not in stats:
            continue
        ms = stats[mark]
        ms['n'] += 1
        if r['fp'] == 1:
            ms['w'] += 1
            ms['tp'] += int(r['to'] * 100) if r['to'] else 0
        if r['fp'] <= 2:
            ms['p2'] += 1
        if r['fp'] <= 3:
            ms['p3'] += 1
    return stats


def print_comparison(old_stats, new_stats, title):
    print(f"\n{'='*95}")
    print(f"  {title}")
    print(f"{'='*95}")
    print(f"  {'印':>3}  {'':>8}  {'件数':>7} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単回収':>7}")
    print("  " + "-" * 75)

    for m in ['◉', '◎', '○', '▲', '△', '★']:
        old = old_stats.get(m, {'n': 0})
        new = new_stats.get(m, {'n': 0})

        for label, s in [("旧モデル", old), ("新モデル", new)]:
            n = s['n']
            if n == 0:
                continue
            wr = s['w'] / n * 100
            p2r = s['p2'] / n * 100
            p3r = s['p3'] / n * 100
            roi = s['tp'] / (n * 100) * 100
            print(f"  {m}  {label:<8} {n:>6,}件 {wr:>6.1f}% {p2r:>6.1f}% {p3r:>6.1f}% {roi:>6.1f}%")

        # 差分表示
        if old['n'] > 0 and new['n'] > 0:
            o_wr = old['w'] / old['n'] * 100
            n_wr = new['w'] / new['n'] * 100
            o_p3 = old['p3'] / old['n'] * 100
            n_p3 = new['p3'] / new['n'] * 100
            o_roi = old['tp'] / (old['n'] * 100) * 100
            n_roi = new['tp'] / (new['n'] * 100) * 100
            d_wr = n_wr - o_wr
            d_p3 = n_p3 - o_p3
            d_roi = n_roi - o_roi
            arrow_wr = "↑" if d_wr > 0.5 else ("↓" if d_wr < -0.5 else "→")
            arrow_p3 = "↑" if d_p3 > 0.5 else ("↓" if d_p3 < -0.5 else "→")
            arrow_roi = "↑" if d_roi > 1 else ("↓" if d_roi < -1 else "→")
            print(f"       {'差分':>8} {'':>7} {d_wr:>+6.1f}%{arrow_wr} {'':>7} {d_p3:>+6.1f}%{arrow_p3} {d_roi:>+6.1f}%{arrow_roi}")
        print()


# 旧モデル印が付いているレコードのみ（predictions照合可能分）
matched = [r for r in new_results if r['old_mark'] != '']
unmatched = [r for r in new_results if r['old_mark'] == '']
print(f"\n旧モデル照合可能: {len(matched):,}件")
print(f"旧モデル照合不可: {len(unmatched):,}件")

old_stats = calc_stats(matched, 'old_mark')
new_stats = calc_stats(matched, 'new_mark')

print_comparison(old_stats, new_stats, "印別成績 新旧モデル比較（照合可能分）")


# ====================================================================
# win_prob順位で直接比較（印付けロジックに依存しない純粋な比較）
# ====================================================================
print(f"\n{'='*95}")
print(f"  win_prob順位別成績（印付けロジック非依存）")
print(f"{'='*95}")

# レースごとにold_wpとnew_wpでランクを付ける
races = defaultdict(list)
for r in matched:
    races[r['rid']].append(r)

rank_stats_old = defaultdict(lambda: {'n': 0, 'w': 0, 'p2': 0, 'p3': 0, 'tp': 0})
rank_stats_new = defaultdict(lambda: {'n': 0, 'w': 0, 'p2': 0, 'p3': 0, 'tp': 0})

for rid, horses in races.items():
    # old_wpでソート
    sorted_old = sorted(horses, key=lambda x: x['old_wp'], reverse=True)
    for i, h in enumerate(sorted_old):
        rank = i + 1
        if rank > 5:
            break
        rs = rank_stats_old[rank]
        rs['n'] += 1
        if h['fp'] == 1:
            rs['w'] += 1
            rs['tp'] += int(h['to'] * 100) if h['to'] else 0
        if h['fp'] <= 2: rs['p2'] += 1
        if h['fp'] <= 3: rs['p3'] += 1

    # new_wpでソート
    sorted_new = sorted(horses, key=lambda x: x['new_wp'], reverse=True)
    for i, h in enumerate(sorted_new):
        rank = i + 1
        if rank > 5:
            break
        rs = rank_stats_new[rank]
        rs['n'] += 1
        if h['fp'] == 1:
            rs['w'] += 1
            rs['tp'] += int(h['to'] * 100) if h['to'] else 0
        if h['fp'] <= 2: rs['p2'] += 1
        if h['fp'] <= 3: rs['p3'] += 1

print(f"\n  {'順位':>4}  {'':>8} {'件数':>7} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単回収':>7}")
print("  " + "-" * 70)

for rank in range(1, 6):
    for label, rs_map in [("旧モデル", rank_stats_old), ("新モデル", rank_stats_new)]:
        rs = rs_map[rank]
        n = rs['n']
        if n == 0:
            continue
        wr = rs['w'] / n * 100
        p2r = rs['p2'] / n * 100
        p3r = rs['p3'] / n * 100
        roi = rs['tp'] / (n * 100) * 100
        print(f"  {rank}位  {label:<8} {n:>6,}件 {wr:>6.1f}% {p2r:>6.1f}% {p3r:>6.1f}% {roi:>6.1f}%")

    # 差分
    o = rank_stats_old[rank]
    ne = rank_stats_new[rank]
    if o['n'] > 0 and ne['n'] > 0:
        d_wr = ne['w']/ne['n']*100 - o['w']/o['n']*100
        d_p3 = ne['p3']/ne['n']*100 - o['p3']/o['n']*100
        d_roi = ne['tp']/(ne['n']*100)*100 - o['tp']/(o['n']*100)*100
        arrow_wr = "↑" if d_wr > 0.5 else ("↓" if d_wr < -0.5 else "→")
        arrow_p3 = "↑" if d_p3 > 0.5 else ("↓" if d_p3 < -0.5 else "→")
        arrow_roi = "↑" if d_roi > 1 else ("↓" if d_roi < -1 else "→")
        print(f"       {'差分':>8} {'':>7} {d_wr:>+6.1f}%{arrow_wr} {'':>7} {d_p3:>+6.1f}%{arrow_p3} {d_roi:>+6.1f}%{arrow_roi}")
    print()
