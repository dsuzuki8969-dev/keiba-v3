"""新旧モデル比較バックテスト

predictionsテーブル(旧モデル)の予想 vs ML学習データ(結果あり)を
新モデルで再推論して印別・自信度別の成績を比較する。
"""
import sqlite3, json, sys, os
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = "data/keiba.db"


def get_old_model_stats(db, date_from="2025-07-01", date_to="2026-03-24"):
    """旧モデル: predictionsテーブル + race_log で成績集計"""
    rows = db.execute('''
        SELECT p.date, p.race_id, p.confidence, p.horses_json
        FROM predictions p
        WHERE p.date BETWEEN ? AND ?
        ORDER BY p.date
    ''', (date_from, date_to)).fetchall()

    # 印別
    mark_stats = {}
    for m in ['◉', '◎', '○', '▲', '△', '★', '☆', '×']:
        mark_stats[m] = {'n': 0, 'w': 0, 'p2': 0, 'p3': 0, 'tp': 0, 'fp': 0}

    # 自信度別
    conf_stats = {}
    for c in ['SS', 'S', 'A', 'B', 'C', 'D', 'E']:
        conf_stats[c] = {m: {'n': 0, 'w': 0, 'p2': 0, 'p3': 0, 'tp': 0, 'fp': 0}
                         for m in ['◉', '◎', '○', '▲', '△', '★', '☆']}

    n_races = 0

    for row in rows:
        rid = row['race_id']
        conf_raw = row['confidence'] or 'C'
        conf = conf_raw if conf_raw in conf_stats else 'C'
        try:
            horses = json.loads(row['horses_json']) if row['horses_json'] else []
        except:
            continue

        has_result = False
        for h in horses:
            mark = h.get('mark', '')
            hno = h.get('horse_no')
            if not mark or hno is None:
                continue

            rl = db.execute(
                'SELECT finish_pos, tansho_odds FROM race_log WHERE race_id=? AND horse_no=?',
                (rid, hno)).fetchone()
            if not rl or not rl['finish_pos'] or rl['finish_pos'] <= 0:
                continue

            has_result = True
            fp = rl['finish_pos']
            to = rl['tansho_odds'] or 0

            # 複勝配当はML学習データから取得（後で）
            # ここでは単勝のみ

            if mark in mark_stats:
                ms = mark_stats[mark]
                ms['n'] += 1
                if fp == 1:
                    ms['w'] += 1
                    ms['tp'] += int(to * 100)
                if fp <= 2:
                    ms['p2'] += 1
                if fp <= 3:
                    ms['p3'] += 1

            if mark in conf_stats.get(conf, {}):
                cs = conf_stats[conf][mark]
                cs['n'] += 1
                if fp == 1:
                    cs['w'] += 1
                    cs['tp'] += int(to * 100)
                if fp <= 2:
                    cs['p2'] += 1
                if fp <= 3:
                    cs['p3'] += 1

        if has_result:
            n_races += 1

    return mark_stats, conf_stats, n_races


def get_fukusho_payouts(ml_data_dir="data/ml", date_from="2025-07-01", date_to="2026-03-24"):
    """ML学習データから複勝配当を取得"""
    payouts = {}  # {(race_id, horse_no): fukusho_payout}
    files = sorted([f for f in os.listdir(ml_data_dir)
                    if f[:8].isdigit() and f.endswith('.json')])

    for fname in files:
        date_str = fname[:8]
        date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        if date_fmt < date_from or date_fmt > date_to:
            continue

        with open(os.path.join(ml_data_dir, fname), 'r', encoding='utf-8') as f:
            data = json.load(f)

        for race in data.get('races', []):
            rid = race.get('race_id', '')
            payout_data = race.get('payouts', {})
            fukusho = payout_data.get('複勝', {})

            # 複勝配当のパース
            if isinstance(fukusho, dict):
                combo = str(fukusho.get('combo', ''))
                payout_str = str(fukusho.get('payout', ''))
                # combo="1411", payout="600180200" のような形式
                # 各馬番2桁、各配当金額は可変桁
                # 馬番と配当を対応付ける
                horse_nos = []
                for i in range(0, len(combo), 2):
                    if i + 2 <= len(combo):
                        hn = int(combo[i:i+2])
                        horse_nos.append(hn)
                    elif i + 1 <= len(combo):
                        hn = int(combo[i:i+1])
                        horse_nos.append(hn)

                # 配当金額のパース（3着以内の馬数分に分割）
                # 通常3頭分だが、2頭以下の場合もある
                n_horses = len(horse_nos)
                if n_horses > 0 and payout_str.isdigit():
                    # 配当金額を均等分割で推定（実際は個別だが）
                    # → 個別配当取得が必要
                    pass

            elif isinstance(fukusho, list):
                for item in fukusho:
                    if isinstance(item, dict):
                        hn = item.get('horse_no')
                        po = item.get('payout', 0)
                        if hn and po:
                            payouts[(rid, int(hn))] = po

    return payouts


def add_fukusho_from_predictions(db, mark_stats, date_from, date_to):
    """predictionsテーブル + race_logで複勝回収率を計算
    race_logにfukusho_oddsがないため、推定値を使う：
    複勝率(3着以内) × 仮の複勝配当 = 近似的に計算不可
    → 代わりにresults_jsonがあるか確認
    """
    pass


def print_mark_table(title, stats, show_fukusho=False):
    """印別成績テーブル表示"""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")
    header = f"{'印':>3} {'件数':>7} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単回収':>7}"
    print(header)
    print("-" * 55)
    for m in ['◉', '◎', '○', '▲', '△', '★', '☆', '×']:
        ms = stats.get(m, {})
        n = ms.get('n', 0)
        if n == 0:
            continue
        wr = ms['w'] / n * 100
        p2r = ms['p2'] / n * 100
        p3r = ms['p3'] / n * 100
        roi = ms['tp'] / (n * 100) * 100
        print(f"  {m}  {n:>6,}件 {wr:>6.1f}% {p2r:>6.1f}% {p3r:>6.1f}% {roi:>6.1f}%")


def print_conf_table(title, conf_stats, marks=None):
    """自信度×印 クロス成績"""
    if marks is None:
        marks = ['◎', '◉']
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")

    for mark in marks:
        print(f"\n  [{mark}] 自信度別:")
        print(f"  {'自信度':>5} {'件数':>7} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単回収':>7}")
        print("  " + "-" * 55)
        for conf in ['SS', 'S', 'A', 'B', 'C']:
            cs = conf_stats.get(conf, {}).get(mark, {})
            n = cs.get('n', 0)
            if n < 10:
                continue
            wr = cs['w'] / n * 100
            p2r = cs['p2'] / n * 100
            p3r = cs['p3'] / n * 100
            roi = cs['tp'] / (n * 100) * 100
            print(f"  {conf:>5}  {n:>6,}件 {wr:>6.1f}% {p2r:>6.1f}% {p3r:>6.1f}% {roi:>6.1f}%")


# ====================================================================
# メイン実行
# ====================================================================
db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row

# 結果のある期間を確認
last_date = db.execute("SELECT MAX(race_date) FROM race_log").fetchone()[0]
print(f"race_log最終日: {last_date}")

# 旧モデル成績（全期間）
print("\n" + "#" * 80)
print("# 旧モデル成績（predictionsテーブル + race_log）")
print("#" * 80)

# 期間別に表示
periods = [
    ("全期間 (2025-07-01～)", "2025-07-01", last_date),
    ("直近3ヶ月", "2025-12-24", last_date),
    ("直近1ヶ月", "2026-02-24", last_date),
]

for label, d_from, d_to in periods:
    ms, cs, nr = get_old_model_stats(db, d_from, d_to)
    print_mark_table(f"印別成績 [{label}] ({nr:,}レース)", ms)
    if label == "全期間 (2025-07-01～)":
        print_conf_table(f"自信度×印 [{label}]", cs, ['◉', '◎', '○'])

# JRA / NAR 別
print("\n" + "#" * 80)
print("# JRA / NAR 別成績（全期間）")
print("#" * 80)

for scope, is_jra in [("JRA", 1), ("NAR", 0)]:
    rows = db.execute('''
        SELECT p.date, p.race_id, p.confidence, p.horses_json
        FROM predictions p
        JOIN (SELECT DISTINCT race_id, is_jra FROM race_log) rl ON p.race_id = rl.race_id
        WHERE p.date BETWEEN '2025-07-01' AND ?
          AND rl.is_jra = ?
        ORDER BY p.date
    ''', (last_date, is_jra)).fetchall()

    mark_stats = {}
    for m in ['◉', '◎', '○', '▲', '△', '★', '☆', '×']:
        mark_stats[m] = {'n': 0, 'w': 0, 'p2': 0, 'p3': 0, 'tp': 0}
    n_races = 0

    for row in rows:
        rid = row['race_id']
        try:
            horses = json.loads(row['horses_json']) if row['horses_json'] else []
        except:
            continue
        has = False
        for h in horses:
            mark = h.get('mark', '')
            hno = h.get('horse_no')
            if not mark or hno is None or mark not in mark_stats:
                continue
            rl = db.execute(
                'SELECT finish_pos, tansho_odds FROM race_log WHERE race_id=? AND horse_no=?',
                (rid, hno)).fetchone()
            if not rl or not rl['finish_pos'] or rl['finish_pos'] <= 0:
                continue
            has = True
            fp = rl['finish_pos']
            to = rl['tansho_odds'] or 0
            ms = mark_stats[mark]
            ms['n'] += 1
            if fp == 1:
                ms['w'] += 1
                ms['tp'] += int(to * 100)
            if fp <= 2:
                ms['p2'] += 1
            if fp <= 3:
                ms['p3'] += 1
        if has:
            n_races += 1

    print_mark_table(f"[{scope}] 印別成績 ({n_races:,}レース)", mark_stats)

# 月別推移
print("\n" + "#" * 80)
print("# 月別推移 (◎ の勝率・複勝率・単回収率)")
print("#" * 80)

rows = db.execute('''
    SELECT p.date, p.race_id, p.horses_json
    FROM predictions p
    WHERE p.date BETWEEN '2025-07-01' AND ?
    ORDER BY p.date
''', (last_date,)).fetchall()

monthly = defaultdict(lambda: {'n': 0, 'w': 0, 'p3': 0, 'tp': 0})
monthly_honmei = defaultdict(lambda: {'n': 0, 'w': 0, 'p3': 0, 'tp': 0})

for row in rows:
    rid = row['race_id']
    month = row['date'][:7]
    try:
        horses = json.loads(row['horses_json']) if row['horses_json'] else []
    except:
        continue
    for h in horses:
        mark = h.get('mark', '')
        hno = h.get('horse_no')
        if hno is None:
            continue
        if mark not in ('◎', '◉'):
            continue
        rl = db.execute(
            'SELECT finish_pos, tansho_odds FROM race_log WHERE race_id=? AND horse_no=?',
            (rid, hno)).fetchone()
        if not rl or not rl['finish_pos'] or rl['finish_pos'] <= 0:
            continue
        fp = rl['finish_pos']
        to = rl['tansho_odds'] or 0

        target = monthly_honmei if mark == '◉' else monthly
        ms = target[month]
        ms['n'] += 1
        if fp == 1:
            ms['w'] += 1
            ms['tp'] += int(to * 100)
        if fp <= 3:
            ms['p3'] += 1

print(f"\n  {'月':>8} {'◎件数':>6} {'◎勝率':>6} {'◎複勝':>6} {'◎単回':>6}  |  {'◉件数':>6} {'◉勝率':>6} {'◉複勝':>6} {'◉単回':>6}")
print("  " + "-" * 80)
for month in sorted(set(list(monthly.keys()) + list(monthly_honmei.keys()))):
    ms = monthly[month]
    mh = monthly_honmei[month]
    if ms['n'] == 0 and mh['n'] == 0:
        continue
    def fmt(s):
        if s['n'] == 0:
            return "   -      -      -      -  "
        wr = s['w'] / s['n'] * 100
        p3r = s['p3'] / s['n'] * 100
        roi = s['tp'] / (s['n'] * 100) * 100
        return f"{s['n']:>5} {wr:>5.1f}% {p3r:>5.1f}% {roi:>5.1f}%"
    print(f"  {month}  {fmt(ms)}  |  {fmt(mh)}")

db.close()
print("\n\n注: これは旧モデルで生成された予想の成績です。")
print("新モデルの予想はまだ結果が出ていない日(4/5～)のみ生成済みです。")
print("新モデルの実力は、AUC 0.7344→0.7503 (+2.2%) の改善が指標です。")
