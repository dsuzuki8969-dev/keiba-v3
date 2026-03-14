import sys, io, json, statistics
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from collections import defaultdict

with open(r'data/course_db_preload.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
db = data.get('course_db', {})

JRA_CODES = {'01','02','03','04','05','06','07','08','09','10'}
CONDITIONS = ['良', '稍', '重', '不']
COND_LABEL = {'稍': '稍重', '重': '重', '不': '不良'}
SKIP = {'65'}

# JRA / NAR 別に (surface, dist, cond) -> [times]
jra = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
nar = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

for cid, runs in db.items():
    for r in runs:
        vc = r.get('venue', '')
        if vc in SKIP:
            continue
        sf = r.get('surface', '')
        if sf not in ('ダート', '芝'):
            continue
        cond = r.get('condition', '')
        if cond not in CONDITIONS:
            continue
        dist = r.get('distance', 0)
        t = r.get('finish_time_sec')
        if not t or t <= 0 or dist <= 0:
            continue
        if r.get('finish_pos') != 1:
            continue
        bucket = jra if vc in JRA_CODES else nar
        bucket[sf][dist][cond].append(t)

lines = []

def render(label, bucket, sf, min_good=5, min_cond=3):
    dist_data = bucket.get(sf, {})
    rows = []
    for dist in sorted(dist_data.keys()):
        cd = dist_data[dist]
        good_n = len(cd.get('良', []))
        if good_n < min_good:
            continue
        base = statistics.mean(cd['良'])
        row = {'dist': dist, 'base': base, 'good_n': good_n, 'diffs': {}}
        for c in ['稍', '重', '不']:
            vals = cd.get(c, [])
            if len(vals) >= min_cond:
                diff = statistics.mean(vals) - base
                diff_per200 = diff / dist * 200
                row['diffs'][c] = (diff, diff_per200, len(vals))
        rows.append(row)
    return rows

lines.append("=" * 78)
lines.append("  距離別・馬場状態別タイム差分析（勝ち馬タイム・良馬場基準）")
lines.append("  ※差分はレースタイム全体の差(秒)、[/200m]は距離補正した単位差")
lines.append("=" * 78)

for org_label, bucket in [("JRA（中央）", jra), ("NAR（地方）", nar)]:
    for sf in ['芝', 'ダート']:
        rows = render(org_label, bucket, sf)
        if not rows:
            continue

        lines.append(f"\n{'─'*78}")
        lines.append(f"  【{org_label} {sf}】")
        lines.append(f"{'─'*78}")
        lines.append(f"  {'距離':>5}  {'良N':>4}  {'良avg':>7}  "
                     f"{'稍重差(秒)':>10} {'[/200m]':>7}  "
                     f"{'重差(秒)':>10} {'[/200m]':>7}  "
                     f"{'不良差(秒)':>10} {'[/200m]':>7}")
        lines.append(f"  {'─'*73}")

        for row in rows:
            dist = row['dist']
            base = row['base']
            good_n = row['good_n']

            def cell(c):
                if c not in row['diffs']:
                    return f"{'N/A':>10}  {'':>7}"
                diff, d200, n = row['diffs'][c]
                sign = '+' if diff >= 0 else ''
                s200 = '+' if d200 >= 0 else ''
                return f"{sign}{diff:>+7.3f}秒({n:>3}) {s200}{d200:>+5.3f}"

            lines.append(
                f"  {dist:>4}m  {good_n:>4}  {base:>6.2f}秒  "
                f"{cell('稍')}  {cell('重')}  {cell('不')}"
            )

        # 傾向サマリー
        lines.append(f"\n  ◆ {sf} 距離帯別傾向（{org_label}）")
        # 短距離/中距離/長距離で括る
        if sf == 'ダート':
            bands = [("短距離(〜1200m)", lambda d: d <= 1200),
                     ("マイル帯(1300〜1600m)", lambda d: 1300 <= d <= 1600),
                     ("中距離(1700〜2000m)", lambda d: 1700 <= d <= 2000),
                     ("長距離(2100m〜)", lambda d: d >= 2100)]
        else:
            bands = [("短距離(〜1400m)", lambda d: d <= 1400),
                     ("マイル帯(1500〜1600m)", lambda d: 1500 <= d <= 1600),
                     ("中距離(1700〜2200m)", lambda d: 1700 <= d <= 2200),
                     ("長距離(2300m〜)", lambda d: d >= 2300)]

        for band_name, band_fn in bands:
            band_rows = [r for r in rows if band_fn(r['dist'])]
            if not band_rows:
                continue
            parts = []
            for c, cl in [('稍', '稍重'), ('重', '重'), ('不', '不良')]:
                diffs = [r['diffs'][c][1] for r in band_rows if c in r['diffs']]
                if diffs:
                    avg = statistics.mean(diffs)
                    sign = '+' if avg >= 0 else ''
                    note = "遅↑" if avg > 0.02 else ("速↓" if avg < -0.02 else "≒良")
                    parts.append(f"{cl}:{sign}{avg:+.3f}/200m({note})")
            if parts:
                lines.append(f"    {band_name}: {' | '.join(parts)}")

# 全体傾向まとめ
lines.append("\n\n" + "=" * 78)
lines.append("  ★ 総括：距離が長いほど馬場悪化の影響は大きくなるか？")
lines.append("=" * 78)

for sf in ['芝', 'ダート']:
    for org_label, bucket in [("JRA", jra)]:
        rows = render(org_label, bucket, sf)
        lines.append(f"\n{sf}（{org_label}）- 距離と稍重/重の200mあたり差の関係:")
        for c, cl in [('稍', '稍重'), ('重', '重')]:
            pts = [(r['dist'], r['diffs'][c][1]) for r in rows if c in r['diffs']]
            if len(pts) < 3:
                continue
            # 相関傾向: 短距離 vs 長距離
            short = [v for d, v in pts if d <= 1400]
            mid   = [v for d, v in pts if 1401 <= d <= 2000]
            long_ = [v for d, v in pts if d > 2000]
            parts = []
            if short:
                parts.append(f"短:{statistics.mean(short):+.3f}")
            if mid:
                parts.append(f"中:{statistics.mean(mid):+.3f}")
            if long_:
                parts.append(f"長:{statistics.mean(long_):+.3f}")
            lines.append(f"  {cl}（/200m平均）: {' → '.join(parts)}")

out = '\n'.join(lines)
print(out)

with open(r'condition_dist_report.txt', 'w', encoding='utf-8') as f:
    f.write(out)
print("\n保存完了: condition_dist_report.txt")
