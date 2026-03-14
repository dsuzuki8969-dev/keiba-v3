import sys, io, json, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from collections import defaultdict
import statistics

VENUE_MAP = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
    '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉',
    '30': '門別', '35': '盛岡', '36': '水沢', '42': '浦和', '43': '船橋',
    '44': '大井', '45': '川崎', '46': '金沢', '47': '笠松', '48': '名古屋',
    '50': '園田', '51': '姫路', '54': '高知', '55': '佐賀',
}
JRA_CODES = {'01','02','03','04','05','06','07','08','09','10'}
CONDITIONS_RAW = ['良', '稍', '重', '不']
CONDITIONS_LABEL = {'良': '良', '稍': '稍重', '重': '重', '不': '不良'}
CONDITIONS = CONDITIONS_RAW

with open(r'data/course_db_preload.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
db = data.get('course_db', {})

# ---- データを集計 ----
# key: (surface, distance) -> condition -> [finish_time_sec / distance * 200]  200mあたり秒換算
# 距離が違うレースを同列比較するため「200mあたり秒」に正規化
# ただし「同一コース・同一距離で比較」も行う

# まず全体（表面×距離×馬場状態）別の平均タイム収集
# surface -> distance -> condition -> [times]
surf_dist_cond = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

# 競馬場×表面×距離×馬場状態 別
venue_surf_dist_cond = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))

SKIP_VENUES = {'65'}  # 帯広除外

for cid, runs in db.items():
    for r in runs:
        vc = r.get('venue', '')
        if vc in SKIP_VENUES:
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
        # 勝ち馬のみ使う（finish_pos == 1）
        if r.get('finish_pos') != 1:
            continue

        surf_dist_cond[sf][dist][cond].append(t)
        venue_surf_dist_cond[vc][sf][dist][cond].append(t)


def fmt_diff(diff_sec):
    if diff_sec is None:
        return "  N/A   "
    sign = '+' if diff_sec > 0 else ''
    return f"{sign}{diff_sec:.3f}秒"


lines = []
lines.append("=" * 70)
lines.append("  馬場状態別タイム差分析（勝ち馬タイムの平均、良馬場を基準）")
lines.append("=" * 70)

for sf in ['ダート', '芝']:
    lines.append(f"\n【{sf}】")
    lines.append(f"{'距離':>6}  {'良N':>6}  {'良avg':>8}  {'稍重(差)':>12}  {'重(差)':>12}  {'不良(差)':>12}")
    lines.append("-" * 72)

    dist_data = surf_dist_cond.get(sf, {})
    for dist in sorted(dist_data.keys()):
        cond_data = dist_data[dist]
        counts = {c: len(cond_data.get(c, [])) for c in CONDITIONS}
        avgs = {}
        for c in CONDITIONS:
            vals = cond_data.get(c, [])
            if len(vals) >= 3:
                avgs[c] = statistics.mean(vals)

        base = avgs.get('良')
        if base is None or counts.get('良', 0) < 5:
            continue

        diffs = {}
        for c in ['稍', '重', '不']:
            if c in avgs:
                diffs[c] = avgs[c] - base

        def fmt_cell(c):
            if c not in diffs:
                return f"{'N/A':>12}"
            n = counts.get(c, 0)
            return f"{fmt_diff(diffs[c])}({n:>4})"

        lines.append(
            f"{dist:>5}m  {counts.get('良',0):>6}  {base:>7.2f}秒  "
            f"{fmt_cell('稍')}  {fmt_cell('重')}  {fmt_cell('不')}"
        )

# ---- 中央/地方 別まとめ ----
lines.append("\n\n" + "=" * 70)
lines.append("  【中央JRA】距離帯別・馬場状態別タイム差（勝ち馬平均）")
lines.append("=" * 70)

for sf in ['ダート', '芝']:
    # JRA合算
    jra_dist_cond = defaultdict(lambda: defaultdict(list))
    for vc in JRA_CODES:
        vd = venue_surf_dist_cond.get(vc, {}).get(sf, {})
        for dist, cd in vd.items():
            for cond, ts in cd.items():
                jra_dist_cond[dist][cond].extend(ts)

    lines.append(f"\n[JRA {sf}]")
    lines.append(f"{'距離':>6}  {'良N':>5}  {'良avg':>8}  {'稍重差':>12}  {'重差':>12}  {'不良差':>12}")
    lines.append("-" * 68)
    for dist in sorted(jra_dist_cond.keys()):
        cd = jra_dist_cond[dist]
        avgs = {c: statistics.mean(cd[c]) for c in CONDITIONS if len(cd.get(c,[])) >= 3}
        base = avgs.get('良')
        if not base or len(cd.get('良',[])) < 5:
            continue
        def jfmt(c):
            if c not in avgs:
                return f"{'N/A':>12}"
            return f"{fmt_diff(avgs[c]-base)}({len(cd.get(c,[])):<4})"
        lines.append(f"{dist:>5}m  {len(cd.get('良',[])):>5}  {base:>7.2f}秒  {jfmt('稍')}  {jfmt('重')}  {jfmt('不')}")

lines.append("\n\n" + "=" * 70)
lines.append("  【地方NAR】距離帯別・馬場状態別タイム差（勝ち馬平均）")
lines.append("=" * 70)

NAR_CODES = set(VENUE_MAP.keys()) - JRA_CODES - {'65'}
for sf in ['ダート', '芝']:
    nar_dist_cond = defaultdict(lambda: defaultdict(list))
    for vc in NAR_CODES:
        vd = venue_surf_dist_cond.get(vc, {}).get(sf, {})
        for dist, cd in vd.items():
            for cond, ts in cd.items():
                nar_dist_cond[dist][cond].extend(ts)

    if not any(nar_dist_cond.values()):
        continue
    lines.append(f"\n[NAR {sf}]")
    lines.append(f"{'距離':>6}  {'良N':>5}  {'良avg':>8}  {'稍重差':>12}  {'重差':>12}  {'不良差':>12}")
    lines.append("-" * 68)
    for dist in sorted(nar_dist_cond.keys()):
        cd = nar_dist_cond[dist]
        avgs = {c: statistics.mean(cd[c]) for c in CONDITIONS if len(cd.get(c,[])) >= 3}
        base = avgs.get('良')
        if not base or len(cd.get('良',[])) < 5:
            continue
        def nfmt(c):
            if c not in avgs:
                return f"{'N/A':>12}"
            return f"{fmt_diff(avgs[c]-base)}({len(cd.get(c,[])):<4})"
        lines.append(f"{dist:>5}m  {len(cd.get('良',[])):>5}  {base:>7.2f}秒  {nfmt('稍')}  {nfmt('重')}  {nfmt('不')}")

# ---- 200mあたり換算サマリー ----
lines.append("\n\n" + "=" * 70)
lines.append("  【まとめ】全距離プール・200mあたりタイム差（大まかな傾向）")
lines.append("=" * 70)

for sf in ['ダート', '芝']:
    per200_cond = defaultdict(list)
    dist_data = surf_dist_cond.get(sf, {})
    for dist, cd in dist_data.items():
        avgs = {c: statistics.mean(cd[c]) for c in CONDITIONS if len(cd.get(c,[])) >= 5}
        base = avgs.get('良')
        if not base:
            continue
        for c in ['稍', '重', '不']:
            if c in avgs:
                diff_per200 = (avgs[c] - base) / dist * 200
                per200_cond[c].append(diff_per200)

    lines.append(f"\n{sf}（良馬場との差・200mあたり）:")
    for c, label in [('稍', '稍重'), ('重', '重'), ('不', '不良')]:
        vals = per200_cond.get(c, [])
        if vals:
            avg = statistics.mean(vals)
            sign = '+' if avg > 0 else ''
            lines.append(f"  {label}：{sign}{avg:.3f}秒/200m（{len(vals)}距離で集計）")
        else:
            lines.append(f"  {label}：データ不足")

out = '\n'.join(lines)
print(out)

with open(r'condition_report.txt', 'w', encoding='utf-8') as f:
    f.write(out)
print("\n\n保存完了: condition_report.txt")
