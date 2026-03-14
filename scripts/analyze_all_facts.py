"""
収集データから引き出せる全事実を一括分析する
各セクションで実データ値を算出し、D指数に反映すべき項目を特定する
"""
import sys, io, json, statistics, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from collections import defaultdict

with open(r'data/course_db_preload.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
db = data.get('course_db', {})

JRA_CODES = {'01','02','03','04','05','06','07','08','09','10'}
COND_NORM = {'稍': '稍重', '不': '不良', '良': '良', '重': '重'}
SKIP_VENUES = {'65'}

VENUE_MAP = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
    '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉',
    '30': '門別', '35': '盛岡', '36': '水沢', '42': '浦和', '43': '船橋',
    '44': '大井', '45': '川崎', '46': '金沢', '47': '笠松', '48': '名古屋',
    '50': '園田', '51': '姫路', '54': '高知', '55': '佐賀',
}

# 全走を展開してフラットなリストを作る
all_runs = []
for cid, runs in db.items():
    for r in runs:
        if r.get('venue', '') not in SKIP_VENUES:
            all_runs.append(r)

total = len(all_runs)
winners = [r for r in all_runs if r.get('finish_pos') == 1]
lines = []
lines.append(f"総走数: {total:,}走  うち1着: {len(winners):,}走")
lines.append("=" * 72)

# ============================================================
# § 1. クラス別 基準タイム差（JRA 芝/ダート 主要距離）
# ============================================================
lines.append("\n§1. クラス別タイム差（1着馬タイム、JRA・良馬場限定）")
lines.append("─" * 72)

JRA_GRADES_ORDER = ['新馬', '未勝利', '1勝', '2勝', '3勝', 'OP', 'L', 'G3', 'G2', 'G1']

for sf in ['ダート', '芝']:
    for dist in ([1400, 1600, 1800, 2000] if sf == '芝' else [1400, 1600, 1800]):
        grade_times = defaultdict(list)
        for r in winners:
            if (r.get('venue', '') in JRA_CODES and
                r.get('surface') == sf and
                r.get('condition') == '良' and
                r.get('distance') == dist):
                g = r.get('grade', '')
                grade_times[g].append(r['finish_time_sec'])

        avgs = {g: statistics.mean(ts) for g, ts in grade_times.items() if len(ts) >= 3}
        if len(avgs) < 3:
            continue

        lines.append(f"\n  【JRA {sf} {dist}m 良馬場】")
        base_grade = '1勝' if '1勝' in avgs else list(avgs.keys())[0]
        base_time = avgs.get(base_grade)
        for g in JRA_GRADES_ORDER:
            if g not in avgs:
                continue
            avg = avgs[g]
            n = len(grade_times[g])
            diff = avg - base_time
            sign = '+' if diff >= 0 else ''
            lines.append(f"    {g:6s}: {avg:.2f}秒  (1勝差: {sign}{diff:.2f}秒, N={n})")

# ============================================================
# § 2. 頭数別タイム傾向（field_count）
# ============================================================
lines.append("\n\n§2. 出走頭数別タイム傾向（JRA 芝2000m・ダート1600m、良馬場1着）")
lines.append("─" * 72)

for sf, dist in [('芝', 2000), ('ダート', 1600)]:
    head_times = defaultdict(list)
    for r in winners:
        if (r.get('venue', '') in JRA_CODES and
            r.get('surface') == sf and
            r.get('condition') == '良' and
            r.get('distance') == dist):
            fc = r.get('field_count', 0)
            if fc > 0:
                head_times[fc].append(r['finish_time_sec'])

    lines.append(f"\n  JRA {sf} {dist}m 良馬場:")
    band_times = {
        '〜8頭': [t for fc, ts in head_times.items() for t in ts if fc <= 8],
        '9〜12頭': [t for fc, ts in head_times.items() for t in ts if 9 <= fc <= 12],
        '13〜16頭': [t for fc, ts in head_times.items() for t in ts if 13 <= fc <= 16],
        '17頭以上': [t for fc, ts in head_times.items() for t in ts if fc >= 17],
    }
    base_t = None
    for band, ts in band_times.items():
        if len(ts) >= 5:
            avg = statistics.mean(ts)
            if base_t is None:
                base_t = avg
            diff = avg - base_t
            sign = '+' if diff >= 0 else ''
            lines.append(f"    {band}: {avg:.2f}秒  ({sign}{diff:.2f}秒, N={len(ts)})")

# ============================================================
# § 3. 月別（季節）タイム傾向
# ============================================================
lines.append("\n\n§3. 月別タイム傾向（JRA 芝2000m・ダート1600m 良馬場1着）")
lines.append("─" * 72)

for sf, dist in [('芝', 2000), ('ダート', 1600)]:
    month_times = defaultdict(list)
    for r in winners:
        if (r.get('venue', '') in JRA_CODES and
            r.get('surface') == sf and
            r.get('condition') == '良' and
            r.get('distance') == dist):
            try:
                m = int(r['race_date'][5:7])
                month_times[m].append(r['finish_time_sec'])
            except Exception:
                pass

    avgs = {m: statistics.mean(ts) for m, ts in month_times.items() if len(ts) >= 5}
    if not avgs:
        continue
    overall_avg = statistics.mean([t for ts in month_times.values() for t in ts])
    lines.append(f"\n  JRA {sf} {dist}m 良馬場 (全月平均: {overall_avg:.2f}秒):")
    for m in range(1, 13):
        if m in avgs:
            diff = avgs[m] - overall_avg
            sign = '+' if diff >= 0 else ''
            lines.append(f"    {m:2d}月: {avgs[m]:.2f}秒  ({sign}{diff:.3f}秒, N={len(month_times[m])})")

# ============================================================
# § 4. 世代戦 vs オープン タイム差
# ============================================================
lines.append("\n\n§4. 世代限定戦 vs 一般戦 タイム差（同コース・同クラス）")
lines.append("─" * 72)

# grade別に世代戦フラグの有無でタイム差を比較
for sf in ['ダート', '芝']:
    for grade in ['未勝利', '1勝']:
        gen_times = defaultdict(list)  # dist -> times
        open_times = defaultdict(list)
        for r in winners:
            if (r.get('venue', '') in JRA_CODES and
                r.get('surface') == sf and
                r.get('condition') == '良' and
                r.get('grade') == grade):
                dist = r.get('distance', 0)
                if r.get('is_generation', False):
                    gen_times[dist].append(r['finish_time_sec'])
                else:
                    open_times[dist].append(r['finish_time_sec'])

        rows = []
        for dist in sorted(set(gen_times.keys()) | set(open_times.keys())):
            gt = gen_times.get(dist, [])
            ot = open_times.get(dist, [])
            if len(gt) >= 5 and len(ot) >= 5:
                gavg = statistics.mean(gt)
                oavg = statistics.mean(ot)
                diff = gavg - oavg
                rows.append((dist, gavg, oavg, diff, len(gt), len(ot)))

        if rows:
            lines.append(f"\n  JRA {sf} {grade}:")
            lines.append(f"    {'距離':>5}  {'世代戦avg':>9}  {'一般戦avg':>9}  {'差(世代-一般)':>12}  N世代/N一般")
            for dist, gavg, oavg, diff, ng, no in rows:
                sign = '+' if diff >= 0 else ''
                lines.append(f"    {dist:>4}m  {gavg:>9.2f}  {oavg:>9.2f}  {sign}{diff:>9.2f}秒  {ng}/{no}")

# ============================================================
# § 5. ペース傾向（前半3F vs 後半3F）
# ============================================================
lines.append("\n\n§5. ペース傾向（前半3F/後半3F比率, 1着馬、良馬場）")
lines.append("─" * 72)
lines.append("  ハイペース = 前半速い・後半遅い (比率 > 1.0)")
lines.append("  スローペース = 前半遅い・後半速い (比率 < 1.0)")

for is_jra, org_label in [(True, "JRA"), (False, "NAR")]:
    venue_set = JRA_CODES if is_jra else (set(VENUE_MAP.keys()) - JRA_CODES - SKIP_VENUES)
    for sf in ['ダート', '芝']:
        dist_pace = defaultdict(list)
        for r in winners:
            if (r.get('venue', '') in venue_set and
                r.get('surface') == sf and
                r.get('condition') == '良'):
                f3 = r.get('first_3f_sec')
                l3 = r.get('last_3f_sec')
                dist = r.get('distance', 0)
                if f3 and l3 and f3 > 0 and l3 > 0 and dist >= 800:
                    dist_pace[dist].append(f3 / l3)

        rows = [(d, statistics.mean(v), len(v)) for d, v in dist_pace.items() if len(v) >= 10]
        if not rows:
            continue
        lines.append(f"\n  {org_label} {sf}:")
        lines.append(f"    {'距離':>5}  {'前後比率avg':>10}  {'傾向':>8}  N")
        for dist, ratio, n in sorted(rows):
            tend = "ハイ" if ratio > 1.03 else ("スロー" if ratio < 0.97 else "ミドル")
            lines.append(f"    {dist:>4}m  {ratio:>10.3f}  {tend:>8}  {n}")

# ============================================================
# § 6. 競馬場別 時計傾向（同距離・同条件での速い/遅い場の特定）
# ============================================================
lines.append("\n\n§6. 競馬場別 時計レベル（JRA ダート1400m・芝2000m 良馬場1着 平均タイム）")
lines.append("─" * 72)

for sf, dist in [('ダート', 1400), ('芝', 2000)]:
    venue_times = defaultdict(list)
    for r in winners:
        if (r.get('venue', '') in JRA_CODES and
            r.get('surface') == sf and
            r.get('condition') == '良' and
            r.get('distance') == dist):
            venue_times[r['venue']].append(r['finish_time_sec'])

    avgs = {v: statistics.mean(ts) for v, ts in venue_times.items() if len(ts) >= 5}
    if not avgs:
        continue
    overall = statistics.mean([t for ts in venue_times.values() for t in ts])
    lines.append(f"\n  JRA {sf} {dist}m 良馬場 (全場平均: {overall:.2f}秒):")
    for vc, avg in sorted(avgs.items(), key=lambda x: x[1]):
        diff = avg - overall
        sign = '+' if diff >= 0 else ''
        vname = VENUE_MAP.get(vc, vc)
        lines.append(f"    {vname}({vc}): {avg:.2f}秒  ({sign}{diff:.2f}秒, N={len(venue_times[vc])})")

# ============================================================
# § 7. 着差分布（勝ち馬の次点との差）
# ============================================================
lines.append("\n\n§7. 勝ち馬の着差分布（接戦率・大差率）")
lines.append("─" * 72)

for sf in ['ダート', '芝']:
    margins = []
    for r in winners:
        if r.get('surface') == sf and r.get('venue', '') not in SKIP_VENUES:
            m = r.get('margin_behind', None)
            if m is not None and m == 0:
                # 1着の margin_behind = 0, margin_ahead = 次馬との差
                m2 = r.get('margin_ahead', None)
                if m2 is not None and m2 >= 0:
                    margins.append(m2)

    if len(margins) < 10:
        continue

    n = len(margins)
    接戦 = sum(1 for m in margins if m <= 0.1)
    中間 = sum(1 for m in margins if 0.1 < m <= 0.5)
    大差 = sum(1 for m in margins if m > 0.5)
    avg_margin = statistics.mean(margins)
    med_margin = statistics.median(margins)

    lines.append(f"\n  {sf} (N={n:,}):")
    lines.append(f"    接戦(0.1秒以内): {接戦:,}回 ({接戦/n*100:.1f}%)")
    lines.append(f"    中間(0.1〜0.5秒): {中間:,}回 ({中間/n*100:.1f}%)")
    lines.append(f"    大差(0.5秒超):   {大差:,}回 ({大差/n*100:.1f}%)")
    lines.append(f"    平均差: {avg_margin:.3f}秒  中央値: {med_margin:.3f}秒")

# ============================================================
# § 8. 上がり3F傾向（距離・馬場状態別）
# ============================================================
lines.append("\n\n§8. 上がり3F傾向（JRA 良馬場1着、距離別平均）")
lines.append("─" * 72)

for sf in ['芝', 'ダート']:
    dist_l3f = defaultdict(list)
    for r in winners:
        if (r.get('venue', '') in JRA_CODES and
            r.get('surface') == sf and
            r.get('condition') == '良'):
            l3 = r.get('last_3f_sec')
            dist = r.get('distance', 0)
            if l3 and l3 > 0 and dist >= 800:
                dist_l3f[dist].append(l3)

    rows = [(d, statistics.mean(v), len(v)) for d, v in dist_l3f.items() if len(v) >= 10]
    if not rows:
        continue
    lines.append(f"\n  JRA {sf}:")
    lines.append(f"    {'距離':>5}  {'上がり3F平均':>11}  N")
    for dist, avg, n in sorted(rows):
        lines.append(f"    {dist:>4}m  {avg:>11.2f}秒  {n}")

# ============================================================
# § 9. 現在のCLASS_SCOREの検証（実データで算出した値 vs 現行設定）
# ============================================================
lines.append("\n\n§9. クラス補正スコア検証（実データ値 vs 現行設定値）")
lines.append("─" * 72)
lines.append("  ※ 実データ: JRA ダート1400m良馬場 1着タイム → 1勝クラスを0基準")
lines.append("  ※ 現行設定: config/settings.py の CLASS_SCORE (相対値)")

# 現行の CLASS_SCORE (ability.py から)
current_class_score = {
    "G1": 6, "G2": 5, "G3": 4,
    "OP": 3, "L": 3,
    "3勝": 2, "1600万": 2,
    "2勝": 1, "1000万": 1,
    "1勝": 0, "500万": 0,
    "未勝利": -1, "新馬": -2,
}

# 複数距離でのクラス別タイム差を算出して平均
grade_diffs_all = defaultdict(list)
for sf_d in [('ダート', 1400), ('ダート', 1600), ('芝', 1600), ('芝', 2000)]:
    sf, dist = sf_d
    grade_times2 = defaultdict(list)
    for r in winners:
        if (r.get('venue', '') in JRA_CODES and
            r.get('surface') == sf and
            r.get('condition') == '良' and
            r.get('distance') == dist and
            not r.get('is_generation', False)):
            g = r.get('grade', '')
            grade_times2[g].append(r['finish_time_sec'])

    avgs2 = {g: statistics.mean(ts) for g, ts in grade_times2.items() if len(ts) >= 5}
    base2 = avgs2.get('1勝')
    if base2 is None:
        continue

    dist_coeff = 1600 / dist
    for g, avg in avgs2.items():
        time_diff = avg - base2  # 秒差（正 = 遅い）
        score_equiv = -time_diff * dist_coeff  # スコア換算（速い = 高スコア）
        grade_diffs_all[g].append(score_equiv)

lines.append(f"  {'クラス':>6}  {'実データスコア':>12}  {'現行設定':>8}  {'差':>6}  サンプル")
for g in ['G1', 'G2', 'G3', 'OP', 'L', '3勝', '2勝', '1勝', '未勝利', '新馬']:
    vals = grade_diffs_all.get(g, [])
    if len(vals) >= 2:
        avg_score = statistics.mean(vals)
        current = current_class_score.get(g, 0)
        diff = avg_score - current
        sign = '+' if diff >= 0 else ''
        lines.append(f"  {g:>6}: 実={avg_score:>+8.2f}  現行={current:>+4d}  差={sign}{diff:>5.2f}  N={len(vals)}")

# ============================================================
# § 10. 斤量補正の検証
# ============================================================
lines.append("\n\n§10. 斤量別タイム傾向（JRA 芝2000m 良馬場1着）")
lines.append("─" * 72)
lines.append("  ※ 現行: 1kgあたり0.15秒換算 → 実データで検証")

weight_times = defaultdict(list)
for r in winners:
    if (r.get('venue', '') in JRA_CODES and
        r.get('surface') == '芝' and
        r.get('condition') == '良' and
        r.get('distance') == 2000 and
        not r.get('is_generation', False)):
        wkg = r.get('weight_kg')
        if wkg:
            wkg_r = round(wkg * 2) / 2  # 0.5kg刻みに丸める
            weight_times[wkg_r].append(r['finish_time_sec'])

avgs_w = {w: statistics.mean(ts) for w, ts in weight_times.items() if len(ts) >= 5}
base_w = avgs_w.get(55.0) or avgs_w.get(56.0) or (list(avgs_w.values())[0] if avgs_w else None)
if avgs_w and base_w:
    lines.append(f"\n  JRA 芝2000m 良馬場 (55kgを基準):")
    base_wkg = 55.0 if 55.0 in avgs_w else 56.0
    base_w = avgs_w[base_wkg]
    for w in sorted(avgs_w.keys()):
        diff = avgs_w[w] - base_w
        per_kg = diff / (w - base_wkg) if w != base_wkg else 0
        sign = '+' if diff >= 0 else ''
        lines.append(f"    {w:>4.1f}kg: {avgs_w[w]:.2f}秒  ({sign}{diff:.3f}秒  {sign}{per_kg:.3f}秒/kg, N={len(weight_times[w])})")

# ============================================================
# まとめ: D指数に反映すべき事実
# ============================================================
lines.append("\n\n" + "=" * 72)
lines.append("★ D指数改善に反映すべき事実まとめ")
lines.append("=" * 72)
lines.append("""
【既反映】
  ✓ §馬場補正: 実データ値で TrackCorrector を刷新（距離帯別・条件別）

【要反映候補】
  § 1. クラス補正スコア → CLASS_SCORE の実データ対応値に更新
  § 3. 季節補正 → SEASON_SCORE の月別実データ値に更新
  § 4. 世代戦補正 → is_generation フラグで別クラス扱い
  § 5. ペース傾向 → 距離帯別ペース傾向をD指数に組み込み
  § 6. 競馬場別補正 → 場ごとの時計レベル差を基準タイムに反映
  § 8. 上がり3F → 末脚評価の絶対値基準を実データから設定
  §10. 斤量補正 → 0.15秒/kg の妥当性検証・実データ値に更新
""")

out = '\n'.join(lines)
print(out)

with open(r'all_facts_report.txt', 'w', encoding='utf-8') as f:
    f.write(out)
print("保存完了: all_facts_report.txt")
