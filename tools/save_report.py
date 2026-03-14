import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import json, re
from collections import defaultdict

_HERE = os.path.dirname(os.path.abspath(__file__))

VENUE_MAP = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
    '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉',
    '30': '門別', '35': '盛岡', '36': '水沢', '42': '浦和', '43': '船橋',
    '44': '大井', '45': '川崎', '46': '金沢', '47': '笠松', '48': '名古屋',
    '50': '園田', '51': '姫路', '54': '高知', '55': '佐賀', '65': '帯広',
}
JRA_CODES = {'01','02','03','04','05','06','07','08','09','10'}
JRA_GRADE_ORDER = ['新馬','未勝利','1勝','2勝','3勝','OP','L','G3','G2','G1']
NAR_GRADE_ORDER = ['新馬','未格付','C3','C2','C1','B3','B2','B1','A2','A1','OP','重賞','交流重賞','その他']

with open(os.path.join(_HERE, 'data', 'course_db_preload.json'), 'r', encoding='utf-8') as f:
    data = json.load(f)
db = data.get('course_db', {})
total = sum(len(v) for v in db.values())

stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
for cid, runs in db.items():
    parts = cid.split('_')
    if len(parts) < 3: continue
    vc, sf, dist = parts[0], parts[1], parts[2]
    course_label = f"{sf}{dist}m"
    for r in runs:
        g = r.get('grade', 'その他')
        stats[vc][course_label][g] += 1

lines = []
lines.append(f"総走数: {total:,}走")
lines.append("=" * 60)
lines.append("  競馬場別・コース別・クラス別 収集走数レポート")
lines.append("=" * 60)

jra_order = ['05','06','08','09','07','04','03','01','02','10']
nar_order = ['44','43','45','42','30','35','36','50','51','48','47','46','54','55','65']

def print_venue(vc):
    vname = VENUE_MAP.get(vc, vc)
    if vc not in stats: return
    is_jra = vc in JRA_CODES
    grade_order = JRA_GRADE_ORDER if is_jra else NAR_GRADE_ORDER
    venue_total = sum(sum(gd.values()) for gd in stats[vc].values())
    lines.append(f"\n●{vname}  (計{venue_total:,}走)")
    for cl in sorted(stats[vc].keys(), key=lambda x: (x[:2], int(re.sub(r'\D','',x) or '0'))):
        gd = stats[vc][cl]
        ct = sum(gd.values())
        lines.append(f"  {cl}（合計：{ct:,}走）")
        for g in grade_order:
            if g in gd:
                lines.append(f"    ・{g}：{gd[g]:,}走")
        for g, c in sorted(gd.items()):
            if g not in grade_order:
                lines.append(f"    ・{g}：{c:,}走")

lines.append("\n【中央競馬（JRA）10場】")
for vc in jra_order:
    print_venue(vc)

lines.append("\n\n【地方競馬（NAR）】")
for vc in nar_order:
    print_venue(vc)

out = '\n'.join(lines)
with open(os.path.join(_HERE, 'db_report.txt'), 'w', encoding='utf-8') as f:
    f.write(out)
print("保存完了: db_report.txt")
