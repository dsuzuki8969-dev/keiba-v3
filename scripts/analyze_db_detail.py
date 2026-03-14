"""
course_db 詳細レポート
競馬場 > コース(馬場+距離) > クラス別 収集走数を出力する
収集完了後に: python analyze_db_detail.py
"""
import json, sys, re
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

VENUE_MAP = {
    '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京',
    '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉',
    '30': '門別', '35': '盛岡', '36': '水沢', '42': '浦和', '43': '船橋',
    '44': '大井', '45': '川崎', '46': '金沢', '47': '笠松', '48': '名古屋',
    '50': '園田', '51': '姫路', '54': '高知', '55': '佐賀', '65': '帯広',
}

JRA_CODES = {'01','02','03','04','05','06','07','08','09','10'}

# JRA クラス順
JRA_GRADE_ORDER = ['新馬','未勝利','1勝','2勝','3勝','OP','L','G3','G2','G1']

# 地方クラス順
NAR_GRADE_ORDER = ['新馬','未格付','C3','C2','C1','B3','B2','B1','A2','A1','OP','重賞','交流重賞']


print("course_db を読み込み中...", flush=True)
with open(r'c:\Users\dsuzu\keiba\keiba-v3\data\course_db_preload.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

db = data.get('course_db', {})
print(f"コースID数: {len(db)}, 総走数: {sum(len(v) for v in db.values()):,}\n")

# venue → surface+dist → grade → count
# venue → surface+dist → grade_gen → count (世代戦フラグ付)
stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
gen_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

for cid, runs in db.items():
    parts = cid.split('_')
    if len(parts) < 3:
        continue
    vc, sf, dist = parts[0], parts[1], parts[2]
    course_label = f"{sf}{dist}m"

    for r in runs:
        g = r.get('grade', 'その他')
        is_gen = r.get('is_generation', False)
        stats[vc][course_label][g] += 1
        gen_key = g + ('【世代戦】' if is_gen else '')
        gen_stats[vc][course_label][gen_key] += 1

print("=" * 60)
print("  競馬場別・コース別・クラス別 収集走数レポート")
print("=" * 60)

# 中央10場 → 地方 の順で出力
jra_codes_ordered = ['05','06','08','09','07','04','03','01','02','10']
nar_codes_ordered = ['44','43','45','42','30','35','36','50','51','48','47','46','54','55','65']

def print_venue(vc):
    vname = VENUE_MAP.get(vc, f'不明({vc})')
    if vc not in stats:
        return
    is_jra = vc in JRA_CODES
    grade_order = JRA_GRADE_ORDER if is_jra else NAR_GRADE_ORDER

    venue_total = sum(sum(gd.values()) for gd in stats[vc].values())
    print(f"\n●{vname}  (計{venue_total:,}走)")

    for course_label in sorted(stats[vc].keys(),
                                key=lambda x: (x[:2], int(re.sub(r'\D','',x) or '0'))):
        gd = stats[vc][course_label]
        course_total = sum(gd.values())
        print(f"  {course_label}（合計：{course_total:,}走）")
        for g in grade_order:
            if g in gd:
                print(f"    ・{g}：{gd[g]:,}走")
        # grade_orderにないものも出す
        for g, c in sorted(gd.items()):
            if g not in grade_order:
                print(f"    ・{g}：{c:,}走")

print("\n【中央競馬（JRA）10場】")
for vc in jra_codes_ordered:
    print_venue(vc)
for vc in sorted(stats.keys()):
    if vc in JRA_CODES and vc not in jra_codes_ordered:
        print_venue(vc)

print("\n\n【地方競馬（NAR）】")
for vc in nar_codes_ordered:
    print_venue(vc)
for vc in sorted(stats.keys()):
    if vc not in JRA_CODES and vc not in nar_codes_ordered:
        print_venue(vc)

# 世代戦サマリー
print("\n\n" + "=" * 60)
print("  世代戦（2歳・3歳限定）フラグサマリー")
print("=" * 60)
gen_total = sum(
    c for courses in gen_stats.values()
    for gd in courses.values()
    for g, c in gd.items() if '世代戦' in g
)
open_total = sum(len(v) for v in db.values()) - gen_total
print(f"  古馬・混合戦: {open_total:,}走")
print(f"  世代限定戦  : {gen_total:,}走 ({gen_total/(open_total+gen_total)*100:.1f}%)")
print("  ※世代戦は基準タイム計算から除外推奨（能力水準が異なるため）")

print("\n\n完了。")
