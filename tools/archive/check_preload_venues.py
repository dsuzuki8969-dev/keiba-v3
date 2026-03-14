import sys, io
sys.path.insert(0, '.')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from src.scraper.course_db_collector import load_preload_course_db
from config.settings import COURSE_DB_PRELOAD_PATH

preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
from data.masters.venue_master import VENUE_MAP

# venue_codeの出現頻度確認
venue_count = {}
for cid, runs in preload.items():
    vc = cid.split('_')[0]
    vname = VENUE_MAP.get(vc, f'不明({vc})')
    venue_count[vname] = venue_count.get(vname, 0) + len(runs)

print("場別走数 (上位20):")
for vname, cnt in sorted(venue_count.items(), key=lambda x: -x[1])[:20]:
    print(f"  {vname}: {cnt:,}走")

# JRA競馬場があるかチェック
jra = ['東京(05)', '中山(06)', '阪神(09)', '京都(08)']
print("\nJRA主要場チェック:")
for vc, vname in [('05','東京'), ('06','中山'), ('08','京都'), ('09','阪神')]:
    has = any(cid.startswith(vc+'_') for cid in preload)
    runs = sum(len(r) for cid, r in preload.items() if cid.startswith(vc+'_'))
    print(f"  {vname}({vc}): {'あり' if has else 'なし'} {runs:,}走")
