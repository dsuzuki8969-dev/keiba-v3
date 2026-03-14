import json, sys
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path

# JRAレースを含むMLファイルを確認
for f in sorted(Path('data/ml').glob('2024*.json'))[:10]:
    d = json.loads(f.read_text('utf-8'))
    for race in d.get('races', []):
        # JRA venue_code: 01-10
        vc = race.get('venue_code', '')
        if vc in ('01','02','03','04','05','06','07','08','09','10'):
            print(f"JRA file={f.name} venue={race.get('venue')} race={race.get('race_name')}")
            for h in race.get('horses', [])[:3]:
                print(f"  horse={h.get('horse_name')} trainer={h.get('trainer')!r}")
            break
    else:
        continue
    break

print()
# backfill_race_log.py が使うキャッシュファイルの種類
import os, re
CACHE_DIR = 'data/cache'
# result.html のファイルを数える
result_files = [f for f in os.listdir(CACHE_DIR) if 'result.html' in f]
print(f"result.html キャッシュ: {len(result_files)}件")
# JRA vs NAR
jra_vc = {'01','02','03','04','05','06','07','08','09','10'}
jra = sum(1 for f in result_files if re.search(r'race_id=\d{4}(\d{2})', f) and re.search(r'race_id=\d{4}(\d{2})', f).group(1) in jra_vc)
nar = len(result_files) - jra
print(f"  JRA: {jra}件  NAR: {nar}件")
