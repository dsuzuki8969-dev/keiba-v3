import json, sys
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path

# data/ml/ の最新JSONから調教師名を確認
ml_files = sorted(Path('data/ml').glob('*.json'))[-5:]
for f in ml_files:
    d = json.loads(f.read_text('utf-8'))
    trainers = set()
    for race in d.get('races', []):
        for h in race.get('horses', []):
            t = h.get('trainer', '')
            if t:
                trainers.add(t)
    has_prefix = [t for t in trainers if t[:2] in ('美浦', '栗東')]
    print(f"{f.name}: {len(trainers)}調教師, prefix付き{len(has_prefix)}件")
    for t in has_prefix[:3]:
        print(f"  {t!r}")

print()
# backfill_race_log.py の列インデックスを確認
# race_logのトレーナー名の由来を特定
import sqlite3
con = sqlite3.connect('data/keiba.db')
# race_idの日付分布 - backfill由来かどうか確認
rows = con.execute("SELECT substr(race_date,1,4) as yr, COUNT(*) FROM race_log GROUP BY yr ORDER BY yr").fetchall()
print("race_log 年別件数:")
for r in rows:
    print(f"  {r[0]}: {r[1]}件")
con.close()
