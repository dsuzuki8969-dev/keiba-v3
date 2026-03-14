import json, sys
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path

# 日付つきのmlファイルから確認
ml_files = sorted(Path('data/ml').glob('2024*.json'))[:3]
for f in ml_files:
    d = json.loads(f.read_text('utf-8'))
    for race in d.get('races', [])[:1]:
        print(f"file={f.name}, race={race.get('race_name')}, venue={race.get('venue')}")
        for h in race.get('horses', [])[:3]:
            print(f"  horse={h.get('horse_name')} trainer={h.get('trainer')!r}")
    break
