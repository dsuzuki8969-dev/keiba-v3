import json

with open('data/personnel_db.json', 'r', encoding='utf-8') as f:
    db = json.load(f)

print("=== 騎手 ===")
for jid, d in list(db['jockeys'].items())[:10]:
    name = d.get('jockey_name', jid)
    udev = d.get('upper_long_dev', 0)
    print(f"  {name:12s}  偏差値={udev:.1f}")

print("\n=== 調教師 ===")
for tid, d in list(db['trainers'].items())[:10]:
    name = d.get('trainer_name', tid)
    rank = d.get('rank', '?')
    print(f"  {name:12s}  rank={rank}")
