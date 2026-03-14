import json

with open('data/personnel_db.json', 'r', encoding='utf-8') as f:
    db = json.load(f)

jockeys = db.get('jockeys', {})
print('騎手数:', len(jockeys))
for jid, d in list(jockeys.items())[:8]:
    name = d.get('name', '')
    udev = d.get('upper_long_dev', 0)
    print(f'  {name:10s} upper_long_dev={udev:.1f}')

print()
trainers = db.get('trainers', {})
print('厩舎数:', len(trainers))
for tid, d in list(trainers.items())[:8]:
    name = d.get('name', '')
    rank = d.get('rank', '?')
    wr   = d.get('win_rate', 0)
    print(f'  {name:10s} rank={rank}  win_rate={wr:.3f}')
