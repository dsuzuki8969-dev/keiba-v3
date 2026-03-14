import json

with open('data/course_db_preload.json', 'r', encoding='utf-8') as f:
    cdb = json.load(f)

inner = cdb['course_db']
print('inner keys count:', len(inner))

for cid, val in list(inner.items())[:5]:
    if isinstance(val, list) and len(val) > 0:
        r = val[0]
        print(f'course_id: {cid}, runs: {len(val)}')
        if isinstance(r, dict):
            print('  keys:', list(r.keys()))
            print('  jockey_id:', r.get('jockey_id', 'NOT FOUND'))
        break
    else:
        print(f'  key: {cid}, type: {type(val).__name__}')

# 今日の東京ダ1600が存在するか
target = '東京_ダート_1600'
if target in inner:
    runs = inner[target]
    print(f'\n{target}: {len(runs)} runs')
    if runs:
        r = runs[0]
        print('  jockey_id:', r.get('jockey_id', 'MISSING'))
        print('  finish_pos:', r.get('finish_pos'))
