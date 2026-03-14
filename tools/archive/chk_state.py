import sys, io, json, os, datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

with open(r'data/course_db_collector_state.json', 'r', encoding='utf-8') as f:
    s = json.load(f)
print('収集状態:')
for k, v in s.items():
    print('  {}: {}'.format(k, v))

with open(r'data/course_db_preload.json', 'r', encoding='utf-8') as f:
    d = json.load(f)
db = d.get('course_db', {})
total = sum(len(v) for v in db.values())
print('DB走数: {:,}走'.format(total))
mtime = datetime.datetime.fromtimestamp(os.path.getmtime(r'data/course_db_preload.json'))
print('DB更新時刻: {}'.format(mtime))

tmp = r'data/course_db_preload.json.tmp'
if os.path.exists(tmp):
    print('WARNING: .tmpファイルが残っています ({})'.format(os.path.getsize(tmp)))
else:
    print('.tmpファイル: なし（正常）')

# 最新日付を確認
dates = []
for runs in db.values():
    for r in runs:
        dates.append(r.get('race_date', ''))
dates = [d for d in dates if d]
if dates:
    print('DB最古日付: {}'.format(min(dates)))
    print('DB最新日付: {}'.format(max(dates)))
