import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
con = sqlite3.connect('data/keiba.db')
rows = con.execute("SELECT DISTINCT trainer_name, COUNT(*) as c FROM race_log WHERE trainer_name != '' GROUP BY trainer_name ORDER BY c DESC LIMIT 20").fetchall()
for name, c in rows:
    print(f"cnt={c:5d} | {name!r}")
print()
# 文字列の先頭2-3文字のパターンを確認
rows2 = con.execute("SELECT DISTINCT trainer_name FROM race_log WHERE trainer_name != '' ORDER BY trainer_name LIMIT 100").fetchall()
prefixes = {}
for (name,) in rows2:
    if ' ' in name:
        pre = name.split(' ')[0]
        prefixes[pre] = prefixes.get(pre, 0) + 1
print("space前のプレフィックス分布:")
for k, v in sorted(prefixes.items(), key=lambda x: -x[1])[:20]:
    print(f"  {k!r}: {v}件")
con.close()
