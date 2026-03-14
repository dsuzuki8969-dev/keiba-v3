import sqlite3
con = sqlite3.connect('data/keiba.db')
rows = con.execute("SELECT DISTINCT trainer_name, COUNT(*) as c FROM race_log WHERE trainer_name != '' GROUP BY trainer_name ORDER BY c DESC LIMIT 20").fetchall()
for name, c in rows:
    print(repr(name), 'cnt=', c)
print()
# 地方っぽいものも確認
rows2 = con.execute("SELECT DISTINCT trainer_name FROM race_log WHERE trainer_name != '' LIMIT 50").fetchall()
for (name,) in rows2:
    # 3文字以上のプレフィックスがついてそうなもの
    if len(name) >= 4 and name[2] == ' ':
        print('prefix-pattern:', repr(name))
con.close()
