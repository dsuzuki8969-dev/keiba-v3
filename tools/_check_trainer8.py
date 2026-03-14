import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')

con = sqlite3.connect('data/keiba.db')
JRA_VC = {'01','02','03','04','05','06','07','08','09','10'}

# JRA vs NAR の trainer_name パターン
rows = con.execute("""
    SELECT is_jra, trainer_name, trainer_id, COUNT(*) as c
    FROM race_log
    WHERE trainer_name != ''
    GROUP BY is_jra, trainer_name, trainer_id
    ORDER BY c DESC
    LIMIT 20
""").fetchall()

print("is_jra | trainer_name | trainer_id | count")
for r in rows:
    print(f"  is_jra={r[0]} | {r[1]!r} | id={r[2]!r} | cnt={r[3]}")

print()
# JRAのみでの trainer_name 形式
jra_rows = con.execute("""
    SELECT DISTINCT trainer_name, trainer_id, COUNT(*) as c
    FROM race_log
    WHERE is_jra = 1 AND trainer_name != ''
    GROUP BY trainer_name, trainer_id
    ORDER BY c DESC LIMIT 10
""").fetchall()
print("JRA trainer_name サンプル:")
for r in jra_rows:
    print(f"  {r[0]!r} id={r[1]!r} cnt={r[2]}")

print()
# NARのみでの trainer_name 形式
nar_rows = con.execute("""
    SELECT DISTINCT trainer_name, trainer_id, COUNT(*) as c
    FROM race_log
    WHERE is_jra = 0 AND trainer_name != ''
    GROUP BY trainer_name, trainer_id
    ORDER BY c DESC LIMIT 10
""").fetchall()
print("NAR trainer_name サンプル:")
for r in nar_rows:
    print(f"  {r[0]!r} id={r[1]!r} cnt={r[2]}")

# JRAの trainer_name が正しいかチェック（1-1パターンがあるか）
wrong_jra = con.execute("""
    SELECT COUNT(*) FROM race_log
    WHERE is_jra=1 AND trainer_name GLOB '*-*' AND LENGTH(trainer_name) < 8
""").fetchone()[0]
print(f"\nJRA trainer_name がコーナー順パターン(X-X)のもの: {wrong_jra}件")

con.close()
