import sqlite3, sys, os, re, lz4.frame
sys.stdout.reconfigure(encoding='utf-8')

con = sqlite3.connect('data/keiba.db')
CACHE_DIR = 'data/cache'
JRA_VC = {'01','02','03','04','05','06','07','08','09','10'}

# NAR で horse weight が trainer_name になっているケース
nar_bad = con.execute("""
    SELECT DISTINCT race_id, trainer_name, trainer_id, venue_code
    FROM race_log
    WHERE is_jra=0 AND trainer_name GLOB '[0-9][0-9][0-9]*'
    LIMIT 5
""").fetchall()
print("NAR 馬体重パターン trainer_name:")
for r in nar_bad:
    print(f"  race_id={r[0]} vc={r[3]} trainer_name={r[1]!r} tid={r[2]!r}")

print()
# そのrace_idのキャッシュHTMLを探す
if nar_bad:
    target_rid = nar_bad[0][0]
    target_vc  = nar_bad[0][3]
    print(f"race_id={target_rid} のキャッシュを確認...")
    for fname in os.listdir(CACHE_DIR):
        if f'race_id={target_rid}' in fname and 'result.html' in fname:
            print(f"  found: {fname}")
            fpath = os.path.join(CACHE_DIR, fname)
            try:
                with open(fpath, 'rb') as f:
                    raw = lz4.frame.decompress(f.read()).decode('utf-8', errors='replace')
                tbl = re.search(r'ResultTableWrap.*?<tbody>(.*?)</tbody>', raw, re.DOTALL)
                if tbl:
                    tbody = tbl.group(1)
                    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', tbody, re.DOTALL):
                        if '/horse/' in tr:
                            tds_raw = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL)
                            tds = [re.sub(r'<[^>]+>', '', td).strip() for td in tds_raw]
                            print(f"  cols={len(tds)}")
                            for i, td in enumerate(tds[:16]):
                                print(f"    td[{i}]: {td!r}")
                            break
            except Exception as e:
                print(f"  ERROR: {e}")
            break
    else:
        print("  キャッシュ見つからず")

print()
# NAR で正しい trainer_name (所属+名前 or 名前のみ) のケース
nar_good = con.execute("""
    SELECT DISTINCT race_id, trainer_name, trainer_id, venue_code
    FROM race_log
    WHERE is_jra=0 AND trainer_name NOT GLOB '[0-9][0-9][0-9]*' AND trainer_name != ''
    LIMIT 5
""").fetchall()
print("NAR 正常 trainer_name:")
for r in nar_good:
    print(f"  race_id={r[0]} vc={r[3]} trainer_name={r[1]!r} tid={r[2]!r}")

con.close()
