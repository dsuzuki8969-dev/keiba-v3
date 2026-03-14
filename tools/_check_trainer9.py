import os, re, sys, lz4.frame
sys.stdout.reconfigure(encoding='utf-8')

CACHE_DIR = 'data/cache'
JRA_VC = {'01','02','03','04','05','06','07','08','09','10'}

def check_html(fname, label):
    fpath = os.path.join(CACHE_DIR, fname)
    try:
        with open(fpath, 'rb') as f:
            raw = lz4.frame.decompress(f.read()).decode('utf-8', errors='replace')
        tbl = re.search(r'ResultTableWrap.*?<tbody>(.*?)</tbody>', raw, re.DOTALL)
        if not tbl:
            print(f"{label}: ResultTableWrap not found")
            return
        tbody = tbl.group(1)
        trs = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody, re.DOTALL)
        if not trs:
            return
        # Find a tr with a horse link
        for tr in trs:
            if '/horse/' in tr:
                tds_raw = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL)
                tds = [re.sub(r'<[^>]+>', '', td).strip() for td in tds_raw]
                print(f"{label} ({len(tds)} cols): fname={fname[:60]}")
                for i, td in enumerate(tds[:16]):
                    marker = ' <<<' if i in (12,13,14) else ''
                    print(f"  td[{i:2d}]: {td!r}{marker}")
                return
    except Exception as e:
        print(f"{label}: ERROR {e}")

# race.netkeiba.com NAR
for fname in sorted(os.listdir(CACHE_DIR)):
    if 'race.netkeiba.com' in fname and 'result.html' in fname and fname.endswith('.lz4'):
        m = re.search(r'race_id=(\d+)', fname)
        if m:
            rid = m.group(1)
            vc = rid[4:6] if len(rid) >= 6 else ''
            if vc not in JRA_VC:
                check_html(fname, f"race.netkeiba NAR vc={vc}")
                break

print()
# nar.netkeiba.com
for fname in sorted(os.listdir(CACHE_DIR)):
    if 'nar.netkeiba.com' in fname and 'result.html' in fname and fname.endswith('.lz4'):
        m = re.search(r'race_id=(\d+)', fname)
        if m:
            rid = m.group(1)
            check_html(fname, f"nar.netkeiba NAR rid={rid}")
            break
