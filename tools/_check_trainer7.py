import os, re, sys, lz4.frame
sys.stdout.reconfigure(encoding='utf-8')

CACHE_DIR = 'data/cache'
JRA_VC = {'01','02','03','04','05','06','07','08','09','10'}

def check_result_html(fname, label):
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
        tr = trs[0]
        tds_raw = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL)
        tds = [re.sub(r'<[^>]+>', '', td).strip() for td in tds_raw]
        print(f"{label} ({len(tds)} cols):")
        for i, td in enumerate(tds[:16]):
            print(f"  td[{i:2d}]: {td!r}")
    except Exception as e:
        print(f"{label}: ERROR {e}")

# JRA result
for fname in os.listdir(CACHE_DIR):
    if 'result.html' in fname and fname.endswith('.lz4'):
        m = re.search(r'race_id=(\d+)', fname)
        if m:
            rid = m.group(1)
            vc = rid[4:6] if len(rid) >= 6 else ''
            if vc in JRA_VC:
                check_result_html(fname, f"JRA race_id={rid}")
                break

print()

# NAR result
for fname in os.listdir(CACHE_DIR):
    if 'result.html' in fname and fname.endswith('.lz4'):
        m = re.search(r'race_id=(\d+)', fname)
        if m:
            rid = m.group(1)
            vc = rid[4:6] if len(rid) >= 6 else ''
            if vc not in JRA_VC and vc.isdigit():
                check_result_html(fname, f"NAR race_id={rid} vc={vc}")
                break
