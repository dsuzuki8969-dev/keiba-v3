import os, re, sys, lz4.frame
sys.stdout.reconfigure(encoding='utf-8')

CACHE_DIR = 'data/cache'
# JRA result.html キャッシュを1件取得
for fname in os.listdir(CACHE_DIR):
    if 'result.html' in fname and fname.endswith('.lz4'):
        # race_id が JRA (venue_code 01-10) のもの
        m = re.search(r'race_id=(\d+)', fname)
        if m:
            rid = m.group(1)
            vc = rid[4:6] if len(rid) >= 6 else ''
            if vc in ('01','02','03','04','05','06','07','08','09','10'):
                fpath = os.path.join(CACHE_DIR, fname)
                try:
                    with open(fpath, 'rb') as f:
                        raw = lz4.frame.decompress(f.read()).decode('utf-8', errors='replace')
                    # 調教師セルを探す
                    tbl = re.search(r'ResultTableWrap.*?<tbody>(.*?)</tbody>', raw, re.DOTALL)
                    if tbl:
                        tbody = tbl.group(1)
                        trs = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody, re.DOTALL)
                        if trs:
                            tr = trs[0]
                            tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL)
                            print(f'race_id={rid}, {len(tds)} tds')
                            for i, td in enumerate(tds[:15]):
                                clean = re.sub(r'<[^>]+>', '', td).strip()
                                print(f'  td[{i:2d}]: {clean!r}')
                            # td[12] を詳しく見る
                            if len(tds) > 12:
                                print(f'\nRAW td[12]: {tds[12][:300]!r}')
                    break
                except Exception as e:
                    print(f'error: {e}')
                    continue
