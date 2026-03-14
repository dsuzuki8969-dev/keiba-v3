"""全233コースAPI検証スクリプト"""
import urllib.request, json, sys
from urllib.parse import quote
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

BASE = 'http://localhost:5051/api/db'

# JRA会場コード
JRA_VENUES = {'01','02','03','04','05','06','07','08','09','10'}

def fetch_json(url):
    resp = urllib.request.urlopen(url)
    return json.loads(resp.read().decode('utf-8'))

def is_jra(key):
    return key.split('_')[0] in JRA_VENUES

# 全キー取得
keys = fetch_json(f'{BASE}/course')['keys']
print(f'=== 全コース検証: {len(keys)}コース ===\n')

# 結果集計
results = {
    'course_description': {'pass': 0, 'fail': 0, 'na': 0, 'errors': []},
    'condition_diff': {'pass': 0, 'fail': 0, 'errors': []},
    'season_diff': {'pass': 0, 'fail': 0, 'errors': []},
    'running_style': {'pass': 0, 'fail': 0, 'errors': []},
    'gate_bias': {'pass': 0, 'fail': 0, 'errors': []},
    'class_avg': {'pass': 0, 'fail': 0, 'errors': []},
}

for i, key in enumerate(keys):
    url = f'{BASE}/course_stats?key={quote(key)}'
    try:
        d = fetch_json(url)
    except Exception as e:
        print(f'ERROR fetching {key}: {e}')
        for cat in results:
            results[cat]['fail'] += 1
            results[cat]['errors'].append(f'{key}: APIエラー {e}')
        continue

    jra = is_jra(key)
    venue = key.split('_')[0]
    race_count = d.get('race_count', 0)
    count = d.get('count', 0)  # 1-3着の頭数

    # --- 1. course_description ---
    desc = d.get('course_description')
    if jra:
        if desc and len(desc) > 5:
            results['course_description']['pass'] += 1
        else:
            results['course_description']['fail'] += 1
            results['course_description']['errors'].append(f'{key}: JRAなのにcourse_description={repr(desc)}')
    else:
        # NARはnullでOK
        results['course_description']['na'] += 1

    # --- 2. condition_diff ---
    cd = d.get('condition_diff')
    cd_ok = True
    cd_errs = []
    if cd:
        # 良のdiffが0またはnull
        if '良' in cd:
            ryou = cd['良']
            if ryou.get('diff') is not None and ryou['diff'] != 0:
                cd_ok = False
                cd_errs.append(f'良のdiff={ryou["diff"]}(0であるべき)')

        # 稍重/重/不良が正のdiff（ダートは逆もありうるが一応チェック）
        for cond in ['稍重', '重', '不良']:
            if cond in cd:
                diff = cd[cond].get('diff', 0)
                if '芝' in key and diff is not None and diff < -0.5:
                    cd_errs.append(f'{cond}のdiff={diff}(芝なのに良より速い)')

        # nの合計チェック（nは各conditionの1-3着頭数、合計≒race_count×3）
        total_n = sum(v.get('n', 0) for v in cd.values())
        expected = race_count * 3
        if expected > 0 and total_n > 0:
            ratio = total_n / expected
            if ratio < 0.5 or ratio > 1.5:
                cd_errs.append(f'n合計={total_n} vs race_count*3={expected} (比率={ratio:.2f})')
    else:
        cd_errs.append('condition_diffが存在しない')

    if cd_errs:
        cd_ok = False
    if cd_ok:
        results['condition_diff']['pass'] += 1
    else:
        results['condition_diff']['fail'] += 1
        for e in cd_errs:
            results['condition_diff']['errors'].append(f'{key}: {e}')

    # --- 3. season_diff ---
    sd = d.get('season_diff')
    sd_ok = True
    sd_errs = []
    if sd:
        for season, sv in sd.items():
            diff = sv.get('diff', 0)
            if abs(diff) > 5:
                sd_ok = False
                sd_errs.append(f'{season}のdiff={diff}(±5秒超)')
    else:
        sd_errs.append('season_diffが存在しない')

    if sd_errs and 'season_diffが存在しない' in sd_errs:
        sd_ok = False
    if sd_ok:
        results['season_diff']['pass'] += 1
    else:
        results['season_diff']['fail'] += 1
        for e in sd_errs:
            results['season_diff']['errors'].append(f'{key}: {e}')

    # --- 4. running_style ---
    rs = d.get('running_style')
    rs_ok = True
    rs_errs = []
    if rs:
        # 逃げのtotal ≒ race_count
        if '逃げ' in rs:
            nige_total = rs['逃げ'].get('total', 0)
            if race_count > 0:
                ratio = nige_total / race_count
                if ratio < 0.5 or ratio > 2.0:
                    rs_ok = False
                    rs_errs.append(f'逃げtotal={nige_total} vs race_count={race_count} (比率={ratio:.2f})')

        # win <= place2 <= place3 <= total
        for style, sv in rs.items():
            w = sv.get('win', 0)
            p2 = sv.get('place2', 0)
            p3 = sv.get('place3', 0)
            t = sv.get('total', 0)
            if not (w <= p2 <= p3 <= t):
                rs_ok = False
                rs_errs.append(f'{style}: win={w} place2={p2} place3={p3} total={t} (順序異常)')
    else:
        rs_ok = False
        rs_errs.append('running_styleが存在しない')

    if rs_ok:
        results['running_style']['pass'] += 1
    else:
        results['running_style']['fail'] += 1
        for e in rs_errs:
            results['running_style']['errors'].append(f'{key}: {e}')

    # --- 5. gate_bias ---
    gb = d.get('gate_bias')
    gb_ok = True
    gb_errs = []
    if gb:
        total_win = sum(v.get('win', 0) for v in gb.values())
        # 全枠のwin合計 ≒ race_count
        if race_count > 0:
            ratio = total_win / race_count
            if ratio < 0.5 or ratio > 2.0:
                gb_ok = False
                gb_errs.append(f'win合計={total_win} vs race_count={race_count} (比率={ratio:.2f})')

        for gate, gv in gb.items():
            w = gv.get('win', 0)
            p2 = gv.get('place2', 0)
            p3 = gv.get('place3', 0)
            runs = gv.get('runs', 0)
            # win <= place2 <= place3 <= runs
            if not (w <= p2 <= p3 <= runs):
                gb_ok = False
                gb_errs.append(f'枠{gate}: win={w} place2={p2} place3={p3} runs={runs} (順序異常)')
            # ROI 500%以下（runs>10のみ）
            roi = gv.get('roi', 0)
            if runs > 10 and roi > 500:
                gb_ok = False
                gb_errs.append(f'枠{gate}: ROI={roi}% (runs={runs})')
    else:
        gb_ok = False
        gb_errs.append('gate_biasが存在しない')

    if gb_ok:
        results['gate_bias']['pass'] += 1
    else:
        results['gate_bias']['fail'] += 1
        for e in gb_errs:
            results['gate_bias']['errors'].append(f'{key}: {e}')

    # --- 6. class_avg ---
    ca = d.get('class_avg')
    ca_ok = True
    ca_errs = []
    if ca:
        dist = int(key.split('_')[-1])
        for cls, cv in ca.items():
            avg = cv.get('avg_sec', 0)
            # 距離に対して妥当か（概算: 芝 1000m≈57s, 2400m≈145s / ダート 1000m≈60s, 2400m≈155s）
            if avg > 0:
                # 大雑把な範囲チェック
                expected_min = dist * 0.04  # 超高速
                expected_max = dist * 0.09  # 超低速
                if avg < expected_min or avg > expected_max:
                    ca_ok = False
                    ca_errs.append(f'{cls}: avg_sec={avg} (距離{dist}mに対して異常)')

            # JRAで「重賞」混入チェック
            if jra and cls == '重賞':
                ca_ok = False
                ca_errs.append(f'JRAに「重賞」クラスが混入')
    else:
        ca_ok = False
        ca_errs.append('class_avgが存在しない')

    if ca_ok:
        results['class_avg']['pass'] += 1
    else:
        results['class_avg']['fail'] += 1
        for e in ca_errs:
            results['class_avg']['errors'].append(f'{key}: {e}')

    if (i + 1) % 50 == 0:
        print(f'  {i+1}/{len(keys)} 処理済み...')

# === 結果出力 ===
print('\n' + '='*60)
print('検証結果サマリ')
print('='*60)

all_pass = True
for cat, r in results.items():
    total = r['pass'] + r['fail'] + r.get('na', 0)
    na_str = f" (N/A: {r.get('na', 0)})" if r.get('na', 0) else ""
    status = '✓ ALL PASS' if r['fail'] == 0 else f'✗ {r["fail"]}件不合格'
    print(f'\n【{cat}】 合格={r["pass"]} 不合格={r["fail"]}{na_str}  → {status}')
    if r['fail'] > 0:
        all_pass = False
        # エラー詳細（最大20件）
        for e in r['errors'][:20]:
            print(f'  NG: {e}')
        if len(r['errors']) > 20:
            print(f'  ... 他 {len(r["errors"])-20}件')

print('\n' + '='*60)
if all_pass:
    print('全項目合格!')
else:
    print('一部不合格あり。上記エラーを確認してください。')
print('='*60)
