#!/usr/bin/env python3
"""印別三連率 + 自信度別成績 集計（JRA/NAR分離）"""
import json, glob, os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

RESULT_DIR = 'data/results'
PRED_DIR = 'data/predictions'
JRA_VENUES = {'東京','中山','阪神','京都','中京','小倉','新潟','福島','函館','札幌'}

results = {}
payouts = {}
for fp in sorted(glob.glob(os.path.join(RESULT_DIR, '*_results.json'))):
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for rid, data in d.items():
            results[rid] = {e['horse_no']: e.get('finish', 99) for e in data.get('order', []) if 'horse_no' in e}
            pay = data.get('payouts', {})
            if pay: payouts[rid] = pay
    except: pass

def get_win_pay(pay, hno):
    w = pay.get('単勝', None)
    if w is None: return 0
    hs = str(hno)
    if isinstance(w, dict):
        return (w.get('payout', 0) or 0) if str(w.get('combo', '')) == hs else 0
    if isinstance(w, list):
        for x in w:
            if isinstance(x, dict) and str(x.get('combo', '')) == hs:
                return x.get('payout', 0) or 0
    return 0

def get_place_pay(pay, hno):
    p = pay.get('複勝', None)
    if p is None: return 0
    hs = str(hno)
    if isinstance(p, dict):
        return (p.get('payout', 0) or 0) if str(p.get('combo', '')) == hs else 0
    if isinstance(p, list):
        for x in p:
            if isinstance(x, dict) and str(x.get('combo', '')) == hs:
                return x.get('payout', 0) or 0
    return 0

# 集計用辞書
mark_stats = {}   # (cat, mark) -> {n, win, p2, p3, win_ret, place_ret}
conf_stats = {}   # (cat, confidence) -> {n, win, p3, win_ret, place_ret}

for fp in sorted(glob.glob(os.path.join(PRED_DIR, '*_pred.json'))):
    bn = os.path.basename(fp)
    if '_prev' in bn or '_backup' in bn: continue
    try: d = json.load(open(fp, 'r', encoding='utf-8'))
    except: continue
    for r in d.get('races', []):
        rid = r.get('race_id', '')
        if rid not in results: continue
        fm = results[rid]
        pay = payouts.get(rid, {})
        venue = r.get('venue', '')
        cat = 'JRA' if venue in JRA_VENUES else 'NAR'
        conf = r.get('confidence', '')

        # 自信度集計（◎/◉本命の成績）
        honmei = None
        for h in r.get('horses', []):
            mk = h.get('mark', '')
            if mk in ['\u25c9', '\u25ce']:  # ◉ or ◎
                honmei = h
                break
        if honmei and conf:
            hno = honmei.get('horse_no')
            fp2 = fm.get(hno, 99)
            if 0 < fp2 < 90:
                key = (cat, conf)
                if key not in conf_stats:
                    conf_stats[key] = {'n': 0, 'win': 0, 'p3': 0, 'win_ret': 0, 'place_ret': 0}
                conf_stats[key]['n'] += 1
                if fp2 == 1:
                    conf_stats[key]['win'] += 1
                    conf_stats[key]['win_ret'] += get_win_pay(pay, hno)
                if fp2 <= 3:
                    conf_stats[key]['p3'] += 1
                    conf_stats[key]['place_ret'] += get_place_pay(pay, hno)

        # 印別集計
        for h in r.get('horses', []):
            mk = h.get('mark', '')
            if not mk or mk == '\uff0d': continue
            hno = h.get('horse_no')
            fp2 = fm.get(hno, 99)
            if fp2 <= 0 or fp2 >= 90: continue
            key = (cat, mk)
            if key not in mark_stats:
                mark_stats[key] = {'n': 0, 'win': 0, 'p2': 0, 'p3': 0, 'win_ret': 0, 'place_ret': 0}
            mark_stats[key]['n'] += 1
            if fp2 == 1:
                mark_stats[key]['win'] += 1
                mark_stats[key]['win_ret'] += get_win_pay(pay, hno)
            if fp2 <= 2: mark_stats[key]['p2'] += 1
            if fp2 <= 3:
                mark_stats[key]['p3'] += 1
                mark_stats[key]['place_ret'] += get_place_pay(pay, hno)

# ======== 出力 ========
mark_order = ['\u25c9', '\u25ce', '\u25cb', '\u25b2', '\u25b3', '\u2605', '\u2606', '\u00d7']
mark_names = {'\u25c9':'◉', '\u25ce':'◎', '\u25cb':'○', '\u25b2':'▲', '\u25b3':'△', '\u2605':'★', '\u2606':'☆', '\u00d7':'×'}
# KPI目標
kpi_marks = {
    '\u25c9': (65.0, 75.0, 90.0),
    '\u25ce': (35.0, 45.0, 60.0),
    '\u25cb': (20.0, 30.0, 45.0),
    '\u25b2': (15.0, 25.0, 40.0),
    '\u25b3': (10.0, 20.0, 35.0),
    '\u2605': (5.0, 15.0, 25.0),
}

for cat in ['JRA', 'NAR']:
    print(f'\n{"="*74}')
    print(f'  {cat} 印別三連率')
    print(f'{"="*74}')
    print(f'  {"印":>2s} {"頭数":>7s} {"勝率":>7s} {"連対率":>7s} {"複勝率":>7s} {"単回収":>7s} {"複回収":>7s}  目標')
    print(f'  {"-"*2} {"-"*7} {"-"*7} {"-"*7} {"-"*7} {"-"*7} {"-"*7}  {"-"*14}')
    for mk in mark_order:
        s = mark_stats.get((cat, mk))
        if not s or s['n'] < 10: continue
        wr = s['win']/s['n']*100
        p2r = s['p2']/s['n']*100
        p3r = s['p3']/s['n']*100
        win_roi = s['win_ret']/s['n']
        place_roi = s['place_ret']/s['n']
        kpi = kpi_marks.get(mk)
        if kpi:
            w_ok = '✅' if wr >= kpi[0] else '❌'
            p2_ok = '✅' if p2r >= kpi[1] else '❌'
            p3_ok = '✅' if p3r >= kpi[2] else '❌'
            target = f'{w_ok}{kpi[0]:.0f}/{p2_ok}{kpi[1]:.0f}/{p3_ok}{kpi[2]:.0f}'
        else:
            target = ''
        print(f'  {mark_names.get(mk,mk):>2s} {s["n"]:>7d} {wr:>6.1f}% {p2r:>6.1f}% {p3r:>6.1f}% {win_roi:>6.1f}% {place_roi:>6.1f}%  {target}')

# 自信度
kpi_conf = {
    'SS': (60.0, 150.0), 'S': (50.0, 120.0), 'A': (40.0, 100.0),
    'B': (25.0, 90.0), 'C': (25.0, 80.0), 'D': (20.0, 70.0), 'E': (10.0, 60.0),
}
for cat in ['JRA', 'NAR']:
    print(f'\n{"="*74}')
    print(f'  {cat} 自信度別（本命◎/◉の成績）')
    print(f'{"="*74}')
    print(f'  {"自信度":>6s} {"R数":>6s} {"単的中":>7s} {"単回収":>7s} {"複的中":>7s} {"複回収":>7s}  目標')
    print(f'  {"-"*6} {"-"*6} {"-"*7} {"-"*7} {"-"*7} {"-"*7}  {"-"*14}')
    for c in ['SS', 'S', 'A', 'B', 'C', 'D', 'E']:
        s = conf_stats.get((cat, c))
        if not s or s['n'] < 5: continue
        wr = s['win']/s['n']*100
        p3r = s['p3']/s['n']*100
        win_roi = s['win_ret']/s['n']
        place_roi = s['place_ret']/s['n']
        kpi = kpi_conf.get(c, (0, 0))
        w_ok = '✅' if wr >= kpi[0] else '❌'
        r_ok = '✅' if win_roi >= kpi[1] else '❌'
        target = f'{w_ok}{kpi[0]:.0f}%/{r_ok}{kpi[1]:.0f}%'
        print(f'  {c:>6s} {s["n"]:>6d} {wr:>6.1f}% {win_roi:>6.1f}% {p3r:>6.1f}% {place_roi:>6.1f}%  {target}')
