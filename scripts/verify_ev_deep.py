"""深掘り検証スクリプト: ◉降格馬プロファイル、☆/×精度、2026年低下分析"""
import sqlite3, json, sys, os
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "keiba.db")

def load():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    cur.execute('SELECT race_id, horse_no, finish_pos, tansho_odds FROM race_log WHERE finish_pos IS NOT NULL AND finish_pos > 0')
    rm = {}
    for row in cur:
        rm[(row['race_id'], row['horse_no'])] = {'f': row['finish_pos'], 'to': row['tansho_odds'] or 0}

    cur.execute('SELECT date, race_id, confidence, field_count, horses_json FROM predictions ORDER BY date, race_id')
    all_h = []
    races = []
    for row in cur:
        rid = row['race_id']
        try: horses = json.loads(row['horses_json']) if row['horses_json'] else []
        except: continue
        is_jra = rid[4:6] in ['01','02','03','04','05','06','07','08','09','10']
        sh = sorted(horses, key=lambda h: h.get('composite') or 0, reverse=True)
        gap = (sh[0].get('composite',0) - sh[1].get('composite',0)) if len(sh)>=2 else 99
        rh = []
        for h in horses:
            hno = h.get('horse_no')
            if hno is None: continue
            key = (rid, hno)
            if key not in rm: continue
            rl = rm[key]
            wp = h.get('win_prob') or 0
            odds = h.get('odds')
            ev = wp * odds if odds and odds > 0 and wp > 0 else None
            eo = h.get('effective_odds') or odds
            tp = int(rl['to']*100) if rl['f']==1 and rl['to']>0 else 0
            rec = {'date':row['date'],'rid':rid,'is_jra':is_jra,'conf':row['confidence'] or '',
                   'mark':h.get('mark',''),'composite':h.get('composite') or 0,
                   'wp':wp,'p3':h.get('place3_prob') or 0,'odds':odds,'eo':eo,'ev':ev,
                   'finish':rl['f'],'tp':tp,'pop':h.get('popularity'),'gap':gap}
            all_h.append(rec)
            rh.append(rec)
        if rh:
            races.append({'rid':rid,'date':row['date'],'conf':row['confidence'],'is_jra':is_jra,'horses':rh})
    db.close()
    return all_h, races

def s(recs):
    n=len(recs)
    if n==0: return None
    w=sum(1 for r in recs if r['finish']==1)
    p2=sum(1 for r in recs if r['finish']<=2)
    p3=sum(1 for r in recs if r['finish']<=3)
    t=sum(r['tp'] for r in recs)
    return {'n':n,'wr':w/n*100,'p2r':p2/n*100,'p3r':p3/n*100,'roi':t/(n*100)*100}

def ev_of(h):
    eo = h.get('eo') or h.get('odds')
    return (h['wp'] or 0) * eo if eo and eo > 0 else 1.0

all_h, races = load()
tek = [h for h in all_h if h['mark']=='◉']

# ============================================================
print('='*90)
print(' K. ◉降格馬(EV<0.80)の詳細プロファイル')
print('='*90)
demoted = [h for h in tek if ev_of(h) < 0.80]
print(f'  降格対象: {len(demoted):,}件')

print(f'\n  降格馬のオッズ分布:')
for lo,hi,label in [(1,1.5,'1-1.5倍'),(1.5,2,'1.5-2倍'),(2,3,'2-3倍'),(3,5,'3-5倍'),(5,10,'5-10倍')]:
    sub = [h for h in demoted if h.get('odds') and lo <= h['odds'] < hi]
    st = s(sub)
    if st:
        print(f'    {label}: {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 回{st["roi"]:>6.1f}%')

print(f'\n  降格馬の人気分布:')
for pop in range(1, 6):
    sub = [h for h in demoted if h.get('pop')==pop]
    st = s(sub)
    if st:
        print(f'    {pop}番人気: {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 回{st["roi"]:>6.1f}%')

print(f'\n  降格馬のwin_prob分布:')
for lo,hi in [(0.3,0.4),(0.4,0.5),(0.5,0.6),(0.6,0.7),(0.7,1.0)]:
    sub = [h for h in demoted if lo<=h['wp']<hi]
    st = s(sub)
    if st and st['n']>=10:
        print(f'    wp {lo*100:.0f}-{hi*100:.0f}%: {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# ============================================================
print('\n' + '='*90)
print(' L. ◉残留馬(EV>=0.80)のオッズ分布')
print('='*90)
remaining = [h for h in tek if ev_of(h) >= 0.80]
for lo,hi,label in [(1,2,'1-2倍'),(2,3,'2-3倍'),(3,5,'3-5倍'),(5,10,'5-10倍'),(10,20,'10-20倍'),(20,99,'20倍+')]:
    sub = [h for h in remaining if h.get('odds') and lo <= h['odds'] < hi]
    st = s(sub)
    if st:
        print(f'    {label}: {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# ============================================================
print('\n' + '='*90)
print(' M. レース単位: 自信度別 ◎◉複勝的中率')
print('='*90)
for conf_label, conf_list in [('SS',['SS']),('S',['S']),('A',['A']),('B',['B']),('C',['C']),('D+E',['D','E'])]:
    races_sub = [r for r in races if r['conf'] in conf_list]
    if not races_sub: continue
    tek_hit = sum(1 for r in races_sub if any(h['mark']=='◉' and h['finish']<=3 for h in r['horses']))
    hon_hit = sum(1 for r in races_sub if any(h['mark']=='◎' and h['finish']<=3 for h in r['horses']))
    total = len(races_sub)
    print(f'  {conf_label:<5}: {total:>5,}R ◉複的中{tek_hit/max(total,1)*100:>5.1f}% ◎複的中{hon_hit/max(total,1)*100:>5.1f}%')

# ============================================================
print('\n' + '='*90)
print(' N. ☆(穴馬)のEVフィルタ効果')
print('='*90)
ana = [h for h in all_h if h['mark']=='☆']
st_all = s(ana)
if st_all:
    print(f'  全体: {st_all["n"]:,}件 勝{st_all["wr"]:.1f}% 複{st_all["p3r"]:.1f}% 回{st_all["roi"]:.1f}%')
for thr in [0.8, 1.0, 1.2, 1.5, 2.0]:
    sub = [h for h in ana if h.get('ev') is not None and h['ev'] >= thr]
    st = s(sub)
    if st and st['n']>=30:
        print(f'  EV>={thr:.1f}: {st["n"]:>6,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# ============================================================
print('\n' + '='*90)
print(' O. ×(危険馬)の精度検証')
print('='*90)
kiken = [h for h in all_h if h['mark']=='×']
st_all = s(kiken)
if st_all:
    print(f'  全体: {st_all["n"]:,}件 勝{st_all["wr"]:.1f}% 複{st_all["p3r"]:.1f}% 回{st_all["roi"]:.1f}%')
for pop in range(1, 8):
    sub = [h for h in kiken if h.get('pop')==pop]
    st = s(sub)
    if st and st['n']>=30:
        print(f'  {pop}番人気×: {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')
for scope, fn in [('JRA', lambda h: h['is_jra']), ('NAR', lambda h: not h['is_jra'])]:
    sub = [h for h in kiken if fn(h)]
    st = s(sub)
    if st:
        print(f'  {scope}: {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# ============================================================
print('\n' + '='*90)
print(' P. 2026年◉回収率低下の原因分析')
print('='*90)
for period, lo_d, hi_d in [('2025H2','2025-07','2026-01'),('2026H1','2026-01','2026-07')]:
    sub = [h for h in tek if lo_d <= h['date'] < hi_d]
    st = s(sub)
    if st:
        print(f'  {period} ◉: {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')

tek_2026 = [h for h in tek if h['date'] >= '2026-01-01']
print(f'\n  2026H1 ◉オッズ分布:')
for lo,hi,label in [(1,1.5,'1-1.5倍'),(1.5,2,'1.5-2倍'),(2,3,'2-3倍'),(3,5,'3-5倍'),(5,10,'5-10倍'),(10,99,'10倍+')]:
    sub = [h for h in tek_2026 if h.get('odds') and lo <= h['odds'] < hi]
    st = s(sub)
    if st and st['n']>=10:
        print(f'    {label}: {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 回{st["roi"]:>6.1f}%')

print(f'\n  2026H1 ◉にEVフィルタ:')
for thr in [0.70, 0.80, 0.90, 1.00]:
    sub = [h for h in tek_2026 if ev_of(h) >= thr]
    st = s(sub)
    if st:
        marker = ' <-- current' if thr==0.80 else ''
        print(f'    EV>={thr:.2f}: {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 回{st["roi"]:>6.1f}%{marker}')

# ============================================================
print('\n' + '='*90)
print(' Q. ◉判定条件の代替案比較')
print('='*90)
top_horses = [h for h in all_h if h['mark'] in ('◉','◎')]

# 案1: 現行(gap>=5, wp>=0.30, p3>=0.65, ev>=0.80) - 実際のformatter条件
print('  案1 現行条件+EVフィルタ (gap>=5, wp>=0.30, p3>=0.65, ev>=0.80):')
a1 = [h for h in top_horses if h['gap']>=5 and h['wp']>=0.30 and h['p3']>=0.65 and ev_of(h)>=0.80]
st = s(a1)
if st: print(f'    {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# 案2: gap緩和+EV厳格化 (gap>=3, ev>=1.00)
print('  案2 gap緩和+EV厳格 (gap>=3, wp>=0.20, ev>=1.00):')
a2 = [h for h in top_horses if h['gap']>=3 and h['wp']>=0.20 and ev_of(h)>=1.00]
st = s(a2)
if st: print(f'    {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# 案3: EV-only (ev>=1.00, wp>=0.15)
print('  案3 EV中心 (ev>=1.00, wp>=0.15):')
a3 = [h for h in top_horses if h['wp']>=0.15 and ev_of(h)>=1.00]
st = s(a3)
if st: print(f'    {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# 案4: gap重視+EV軽め (gap>=7, ev>=0.70)
print('  案4 gap重視 (gap>=7, wp>=0.30, p3>=0.65, ev>=0.70):')
a4 = [h for h in top_horses if h['gap']>=7 and h['wp']>=0.30 and h['p3']>=0.65 and ev_of(h)>=0.70]
st = s(a4)
if st: print(f'    {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# 案5: 件数重視 (gap>=4, ev>=0.80)
print('  案5 件数重視 (gap>=4, wp>=0.25, ev>=0.80):')
a5 = [h for h in top_horses if h['gap']>=4 and h['wp']>=0.25 and ev_of(h)>=0.80]
st = s(a5)
if st: print(f'    {st["n"]:>5,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# ============================================================
print('\n' + '='*90)
print(' R. 月別推移: ◉(EV>=0.80適用後)の成績トレンド')
print('='*90)
months = defaultdict(list)
for h in tek:
    if ev_of(h) >= 0.80:
        months[h['date'][:7]].append(h)
for m in sorted(months.keys()):
    if m >= '2025-01':
        st = s(months[m])
        if st:
            print(f'  {m}: {st["n"]:>4,}件 勝{st["wr"]:>5.1f}% 複{st["p3r"]:>5.1f}% 回{st["roi"]:>6.1f}%')

# ============================================================
print('\n' + '='*90)
print(' S. 全印まとめ: 現行 vs 最適化案の損益シミュレーション')
print('='*90)
print('  100円均一単勝で全レース賭けた場合の年間損益')
for mark in ['◉','◎','○','▲','△','★','☆','×']:
    sub = [h for h in all_h if h['mark']==mark]
    st = s(sub)
    if st:
        investment = st['n'] * 100
        payout = sum(h['tp'] for h in sub)
        profit = payout - investment
        print(f'  {mark}: 投資{investment/10000:>7,.0f}万 払戻{payout/10000:>7,.0f}万 損益{profit/10000:>+7,.0f}万 ({st["roi"]:.1f}%)')

# ◉EVフィルタ後
print()
new_tek = [h for h in tek if ev_of(h) >= 0.80]
st = s(new_tek)
if st:
    inv = st['n']*100
    pay = sum(h['tp'] for h in new_tek)
    print(f'  新◉(EV>=0.80): 投資{inv/10000:>7,.0f}万 払戻{pay/10000:>7,.0f}万 損益{(pay-inv)/10000:>+7,.0f}万 ({st["roi"]:.1f}%)')
