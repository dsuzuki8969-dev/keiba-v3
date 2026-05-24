# -*- coding: utf-8 -*-
"""R-1: 券種別 × 自信度別 ROI 集計 (全期間 2024-2026)

F-2 (2026-05-25): payout 形式統一を src/utils/payout_normalizer 共通モジュール化。
"""
import json
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

PRED_DIR = PROJECT_ROOT / "data" / "predictions"
DB_PATH = PROJECT_ROOT / "data" / "keiba.db"

NAR_VENUES = {'大井','船橋','川崎','浦和','園田','姫路','名古屋','笠松','金沢','門別','盛岡','水沢','高知','佐賀'}

# F-2: 共通モジュール経由で payout 形式統一
from src.utils.payout_normalizer import normalize_payouts, combo_match, TICKET_TYPE_KEY_MAP

# ticket type の日本語 → romaji マップ (R-1 集計用・key map と内容同一)
TICKET_TYPE_MAP = TICKET_TYPE_KEY_MAP


def main():
    conn = sqlite3.connect(DB_PATH)
    print('結果データロード中...', flush=True)
    results_cache = {}
    for rid, oj, pj in conn.execute('SELECT race_id, order_json, payouts_json FROM race_results'):
        try:
            order = json.loads(oj)
            payouts = json.loads(pj)
            finish_map = {}
            for o in order:
                f = o.get('finish')
                hno = o.get('horse_no')
                if f and hno:
                    finish_map[hno] = f
            results_cache[rid] = {'finish': finish_map, 'payouts': payouts}
        except Exception:
            pass
    print(f'  -> {len(results_cache):,} R', flush=True)

    stats = defaultdict(lambda: {'bet': 0, 'pay': 0, 'tickets': 0, 'hits': 0})

    print('pred.json 集計中...', flush=True)
    pred_files = sorted(PRED_DIR.glob('*_pred.json'))
    processed = 0
    for pf in pred_files:
        date_str = pf.name[:8]
        if not date_str.startswith('20') or date_str[:4] not in ('2024', '2025', '2026'):
            continue
        try:
            pred = json.loads(pf.read_text(encoding='utf-8'))
        except Exception:
            continue
        for race in pred.get('races', []):
            rid = race.get('race_id')
            if not rid or rid not in results_cache:
                continue
            res = results_cache[rid]
            if not res['finish']:
                continue
            venue = race.get('venue', '')
            jra_nar = 'NAR' if venue in NAR_VENUES else 'JRA'

            confidence = 'B'
            tbm = race.get('tickets_by_mode', {})
            if isinstance(tbm, dict):
                meta = tbm.get('_meta', {})
                if isinstance(meta, dict) and meta.get('confidence'):
                    confidence = meta['confidence']
            if not confidence or confidence == 'B':
                confidence = race.get('overall_confidence', 'B') or 'B'

            tickets = race.get('tickets', [])
            if isinstance(tbm, dict) and tbm.get('fixed'):
                tickets = tbm['fixed']
            if not tickets:
                continue

            payouts_norm = normalize_payouts(res['payouts'])

            for tk in tickets:
                ttype = tk.get('type', '')
                ttype_norm = TICKET_TYPE_MAP.get(ttype, ttype)
                stake = tk.get('stake', 100) or 100
                # 単勝/複勝は combo がなく horse_no、それ以外は combo
                if ttype in ('単勝', '複勝') and 'horse_no' in tk:
                    combo = [tk['horse_no']]
                else:
                    combo = tk.get('combo', [])
                key = (jra_nar, ttype, confidence)
                stats[key]['bet'] += stake
                stats[key]['tickets'] += 1
                pay_list = payouts_norm.get(ttype_norm, [])
                for p in pay_list:
                    p_combo = p.get('combo', '')
                    p_amt = p.get('payout', 0) or 0
                    if combo_match(combo, p_combo, ttype):
                        stats[key]['pay'] += p_amt * (stake / 100)
                        stats[key]['hits'] += 1
                        break
        processed += 1

    print(f'  処理 {processed:,} ファイル', flush=True)
    print()

    ticket_types = ['単勝','複勝','馬連','ワイド','三連複','三連単','馬単']
    conf_order = ['SS','S','A','B','C','D','E']

    print('=' * 100)
    print('  R-1: 券種別 x 自信度別 ROI (全期間 2024-2026)')
    print('=' * 100)

    for jra_nar in ('JRA','NAR'):
        print(f'\n[{jra_nar}]')
        print(f'{"券種":>6s} | {"信度":>4s} | {"点数":>7s} | {"的中":>5s} | {"的中率":>6s} | {"投資":>11s} | {"回収":>11s} | {"ROI":>7s}')
        print('-' * 90)
        grand_bet = 0
        grand_pay = 0
        for tt in ticket_types:
            sub_bet = 0
            sub_pay = 0
            sub_tickets = 0
            sub_hits = 0
            for cf in conf_order:
                key = (jra_nar, tt, cf)
                if key not in stats:
                    continue
                st = stats[key]
                if st['bet'] == 0:
                    continue
                hr = st['hits'] / st['tickets'] * 100 if st['tickets'] else 0
                roi = st['pay'] / st['bet'] * 100 if st['bet'] else 0
                print(f'{tt:>6s} | {cf:>4s} | {st["tickets"]:>7,d} | {st["hits"]:>5,d} | {hr:>5.1f}% | {st["bet"]:>11,.0f} | {st["pay"]:>11,.0f} | {roi:>6.1f}%')
                sub_bet += st['bet']
                sub_pay += st['pay']
                sub_tickets += st['tickets']
                sub_hits += st['hits']
            if sub_bet > 0:
                roi = sub_pay / sub_bet * 100
                hr = sub_hits / sub_tickets * 100 if sub_tickets else 0
                print(f'{tt:>6s} | {"小計":>4s} | {sub_tickets:>7,d} | {sub_hits:>5,d} | {hr:>5.1f}% | {sub_bet:>11,.0f} | {sub_pay:>11,.0f} | {roi:>6.1f}%')
                print()
                grand_bet += sub_bet
                grand_pay += sub_pay
        if grand_bet > 0:
            print(f'{"合計":>6s} | {"-":>4s} | {"-":>7s} | {"-":>5s} | {"-":>6s} | {grand_bet:>11,.0f} | {grand_pay:>11,.0f} | {grand_pay/grand_bet*100:>6.1f}%')

    conn.close()


if __name__ == '__main__':
    main()
