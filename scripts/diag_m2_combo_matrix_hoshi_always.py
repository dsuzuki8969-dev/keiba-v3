"""H-0 ☆ 常時仮説検証: composite TOP6 を ☆ として全 race に付与 + 105 セルマトリクス再集計

マスター指示 (2026-05-28): 「☆ は常時 1 頭」を仮設準拠する形で post-hoc 検証。
既存 pred.json の ☆ horse_no を無視し、composite 降順 TOP6 を ☆ として扱う。

データソース: predictions_pre_markint_20260527.tar.gz (リーク排除済 WF pred.json)

差分: diag_m2_combo_matrix.py との違いは「☆ horse_no 取得を composite 順位ベースに置換」のみ。
"""

import io
import json
import os
import sys
import tarfile
from itertools import combinations

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.stdout.reconfigure(encoding="utf-8")

from data.masters.venue_master import BANEI_VENUE_CODES

TAR_PATH = os.path.join(ROOT, "data", "_archive", "predictions_pre_markint_20260527.tar.gz")
RESULTS_DIR = os.path.join(ROOT, "data", "results")
CONF_LEVELS = ("SS", "S", "A", "B", "C", "D", "E")
PARTNERS = [("renka", "▲"), ("wide1", "△"), ("wide2", "★"), ("wide3_override", "☆")]


def _lookup_trio_payout(payouts, nos):
    bucket = payouts.get("三連複")
    if bucket is None: return 0
    def _match(item):
        if not isinstance(item, dict): return 0
        combo = item.get("combo", "")
        try:
            cs = set(int(x) for x in combo.split("-")) if combo else set()
        except (ValueError, AttributeError):
            return 0
        if cs == nos:
            p = item.get("payout", 0)
            return int(p) if isinstance(p, (int, float)) else 0
        return 0
    if isinstance(bucket, dict): return _match(bucket)
    if isinstance(bucket, list):
        for it in bucket:
            v = _match(it)
            if v: return v
    return 0


def _empty():
    return {"played": 0, "hits": 0, "stake": 0, "payback": 0}


def _derive(d):
    p, s, pb = d["played"], d["stake"], d["payback"]
    return {
        "played": p, "hits": d["hits"],
        "hit_rate_pct": d["hits"] / p * 100 if p else 0.0,
        "roi_pct": pb / s * 100 if s else 0.0,
        "stake": s, "payback": pb, "balance": pb - s,
    }


def main():
    print("=" * 110)
    print("H-0 ☆ 常時 (composite TOP6) 仮説検証 — 15 戦略 × 7 confidence マトリクス")
    print("=" * 110)
    print("差分: ☆ = composite 順位 6 番目 (既存 pred.json ☆ horse_no を override)")
    print()

    all_combos = []
    for r in range(1, 5):
        for c in combinations(PARTNERS, r):
            label = "".join(m for _, m in c)
            keys = [k for k, _ in c]
            all_combos.append((label, keys, len(c)))

    print(f"[1/2] WF pred.json ロード中...")
    preds = {}
    with tarfile.open(TAR_PATH, "r:gz") as tar:
        for m in tar.getmembers():
            if not m.name.endswith("_pred.json"): continue
            date_key = os.path.basename(m.name)[:8]
            if not date_key.isdigit() or len(date_key) != 8: continue
            f = tar.extractfile(m)
            if f: preds[date_key] = json.load(io.TextIOWrapper(f, encoding="utf-8"))
    print(f"  {len(preds)} 日分ロード完了")

    matrix = {label: {lv: _empty() for lv in CONF_LEVELS + ("ALL",)} for label, _, _ in all_combos}

    print("\n[2/2] 集計中...")
    processed = 0
    hoshi_override_count = 0  # ☆ override が新規付与した race 数

    for date_key in sorted(preds.keys()):
        rpath = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
        if not os.path.exists(rpath): continue
        with open(rpath, encoding="utf-8") as f:
            results = json.load(f)

        for race in preds[date_key].get("races", []):
            race_id = str(race.get("race_id", ""))
            if len(race_id) >= 6 and race_id[4:6] in BANEI_VENUE_CODES: continue
            rdata = results.get(race_id)
            if rdata is None or not rdata.get("payouts") or rdata["payouts"].get("三連複") is None: continue
            order = rdata.get("order", [])
            if not order: continue
            finish_map = {r.get("horse_no"): r.get("finish")
                          for r in order if r.get("horse_no") is not None and r.get("finish") is not None}
            confidence = race.get("confidence", "") or ""
            if confidence not in CONF_LEVELS: continue

            horses = race.get("horses", [])
            # 印 取得 (☆ は既存 mark で取らず override する)
            mark_map = {}
            for h in horses:
                m = h.get("mark", "")
                hn = h.get("horse_no")
                if m in ("◎", "◉") and "honmei" not in mark_map: mark_map["honmei"] = hn
                elif m in ("○", "〇") and "taikou" not in mark_map: mark_map["taikou"] = hn
                elif m == "▲" and "renka" not in mark_map: mark_map["renka"] = hn
                elif m == "△" and "wide1" not in mark_map: mark_map["wide1"] = hn
                elif m == "★" and "wide2" not in mark_map: mark_map["wide2"] = hn

            # ☆ override: composite 順位 6 番目を ☆ として扱う
            # (既存 ☆ horse_no は無視 = マスター指示「☆ 常時」遵守)
            sorted_horses = sorted(
                [h for h in horses if h.get("composite") is not None],
                key=lambda h: h.get("composite") or 0,
                reverse=True,
            )
            if len(sorted_horses) >= 6:
                # ◎○▲△★ で既に取られた horse_no を除外
                taken = {mark_map.get(k) for k in ("honmei", "taikou", "renka", "wide1", "wide2") if mark_map.get(k) is not None}
                hoshi_no = None
                for h in sorted_horses:
                    hn = h.get("horse_no")
                    if hn not in taken:
                        hoshi_no = hn
                        break
                if hoshi_no is not None:
                    mark_map["wide3_override"] = hoshi_no
                    hoshi_override_count += 1

            honmei = mark_map.get("honmei")
            taikou = mark_map.get("taikou")
            if not (honmei and taikou and honmei != taikou): continue

            for label, partner_keys, n_combos in all_combos:
                partners_nos = [mark_map.get(k) for k in partner_keys]
                if not all(partners_nos): continue
                if len(set(partners_nos) | {honmei, taikou}) != n_combos + 2: continue

                combos = [{honmei, taikou, p} for p in partners_nos]
                stake_per_race = 100 * n_combos
                race_payout = 0
                race_hit = False
                for nos in combos:
                    if all(finish_map.get(h) in (1, 2, 3) for h in nos):
                        p = _lookup_trio_payout(rdata["payouts"], nos)
                        race_payout += p
                        race_hit = True
                        break

                for key in (confidence, "ALL"):
                    bc = matrix[label][key]
                    bc["played"] += 1
                    bc["stake"] += stake_per_race
                    bc["payback"] += race_payout
                    if race_hit:
                        bc["hits"] += 1

            processed += 1
            if processed % 10000 == 0:
                print(f"  ... {processed:,} race")

    print(f"\n  処理 race: {processed:,}")
    print(f"  ☆ override 付与: {hoshi_override_count:,} race ({hoshi_override_count/processed*100:.1f}%)")

    # 100% 超セル抽出
    print()
    print("=" * 110)
    print("【ROI 100% 超 セル一覧】 (race >= 30) — ☆ 常時版")
    print("=" * 110)
    print(f"{'戦略':<14} {'通り':>4} {'conf':<10} {'played':>7} {'hits':>6} {'hit%':>7} {'ROI':>8} {'balance':>15}")
    print("-" * 110)
    found_100 = []
    for label, _, n in all_combos:
        for lv in ("ALL",) + CONF_LEVELS:
            d = _derive(matrix[label][lv])
            if d['played'] >= 30 and d['roi_pct'] >= 100:
                found_100.append((d['roi_pct'], label, n, lv, d))
    found_100.sort(key=lambda x: -x[0])
    if found_100:
        for roi, label, n, lv, d in found_100:
            print(f"◎-○-{label:<11} {n:>4} {lv:<10} {d['played']:>7,} {d['hits']:>6,} {d['hit_rate_pct']:>6.2f}% {d['roi_pct']:>7.2f}% {d['balance']:>+15,} ⭐")
    else:
        print("  (該当なし)")

    # ☆ 関連戦略のみ抽出
    print()
    print("=" * 110)
    print("【☆ 関連戦略】 全期間 TOTAL ROI (☆ 常時版 / SS C のみ表示)")
    print("=" * 110)
    print(f"{'戦略':<14} {'通り':>4} {'conf':<6} {'played':>7} {'hits':>6} {'hit%':>7} {'ROI':>8} {'balance':>15}")
    print("-" * 110)
    for label, _, n in all_combos:
        if "☆" not in label: continue
        for lv in ("ALL", "SS", "S", "A", "B", "C"):
            d = _derive(matrix[label][lv])
            if d['played'] < 30: continue
            marker = " ⭐" if d['roi_pct'] >= 100 else "   "
            print(f"◎-○-{label:<11} {n:>4} {lv:<6} {d['played']:>7,} {d['hits']:>6,} {d['hit_rate_pct']:>6.2f}% {d['roi_pct']:>7.2f}% {d['balance']:>+15,}{marker}")

    # 全期間 ALL Top 15
    print()
    print("=" * 110)
    print("【全期間 TOTAL】 戦略別 ROI ランキング (☆ 常時版)")
    print("=" * 110)
    rankings = []
    for label, _, n in all_combos:
        d = _derive(matrix[label]["ALL"])
        rankings.append((d['roi_pct'], label, n, d))
    rankings.sort(key=lambda x: -x[0])
    print(f"{'戦略':<14} {'通り':>4} {'played':>7} {'hits':>6} {'hit%':>7} {'ROI':>8} {'balance':>15}")
    print("-" * 110)
    for roi, label, n, d in rankings:
        marker = " ⭐" if roi >= 100 else "   "
        print(f"◎-○-{label:<11} {n:>4} {d['played']:>7,} {d['hits']:>6,} {d['hit_rate_pct']:>6.2f}% {d['roi_pct']:>7.2f}% {d['balance']:>+15,}{marker}")

    # 結論
    print()
    print("=" * 110)
    print("【H-0 結論】 ☆ 常時 vs 動的 比較")
    print("=" * 110)
    print(f"  ☆ override race 数: {hoshi_override_count:,} / {processed:,} ({hoshi_override_count/processed*100:.1f}%)")
    if found_100:
        print(f"  ROI 100% 超セル: {len(found_100)} 個 (動的版は 4 個 / 増減: {len(found_100)-4:+d})")
        best = found_100[0]
        print(f"  最強: ◎-○-{best[1]} × {best[3]} = ROI {best[0]:.2f}% (balance {best[4]['balance']:+,} 円)")
    else:
        print("  ROI 100% 超セル: なし")


if __name__ == "__main__":
    main()
