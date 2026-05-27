"""全組合せマトリクス試行 (◉◎-〇-X 流し 1-4 通り全組合せ × confidence)

戦略: ◎-○ 軸 2 頭 + 流し相手 X (▲△★☆ から 1-4 通り組合せ)
組合せ数: C(4,1)+C(4,2)+C(4,3)+C(4,4) = 4+6+4+1 = 15 戦略
confidence: SS/S/A/B/C/D/E の 7 段階
合計セル: 15 × 7 = 105

データソース: predictions_pre_markint_20260527.tar.gz (リーク排除済 WF pred.json)
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

# 流し相手 4 種類
PARTNERS = [("renka", "▲"), ("wide1", "△"), ("wide2", "★"), ("wide3", "☆")]


def _lookup_trio_payout(payouts, nos):
    bucket = payouts.get("三連複")
    if bucket is None:
        return 0
    def _match(item):
        if not isinstance(item, dict):
            return 0
        combo = item.get("combo", "")
        try:
            cs = set(int(x) for x in combo.split("-")) if combo else set()
        except (ValueError, AttributeError):
            return 0
        if cs == nos:
            p = item.get("payout", 0)
            return int(p) if isinstance(p, (int, float)) else 0
        return 0
    if isinstance(bucket, dict):
        return _match(bucket)
    if isinstance(bucket, list):
        for it in bucket:
            v = _match(it)
            if v:
                return v
    return 0


def _empty():
    return {"played": 0, "hits": 0, "stake": 0, "payback": 0}


def _derive(d):
    p, s, pb = d["played"], d["stake"], d["payback"]
    return {
        "played": p,
        "hits": d["hits"],
        "hit_rate_pct": d["hits"] / p * 100 if p else 0.0,
        "roi_pct": pb / s * 100 if s else 0.0,
        "stake": s,
        "payback": pb,
        "balance": pb - s,
    }


def main():
    print("=" * 110)
    print("全組合せマトリクス試行 (15 戦略 × 7 confidence = 105 セル)")
    print("=" * 110)

    # 全組合せ生成 (1-4 通り)
    all_combos = []
    for r in range(1, 5):
        for c in combinations(PARTNERS, r):
            label = "".join(m for _, m in c)  # 例: ▲△ / ▲△★☆
            keys = [k for k, _ in c]
            all_combos.append((label, keys, len(c)))
    print(f"組合せ数: {len(all_combos)} 戦略")

    # tar load
    print("\n[1/2] WF pred.json ロード中...")
    preds = {}
    with tarfile.open(TAR_PATH, "r:gz") as tar:
        for m in tar.getmembers():
            if not m.name.endswith("_pred.json"):
                continue
            date_key = os.path.basename(m.name)[:8]
            if not date_key.isdigit() or len(date_key) != 8:
                continue
            f = tar.extractfile(m)
            if f:
                preds[date_key] = json.load(io.TextIOWrapper(f, encoding="utf-8"))
    print(f"  {len(preds)} 日分ロード完了")

    # 集計バッファ: combo_label → {confidence → stats}
    # confidence 別 + total
    matrix = {label: {lv: _empty() for lv in CONF_LEVELS + ("ALL",)} for label, _, _ in all_combos}

    print("\n[2/2] 集計中...")
    processed = 0

    for date_key in sorted(preds.keys()):
        rpath = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
        if not os.path.exists(rpath):
            continue
        with open(rpath, encoding="utf-8") as f:
            results = json.load(f)

        for race in preds[date_key].get("races", []):
            race_id = str(race.get("race_id", ""))
            if len(race_id) >= 6 and race_id[4:6] in BANEI_VENUE_CODES:
                continue
            rdata = results.get(race_id)
            if rdata is None or not rdata.get("payouts") or rdata["payouts"].get("三連複") is None:
                continue
            order = rdata.get("order", [])
            if not order:
                continue
            finish_map = {r.get("horse_no"): r.get("finish")
                          for r in order if r.get("horse_no") is not None and r.get("finish") is not None}
            confidence = race.get("confidence", "") or ""
            if confidence not in CONF_LEVELS:
                continue

            # 印取得
            mark_map = {}
            for h in race.get("horses", []):
                m = h.get("mark", "")
                hn = h.get("horse_no")
                if m in ("◎", "◉") and "honmei" not in mark_map: mark_map["honmei"] = hn
                elif m in ("○", "〇") and "taikou" not in mark_map: mark_map["taikou"] = hn
                elif m == "▲" and "renka" not in mark_map: mark_map["renka"] = hn
                elif m == "△" and "wide1" not in mark_map: mark_map["wide1"] = hn
                elif m == "★" and "wide2" not in mark_map: mark_map["wide2"] = hn
                elif m == "☆" and "wide3" not in mark_map: mark_map["wide3"] = hn

            honmei = mark_map.get("honmei")
            taikou = mark_map.get("taikou")
            if not (honmei and taikou and honmei != taikou):
                continue

            # 各組合せで集計
            for label, partner_keys, n_combos in all_combos:
                partners = [mark_map.get(k) for k in partner_keys]
                if not all(partners):
                    continue  # 必要印が揃わなければスキップ
                # 重複馬番チェック
                if len(set(partners) | {honmei, taikou}) != n_combos + 2:
                    continue

                combos = [{honmei, taikou, p} for p in partners]
                stake_per_race = 100 * n_combos
                race_payout = 0
                race_hit = False
                for nos in combos:
                    if all(finish_map.get(h) in (1, 2, 3) for h in nos):
                        p = _lookup_trio_payout(rdata["payouts"], nos)
                        race_payout += p
                        race_hit = True
                        break  # 三連複は 1 race 1 通り

                # ALL + confidence
                for key in (confidence, "ALL"):
                    bc = matrix[label][key]
                    bc["played"] += 1
                    bc["stake"] += stake_per_race
                    bc["payback"] += race_payout
                    if race_hit:
                        bc["hits"] += 1

            processed += 1
            if processed % 10000 == 0:
                print(f"  ... {processed:,} race 処理済")

    print(f"\n  処理 race: {processed:,}")

    # ROI 100% 超セル + Top 10 抽出
    print()
    print("=" * 110)
    print("【ROI 100% 超 セル一覧】 (race 数 >= 30)")
    print("=" * 110)
    print(f"{'戦略':<14} {'通り':>4} {'confidence':<10} {'played':>7} {'hits':>6} {'hit%':>7} {'ROI':>8} {'balance':>15}")
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

    # 全期間 TOTAL (ALL confidence) Top 15
    print()
    print("=" * 110)
    print("【全期間 TOTAL (ALL confidence)】 戦略別 ROI ランキング")
    print("=" * 110)
    print(f"{'戦略':<14} {'通り':>4} {'played':>7} {'hits':>6} {'hit%':>7} {'ROI':>8} {'balance':>15}")
    print("-" * 110)
    rankings = []
    for label, _, n in all_combos:
        d = _derive(matrix[label]["ALL"])
        rankings.append((d['roi_pct'], label, n, d))
    rankings.sort(key=lambda x: -x[0])
    for roi, label, n, d in rankings:
        marker = " ⭐" if roi >= 100 else ("  +" if roi > 77.4 else "   ")
        print(f"◎-○-{label:<11} {n:>4} {d['played']:>7,} {d['hits']:>6,} {d['hit_rate_pct']:>6.2f}% {d['roi_pct']:>7.2f}% {d['balance']:>+15,}{marker}")

    # SS confidence Top 15
    print()
    print("=" * 110)
    print("【SS 自信度限定】 戦略別 ROI ランキング")
    print("=" * 110)
    print(f"{'戦略':<14} {'通り':>4} {'played':>7} {'hits':>6} {'hit%':>7} {'ROI':>8} {'balance':>15}")
    print("-" * 110)
    rankings_ss = []
    for label, _, n in all_combos:
        d = _derive(matrix[label]["SS"])
        rankings_ss.append((d['roi_pct'], label, n, d))
    rankings_ss.sort(key=lambda x: -x[0])
    for roi, label, n, d in rankings_ss:
        marker = " ⭐" if roi >= 100 else ("   ")
        print(f"◎-○-{label:<11} {n:>4} {d['played']:>7,} {d['hits']:>6,} {d['hit_rate_pct']:>6.2f}% {d['roi_pct']:>7.2f}% {d['balance']:>+15,}{marker}")

    # 結論
    print()
    print("=" * 110)
    print("結論")
    print("=" * 110)
    if found_100:
        best_roi, best_label, best_n, best_lv, best_d = found_100[0]
        print(f"⭐ ROI 100% 超セル {len(found_100)} 個発見!")
        print(f"   最強: ◎-○-{best_label} × {best_lv} = ROI {best_roi:.2f}% (played {best_d['played']:,} / balance {best_d['balance']:+,} 円)")
    else:
        print("❌ ROI 100% 超セル なし (race >= 30)")


if __name__ == "__main__":
    main()
