"""4点合算戦略 (◉◎-〇-▲△★☆ 三連複 4 通り買い) post-hoc 集計

マスター提案 (5/28): 全 race で ◎-○-▲, ◎-○-△, ◎-○-★, ◎-○-☆ の 4 通りを購入。
1 race あたり投資 400 円。当たり 0-1 通り (組合せ排反)。自信度 7 段階 (SS/S/A/B/C/D/E) 別に集計。

データソース: predictions_pre_markint_20260527.tar.gz (リーク排除済 WF pred.json)
"""

import io
import json
import os
import sys
import tarfile
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.stdout.reconfigure(encoding="utf-8")

from data.masters.venue_master import BANEI_VENUE_CODES

TAR_PATH = os.path.join(ROOT, "data", "_archive", "predictions_pre_markint_20260527.tar.gz")
RESULTS_DIR = os.path.join(ROOT, "data", "results")
CONF_LEVELS = ("SS", "S", "A", "B", "C", "D", "E")
WF_PERIODS = {
    "wf_2024": ("20240101", "20241231"),
    "wf_2025": ("20250101", "20251231"),
    "wf_2026": ("20260101", "20261231"),
}


def _lookup_trio_payout(payouts: dict, nos: set) -> int:
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


def _empty_stats():
    return {"played": 0, "hits": 0, "stake": 0, "payback": 0}


def _derive(d):
    p = d["played"]
    s = d["stake"]
    pb = d["payback"]
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
    print("=" * 100)
    print("4点合算戦略 (◉◎-〇-▲△★☆ 三連複 4 通り) post-hoc 集計")
    print("=" * 100)

    # tar load
    print("[1/2] WF pred.json ロード中...")
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

    # 集計バッファ: total + 自信度別 + 期間別
    total = _empty_stats()
    by_conf = {lv: _empty_stats() for lv in CONF_LEVELS}
    by_period = {pn: _empty_stats() for pn in WF_PERIODS}
    by_period_conf = {pn: {lv: _empty_stats() for lv in CONF_LEVELS} for pn in WF_PERIODS}

    print("\n[2/2] 集計中...")
    skipped_marks = skipped_banei = skipped_payouts = skipped_conf = 0
    processed = 0

    for date_key in sorted(preds.keys()):
        rpath = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
        if not os.path.exists(rpath):
            continue
        with open(rpath, encoding="utf-8") as f:
            results = json.load(f)

        # 期間判定
        period = None
        for pn, (s, e) in WF_PERIODS.items():
            if s <= date_key <= e:
                period = pn
                break

        for race in preds[date_key].get("races", []):
            race_id = str(race.get("race_id", ""))
            # ばんえい除外
            if len(race_id) >= 6 and race_id[4:6] in BANEI_VENUE_CODES:
                skipped_banei += 1
                continue

            rdata = results.get(race_id)
            if rdata is None or not rdata.get("payouts") or rdata["payouts"].get("三連複") is None:
                skipped_payouts += 1
                continue

            order = rdata.get("order", [])
            if not order:
                skipped_payouts += 1
                continue
            finish_map = {r.get("horse_no"): r.get("finish")
                          for r in order if r.get("horse_no") is not None and r.get("finish") is not None}

            confidence = race.get("confidence", "") or ""
            if confidence not in CONF_LEVELS:
                skipped_conf += 1
                continue

            # 印取得
            honmei = taikou = renka = wide1 = wide2 = wide3 = None
            for h in race.get("horses", []):
                m = h.get("mark", "")
                hn = h.get("horse_no")
                if m in ("◎", "◉") and honmei is None: honmei = hn
                elif m in ("○", "〇") and taikou is None: taikou = hn
                elif m == "▲" and renka is None: renka = hn
                elif m == "△" and wide1 is None: wide1 = hn
                elif m == "★" and wide2 is None: wide2 = hn
                elif m == "☆" and wide3 is None: wide3 = hn

            # 4 印 (▲△★☆) すべて + ◎○ 揃わなければスキップ
            if not all([honmei, taikou, renka, wide1, wide2, wide3]):
                skipped_marks += 1
                continue
            # 重複馬番チェック
            base = {honmei, taikou}
            partners = [renka, wide1, wide2, wide3]
            partner_set = set(partners)
            if len(base) != 2 or honmei in partner_set or taikou in partner_set or len(partner_set) != 4:
                skipped_marks += 1
                continue

            # 4 通り購入
            combos = [{honmei, taikou, p} for p in partners]
            stake_per_race = 400  # 4 通り × 100 円
            race_payout = 0
            race_hit = False
            for nos in combos:
                if all(finish_map.get(h) in (1, 2, 3) for h in nos):
                    p = _lookup_trio_payout(rdata["payouts"], nos)
                    race_payout += p
                    race_hit = True
                    break  # 三連複の的中は 1 race 1 通りのみ

            # 集計
            total["played"] += 1
            total["stake"] += stake_per_race
            total["payback"] += race_payout
            if race_hit:
                total["hits"] += 1

            bc = by_conf[confidence]
            bc["played"] += 1
            bc["stake"] += stake_per_race
            bc["payback"] += race_payout
            if race_hit:
                bc["hits"] += 1

            if period:
                bp = by_period[period]
                bp["played"] += 1
                bp["stake"] += stake_per_race
                bp["payback"] += race_payout
                if race_hit:
                    bp["hits"] += 1

                bpc = by_period_conf[period][confidence]
                bpc["played"] += 1
                bpc["stake"] += stake_per_race
                bpc["payback"] += race_payout
                if race_hit:
                    bpc["hits"] += 1

            processed += 1
            if processed % 5000 == 0:
                print(f"  ... {processed:,} race 処理済")

    print(f"\n  処理 race: {processed:,}")
    print(f"  スキップ (印不揃い ▲△★☆☆): {skipped_marks:,}")
    print(f"  スキップ (ばんえい): {skipped_banei:,}")
    print(f"  スキップ (payouts/results 無): {skipped_payouts:,}")
    print(f"  スキップ (confidence 無効): {skipped_conf:,}")

    # 結果出力
    print()
    print("=" * 100)
    print("【全期間 TOTAL】 4点合算 ROI")
    print("=" * 100)
    t = _derive(total)
    print(f"  played={t['played']:>7,} hits={t['hits']:>6,} hit%={t['hit_rate_pct']:>5.2f}% ROI={t['roi_pct']:>6.2f}% balance={t['balance']:>+15,} 円")
    print(f"  (stake={t['stake']:,} 円 / payback={t['payback']:,} 円)")

    print()
    print("=" * 100)
    print("【全期間】 自信度別 7 段階 ROI")
    print("=" * 100)
    print(f"{'自信度':<6} {'played':>8} {'hits':>7} {'hit%':>7} {'ROI':>8} {'balance':>15} {'純利率':>10}")
    print("-" * 100)
    for lv in CONF_LEVELS:
        d = _derive(by_conf[lv])
        marker = " ⭐" if d['roi_pct'] >= 100 else "   "
        bal_per_race = d['balance'] / d['played'] if d['played'] else 0
        print(f"{lv:<6} {d['played']:>8,} {d['hits']:>7,} {d['hit_rate_pct']:>6.2f}% {d['roi_pct']:>7.2f}% {d['balance']:>+15,} {bal_per_race:>+9.1f}円/R{marker}")

    print()
    print("=" * 100)
    print("【期間別 × 自信度】 ROI 100% 超セル抽出 (race 数 >= 30 限定)")
    print("=" * 100)
    print(f"{'期間':<8} {'自信度':<6} {'played':>8} {'hits':>7} {'hit%':>7} {'ROI':>8} {'balance':>15}")
    print("-" * 100)
    found = False
    for pn in WF_PERIODS:
        for lv in CONF_LEVELS:
            d = _derive(by_period_conf[pn][lv])
            if d['played'] >= 30 and d['roi_pct'] >= 100:
                print(f"{pn:<8} {lv:<6} {d['played']:>8,} {d['hits']:>7,} {d['hit_rate_pct']:>6.2f}% {d['roi_pct']:>7.2f}% {d['balance']:>+15,} ⭐")
                found = True
    if not found:
        print("  (race 数 >= 30 で ROI 100% 超セルなし)")

    print()
    print("=" * 100)
    print("【期間別 TOTAL】 ROI")
    print("=" * 100)
    for pn in WF_PERIODS:
        d = _derive(by_period[pn])
        marker = " ⭐" if d['roi_pct'] >= 100 else "   "
        print(f"  {pn:<8} played={d['played']:>6,} hits={d['hits']:>5,} hit%={d['hit_rate_pct']:>5.2f}% ROI={d['roi_pct']:>6.2f}% balance={d['balance']:>+13,}{marker}")

    print()
    print("=" * 100)
    print("結論")
    print("=" * 100)
    if t['roi_pct'] >= 100:
        print("✅ 全期間 TOTAL で ROI 100% 達成!")
    elif any(_derive(by_conf[lv])['roi_pct'] >= 100 for lv in CONF_LEVELS):
        print("⭐ 自信度別で ROI 100% 超セルあり (実運用候補)")
    else:
        print(f"❌ 全期間 ROI {t['roi_pct']:.1f}% / 100% まで -{100 - t['roi_pct']:.1f}pt 不足")


if __name__ == "__main__":
    main()
