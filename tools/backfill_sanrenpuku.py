"""既存の結果JSONに三連複・三連単の払戻を補完するスクリプト。
netkeiba結果ページのキャッシュHTMLから三連複/三連単をパースして追加する。
新規スクレイピングは行わない（キャッシュのみ使用）。
"""
import json, re, sys, io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

try:
    import lz4.frame
    from bs4 import BeautifulSoup
except ImportError:
    print("lz4, bs4 が必要です")
    sys.exit(1)

RES_DIR = Path("data/results")
CACHE_DIR = Path("data/cache")

LABEL_NORM = {"3連複": "三連複", "3連単": "三連単"}
TARGETS = {"馬連", "馬単", "ワイド", "三連複", "三連単", "複勝", "単勝", "3連複", "3連単", "枠連"}


def parse_payouts_from_cache(race_id: str) -> dict:
    """キャッシュHTMLから全券種の払戻をパース"""
    vc = race_id[4:6]
    # JRA (venue 01-10) or NAR
    jra_codes = {"01","02","03","04","05","06","07","08","09","10"}
    if vc in jra_codes:
        prefix = "race.netkeiba.com"
    else:
        prefix = "nar.netkeiba.com"
    cf = CACHE_DIR / f"{prefix}_race_result.html_race_id={race_id}.html.lz4"
    if not cf.exists():
        return {}
    try:
        with open(cf, "rb") as f:
            html = lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return {}

    payouts = {}
    payout_tables = soup.select(".Payout_Detail_Table, table.payout, table.pay_table_01")
    for payout_table in payout_tables:
        for tr in payout_table.select("tr"):
            cells = tr.select("td, th")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            if label not in TARGETS:
                continue
            label = LABEL_NORM.get(label, label)
            combo_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            payout_cell = cells[2].get_text(strip=True).replace(",", "") if len(cells) > 2 else ""
            try:
                payout_val = int(re.sub(r"[^\d]", "", payout_cell)) if payout_cell else 0
            except ValueError:
                payout_val = 0
            entry = {"combo": combo_cell, "payout": payout_val}
            if label == "ワイド":
                payouts.setdefault("ワイド", [])
                payouts["ワイド"].append(entry)
            elif label not in payouts:
                payouts[label] = entry
    return payouts


def main():
    res_files = sorted(RES_DIR.glob("*_results.json"))
    print(f"結果ファイル数: {len(res_files)}")

    total_updated = 0
    total_already = 0
    total_no_cache = 0

    for rf in res_files:
        data = json.loads(rf.read_text(encoding="utf-8"))
        modified = False

        for race_id, rdata in data.items():
            payouts = rdata.get("payouts", {})
            # 既に三連複がある場合はスキップ
            if "三連複" in payouts or "sanrenpuku" in payouts:
                total_already += 1
                continue

            # キャッシュからパース
            new_payouts = parse_payouts_from_cache(race_id)
            if not new_payouts:
                total_no_cache += 1
                continue

            # 三連複・三連単を追加
            added = False
            for key in ("三連複", "三連単", "枠連", "馬単"):
                if key in new_payouts and key not in payouts:
                    payouts[key] = new_payouts[key]
                    added = True
            # ワイドも補完
            if "ワイド" in new_payouts and "ワイド" not in payouts:
                payouts["ワイド"] = new_payouts["ワイド"]
                added = True

            if added:
                rdata["payouts"] = payouts
                modified = True
                total_updated += 1

        if modified:
            rf.write_text(json.dumps(data, ensure_ascii=False, indent=None), encoding="utf-8")

    print(f"更新: {total_updated}R, 既存: {total_already}R, キャッシュなし: {total_no_cache}R")


if __name__ == "__main__":
    main()
