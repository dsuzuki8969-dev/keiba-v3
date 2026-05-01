"""既存 results.json の三連複/三連単/ワイド combo がハイフン無し (バグ) の場合、
HTML キャッシュから再 parse して修正する。

マスター指示 2026-05-01:
backfill_all_payouts.py の parse_payouts バグで combo が "91011" のような連結文字列
になっていた問題への修復。 HTML キャッシュ (data/cache/{prefix}_race_result*.lz4)
から再パースしてハイフン区切りに復元する。
"""
from __future__ import annotations

import io
import json
import re
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

try:
    import lz4.frame
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"必要モジュール不足: {e}")
    sys.exit(1)

RES_DIR = Path("data/results")
CACHE_DIR = Path("data/cache")

JRA_VENUE_CODES = {"01","02","03","04","05","06","07","08","09","10"}
LABEL_NORM = {"3連複": "三連複", "3連単": "三連単"}
TARGETS = {"馬連", "馬単", "ワイド", "三連複", "三連単", "複勝", "単勝", "3連複", "3連単", "枠連"}


def _cache_path(race_id: str) -> Path:
    vc = race_id[4:6]
    prefix = "race.netkeiba.com" if vc in JRA_VENUE_CODES else "nar.netkeiba.com"
    return CACHE_DIR / f"{prefix}_race_result.html_race_id={race_id}.html.lz4"


def parse_payouts(html: str) -> dict:
    """修正版 parse: separator='-' で <br> 区切りを保持する。"""
    payouts: dict = {}
    if not html:
        return payouts
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return payouts
    payout_tables = soup.select(".Payout_Detail_Table, table.payout, table.pay_table_01")
    for tbl in payout_tables:
        for tr in tbl.select("tr"):
            cells = tr.select("td, th")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            if label not in TARGETS:
                continue
            label = LABEL_NORM.get(label, label)
            combo_raw = cells[1].get_text(separator='-', strip=True) if len(cells) > 1 else ""
            payout_raw = cells[2].get_text(separator='\n', strip=True) if len(cells) > 2 else ""
            combo = re.sub(r"-+", "-", re.sub(r"[^\d\-]", "-", combo_raw)).strip("-")
            payout_val_str = re.sub(r"[^\d]", "", payout_raw.split("\n")[0] if payout_raw else "")
            try:
                payout_val = int(payout_val_str) if payout_val_str else 0
            except ValueError:
                payout_val = 0
            if not combo:
                continue
            entry = {"combo": combo, "payout": payout_val}
            if label == "ワイド":
                payouts.setdefault("ワイド", [])
                payouts["ワイド"].append(entry)
            elif label not in payouts:
                payouts[label] = entry
    return payouts


def _is_broken_combo(combo: str, ticket_type: str) -> bool:
    """combo がハイフン区切りでない (= バグ生成) かを判定。

    三連複/三連単/馬連/馬単/ワイド: 必ずハイフン含む (複数馬番)
    単勝/複勝: ハイフン無しが正常
    """
    if not combo:
        return False
    multi_horse_types = {"三連複", "三連単", "馬連", "馬単", "ワイド", "枠連"}
    if ticket_type in multi_horse_types and "-" not in combo:
        return True
    return False


def main():
    res_files = sorted(RES_DIR.glob("*_results.json"))
    print(f"results.json 数: {len(res_files)}")

    started = time.time()
    n_total = 0
    n_broken = 0
    n_fixed = 0
    n_no_cache = 0

    for fi, rf in enumerate(res_files):
        try:
            data = json.loads(rf.read_text(encoding="utf-8"))
        except Exception:
            continue

        modified = False
        for rid, rdata in data.items():
            payouts = rdata.get("payouts", {})
            n_total += 1

            # 壊れた combo を検出
            need_fix = False
            for ttype, entry in payouts.items():
                if isinstance(entry, dict):
                    if _is_broken_combo(entry.get("combo", ""), ttype):
                        need_fix = True
                        break
                elif isinstance(entry, list):
                    if any(_is_broken_combo(e.get("combo",""), ttype) for e in entry):
                        need_fix = True
                        break

            if not need_fix:
                continue

            n_broken += 1
            cf = _cache_path(rid)
            if not cf.exists():
                n_no_cache += 1
                continue

            try:
                with cf.open("rb") as f:
                    html = lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
            except Exception:
                continue

            new_payouts = parse_payouts(html)
            if not new_payouts:
                continue

            # 壊れたキーのみ修復 (他の正常なキーは保持)
            updated_keys = []
            for ttype in list(payouts.keys()):
                entry = payouts[ttype]
                broken = False
                if isinstance(entry, dict):
                    broken = _is_broken_combo(entry.get("combo", ""), ttype)
                elif isinstance(entry, list):
                    broken = any(_is_broken_combo(e.get("combo",""), ttype) for e in entry)
                if broken and ttype in new_payouts:
                    payouts[ttype] = new_payouts[ttype]
                    updated_keys.append(ttype)

            if updated_keys:
                rdata["payouts"] = payouts
                modified = True
                n_fixed += 1

        if modified:
            try:
                rf.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            except Exception as e:
                print(f"  保存失敗 {rf.name}: {e}", flush=True)

        if (fi + 1) % 50 == 0 or (fi + 1) == len(res_files):
            elapsed = time.time() - started
            print(f"  {fi+1}/{len(res_files)}: total={n_total} broken={n_broken} "
                  f"fixed={n_fixed} no_cache={n_no_cache} elapsed={elapsed:.1f}s", flush=True)

    print(f"\n完了: total={n_total}, broken={n_broken}, fixed={n_fixed}, "
          f"no_cache={n_no_cache}, elapsed={time.time()-started:.1f}s")


if __name__ == "__main__":
    main()
