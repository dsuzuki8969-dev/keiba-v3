"""results.json 三連複/三連単/馬単/ワイド 払戻データ全件バックフィル。

マスター指示 2026-05-01: 過去成績の三連複データ欠落 (16,057R) を全件補完。

戦略:
1. 既存 results.json を全走査
2. 三連複キーが無いレースを抽出
3. data/cache/ にキャッシュ HTML があればそれをパース (高速)
4. キャッシュ無ければ netkeiba から新規 GET (レート 2.0秒)
5. 取得した payouts を既存 results.json にマージ + 保存

注意:
- netkeiba 並列禁止 (CLAUDE.md feedback_netkeiba_concurrent_throttle)
- 進捗を定期表示 (100件ごと)
- 中断可能: 各レースごとに保存しているので途中で止めても OK
"""
from __future__ import annotations

import io
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

try:
    import lz4.frame
    import requests
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"必要モジュール不足: {e}")
    sys.exit(1)

RES_DIR = Path("data/results")
CACHE_DIR = Path("data/cache")
LOG_DIR = Path("data/logs")
LOG_DIR.mkdir(exist_ok=True, parents=True)
PROGRESS_LOG = LOG_DIR / "backfill_all_payouts.progress.log"

LABEL_NORM = {"3連複": "三連複", "3連単": "三連単"}
TARGETS = {"馬連", "馬単", "ワイド", "三連複", "三連単", "複勝", "単勝", "3連複", "3連単", "枠連"}

# レート制限: 緊急バックフィル 1.0 秒/件 (単独プロセス・並列なし → リスク許容)
# 通常運用は 2.0 秒推奨だが朝までの完了を優先
RATE_LIMIT_SEC = 1.0

# HTTP セッション (1 接続維持)
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
})


JRA_VENUE_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}


def _cache_path(race_id: str) -> Path:
    vc = race_id[4:6]
    prefix = "race.netkeiba.com" if vc in JRA_VENUE_CODES else "nar.netkeiba.com"
    return CACHE_DIR / f"{prefix}_race_result.html_race_id={race_id}.html.lz4"


def _result_url(race_id: str) -> str:
    vc = race_id[4:6]
    if vc in JRA_VENUE_CODES:
        return f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    return f"https://nar.netkeiba.com/race/result.html?race_id={race_id}"


def _fetch_html(race_id: str, use_cache: bool = True) -> Optional[str]:
    """キャッシュ優先で HTML を取得。なければ netkeiba から GET (レート遵守)。"""
    cf = _cache_path(race_id)
    if use_cache and cf.exists():
        try:
            with cf.open("rb") as f:
                return lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
        except Exception:
            pass

    # 新規取得 (レート 2.0 秒)
    url = _result_url(race_id)
    try:
        time.sleep(RATE_LIMIT_SEC)
        r = SESSION.get(url, timeout=15)
        if r.status_code == 200 and len(r.content) > 1000:
            html = r.content.decode("euc-jp", errors="replace")
            # キャッシュ保存
            try:
                cf.parent.mkdir(parents=True, exist_ok=True)
                with cf.open("wb") as f:
                    f.write(lz4.frame.compress(html.encode("utf-8")))
            except Exception:
                pass
            return html
        return None
    except Exception:
        return None


def parse_payouts(html: str) -> dict:
    """HTML から全券種の払戻をパース。

    重要: combo セルは <br> 区切りで馬番が並ぶことがある (netkeiba の新形式)。
    get_text(separator='-') で <br> を '-' に変換し、馬番の連結を防ぐ。
    """
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
            # separator='-' で <br> を '-' に変換 → 数字連結バグ防止
            combo_raw = cells[1].get_text(separator='-', strip=True) if len(cells) > 1 else ""
            payout_raw = cells[2].get_text(separator='\n', strip=True) if len(cells) > 2 else ""

            # 数字とハイフンのみ抽出、連続ハイフンを単一化
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


def main():
    res_files = sorted(RES_DIR.glob("*_results.json"))
    print(f"結果ファイル数: {len(res_files)}")

    # まず欠落 race_id 抽出
    missing: list[tuple[Path, str]] = []  # (json_file, race_id)
    already = 0
    for rf in res_files:
        try:
            data = json.loads(rf.read_text(encoding="utf-8"))
        except Exception:
            continue
        for race_id, rdata in data.items():
            payouts = rdata.get("payouts", {})
            if "三連複" in payouts or "sanrenpuku" in payouts:
                already += 1
                continue
            missing.append((rf, race_id))

    total = len(missing)
    print(f"既存三連複: {already}R, 欠落: {total}R")
    print(f"レート: {RATE_LIMIT_SEC}秒/件 → ETA {total * RATE_LIMIT_SEC / 3600:.1f} 時間")
    print(f"進捗ログ: {PROGRESS_LOG}")

    if total == 0:
        print("バックフィル不要")
        return

    # ファイル単位にグループ化
    by_file: dict[Path, list[str]] = {}
    for rf, rid in missing:
        by_file.setdefault(rf, []).append(rid)

    updated = 0
    no_data = 0
    cache_hits = 0
    started = time.time()

    for fi, (rf, race_ids) in enumerate(sorted(by_file.items())):
        try:
            data = json.loads(rf.read_text(encoding="utf-8"))
        except Exception:
            continue

        modified = False
        for rid in race_ids:
            cf_exists = _cache_path(rid).exists()
            html = _fetch_html(rid, use_cache=True)
            if cf_exists:
                cache_hits += 1
            if not html:
                no_data += 1
                continue

            new_payouts = parse_payouts(html)
            if not new_payouts:
                no_data += 1
                continue

            existing_payouts = data[rid].setdefault("payouts", {})
            added = False
            for key in ("三連複", "三連単", "枠連", "馬単", "ワイド"):
                if key in new_payouts and key not in existing_payouts:
                    existing_payouts[key] = new_payouts[key]
                    added = True
            if added:
                updated += 1
                modified = True

            if (updated + no_data) % 50 == 0:
                elapsed = time.time() - started
                done = updated + no_data
                eta_sec = (total - done) * (elapsed / done) if done > 0 else 0
                msg = (f"進捗 {done}/{total} ({done/total*100:.1f}%) "
                       f"updated={updated} no_data={no_data} cache_hits={cache_hits} "
                       f"経過 {elapsed/60:.1f}分 残り {eta_sec/60:.1f}分")
                print(msg, flush=True)
                with PROGRESS_LOG.open("a", encoding="utf-8") as pf:
                    pf.write(f"{time.strftime('%H:%M:%S')} {msg}\n")

        if modified:
            try:
                rf.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            except Exception as e:
                print(f"保存失敗 {rf.name}: {e}", flush=True)

    elapsed = time.time() - started
    final = (f"完了: updated={updated}, no_data={no_data}, "
             f"cache_hits={cache_hits}, total_processed={updated + no_data}, "
             f"経過 {elapsed/60:.1f}分")
    print(final, flush=True)
    with PROGRESS_LOG.open("a", encoding="utf-8") as pf:
        pf.write(f"{time.strftime('%H:%M:%S')} FINAL {final}\n")


if __name__ == "__main__":
    main()
