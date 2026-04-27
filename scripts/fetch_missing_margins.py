"""
race_log.margin_ahead が誤値 (0 で fin>1) または NULL の race_id について、
netkeiba 結果ページから着差データを再取得して UPDATE する。

背景:
- NAR 岩手系（水沢 venue 36）等で race_results に時刻・着差が欠損
- 既存 race_log の margin_ahead=0 が「+0.0」と誤表示される
- マスター指示「— 表記は逃げ道、しっかり拾ってこい」

動作:
- 対象 race_id 抽出 (race_log で fin>1 かつ margin_ahead=0 or NULL)
- NAR/JRA URL 自動判定で netkeiba 結果ページ取得
- _parse_result_table で margin 取得、_parse_margin で秒換算
- race_log.margin_ahead を UPDATE
- 完了後 pred.json 再注入用情報を margin マップで返す
"""
from __future__ import annotations
import argparse, json, re, sqlite3, sys, time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.scraper.netkeiba import OddsScraper  # _parse_margin 流用

DB = ROOT / "data" / "keiba.db"
JRA_VENUES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept-Language": "ja"}


def _parse_margin_text(text: str) -> float | None:
    """着差テキスト→秒換算 (netkeiba._parse_margin と同等)"""
    if not text or text.strip() in ("", "同着", "---", "------", "—"):
        return None
    text = text.strip()
    m = re.match(r"^(-?\d+\.\d+)$", text)
    if m:
        return float(m.group(1))
    m = re.match(r"(\d+)\.(\d)/(\d)", text)
    if m:
        whole = int(m.group(1)); numer = int(m.group(2)); denom = int(m.group(3))
        return (whole + numer / denom) * 0.2
    margin_map = {
        "ハナ": 0.05, "クビ": 0.1, "アタマ": 0.15,
        "1/2": 0.1, "3/4": 0.15, "大": 2.0,
    }
    for key, val in margin_map.items():
        if key in text:
            return val
    m = re.match(r"^(\d+)$", text)
    if m:
        return float(m.group(1)) * 0.2
    return None


def fetch_result(race_id: str, session: requests.Session) -> list:
    """指定 race_id の結果ページから (horse_no, finish, margin_text) のリストを返す"""
    venue_code = race_id[4:6]
    base = "https://race.netkeiba.com" if venue_code in JRA_VENUES else "https://nar.netkeiba.com"
    url = f"{base}/race/result.html?race_id={race_id}"
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.content, "html.parser")
    except Exception:
        return []

    # JRA: .ResultTableWrap table / NAR: #All_Result_Table
    table = soup.select_one(".ResultTableWrap table") or soup.select_one("#All_Result_Table")
    if not table:
        return []

    # tbody tr または直接 tr (NAR は tbody タグなし)
    rows = table.select("tbody tr") or [r for r in table.select("tr") if r.select("td")]
    out = []
    for row in rows:
        cells = row.select("td")
        if len(cells) < 10:
            continue
        try:
            finish_text = cells[0].get_text(strip=True)
            if not finish_text.isdigit():
                continue
            finish = int(finish_text)
            horse_no = int(cells[2].get_text(strip=True))
            margin_text = cells[8].get_text(strip=True)
            margin_sec = _parse_margin_text(margin_text) if finish > 1 else None
            out.append({"horse_no": horse_no, "finish": finish, "margin": margin_sec, "margin_text": margin_text})
        except Exception:
            continue
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="先頭 N race のみ処理 (0=全部)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.5, help="リクエスト間 sleep 秒")
    parser.add_argument("--from-pred", default=None, help="この日付の pred.json 内 race_id のみ対象 (YYYYMMDD)")
    parser.add_argument("--nar-only", action="store_true", help="NAR レースのみ処理 (venue !=01-10)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))

    # 対象 race_id 抽出
    if args.from_pred:
        # pred.json の past_3_runs 内 race_id (margin 未取得分)
        ppath = ROOT / "data" / "predictions" / f"{args.from_pred}_pred.json"
        with open(ppath, encoding="utf-8") as f:
            pred = json.load(f)
        rids = set()
        for r in pred.get("races", []):
            for h in r.get("horses", []):
                for p in (h.get("past_3_runs") or h.get("past_runs") or []):
                    if (p.get("margin") is None or p.get("margin") == 0) and (p.get("finish_pos") or 0) > 1:
                        rid = p.get("race_id")
                        if rid:
                            rids.add(rid)
        target_rids = sorted(rids)
        print(f"[INFO] pred.json {args.from_pred} 由来: {len(target_rids)} 件")
    else:
        # race_log で fin>1 かつ margin_ahead が 0 or NULL
        rows = conn.execute(
            "SELECT DISTINCT race_id FROM race_log "
            "WHERE finish_pos > 1 AND finish_pos < 90 "
            "AND (margin_ahead = 0 OR margin_ahead IS NULL)"
        ).fetchall()
        target_rids = [r[0] for r in rows if r[0]]
        print(f"[INFO] race_log 由来: {len(target_rids)} 件")

    if args.nar_only:
        target_rids = [r for r in target_rids if r[4:6] not in JRA_VENUES]
        print(f"[INFO] NAR フィルタ後: {len(target_rids)} 件")

    if args.limit > 0:
        target_rids = target_rids[: args.limit]

    session = requests.Session()
    updated_rows = 0
    fetched_races = 0
    no_data = 0
    t0 = time.time()

    for i, rid in enumerate(target_rids):
        if i > 0 and i % 10 == 0:
            print(f"  {i}/{len(target_rids)} ({time.time()-t0:.0f}s) updated={updated_rows} no_data={no_data}", flush=True)

        time.sleep(args.sleep)
        results = fetch_result(rid, session)
        if not results:
            no_data += 1
            continue
        fetched_races += 1

        if args.dry_run:
            continue

        # race_log UPDATE
        for r in results:
            if r["margin"] is None:
                continue
            try:
                cur = conn.execute(
                    "UPDATE race_log SET margin_ahead=? WHERE race_id=? AND horse_no=?",
                    (r["margin"], rid, r["horse_no"]),
                )
                if cur.rowcount > 0:
                    updated_rows += 1
            except Exception:
                pass
        conn.commit()

    print(f"\n[完了 {time.time()-t0:.0f}s]")
    print(f"  対象 race_id: {len(target_rids)}")
    print(f"  fetched: {fetched_races}")
    print(f"  no_data: {no_data}")
    print(f"  race_log.margin_ahead UPDATE: {updated_rows} 行")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
