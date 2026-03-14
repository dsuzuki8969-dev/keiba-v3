"""
race_log の jockey_id / trainer_id 空欄をキャッシュHTMLから再パースして埋める。

原因: backfill_race_log.py の正規表現が \\d+ だったため
      NARの英数字ID (a0233 等) を取りこぼしていた。
      + 2026年1-2月の trainer_id シリアライズ漏れ。

実行: python scripts/backfill_missing_ids.py [--dry-run]
"""
import os
import re
import sys
import time
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import lz4.frame
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

from src.database import get_db

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "cache")
JRA_VENUE_CODES = frozenset(["01","02","03","04","05","06","07","08","09","10"])

# 修正済み正規表現 (\w+ で英数字IDも対応)
_RE_JOCKEY_ID  = re.compile(r'/jockey/(?:result/recent/)?(\w+)/?')
_RE_TRAINER_ID = re.compile(r'/trainer/(?:result/recent/)?(\w+)/?')


def _read_html(path: str) -> str:
    if path.endswith('.lz4'):
        if not HAS_LZ4:
            return ""
        with open(path, 'rb') as f:
            return lz4.frame.decompress(f.read()).decode('utf-8', errors='replace')
    with open(path, encoding='utf-8', errors='replace') as f:
        return f.read()


def _cache_path_for_race(race_id: str) -> str:
    vc = race_id[4:6] if len(race_id) >= 6 else ""
    is_jra = vc in JRA_VENUE_CODES
    domain = "race.netkeiba.com" if is_jra else "nar.netkeiba.com"
    fname = f"{domain}_race_result.html_race_id={race_id}.html"
    base = os.path.join(CACHE_DIR, fname)
    # lz4 優先
    if HAS_LZ4 and os.path.exists(base + ".lz4"):
        return base + ".lz4"
    if os.path.exists(base):
        return base
    return ""


def _extract_ids_from_html(html: str, race_id: str):
    """
    HTMLの着順テーブルから horse_no → (jockey_id, trainer_id) のマッピングを返す。
    """
    vc = race_id[4:6] if len(race_id) >= 6 else ""
    is_jra = vc in JRA_VENUE_CODES

    tbl_m = re.search(r'ResultTableWrap.*?<tbody>(.*?)</tbody>', html, re.DOTALL)
    if not tbl_m:
        return {}

    tbody = tbl_m.group(1)
    result = {}

    for tr_m in re.finditer(r'<tr[^>]*>(.*?)</tr>', tbody, re.DOTALL):
        tr_html = tr_m.group(1)
        tds = re.findall(r'<td[^>]*>(.*?)</td>', tr_html, re.DOTALL)
        if len(tds) < 8:
            continue

        def td_text(raw):
            return re.sub(r'<[^>]+>', '', raw).strip()

        finish_str = td_text(tds[0])
        if not finish_str.isdigit():
            continue

        hno_str = td_text(tds[2])
        horse_no = int(hno_str) if hno_str.isdigit() else 0
        if horse_no <= 0:
            continue

        # 騎手ID: jockey_col (tds[6]) 付近のリンクから
        jockey_col = tds[6] if len(tds) > 6 else ""
        jm = _RE_JOCKEY_ID.search(jockey_col)
        jockey_id = jm.group(1) if jm else ""

        # 調教師ID: JRA(15列) tds[13], NAR(14列以下) tds[12]
        if is_jra and len(tds) >= 15:
            trainer_col = tds[13]
        elif len(tds) > 12:
            trainer_col = tds[12]
        else:
            trainer_col = ""
        tm = _RE_TRAINER_ID.search(trainer_col)
        trainer_id = tm.group(1) if tm else ""

        # 調教師名
        trainer_name = td_text(trainer_col)

        result[horse_no] = (jockey_id, trainer_id, trainer_name)

    return result


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = get_db()

    # 欠損レースを取得
    missing_rows = conn.execute("""
        SELECT DISTINCT race_id
        FROM race_log
        WHERE jockey_id = '' OR trainer_id = ''
    """).fetchall()
    race_ids = [r[0] for r in missing_rows]
    print(f"欠損IDを持つレース: {len(race_ids):,} 件")

    if not race_ids:
        print("処理対象なし。")
        return

    updated_jockey = 0
    updated_trainer = 0
    cache_miss = 0
    parse_fail = 0
    t0 = time.time()

    BATCH = 200
    pending_updates = []

    def flush():
        nonlocal updated_jockey, updated_trainer
        if args.dry_run or not pending_updates:
            pending_updates.clear()
            return
        for (rid, hno, jid, tid, tname) in pending_updates:
            if jid:
                conn.execute(
                    "UPDATE race_log SET jockey_id = ? WHERE race_id = ? AND horse_no = ? AND jockey_id = ''",
                    (jid, rid, hno))
            if tid:
                conn.execute(
                    "UPDATE race_log SET trainer_id = ? WHERE race_id = ? AND horse_no = ? AND trainer_id = ''",
                    (tid, rid, hno))
            if tname:
                conn.execute(
                    "UPDATE race_log SET trainer_name = ? WHERE race_id = ? AND horse_no = ? AND trainer_name = ''",
                    (tname, rid, hno))
        conn.commit()
        pending_updates.clear()

    for i, race_id in enumerate(race_ids):
        cache_path = _cache_path_for_race(race_id)
        if not cache_path:
            cache_miss += 1
            continue

        try:
            html = _read_html(cache_path)
        except Exception:
            cache_miss += 1
            continue

        id_map = _extract_ids_from_html(html, race_id)
        if not id_map:
            parse_fail += 1
            continue

        # このレースの欠損行を取得
        rows = conn.execute(
            "SELECT horse_no, jockey_id, trainer_id FROM race_log WHERE race_id = ? AND (jockey_id = '' OR trainer_id = '')",
            (race_id,)).fetchall()

        for (hno, cur_jid, cur_tid) in rows:
            if hno not in id_map:
                continue
            new_jid, new_tid, new_tname = id_map[hno]
            fill_jid = new_jid if (not cur_jid and new_jid) else ""
            fill_tid = new_tid if (not cur_tid and new_tid) else ""
            fill_tname = new_tname if (not cur_tid and new_tname) else ""
            if fill_jid or fill_tid:
                pending_updates.append((race_id, hno, fill_jid, fill_tid, fill_tname))
                if fill_jid:
                    updated_jockey += 1
                if fill_tid:
                    updated_trainer += 1

        if len(pending_updates) >= BATCH:
            flush()

        if (i + 1) % 2000 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            print(f"  [{i+1:,}/{len(race_ids):,}] jockey: +{updated_jockey:,}, trainer: +{updated_trainer:,}, "
                  f"{rate:.0f} races/s")

    flush()
    elapsed = time.time() - t0

    print(f"\n完了 ({elapsed:.1f}秒)")
    print(f"  jockey_id 更新: {updated_jockey:,}")
    print(f"  trainer_id 更新: {updated_trainer:,}")
    print(f"  キャッシュなし: {cache_miss:,}")
    print(f"  パース失敗: {parse_fail:,}")
    if args.dry_run:
        print("  (dry-run: 実際の更新なし)")


if __name__ == "__main__":
    main()
