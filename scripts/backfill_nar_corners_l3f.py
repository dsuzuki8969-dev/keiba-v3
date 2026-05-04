"""
NAR race_log 通過順 / 上り3F バックフィルスクリプト

既存の nar.netkeiba.com_race_result キャッシュ (lz4) を再パースして
race_log の positions_corners と last_3f_sec が空のレコードを補完する。

netkeiba には一切アクセスしない（キャッシュのみ使用）。

使い方:
    python scripts/backfill_nar_corners_l3f.py           # dry-run (変更なし)
    python scripts/backfill_nar_corners_l3f.py --execute  # 実際に更新
    python scripts/backfill_nar_corners_l3f.py --days 60  # 直近60日分
    python scripts/backfill_nar_corners_l3f.py --venue 54 # 特定 venue_code のみ
"""

import argparse
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
_PROJ_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJ_ROOT))

try:
    import lz4.frame
except ImportError:
    print("[ERROR] lz4 がインストールされていません: pip install lz4", flush=True)
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("[ERROR] beautifulsoup4 がインストールされていません: pip install beautifulsoup4", flush=True)
    sys.exit(1)

_DB_PATH = str(_PROJ_ROOT / "data" / "keiba.db")
_CACHE_DIR = str(_PROJ_ROOT / "data" / "cache")

# ばんえい競馬(帯広) venue_code
_BANEI_VC = "65"


def _load_html_cache(race_id: str) -> str | None:
    """race_id に対応するキャッシュ HTML を返す (lz4 優先)"""
    for prefix in [
        "nar.netkeiba.com_race_result.html_race_id=",
        "race.netkeiba.com_race_result.html_race_id=",
    ]:
        for ext in [".html.lz4", ".html"]:
            path = os.path.join(_CACHE_DIR, f"{prefix}{race_id}{ext}")
            if os.path.exists(path):
                try:
                    if ext == ".html.lz4":
                        with open(path, "rb") as f:
                            return lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
                    else:
                        with open(path, "r", encoding="utf-8", errors="replace") as f:
                            return f.read()
                except Exception as e:
                    print(f"  [WARN] キャッシュ読込失敗 {path}: {e}", flush=True)
    return None


def _parse_l3f(soup: "BeautifulSoup", is_banei: bool) -> dict:
    """RaceTable01 から {horse_no: last_3f_sec} を抽出"""
    l3f_map = {}
    table = soup.select_one("table.race_table_01") or soup.select_one("table.RaceTable01")
    if not table:
        return l3f_map

    # ばんえいは上り3Fが60-150秒、通常は28-50秒
    l3f_min = 60.0 if is_banei else 28.0
    l3f_max = 150.0 if is_banei else 50.0

    for row in table.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        ft = cells[0].get_text(strip=True)
        if not ft.isdigit():
            continue
        hno_t = cells[2].get_text(strip=True)
        if not hno_t.isdigit():
            continue
        hno = int(hno_t)
        for ci in range(7, min(len(cells), 16)):
            t_text = cells[ci].get_text(strip=True)
            # 通常: "38.1" / ばんえい: "75.4" / 3桁の場合: "100.0"
            if re.match(r"^\d{2,3}\.\d$", t_text):
                val = float(t_text)
                if l3f_min <= val <= l3f_max:
                    l3f_map[hno] = val
                    break
    return l3f_map


def _parse_corners(soup: "BeautifulSoup") -> dict:
    """Corner_Num テーブルから {horse_no: [corner_pos, ...]} を抽出

    Returns:
        {馬番: [コーナー別通過順位リスト]}
    """
    ctable = soup.select_one("table.Corner_Num")
    if not ctable:
        return {}

    corner_orders = {}  # {corner_idx: {horse_no: rank}}
    for tr in ctable.select("tr"):
        cells_c = tr.find_all(["th", "td"])
        if len(cells_c) < 2:
            continue
        m = re.search(r"(\d)", cells_c[0].get_text(strip=True))
        if not m:
            continue
        ci = int(m.group(1))
        raw = cells_c[1].get_text()
        # 末尾の除外馬 "=X" を除去してから = を , に変換
        raw = re.sub(r"\s*=\s*\d+\s*$", "", raw)
        raw = raw.replace("（", "(").replace("）", ")")
        raw = raw.replace("=", ",")

        horse_pos = {}
        pos = 1
        i = 0
        while i < len(raw):
            ch = raw[i]
            if ch == "(":
                end = raw.find(")", i)
                if end < 0:
                    end = len(raw)
                group_text = raw[i + 1:end]
                group_nos = [
                    int(x.strip())
                    for x in re.split(r"[,\-]", group_text)
                    if x.strip().isdigit()
                ]
                for hno in group_nos:
                    horse_pos[hno] = pos
                pos += len(group_nos)
                i = end + 1
            elif ch.isdigit():
                j = i
                while j < len(raw) and raw[j].isdigit():
                    j += 1
                hno = int(raw[i:j])
                if 1 <= hno <= 30:
                    horse_pos[hno] = pos
                    pos += 1
                i = j
            else:
                i += 1
        corner_orders[ci] = horse_pos

    if not corner_orders:
        return {}

    # horse_no -> [corner_pos_list] に変換
    result = {}
    for ci_key in sorted(corner_orders.keys()):
        for hno, rank in corner_orders[ci_key].items():
            result.setdefault(hno, []).append(rank)
    return result


def run_backfill(
    days: int = 30,
    venue_filter: str | None = None,
    execute: bool = False,
) -> dict:
    """
    メインバックフィル処理

    Args:
        days: 直近N日分を対象にする
        venue_filter: 特定 venue_code のみ処理 (例: "54")
        execute: True なら DB を実際に更新

    Returns:
        {"l3f_target": N, "corner_target": N, "l3f_updated": N, "corner_updated": N}
    """
    stats = {
        "l3f_target": 0,
        "corner_target": 0,
        "l3f_updated": 0,
        "corner_updated": 0,
        "no_cache": 0,
        "skipped_banei_corner": 0,
    }

    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row

    # 対象 race_id を取得
    venue_clause = f" AND SUBSTR(race_id, 5, 2) = '{venue_filter}'" if venue_filter else ""

    l3f_rows = conn.execute(
        f"SELECT DISTINCT race_id, SUBSTR(race_id, 5, 2) as vc "
        f"FROM race_log "
        f"WHERE (last_3f_sec IS NULL OR last_3f_sec = 0) "
        f"  AND finish_pos > 0 AND finish_pos < 90 "
        f"  AND race_date >= date('now', '-{days} days') "
        f"  AND is_jra = 0"
        f"{venue_clause}"
    ).fetchall()

    corner_rows = conn.execute(
        f"SELECT DISTINCT race_id, SUBSTR(race_id, 5, 2) as vc "
        f"FROM race_log "
        f"WHERE (positions_corners IS NULL OR positions_corners = '' OR positions_corners = '[]') "
        f"  AND finish_pos > 0 AND finish_pos < 90 "
        f"  AND race_date >= date('now', '-{days} days') "
        f"  AND is_jra = 0 "
        f"  AND SUBSTR(race_id, 5, 2) != '{_BANEI_VC}'"  # ばんえいはコーナーなし
        f"{venue_clause}"
    ).fetchall()

    l3f_ids = {r["race_id"]: r["vc"] for r in l3f_rows}
    corner_ids = {r["race_id"]: r["vc"] for r in corner_rows}
    all_ids = set(l3f_ids) | set(corner_ids)

    stats["l3f_target"] = sum(
        conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE race_id = ? AND (last_3f_sec IS NULL OR last_3f_sec = 0)",
            (rid,)
        ).fetchone()[0]
        for rid in l3f_ids
    )
    stats["corner_target"] = sum(
        conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE race_id = ? "
            "AND (positions_corners IS NULL OR positions_corners = '' OR positions_corners = '[]')",
            (rid,)
        ).fetchone()[0]
        for rid in corner_ids
    )

    print(
        f"[バックフィル] 対象: last_3f={len(l3f_ids)}レース({stats['l3f_target']}頭) "
        f"/ corners={len(corner_ids)}レース({stats['corner_target']}頭) "
        f"/ ばんえいcorners除外",
        flush=True,
    )

    for idx, race_id in enumerate(sorted(all_ids)):
        vc = l3f_ids.get(race_id) or corner_ids.get(race_id, "")
        is_banei = (vc == _BANEI_VC)

        # キャッシュ読み込み
        html = _load_html_cache(race_id)
        if not html:
            stats["no_cache"] += 1
            continue

        soup = BeautifulSoup(html, "html.parser")

        # last_3f 抽出
        l3f_map = {}
        if race_id in l3f_ids:
            l3f_map = _parse_l3f(soup, is_banei)

        # corners 抽出 (ばんえいはスキップ)
        corners_map = {}
        if race_id in corner_ids and not is_banei:
            corners_map = _parse_corners(soup)

        if not l3f_map and not corners_map:
            continue

        # DB 取得
        horses = conn.execute(
            "SELECT horse_no, last_3f_sec, positions_corners FROM race_log WHERE race_id = ?",
            (race_id,),
        ).fetchall()

        for h in horses:
            hno = h[0]
            if not hno:
                continue

            # last_3f 更新
            if hno in l3f_map and (not h[1] or h[1] <= 0):
                if execute:
                    conn.execute(
                        "UPDATE race_log SET last_3f_sec = ? WHERE race_id = ? AND horse_no = ?",
                        (l3f_map[hno], race_id, hno),
                    )
                stats["l3f_updated"] += 1

            # corners 更新
            if hno in corners_map:
                new_positions = corners_map[hno]
                old_raw = h[2] or ""
                try:
                    old = json.loads(old_raw) if old_raw.startswith("[") else []
                except Exception:
                    old = []
                if len(new_positions) > len(old) or len(old) <= 1:
                    if execute:
                        conn.execute(
                            "UPDATE race_log SET positions_corners = ?, position_4c = ? "
                            "WHERE race_id = ? AND horse_no = ?",
                            (json.dumps(new_positions), new_positions[-1], race_id, hno),
                        )
                    stats["corner_updated"] += 1

        # 進捗表示 (50件ごと)
        if (idx + 1) % 50 == 0:
            print(
                f"  [{idx + 1}/{len(all_ids)}] last_3f={stats['l3f_updated']}, "
                f"corners={stats['corner_updated']}",
                flush=True,
            )

    if execute:
        conn.commit()
        print("[バックフィル] DB コミット完了", flush=True)
    else:
        print("[バックフィル] dry-run モード: DB は変更していません (--execute を付けると実行)", flush=True)

    conn.close()
    return stats


def main():
    parser = argparse.ArgumentParser(description="NAR race_log 通過順/上り3F バックフィル")
    parser.add_argument("--days", type=int, default=30, help="直近N日分を対象 (デフォルト: 30)")
    parser.add_argument("--venue", type=str, default=None, help="venue_code を絞り込む (例: 54)")
    parser.add_argument("--execute", action="store_true", help="DB を実際に更新する (省略時は dry-run)")
    args = parser.parse_args()

    mode = "実行モード" if args.execute else "dry-run モード"
    print(f"[バックフィル開始] {mode} / 直近{args.days}日 / venue={args.venue or '全場'}", flush=True)

    stats = run_backfill(days=args.days, venue_filter=args.venue, execute=args.execute)

    print(f"\n[バックフィル結果]", flush=True)
    print(f"  last_3f: 対象{stats['l3f_target']}頭 → {'更新' if args.execute else '更新予定'}{stats['l3f_updated']}頭", flush=True)
    print(f"  corners: 対象{stats['corner_target']}頭 → {'更新' if args.execute else '更新予定'}{stats['corner_updated']}頭", flush=True)
    print(f"  キャッシュなし: {stats['no_cache']}レース", flush=True)
    print(f"  ばんえい corners 除外 (構造的に不存在): 確認済み", flush=True)


if __name__ == "__main__":
    main()
