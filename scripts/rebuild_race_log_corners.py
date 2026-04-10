"""
race_logのpositions_corners + position_4cを
HTMLキャッシュ（result.htmlのCorner_Numテーブル）から全件再構築する。

問題: MLデータ経由で投入されたpositions_cornersが不正確
  - スクレイピングタイミングで通過順が未反映だった
  - 1要素のみ（4角位置のみ）で全コーナー通過順がない
原因: MLデータ生成時のnetkeiba馬ページの通過順がレース直後で未確定
解決: result.htmlのCorner_Numテーブル（確定データ）から再取得

Usage:
    python scripts/rebuild_race_log_corners.py           # 全件
    python scripts/rebuild_race_log_corners.py --dry-run  # 確認のみ
"""

import sqlite3
import os
import sys
import io
import re
import json
import time
import argparse
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATABASE_PATH


def parse_corner_table(html: str) -> dict:
    """
    result.htmlのCorner_Numテーブルをパースする。
    Returns: {corner_index: {horse_no: position}} 例: {3: {5:1, 7:2, 12:3}, 4: {5:1, 7:2}}

    Corner_Numの書式例:
      3コーナー: 11-9-(4,10)(6,13)(7,14,15)(2,8)(3,12,16)1=5
      4コーナー: 11,9,10,4,13(6,7,15)14(2,8,12,16)3,1=5
    カッコは同着（同位置）。ハイフンとカンマは区切り。=以降は除外馬。
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.Corner_Num")
    if not table:
        return {}

    corner_orders = {}
    for tr in table.select("tr"):
        th = tr.select_one("th")
        td = tr.select_one("td")
        if not th or not td:
            continue
        m = re.search(r"(\d)", th.get_text(strip=True))
        if not m:
            continue
        ci = int(m.group(1))

        raw = td.get_text()
        # 末尾の除外馬表記を除去（末尾に "=番号" がある場合のみ）
        # 注意: "9=6,5..." の = は「大差」セパレータ。末尾 "...=5" は除外馬
        raw = re.sub(r'\s*=\s*(\d+)\s*$', '', raw)
        # 全角→半角カッコ統一
        raw = raw.replace("（", "(").replace("）", ")")
        # 大差セパレータ "=" を通常セパレータとして扱う
        raw = raw.replace("=", ",")

        # トークン化: カッコ内グループと個別馬番を区別して抽出
        # 結果: {horse_no: position} のマッピング
        horse_pos = {}
        pos = 1  # 現在の位置（1始まり）
        i = 0
        while i < len(raw):
            ch = raw[i]
            if ch == '(':
                # カッコ内: 同着グループ
                end = raw.find(')', i)
                if end < 0:
                    end = len(raw)
                group_text = raw[i+1:end]
                group_nos = [int(x.strip()) for x in re.split(r'[,\-]', group_text) if x.strip().isdigit()]
                for hno in group_nos:
                    horse_pos[hno] = pos
                pos += len(group_nos)
                i = end + 1
            elif ch.isdigit():
                # 個別馬番（複数桁対応）
                j = i
                while j < len(raw) and raw[j].isdigit():
                    j += 1
                hno = int(raw[i:j])
                horse_pos[hno] = pos
                pos += 1
                i = j
            else:
                # 区切り文字（カンマ、ハイフン、スペース等）→スキップ
                i += 1

        corner_orders[ci] = horse_pos

    return corner_orders


def get_position_for_horse(corner_orders: dict, horse_no: int) -> tuple:
    """
    corner_ordersから指定馬番の各コーナー通過順位と4角位置を返す。
    corner_orders: {corner_index: {horse_no: position}} （同着対応）
    Returns: (positions_list, position_4c)
    """
    positions = []
    for ci in sorted(corner_orders.keys()):
        pos_map = corner_orders[ci]
        if horse_no in pos_map:
            positions.append(pos_map[horse_no])
        # Corner_Numに馬番がない（取消・大幅出遅れ等）→ スキップ

    position_4c = positions[-1] if positions else 0
    return positions, position_4c


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="確認のみ、DB更新なし")
    parser.add_argument("--limit", type=int, default=0, help="処理レース数上限（0=無制限）")
    args = parser.parse_args()

    import lz4.frame

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "cache")

    # 全race_idを取得
    race_ids = conn.execute(
        "SELECT DISTINCT race_id FROM race_log WHERE race_id IS NOT NULL AND race_id != '' ORDER BY race_id"
    ).fetchall()
    total_races = len(race_ids)
    print(f"対象レース数: {total_races}")

    updated = 0
    skipped = 0
    no_cache = 0
    errors = 0
    fixed_corners = 0
    t0 = time.time()

    for idx, row in enumerate(race_ids):
        race_id = row["race_id"]

        if args.limit and idx >= args.limit:
            break

        # 進捗表示
        if (idx + 1) % 1000 == 0 or idx == 0:
            elapsed = time.time() - t0
            remaining = elapsed / (idx + 1) * (total_races - idx - 1) if idx > 0 else 0
            print(f"  ({idx+1}/{total_races}) {idx/total_races*100:.1f}% "
                  f"updated={updated} fixed={fixed_corners} nocache={no_cache} "
                  f"経過{elapsed:.0f}秒 残り{remaining:.0f}秒")

        # HTMLキャッシュ読み込み
        html = None
        for prefix in ["nar.netkeiba.com_race_result.html_race_id=",
                        "race.netkeiba.com_race_result.html_race_id="]:
            key = f"{prefix}{race_id}"
            for ext in [".html.lz4", ".html"]:
                path = os.path.join(cache_dir, f"{key}{ext}")
                if os.path.exists(path):
                    try:
                        if ext == ".html.lz4":
                            with open(path, "rb") as f:
                                html = lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
                        else:
                            with open(path, "r", encoding="utf-8", errors="replace") as f:
                                html = f.read()
                        break
                    except Exception:
                        pass
            if html:
                break

        if not html:
            no_cache += 1
            continue

        # Corner_Numパース
        try:
            corner_orders = parse_corner_table(html)
        except Exception:
            errors += 1
            continue

        if not corner_orders:
            skipped += 1
            continue

        # このレースの全馬を取得
        horses = conn.execute(
            "SELECT horse_no, positions_corners, position_4c FROM race_log WHERE race_id = ?",
            (race_id,)
        ).fetchall()

        for h in horses:
            horse_no = h["horse_no"]
            if not horse_no:
                continue

            positions, pos_4c = get_position_for_horse(corner_orders, horse_no)
            if not positions or all(p == 0 for p in positions):
                continue

            new_corners = json.dumps(positions)
            old_corners = h["positions_corners"] or ""
            old_4c = h["position_4c"] or 0

            # 変更があるか
            if new_corners != old_corners or pos_4c != old_4c:
                fixed_corners += 1
                if not args.dry_run:
                    conn.execute(
                        "UPDATE race_log SET positions_corners = ?, position_4c = ? "
                        "WHERE race_id = ? AND horse_no = ?",
                        (new_corners, pos_4c, race_id, horse_no)
                    )

        updated += 1

    if not args.dry_run:
        conn.commit()

    conn.close()

    elapsed = time.time() - t0
    print(f"\n完了: {elapsed:.0f}秒")
    print(f"  処理レース: {updated}")
    print(f"  修正した通過順: {fixed_corners}")
    print(f"  キャッシュなし: {no_cache}")
    print(f"  Corner_Numなし: {skipped}")
    print(f"  エラー: {errors}")
    if args.dry_run:
        print("  ※ dry-runモード: DB更新なし")


if __name__ == "__main__":
    main()
