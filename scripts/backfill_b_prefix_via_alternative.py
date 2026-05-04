#!/usr/bin/env python3
"""
backfill_b_prefix_via_alternative.py — horses テーブルの B_prefix 馬に NAR lineage_code 補完
                                       (netkeiba 不使用版)

horses テーブルで horse_id が B_XXXXXXXX 形式 (暫定 NAR ID) の馬について、
race_log のレース情報から NAR 公式 DebaTable にアクセスし、
馬名マッチングで NAR lineage_code を取得して horses.netkeiba_id に格納する。

なぜ netkeiba_id 列か:
  netkeiba_id は実態として「NAR 公式 lineage_code の互換格納先」として使用されている。

フロー:
  1. B_prefix 馬の horse_id と horse_name を取得
  2. 各馬の race_log から代表 race_id を取得
  3. race_id から NAR 公式 DebaTable の URL を組み立て
  4. DebaTable にアクセスし、馬名マッチングで lineage_code を取得
  5. horses.netkeiba_id に格納

制約:
  - NAR 公式のレート制限: 2.0 秒/件以上
  - DebaTable は「当日レース情報」 → 過去レースは取得できない場合あり
  - 2023 年以前のレースは応答なしの可能性あり → 最新 race_id を優先
  - 廃馬・引退馬はどのページにも出走しないため補完不可 (正直に記録)

使い方:
    python scripts/backfill_b_prefix_via_alternative.py --dry-run
    python scripts/backfill_b_prefix_via_alternative.py --execute
    python scripts/backfill_b_prefix_via_alternative.py --execute --max-fetch 50
    python scripts/backfill_b_prefix_via_alternative.py --execute --reset-done
"""

import argparse
import os
import re
import shutil
import sqlite3
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

import requests
from bs4 import BeautifulSoup

from config.settings import DATABASE_PATH
from src.log import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────
# 定数
# ─────────────────────────────────────────────────────────────

_BASE_NAR = "https://www.keiba.go.jp/KeibaWeb"
_REQ_INTERVAL = 2.1  # 2.0秒以上を確実に担保
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
}

# netkeiba venue_code → NAR baba_code
_VC_TO_BABA: Dict[str, str] = {
    "30": "36",  # 門別
    "35": "10",  # 盛岡
    "36": "11",  # 水沢
    "42": "18",  # 浦和
    "43": "19",  # 船橋
    "44": "20",  # 大井
    "45": "21",  # 川崎
    "46": "22",  # 金沢
    "47": "23",  # 笠松
    "48": "24",  # 名古屋
    "49": "27",  # 園田 旧
    "50": "27",  # 園田 正規
    "51": "28",  # 姫路
    "54": "31",  # 高知
    "55": "32",  # 佐賀
    "65": "3",   # 帯広
}

# バックアップ・完了マーカーパス
BACKUP_DIR = os.path.join(os.path.dirname(DATABASE_PATH), "backups")
DONE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "tmp", "backfill_bprefix_done.txt"
)
FAIL_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "tmp", "backfill_bprefix_fail.txt"
)


# ─────────────────────────────────────────────────────────────
# ユーティリティ
# ─────────────────────────────────────────────────────────────

def _backup_db(db_path: str) -> str:
    """DB をタイムスタンプ付きでバックアップ"""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(BACKUP_DIR, f"keiba_{ts}_pre_bprefix_backfill.db")
    shutil.copy2(db_path, dest)
    print(f"[バックアップ] {dest}")
    return dest


def _load_set(path: str) -> set:
    s = set()
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                v = line.strip()
                if v:
                    s.add(v)
    return s


def _append_line(path: str, value: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(value + "\n")


# ─────────────────────────────────────────────────────────────
# DB クエリ
# ─────────────────────────────────────────────────────────────

def _get_target_horses(conn: sqlite3.Connection) -> List[Tuple[str, str]]:
    """
    netkeiba_id が NULL の B_prefix 馬を全件取得。
    返却: [(horse_id, horse_name), ...]
    """
    rows = conn.execute("""
        SELECT horse_id, COALESCE(horse_name, '') AS horse_name
        FROM horses
        WHERE horse_id LIKE 'B_%'
          AND (netkeiba_id IS NULL OR netkeiba_id = '')
        ORDER BY horse_id
    """).fetchall()
    return [(r[0], r[1]) for r in rows]


def _get_best_race_ids(conn: sqlite3.Connection, horse_id: str) -> List[Tuple[str, str, str]]:
    """
    指定 horse_id の race_log から NAR レース (venue_code が NAR 会場) の
    race_id を最新順で返す。
    返却: [(race_id, race_date, venue_code), ...]
    """
    rows = conn.execute("""
        SELECT race_id, race_date, venue_code
        FROM race_log
        WHERE horse_id = ?
          AND race_id IS NOT NULL
          AND race_id != ''
          AND is_jra = 0
        ORDER BY race_date DESC
        LIMIT 10
    """, (horse_id,)).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


# ─────────────────────────────────────────────────────────────
# NAR公式スクレイピング
# ─────────────────────────────────────────────────────────────

class NARDebaTableFetcher:
    """NAR公式 DebaTable から lineage_code を取得するクラス"""

    def __init__(self):
        self._sess = requests.Session()
        self._sess.headers.update(_HEADERS)
        self._last_req = 0.0

    def _wait(self):
        elapsed = time.time() - self._last_req
        if elapsed < _REQ_INTERVAL:
            time.sleep(_REQ_INTERVAL - elapsed)
        self._last_req = time.time()

    def fetch_lineage_code(
        self, race_id: str, race_date: str, venue_code: str, target_horse_name: str
    ) -> Optional[str]:
        """
        DebaTable から対象馬名に一致する lineage_code を返す。
        見つからない場合は None。

        Args:
            race_id: 12桁の race_id
            race_date: "YYYY-MM-DD"
            venue_code: netkeiba 形式の venue_code (2桁)
            target_horse_name: 検索対象の馬名
        """
        vc_str = str(venue_code).zfill(2)
        baba_code = _VC_TO_BABA.get(vc_str)
        if not baba_code:
            logger.debug("venue_code=%s に対応する baba_code なし", venue_code)
            return None

        if not race_id or len(race_id) < 12:
            logger.debug("race_id 不正: %s", race_id)
            return None

        try:
            race_no = int(race_id[10:12])
        except ValueError:
            return None

        date_nar = race_date.replace("-", "/")
        url = (
            f"{_BASE_NAR}/TodayRaceInfo/DebaTable"
            f"?k_raceDate={date_nar}&k_raceNo={race_no}&k_babaCode={baba_code}"
        )

        self._wait()
        try:
            resp = self._sess.get(url, timeout=15)
        except requests.RequestException as e:
            logger.debug("DebaTable 取得失敗: %s → %s", url, e)
            return None

        if resp.status_code != 200:
            logger.debug("DebaTable HTTP %d: %s", resp.status_code, url)
            return None

        return self._match_horse(resp.text, target_horse_name, race_id)

    def _match_horse(self, html: str, target_name: str, race_id: str) -> Optional[str]:
        """HTML 内の馬名リンクから target_name に一致する lineage_code を返す"""
        soup = BeautifulSoup(html, "html.parser")
        links = soup.select("a[href*='lineageLoginCode']")

        for link in links:
            horse_name = link.get_text(strip=True)
            m = re.search(r"k_lineageLoginCode=(\d+)", link.get("href", ""))
            if not m:
                continue
            lineage_code = m.group(1)

            # 完全一致
            if horse_name == target_name:
                logger.debug("馬名完全一致: %s → lineage=%s", target_name, lineage_code)
                return lineage_code

            # 部分一致 (カタカナ揺れ対応: 長音符など)
            if target_name and len(target_name) >= 3:
                if target_name in horse_name or horse_name in target_name:
                    logger.debug("馬名部分一致: %s ≈ %s → lineage=%s",
                                 target_name, horse_name, lineage_code)
                    return lineage_code

        return None


# ─────────────────────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="horses の B_prefix 馬に NAR lineage_code を補完 (netkeiba 不使用)"
    )
    parser.add_argument("--dry-run", action="store_true", help="件数確認のみ、DB更新なし")
    parser.add_argument("--execute", action="store_true", help="実際に DB 更新")
    parser.add_argument("--max-fetch", type=int, default=0, help="最大取得件数 (0=無制限)")
    parser.add_argument("--reset-done", action="store_true", help="完了マーカーをリセット")
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("--dry-run または --execute を指定してください")
        parser.print_help()
        sys.exit(1)

    dry_run = args.dry_run
    mode = "DRY-RUN" if dry_run else "EXECUTE"
    print(f"\n{'='*60}")
    print(f"backfill_b_prefix_via_alternative  [{mode}]")
    print(f"{'='*60}")

    # 中断再開マーカー
    if args.reset_done:
        for f in [DONE_FILE, FAIL_FILE]:
            if os.path.exists(f):
                os.remove(f)
        print("[リセット] 完了・失敗マーカーを削除しました")

    done_set = _load_set(DONE_FILE)
    fail_set = _load_set(FAIL_FILE)
    if done_set:
        print(f"[再開] 完了済み: {len(done_set)}件")
    if fail_set:
        print(f"[再開] 既知失敗: {len(fail_set)}件 (スキップ)")

    # DB接続
    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    # バックアップ (EXECUTE時のみ)
    if not dry_run:
        _backup_db(DATABASE_PATH)

    # 対象取得
    print("\n[1/4] B_prefix 馬 (netkeiba_id=NULL) を取得中...")
    targets = _get_target_horses(conn)
    total = len(targets)
    print(f"  → 対象: {total}件")

    if dry_run:
        # DRY-RUN: race_log に NAR レースがある馬の件数を確認
        has_nar_race = 0
        no_nar_race = 0
        for horse_id, horse_name in targets:
            races = _get_best_race_ids(conn, horse_id)
            if races:
                has_nar_race += 1
            else:
                no_nar_race += 1
        print(f"\n[DRY-RUN 分析]")
        print(f"  NAR race_log あり: {has_nar_race}件  → lineage_code 取得試行可能")
        print(f"  NAR race_log なし: {no_nar_race}件  → 補完不可 (race_log 未登録)")
        print(f"\n  ※ NAR 公式 DebaTable は過去レースが応答しない場合あり")
        print(f"     (2025-2026 の最新レースから優先取得)")
        print(f"\n[DRY-RUN完了] --execute で実行すると最大 {has_nar_race}件 の補完を試みます")
        print(f"  ※ 実際の成功率は DebaTable の可用性に依存 (推定 30-50%)")
        conn.close()
        return

    # 本実行
    print(f"\n[2/4] NAR 公式 DebaTable で lineage_code を取得中...")
    print(f"      レート: {_REQ_INTERVAL}秒/件以上 | max-fetch={args.max_fetch or '無制限'}")
    fetcher = NARDebaTableFetcher()

    found = 0
    not_found = 0
    skipped_done = 0
    skipped_fail = 0
    no_race = 0
    fetch_count = 0

    for i, (horse_id, horse_name) in enumerate(targets, 1):
        if horse_id in done_set:
            skipped_done += 1
            continue
        if horse_id in fail_set:
            skipped_fail += 1
            continue

        if args.max_fetch > 0 and fetch_count >= args.max_fetch:
            print(f"\n  [上限到達] --max-fetch={args.max_fetch} 件に達しました")
            break

        # race_log から NAR レースを取得
        nar_races = _get_best_race_ids(conn, horse_id)
        if not nar_races:
            no_race += 1
            _append_line(FAIL_FILE, horse_id)
            continue

        # 最新 race から順に試行
        lineage_code = None
        tried = 0
        for race_id, race_date, venue_code in nar_races:
            fetch_count += 1
            tried += 1
            lc = fetcher.fetch_lineage_code(race_id, race_date, venue_code, horse_name)
            if lc:
                lineage_code = lc
                break
            # 1 馬につき最大 3 レース試行
            if tried >= 3:
                break

        # バー表示
        pct = (i / total) * 100
        bar_len = 25
        filled = int(bar_len * pct / 100)
        bar = "█" * filled + "░" * (bar_len - filled)
        print(
            f"\r  [{bar}] {pct:.1f}%  found={found} not_found={not_found} "
            f"no_race={no_race}  {horse_name[:12]:12s}",
            end="", flush=True
        )

        if lineage_code:
            # horses.netkeiba_id を更新
            conn.execute(
                "UPDATE horses SET netkeiba_id=?, updated_at=? WHERE horse_id=?",
                (lineage_code, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), horse_id)
            )
            conn.commit()
            found += 1
            _append_line(DONE_FILE, horse_id)
            logger.info("[%d/%d] %s %s → lineage=%s", i, total, horse_id, horse_name, lineage_code)
        else:
            not_found += 1
            _append_line(FAIL_FILE, horse_id)

    print()  # 改行

    # 結果確認
    print(f"\n[3/4] 結果確認...")
    cur = conn.execute(
        "SELECT COUNT(*), COUNT(netkeiba_id) FROM horses WHERE horse_id LIKE 'B_%'"
    )
    b_total, b_with_id = cur.fetchone()
    conn.close()

    print(f"\n{'='*60}")
    print(f"  lineage_code 取得成功: {found}件")
    print(f"  取得失敗 (DebaTable 応答なし/馬名不一致): {not_found}件")
    print(f"  race_log なし (補完不可): {no_race}件")
    print(f"  スキップ (完了済): {skipped_done}件")
    print(f"  スキップ (既知失敗): {skipped_fail}件")
    print(f"  ---")
    print(f"  B_prefix 馬 総数: {b_total}件")
    print(f"  netkeiba_id 補完済: {b_with_id}件  ({b_with_id/b_total*100:.1f}%)")
    print(f"  未補完残り: {b_total - b_with_id}件")
    print(f"{'='*60}")
    print()
    print("[注意] 補完できなかった馬の主な理由:")
    print("  1. DebaTable は当日・直近レース用のため、過去レースが取得できない場合あり")
    print("  2. 廃馬・引退馬は現在出走しないため取得不可")
    print("  3. 帯広 (venue_code=65) は DebaTable 非対応の可能性あり")
    print("  4. 馬名がrace_logに空のまま登録されている場合は補完不可")


if __name__ == "__main__":
    main()
