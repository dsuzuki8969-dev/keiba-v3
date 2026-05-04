#!/usr/bin/env python3
"""
backfill_b_prefix_horses.py — B_prefix 馬の netkeiba_id 補完スクリプト

horses テーブルに horse_id が 'B_XXXXXXXX' 形式 (例: B200900003) で存在する 1,253 件について、
netkeiba 馬名検索ページで対応する netkeiba horse_id を特定し、
horses.netkeiba_id カラムに UPDATE する。

背景:
  B_prefix は 2026-04-28 horses マスター D Phase 1+2+3 整理時に
  NAR 公式コードとの突合ができなかった馬に割り当てた暫定 ID。
  horse_name はすでに入っているので馬名 → netkeiba horse_id の逆引きを行う。

アプローチ (B案): netkeiba 馬名検索 → horse_id 候補取得
  URL: https://db.netkeiba.com/?pid=horse_search_detail
  パラメータ: name=<馬名カタカナ>

安全装置:
  - --execute 必須 (省略時は --dry-run として動作)
  - 危険時間帯 (06:00-06:30 / 22:00-23:30) は自動 abort
  - 競合プロセス検出時 abort (run_analysis_date.py 等)
  - 中断再開対応 (tmp/backfill_b_prefix_done.txt)
  - netkeiba レート制限 2.0 秒/件以上厳守

使い方:
    # 件数と推定所要時間のみ確認 (DB 変更なし)
    python scripts/backfill_b_prefix_horses.py --dry-run

    # 本実行 (マスター起床後・T-063b 完了後に実行)
    python scripts/backfill_b_prefix_horses.py --execute

    # smoke test (先頭 20 件のみ)
    python scripts/backfill_b_prefix_horses.py --execute --max-fetch 20

推定所要時間: 1,253 件 × 2.0 秒 = 約 42 分
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── UTF-8 出力 (Windows 対応) ─────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DATABASE_PATH, CACHE_DIR
from src.log import get_logger

logger = get_logger(__name__)

# ── 定数 ──────────────────────────────────────────────────────────────────────
# レート制限: 2.0 秒 / 件 (違反歴 1 回、絶対厳守)
RATE_LIMIT_SEC = 2.0

# 中断再開マーカー
DONE_MARKER_FILE = Path(__file__).resolve().parent.parent / "tmp" / "backfill_b_prefix_done.txt"

# 危険時間帯 (netkeiba 並列実行スケジュールと重複する時間帯)
DANGER_HOURS = [
    (6, 0, 6, 30),    # 06:00-06:30: DAI_Keiba_Predict
    (22, 0, 23, 30),  # 22:00-23:30: DAI_Keiba_Results + DAI_Keiba_Maintenance
]

# 競合プロセスキーワード (これらが動いていたら abort)
CONFLICT_PROCESSES = [
    "run_analysis_date.py",
    "backfill_race_log",
    "backfill_horses_2023h",
    "backfill_b_prefix",  # 自己重複防止 (別プロセス)
]

# netkeiba 馬名検索 URL
NETKEIBA_SEARCH_URL = "https://db.netkeiba.com/"
SEARCH_PARAMS_BASE = {
    "pid": "horse_search_detail",
    "match": "1",   # 完全一致
}

# バックアップ保存先
BACKUP_DIR = Path(DATABASE_PATH).parent / "backups"

# ── ユーティリティ ─────────────────────────────────────────────────────────────

def _is_danger_time() -> bool:
    """現在が危険時間帯かどうかを返す"""
    now = datetime.now()
    h, m = now.hour, now.minute
    for (sh, sm, eh, em) in DANGER_HOURS:
        start_min = sh * 60 + sm
        end_min = eh * 60 + em
        now_min = h * 60 + m
        if start_min <= now_min < end_min:
            return True
    return False


def _check_conflict_processes() -> list[str]:
    """競合プロセスが動いていればそのリストを返す"""
    try:
        result = subprocess.run(
            ["ps", "-ef"],
            capture_output=True, text=True, timeout=5
        )
        lines = result.stdout.splitlines()
        conflicts = []
        for proc in CONFLICT_PROCESSES:
            for line in lines:
                if proc in line and "grep" not in line:
                    conflicts.append(proc)
                    break
        return conflicts
    except Exception:
        return []  # 確認できない場合は通過させる


def _load_done_ids() -> set[str]:
    """中断再開マーカーから処理済み horse_id を読み込む"""
    if not DONE_MARKER_FILE.exists():
        return set()
    done = set()
    with open(DONE_MARKER_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                done.add(line)
    return done


def _mark_done(horse_id: str) -> None:
    """処理済み horse_id をマーカーファイルに追記"""
    DONE_MARKER_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DONE_MARKER_FILE, "a", encoding="utf-8") as f:
        f.write(horse_id + "\n")


def _backup_db() -> str:
    """DB をタイムスタンプ付きでバックアップし、パスを返す"""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = BACKUP_DIR / f"keiba_{ts}_pre_b_prefix_backfill.db"
    import shutil
    shutil.copy2(DATABASE_PATH, str(dest))
    print(f"[バックアップ] {dest}")
    return str(dest)


def _progress_bar(done: int, total: int, width: int = 25) -> str:
    if total <= 0:
        return f"[{'?' * width}] ?%"
    pct = done / total * 100
    filled = int(width * done / total)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {pct:.1f}% ({done:,}/{total:,})"


# ── DB 操作 ───────────────────────────────────────────────────────────────────

def _get_b_prefix_horses(conn: sqlite3.Connection) -> list[dict]:
    """horses テーブルから B_prefix の馬を全件取得"""
    rows = conn.execute(
        """
        SELECT horse_id, horse_name, birth_year, sex, is_jra, netkeiba_id
        FROM horses
        WHERE horse_id LIKE 'B_%'
        ORDER BY horse_id
        """
    ).fetchall()
    return [
        {
            "horse_id": r[0],
            "horse_name": r[1],
            "birth_year": r[2],
            "sex": r[3],
            "is_jra": r[4],
            "netkeiba_id": r[5],
        }
        for r in rows
    ]


def _guess_birth_year_from_id(horse_id: str) -> int | None:
    """B200900003 → 2009 を推定 (B + 4桁年 + 5桁連番)"""
    m = re.match(r"^B(\d{4})\d{5}$", horse_id)
    if m:
        year = int(m.group(1))
        # 合理的な範囲チェック
        if 1990 <= year <= 2030:
            return year
    return None


# ── netkeiba 馬名検索 ─────────────────────────────────────────────────────────

def _search_netkeiba_horse_id(
    client,
    horse_name: str,
    birth_year: int | None,
) -> list[dict]:
    """
    netkeiba 馬名検索で horse_id 候補リストを返す。

    Returns:
        [{"horse_id": str, "horse_name": str, "birth_year": int|None, "sex": str}, ...]
    """
    try:
        params = dict(SEARCH_PARAMS_BASE)
        params["name"] = horse_name
        if birth_year:
            params["birthday[0]"] = str(birth_year)
            params["birthday[1]"] = str(birth_year)

        soup = client.get(NETKEIBA_SEARCH_URL, params=params)
        if soup is None:
            return []

        # 検索結果テーブルから horse_id を抽出
        # テーブル内の馬リンク: /horse/<id>/
        candidates = []
        for a in soup.select("a[href*='/horse/']"):
            href = a.get("href", "")
            m = re.search(r"/horse/([A-Za-z]?\d+)", href)
            if not m:
                continue
            nk_id = m.group(1)

            # 馬名取得
            name_text = a.get_text(strip=True)
            if not name_text:
                continue

            # 行の生年・性別情報を取得 (td の兄弟要素)
            tr = a.find_parent("tr")
            found_year = None
            found_sex = ""
            if tr:
                tds = tr.find_all("td")
                for td in tds:
                    txt = td.get_text(strip=True)
                    # 生年: 4桁数字
                    ym = re.match(r"^(\d{4})年?$", txt)
                    if ym:
                        found_year = int(ym.group(1))
                    # 性別: 牡牝セ
                    if txt in ("牡", "牝", "セ", "騸"):
                        found_sex = txt

            candidates.append({
                "horse_id": nk_id,
                "horse_name": name_text,
                "birth_year": found_year,
                "sex": found_sex,
            })

        # 重複除去 (horse_id ユニーク)
        seen = set()
        unique = []
        for c in candidates:
            if c["horse_id"] not in seen:
                seen.add(c["horse_id"])
                unique.append(c)

        return unique

    except Exception as e:
        logger.warning("netkeiba 馬名検索失敗 name=%s: %s", horse_name, e)
        return []


def _pick_best_candidate(
    candidates: list[dict],
    target_name: str,
    target_birth_year: int | None,
    target_sex: str,
) -> dict | None:
    """
    候補リストから最も一致度が高いものを 1 件選ぶ。

    採用条件:
      1. 馬名が完全一致 (必須)
      2. 生年一致 (あれば加点)
      3. 性別一致 (あれば加点)
    候補が 1 件のみで馬名一致 → 採用。
    複数候補で馬名一致が 1 件のみ → 採用。
    複数候補で馬名一致が複数 → 生年・性別で絞り込み。それでも複数 → スキップ (曖昧)。
    """
    # 馬名完全一致フィルタ
    name_matched = [c for c in candidates if c["horse_name"] == target_name]
    if not name_matched:
        return None

    if len(name_matched) == 1:
        return name_matched[0]

    # 複数: 生年 + 性別でスコアリング
    def score(c: dict) -> int:
        s = 0
        if target_birth_year and c["birth_year"] == target_birth_year:
            s += 10
        if target_sex and c["sex"] == target_sex:
            s += 5
        return s

    scored = sorted(name_matched, key=score, reverse=True)
    best_score = score(scored[0])

    # 同点が複数 → 曖昧なのでスキップ
    if len(scored) >= 2 and score(scored[1]) == best_score:
        logger.warning(
            "馬名=%s で同点候補が複数のためスキップ: %s",
            target_name,
            [c["horse_id"] for c in scored[:3]],
        )
        return None

    return scored[0]


# ── dry-run ───────────────────────────────────────────────────────────────────

def run_dry_run(conn: sqlite3.Connection) -> None:
    """対象件数と推定所要時間のみ表示 (DB 変更なし)"""
    print("=" * 65)
    print("【dry-run】B_prefix 馬 netkeiba_id 補完スクリプト")
    print("=" * 65)

    horses = _get_b_prefix_horses(conn)
    print(f"\n対象 horse_id: {len(horses):,} 件 (horses テーブル, B_prefix)")

    # 処理済みマーカー確認
    done_ids = _load_done_ids()
    remaining = [h for h in horses if h["horse_id"] not in done_ids]
    already_with_nkid = [h for h in horses if h["netkeiba_id"] is not None]
    print(f"  うち netkeiba_id 設定済: {len(already_with_nkid):,} 件 (スキップ)")
    print(f"  うち 処理済マーカー: {len(done_ids):,} 件 (スキップ)")

    # 実処理対象
    targets = [h for h in remaining if h["netkeiba_id"] is None]
    print(f"  実処理対象: {len(targets):,} 件")

    # 推定所要時間
    est_sec = len(targets) * RATE_LIMIT_SEC
    est_min = est_sec / 60
    print(f"\n推定所要時間: 約 {est_min:.0f} 分 ({len(targets):,} 件 × {RATE_LIMIT_SEC} 秒)")

    # birth_year 推定できる件数
    with_year = sum(1 for h in targets if _guess_birth_year_from_id(h["horse_id"]) is not None)
    print(f"\n生年 (horse_id から推定可能): {with_year:,} 件")
    print(f"生年 (推定不可):              {len(targets) - with_year:,} 件")

    # horse_name サンプル
    print("\nサンプル 5 件:")
    for h in targets[:5]:
        guessed_year = _guess_birth_year_from_id(h["horse_id"])
        print(
            f"  horse_id={h['horse_id']} name={h['horse_name']} "
            f"sex={h['sex']} 推定生年={guessed_year}"
        )

    # 危険時間帯チェック
    if _is_danger_time():
        print(f"\n[警告] 現在は危険時間帯です。--execute 実行時は自動 abort します。")
    else:
        print(f"\n[安全] 現在は実行可能時間帯です。")

    print(f"\n実行コマンド:")
    print(f"  python scripts/backfill_b_prefix_horses.py --execute")
    print(f"\n[dry-run 完了] DB への書き込みは行っていません。")


# ── 本実行 ────────────────────────────────────────────────────────────────────

def run_execute(
    conn: sqlite3.Connection,
    max_fetch: int | None = None,
) -> None:
    """netkeiba_id を検索・UPDATE する本実行"""
    print("=" * 65)
    print("【execute】B_prefix 馬 netkeiba_id 補完 開始")
    print("=" * 65)

    # ──── 安全チェック ────
    if _is_danger_time():
        now_str = datetime.now().strftime("%H:%M")
        print(f"[ABORT] 危険時間帯 ({now_str}) のため実行を中止します。")
        print("  実行可能時間: 06:31〜21:59 / 23:31〜05:59")
        sys.exit(1)

    conflicts = _check_conflict_processes()
    if conflicts:
        print(f"[ABORT] 競合プロセス検出: {conflicts}")
        print("  netkeiba 並列アクセス禁止 (違反歴 1 回・業務影響大)")
        sys.exit(1)

    # ──── Step 1: 対象抽出 ────
    print("\n[1/5] 対象 horse_id 抽出中...")
    all_horses = _get_b_prefix_horses(conn)
    done_ids = _load_done_ids()

    # 未処理 & netkeiba_id 未設定の馬のみ
    targets = [
        h for h in all_horses
        if h["netkeiba_id"] is None and h["horse_id"] not in done_ids
    ]
    print(f"  全 B_prefix: {len(all_horses):,} 件")
    print(f"  処理済マーカー: {len(done_ids):,} 件 (スキップ)")
    print(f"  実処理対象: {len(targets):,} 件")

    if max_fetch:
        targets = targets[:max_fetch]
        print(f"  --max-fetch {max_fetch} が指定されたため {max_fetch} 件に制限")

    if not targets:
        print("\n処理対象なし。終了します。")
        return

    # ──── Step 2: バックアップ ────
    print("\n[2/5] DB バックアップ取得...")
    try:
        _backup_db()
    except Exception as e:
        print(f"[ERROR] バックアップ失敗: {e}")
        print("バックアップなしでの本実行は禁止されています。終了します。")
        sys.exit(1)

    # ──── Step 3: NetkeibaClient 初期化 ────
    print(f"\n[3/5] netkeiba 馬名検索開始 ({len(targets):,} 件, {RATE_LIMIT_SEC} 秒間隔)")

    from src.scraper.netkeiba import NetkeibaClient
    client = NetkeibaClient(
        cache_dir=CACHE_DIR,
        ignore_ttl=True,
        request_interval=RATE_LIMIT_SEC,  # 2.0 秒 (違反歴厳守)
    )

    # ──── Step 4: 検索 & UPDATE ────
    print(f"\n[4/5] 検索・UPDATE ループ")
    matched = 0
    no_candidate = 0
    ambiguous = 0
    fail_list: list[tuple[str, str, str]] = []  # (horse_id, horse_name, reason)

    t_start = time.time()

    for i, horse in enumerate(targets, 1):
        horse_id = horse["horse_id"]
        horse_name = horse["horse_name"] or ""
        sex = horse["sex"] or ""

        # birth_year: horse テーブルにない場合は horse_id から推定
        birth_year = horse["birth_year"]
        if not birth_year:
            birth_year = _guess_birth_year_from_id(horse_id)

        # プログレスバー (20 件ごと or 最初・最後)
        if i == 1 or i == len(targets) or i % 20 == 0:
            elapsed = time.time() - t_start
            bar = _progress_bar(i, len(targets))
            rate = i / elapsed if elapsed > 0 else 0
            remaining_sec = (len(targets) - i) / rate if rate > 0 else 0
            print(
                f"{bar} "
                f"経過{elapsed:.0f}s 残り約{remaining_sec:.0f}s "
                f"一致{matched} 候補なし{no_candidate} 曖昧{ambiguous}"
            )

        # 馬名が空の場合はスキップ (推定・フォールバック禁止)
        if not horse_name:
            logger.warning("horse_name 空 horse_id=%s → スキップ", horse_id)
            fail_list.append((horse_id, "", "horse_name 空"))
            _mark_done(horse_id)
            no_candidate += 1
            continue

        # netkeiba 馬名検索
        candidates = _search_netkeiba_horse_id(client, horse_name, birth_year)

        if not candidates:
            logger.info("候補なし horse_id=%s name=%s", horse_id, horse_name)
            fail_list.append((horse_id, horse_name, "候補なし"))
            _mark_done(horse_id)
            no_candidate += 1
            continue

        # 最良候補選択
        best = _pick_best_candidate(candidates, horse_name, birth_year, sex)

        if best is None:
            logger.info("候補あるが一致なし horse_id=%s name=%s candidates=%d", horse_id, horse_name, len(candidates))
            fail_list.append((horse_id, horse_name, f"曖昧({len(candidates)}候補)"))
            _mark_done(horse_id)
            ambiguous += 1
            continue

        # DB UPDATE
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """
            UPDATE horses
            SET netkeiba_id = ?,
                updated_at = ?
            WHERE horse_id = ?
            """,
            (best["horse_id"], now_str, horse_id),
        )
        conn.commit()
        matched += 1
        _mark_done(horse_id)

        logger.info(
            "UPDATE: horse_id=%s name=%s → netkeiba_id=%s (birth_year=%s sex=%s)",
            horse_id, horse_name, best["horse_id"], best.get("birth_year"), best.get("sex"),
        )

    total_elapsed = time.time() - t_start

    # ──── Step 5: 検証 ────
    print(f"\n[5/5] 検証...")
    total_processed = len(targets)
    print(f"\n{'=' * 65}")
    print(f"  処理完了: {total_processed:,} 件 / 所要時間: {total_elapsed / 60:.1f} 分")
    print(f"  netkeiba_id 更新成功:  {matched:,} 件")
    print(f"  候補なし:              {no_candidate:,} 件")
    print(f"  曖昧 (複数候補):       {ambiguous:,} 件")

    # DB 検証
    remaining_null = conn.execute(
        "SELECT COUNT(*) FROM horses WHERE horse_id LIKE 'B_%' AND netkeiba_id IS NULL"
    ).fetchone()[0]
    filled = conn.execute(
        "SELECT COUNT(*) FROM horses WHERE horse_id LIKE 'B_%' AND netkeiba_id IS NOT NULL"
    ).fetchone()[0]
    print(f"\n  B_prefix netkeiba_id 設定済: {filled:,} 件")
    print(f"  B_prefix netkeiba_id 未設定: {remaining_null:,} 件")

    # 失敗リスト記録
    if fail_list:
        fail_log_path = Path(__file__).resolve().parent.parent / "tmp" / "backfill_b_prefix_fails.tsv"
        with open(fail_log_path, "w", encoding="utf-8") as f:
            f.write("horse_id\thorse_name\treason\n")
            for horse_id, name, reason in fail_list:
                f.write(f"{horse_id}\t{name}\t{reason}\n")
        print(f"\n  失敗リスト: {fail_log_path}")

    print(f"\n[完了] backfill_b_prefix_horses.py 終了")
    print(f"  中断再開マーカー: {DONE_MARKER_FILE}")


# ── エントリーポイント ─────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="B_prefix 馬の netkeiba_id を馬名検索で補完する"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="件数と推定所要時間のみ表示 (DB 変更なし、デフォルト)",
    )
    mode.add_argument(
        "--execute",
        action="store_true",
        help="netkeiba 検索を実行し DB を UPDATE する (本実行)",
    )
    parser.add_argument(
        "--max-fetch",
        type=int,
        default=None,
        metavar="N",
        help="処理上限 (smoke test 用。例: --max-fetch 20)",
    )
    parser.add_argument(
        "--reset-marker",
        action="store_true",
        help="中断再開マーカーをリセットして全件再処理 (--dry-run と併用不可)",
    )
    args = parser.parse_args()

    # デフォルトは dry-run
    if not args.execute:
        args.dry_run = True

    if args.reset_marker and args.dry_run:
        parser.error("--reset-marker は --execute と組み合わせて使用してください")

    if args.reset_marker and DONE_MARKER_FILE.exists():
        DONE_MARKER_FILE.unlink()
        print(f"[マーカーリセット] {DONE_MARKER_FILE}")

    # DB 接続
    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row

    try:
        if args.dry_run:
            run_dry_run(conn)
        else:
            run_execute(conn, max_fetch=args.max_fetch)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
