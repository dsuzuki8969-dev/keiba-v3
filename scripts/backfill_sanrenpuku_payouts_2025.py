#!/usr/bin/env python3
"""
三連複払戻バックフィル専用スクリプト (T-063b)
=============================================
対象: DB の race_results.payouts_json に三連複キーが存在しないレース
     デフォルト期間: 2025-01-01 〜 2026-03-31

使い方:
  # 対象件数だけ確認 (DB 書き込みなし)
  python scripts/backfill_sanrenpuku_payouts_2025.py --dry-run

  # 本実行 (--execute 必須)
  python scripts/backfill_sanrenpuku_payouts_2025.py --execute

  # 期間カスタム
  python scripts/backfill_sanrenpuku_payouts_2025.py --from 2025-01-01 --to 2026-03-31 --dry-run

安全装置:
  - --execute なしでは DB を一切 UPDATE しない
  - 他の Python プロセス (auto_fetch / predict_tomorrow / results 等) 実行中は abort
  - 22:00-23:30 / 06:00-06:30 の時間帯は abort (DAI_Keiba_Results / Maintenance / Predict)
  - 中断再開: tmp/backfill_sanrenpuku_done.txt に処理済 race_id を追記

レート制限: 2.0 秒/件以上 (CLAUDE.md feedback_netkeiba_concurrent_throttle 準拠)
並列処理: 禁止 (シリアル実行のみ)

依存:
  pip install requests beautifulsoup4 lz4
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# UTF-8 出力強制 (Windows Git Bash 対応)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ─── パス定義 ───────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
DB_PATH   = BASE_DIR / "data" / "keiba.db"
CACHE_DIR = BASE_DIR / "data" / "cache"
LOG_DIR   = BASE_DIR / "logs"
TMP_DIR   = BASE_DIR / "tmp"

LOG_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

TODAY_STR   = datetime.now().strftime("%Y%m%d")
LOG_FILE    = LOG_DIR / f"backfill_sanrenpuku_payouts_{TODAY_STR}.log"
DONE_FILE   = TMP_DIR / "backfill_sanrenpuku_done.txt"

# ─── レート制限 ──────────────────────────────────────
RATE_LIMIT_SEC = 2.0   # 最低 2.0 秒 (CLAUDE.md 遵守)

# ─── 危険時間帯 ──────────────────────────────────────
# (hhmm_start, hhmm_end) — 端点含む
BLOCKED_PERIODS = [
    (600,  630),   # 06:00-06:30  DAI_Keiba_Predict
    (2200, 2330),  # 22:00-23:30  DAI_Keiba_Results + Maintenance
]

# ─── 競合プロセス名キーワード ─────────────────────────
CONFLICT_KEYWORDS = [
    "auto_fetch_odds",
    "predict_tomorrow_runner",
    "results_tracker",
    "scheduler_tasks",
    "run_analysis_date",
    "backfill_all_payouts",    # 他の backfill が動いていたら衝突
    "backfill_payouts",
    "backfill_recent_days",
    "backfill_2026_gaps",
]

# ─── JRA 会場コード ──────────────────────────────────
JRA_VENUE_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}

# ─── ラベル正規化 ────────────────────────────────────
LABEL_NORM = {"3連複": "三連複", "3連単": "三連単"}
TARGETS    = {"馬連", "馬単", "ワイド", "三連複", "三連単", "複勝", "単勝",
              "3連複", "3連単", "枠連"}


# ============================================================
# ログ
# ============================================================

def log(msg: str) -> None:
    ts   = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ============================================================
# 安全装置 1: 危険時間帯チェック
# ============================================================

def check_blocked_time() -> None:
    now_hhmm = int(datetime.now().strftime("%H%M"))
    for start, end in BLOCKED_PERIODS:
        if start <= now_hhmm <= end:
            log(f"[ABORT] 危険時間帯 ({start:04d}-{end:04d}) のため中止します。")
            log("       推奨実行時間帯: 24:00 以降 (Results 22:00 / Maintenance 23:00 完了後)")
            sys.exit(1)


# ============================================================
# 安全装置 2: 競合プロセスチェック
# ============================================================

def check_conflict_processes() -> None:
    """psutil なしでも動作するように subprocess で ps を使う。"""
    try:
        import subprocess
        # Windows では wmic、Git Bash では ps 両対応
        if sys.platform == "win32":
            result = subprocess.run(
                ["wmic", "process", "get", "commandline"],
                capture_output=True, text=True, timeout=10
            )
            cmdlines = result.stdout
        else:
            result = subprocess.run(
                ["ps", "-ef"],
                capture_output=True, text=True, timeout=10
            )
            cmdlines = result.stdout
    except Exception as e:
        log(f"[警告] プロセスチェック失敗 (スキップ): {e}")
        return

    my_pid = os.getpid()
    conflicts = []
    for line in cmdlines.splitlines():
        # 自分自身は除外
        if "backfill_sanrenpuku_payouts_2025" in line:
            # PID が自分かどうか確認 (簡易)
            continue
        for kw in CONFLICT_KEYWORDS:
            if kw in line:
                conflicts.append(line.strip()[:120])
                break

    if conflicts:
        log("[ABORT] 競合する Python プロセスが動作中です:")
        for c in conflicts:
            log(f"  {c}")
        log("競合プロセスが終了してから再実行してください。")
        sys.exit(1)


# ============================================================
# 中断再開: 処理済み race_id 管理
# ============================================================

def load_done_set() -> set[str]:
    if not DONE_FILE.exists():
        return set()
    try:
        return set(DONE_FILE.read_text(encoding="utf-8").splitlines())
    except Exception:
        return set()


def append_done(race_id: str) -> None:
    try:
        with DONE_FILE.open("a", encoding="utf-8") as f:
            f.write(race_id + "\n")
    except Exception:
        pass


# ============================================================
# DB 操作
# ============================================================

def get_target_races(conn: sqlite3.Connection,
                     date_from: str,
                     date_to: str) -> list[tuple[str, str, str]]:
    """
    三連複キーが存在しない race_results レコードを返す。
    Returns: [(date, race_id, payouts_json), ...]
    """
    rows = conn.execute(
        """
        SELECT date, race_id, payouts_json
        FROM race_results
        WHERE date >= ?
          AND date <= ?
          AND (
              json_extract(payouts_json, '$.三連複') IS NULL
              AND json_extract(payouts_json, '$.3連複') IS NULL
              AND json_extract(payouts_json, '$.sanrenpuku') IS NULL
          )
          AND order_json != '[]'
        ORDER BY date ASC, race_id ASC
        """,
        (date_from, date_to),
    ).fetchall()
    return [(r["date"], r["race_id"], r["payouts_json"]) for r in rows]


def update_payouts_json(conn: sqlite3.Connection,
                        date: str,
                        race_id: str,
                        new_payouts: dict) -> None:
    """
    payouts_json に三連複キーをマージして UPDATE する。
    既存の他のキー (単勝・複勝・馬連等) は保持する。
    """
    row = conn.execute(
        "SELECT payouts_json FROM race_results WHERE date=? AND race_id=?",
        (date, race_id),
    ).fetchone()
    if row is None:
        return

    try:
        existing = json.loads(row["payouts_json"]) or {}
    except Exception:
        existing = {}

    # 三連複 / ワイド / 馬単 / 三連単 / 枠連 を追記（既存キーは上書きしない）
    for key in ("三連複", "三連単", "枠連", "馬単", "ワイド"):
        if key in new_payouts and key not in existing:
            existing[key] = new_payouts[key]

    conn.execute(
        "UPDATE race_results SET payouts_json=? WHERE date=? AND race_id=?",
        (json.dumps(existing, ensure_ascii=False), date, race_id),
    )


# ============================================================
# キャッシュ / HTML 取得
# ============================================================

def _cache_path(race_id: str) -> Path:
    vc     = race_id[4:6]
    prefix = "race.netkeiba.com" if vc in JRA_VENUE_CODES else "nar.netkeiba.com"
    return CACHE_DIR / f"{prefix}_race_result.html_race_id={race_id}.html.lz4"


def _result_url(race_id: str) -> str:
    vc = race_id[4:6]
    if vc in JRA_VENUE_CODES:
        return f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    return f"https://nar.netkeiba.com/race/result.html?race_id={race_id}"


def _build_session():
    """HTTP セッション構築。requests が無ければ None を返す。"""
    try:
        import requests
        s = requests.Session()
        s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/138.0.0.0 Safari/537.36"
            ),
            "Accept":          "text/html,application/xhtml+xml",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Referer":         "https://race.netkeiba.com/top/race_list.html",
        })
        return s
    except ImportError:
        return None


def fetch_html(race_id: str, session) -> Optional[str]:
    """
    キャッシュ優先で HTML を返す。
    キャッシュがなければ netkeiba から GET (RATE_LIMIT_SEC 遵守)。
    """
    try:
        import lz4.frame
        have_lz4 = True
    except ImportError:
        have_lz4 = False

    cf = _cache_path(race_id)

    # キャッシュ優先
    if cf.exists() and have_lz4:
        try:
            with cf.open("rb") as fh:
                return lz4.frame.decompress(fh.read()).decode("utf-8", errors="replace")
        except Exception:
            pass

    # netkeiba から新規取得
    if session is None:
        return None

    url = _result_url(race_id)
    try:
        time.sleep(RATE_LIMIT_SEC)   # レート制限 (2.0 秒厳守)
        r = session.get(url, timeout=15)
        if r.status_code == 200 and len(r.content) > 1000:
            html = r.content.decode("euc-jp", errors="replace")
            # lz4 でキャッシュ保存 (失敗しても続行)
            if have_lz4:
                try:
                    cf.parent.mkdir(parents=True, exist_ok=True)
                    with cf.open("wb") as fh:
                        fh.write(lz4.frame.compress(html.encode("utf-8")))
                except Exception:
                    pass
            return html
        log(f"  HTTP {r.status_code} for {race_id}")
        return None
    except Exception as e:
        log(f"  GET エラー {race_id}: {e}")
        return None


# ============================================================
# HTML → payouts パース
# ============================================================

def parse_payouts(html: str) -> dict:
    """
    HTML から全券種の払戻をパース。
    backfill_all_payouts.py の実装を踏襲。
    """
    payouts: dict = {}
    if not html:
        return payouts
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
    except ImportError:
        # lxml がなければ html.parser にフォールバック
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
        except ImportError:
            log("  [警告] beautifulsoup4 未インストール。pip install beautifulsoup4 lz4 requests")
            return payouts
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

            # separator='-' で <br> を '-' に変換 → 馬番連結バグ防止
            combo_raw  = cells[1].get_text(separator="-", strip=True) if len(cells) > 1 else ""
            payout_raw = cells[2].get_text(separator="\n", strip=True) if len(cells) > 2 else ""

            combo = re.sub(r"-+", "-", re.sub(r"[^\d\-]", "-", combo_raw)).strip("-")
            payout_str = re.sub(r"[^\d]", "", (payout_raw.split("\n")[0] if payout_raw else ""))
            try:
                payout_val = int(payout_str) if payout_str else 0
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


# ============================================================
# メイン処理
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="三連複払戻バックフィル (T-063b)"
    )
    parser.add_argument("--dry-run",  action="store_true",
                        help="対象件数・推定所要時間を表示するだけ (DB 書き込みなし)")
    parser.add_argument("--execute",  action="store_true",
                        help="実際に DB を UPDATE する (このフラグなしでは絶対に書き込みしない)")
    parser.add_argument("--from",  dest="date_from", default="2025-01-01",
                        help="対象期間 開始日 (YYYY-MM-DD, デフォルト: 2025-01-01)")
    parser.add_argument("--to",    dest="date_to",   default="2026-03-31",
                        help="対象期間 終了日 (YYYY-MM-DD, デフォルト: 2026-03-31)")
    parser.add_argument("--reset-done", action="store_true",
                        help=f"{DONE_FILE} を削除して最初からやり直す")
    args = parser.parse_args()

    # --dry-run も --execute もなければ dry-run として扱い、警告表示
    if not args.dry_run and not args.execute:
        print("使い方:")
        print("  確認のみ : python scripts/backfill_sanrenpuku_payouts_2025.py --dry-run")
        print("  本実行   : python scripts/backfill_sanrenpuku_payouts_2025.py --execute")
        sys.exit(0)

    log("=" * 60)
    log("三連複払戻バックフィル (T-063b)")
    log(f"  モード     : {'DRY-RUN (DB 書き込みなし)' if args.dry_run else '★ EXECUTE (DB UPDATE あり)'}")
    log(f"  対象期間   : {args.date_from} 〜 {args.date_to}")
    log(f"  DB         : {DB_PATH}")
    log(f"  ログ       : {LOG_FILE}")
    log(f"  進捗ファイル: {DONE_FILE}")
    log("=" * 60)

    # ── 安全装置 ────────────────────────────────────────────
    if args.execute:
        check_blocked_time()
        check_conflict_processes()

    # ── done ファイルリセット ──────────────────────────────
    if args.reset_done and DONE_FILE.exists():
        DONE_FILE.unlink()
        log(f"処理済みリスト削除: {DONE_FILE}")

    # ── DB 接続 (本実行時は WAL モード / check_same_thread=False) ─
    if not DB_PATH.exists():
        log(f"[ERROR] DB が見つかりません: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(
        str(DB_PATH),
        isolation_level=None,   # autocommit 無効化して手動コミット管理
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # ── 対象レース抽出 ─────────────────────────────────────
    log("対象レース抽出中...")
    targets = get_target_races(conn, args.date_from, args.date_to)
    total   = len(targets)
    eta_h   = total * RATE_LIMIT_SEC / 3600

    log(f"対象レース数 : {total:,} 件")
    log(f"推定所要時間 : 約 {eta_h:.1f} 時間 (レート {RATE_LIMIT_SEC} 秒/件)")
    log(f"推奨実行時間 : 24:00 以降 (Results 22:00 / Maintenance 23:00 完了後)")

    if args.dry_run:
        # 月別内訳も表示
        monthly: dict[str, int] = {}
        for date, race_id, _ in targets:
            ym = date[:7]
            monthly[ym] = monthly.get(ym, 0) + 1
        log("\n月別内訳:")
        for ym in sorted(monthly):
            log(f"  {ym}: {monthly[ym]:>5} 件")
        log("\n[DRY-RUN 完了] DB は変更されていません。")
        log(f"本実行コマンド: python scripts/backfill_sanrenpuku_payouts_2025.py --execute")
        conn.close()
        return

    # ── 以下は --execute 時のみ実行 ───────────────────────

    # 処理済みセット (中断再開用)
    done_set = load_done_set()
    todo     = [(d, r, p) for d, r, p in targets if r not in done_set]
    log(f"処理済みスキップ: {total - len(todo):,} 件 → 残り {len(todo):,} 件")

    if not todo:
        log("全件処理済みです。")
        conn.close()
        return

    # HTTP セッション構築
    session = _build_session()
    if session is None:
        log("[ERROR] requests がインストールされていません: pip install requests")
        conn.close()
        sys.exit(1)

    # 統計カウンタ
    updated     = 0
    cache_hits  = 0
    no_html     = 0
    no_sanren   = 0
    failed      = 0

    t0 = time.time()

    for i, (date, race_id, payouts_json_raw) in enumerate(todo):
        # ── HTML 取得 ────────────────────────────────────────
        cache_existed = _cache_path(race_id).exists()
        html = fetch_html(race_id, session)
        if cache_existed:
            cache_hits += 1

        if not html:
            no_html += 1
            append_done(race_id)   # 取得不能は done 扱いにして次回スキップ
            continue

        # ── パース ──────────────────────────────────────────
        new_payouts = parse_payouts(html)
        if "三連複" not in new_payouts and "3連複" not in new_payouts:
            # 三連複が取れなかった (古すぎ or ページ未対応)
            no_sanren += 1
            append_done(race_id)
            continue

        # ── DB UPDATE ────────────────────────────────────────
        try:
            conn.execute("BEGIN")
            update_payouts_json(conn, date, race_id, new_payouts)
            conn.execute("COMMIT")
            updated += 1
            append_done(race_id)
        except Exception as e:
            conn.execute("ROLLBACK")
            failed += 1
            log(f"  [ERROR] DB UPDATE 失敗 {date} {race_id}: {e}")

        # ── 進捗表示 (50 件ごと) ────────────────────────────
        done_count = i + 1
        if done_count % 50 == 0 or done_count == len(todo):
            elapsed  = time.time() - t0
            rate     = done_count / elapsed if elapsed > 0 else 0
            eta_sec  = (len(todo) - done_count) / rate if rate > 0 else 0
            pct      = done_count / len(todo) * 100
            bar_len  = 30
            filled   = int(bar_len * done_count / len(todo))
            bar      = "█" * filled + "░" * (bar_len - filled)
            log(
                f"[{bar}] {pct:5.1f}% {done_count}/{len(todo)} "
                f"更新={updated} noHTML={no_html} noSR={no_sanren} "
                f"cacheHit={cache_hits} FAIL={failed} "
                f"経過={elapsed/60:.1f}分 残={eta_sec/60:.1f}分"
            )

    elapsed = time.time() - t0
    log("=" * 60)
    log(f"完了: 総所要 {elapsed/60:.1f} 分")
    log(f"  DB UPDATE 成功  : {updated:>6,} 件")
    log(f"  三連複なし (古) : {no_sanren:>6,} 件")
    log(f"  HTML 取得失敗   : {no_html:>6,} 件")
    log(f"  DB UPDATE 失敗  : {failed:>6,} 件")
    log(f"  キャッシュヒット : {cache_hits:>6,} 件 (新規 GET 不要)")
    log(f"  進捗ファイル    : {DONE_FILE}")
    log(f"  ログ            : {LOG_FILE}")

    conn.close()


if __name__ == "__main__":
    main()
