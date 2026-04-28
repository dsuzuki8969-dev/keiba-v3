"""
normalize_race_log_horse_id.py
=================================
race_log.horse_id の形式を調査・分析し、整合性チェックを行うスクリプト。

horse_id の形式:
  - old_10digit  : 10桁数字 例: 2019100043  (JRA/NAR 旧形式)
  - B_prefix     : B202XXXXXX 形式 (netkeiba NAR 正式 horse_id)
  - nar_prefix   : nar_XXXXXXXXXX 形式 (官公式 NAR スクレイパー由来, 2026-03 以降)
  - empty        : NULL or '' (horse_id 未取得)

重要制約:
  - horses マスターテーブルが DB に存在しないため、
    旧形式 ↔ 新形式の自動マッピングは不可能。
  - nar_prefix レコードは horse_name が全て空のため horse_name ベースのマッピングも不可。
  - したがって本スクリプトは現状分析 (dry-run) のみ実施。
  - apply モードは「B_prefix → old_10digit」の重複排除など将来実装用に予約。

使い方:
  python scripts/normalize_race_log_horse_id.py --dry-run
  python scripts/normalize_race_log_horse_id.py --apply          # 現時点では dry-run と同等
  python scripts/normalize_race_log_horse_id.py --apply --resolve-empty  # 将来実装
"""

import argparse
import os
import shutil
import sqlite3
import sys
from datetime import datetime

# ── 共通設定 ──────────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "keiba.db")
DB_PATH = os.path.normpath(DB_PATH)

BACKUP_SUFFIX = "bak_pre_horseid_20260428"
BACKUP_PATH = DB_PATH + "." + BACKUP_SUFFIX


def _connect(db_path: str) -> sqlite3.Connection:
    """SQLite 接続（WAL モード）"""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


# ── 分析関数 ─────────────────────────────────────────────────────────────────

def analyze_current_state(con: sqlite3.Connection) -> dict:
    """現在の horse_id 分布を集計して返す"""
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM race_log")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT
          CASE
            WHEN horse_id IS NULL OR horse_id = '' THEN 'empty'
            WHEN horse_id LIKE 'nar_%'             THEN 'nar_prefix'
            WHEN horse_id LIKE 'B%'                THEN 'B_prefix'
            WHEN horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                                                   THEN 'old_10digit'
            ELSE 'other'
          END AS kind,
          COUNT(*) AS cnt
        FROM race_log
        GROUP BY kind
        ORDER BY cnt DESC
    """)
    dist = {row["kind"]: row["cnt"] for row in cur.fetchall()}

    # is_jra 別内訳
    cur.execute("""
        SELECT
          is_jra,
          CASE
            WHEN horse_id IS NULL OR horse_id = '' THEN 'empty'
            WHEN horse_id LIKE 'nar_%'             THEN 'nar_prefix'
            WHEN horse_id LIKE 'B%'                THEN 'B_prefix'
            WHEN horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                                                   THEN 'old_10digit'
            ELSE 'other'
          END AS kind,
          COUNT(*) AS cnt
        FROM race_log
        GROUP BY is_jra, kind
        ORDER BY is_jra DESC, cnt DESC
    """)
    by_jra = cur.fetchall()

    # race_date 範囲（形式別）
    cur.execute("""
        SELECT
          CASE
            WHEN horse_id LIKE 'nar_%' THEN 'nar_prefix'
            WHEN horse_id LIKE 'B%'    THEN 'B_prefix'
            WHEN horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                                       THEN 'old_10digit'
            WHEN horse_id IS NULL OR horse_id = '' THEN 'empty'
            ELSE 'other'
          END AS kind,
          MIN(race_date) AS min_date,
          MAX(race_date) AS max_date
        FROM race_log
        GROUP BY kind
    """)
    date_ranges = {row["kind"]: (row["min_date"], row["max_date"]) for row in cur.fetchall()}

    # nar_prefix の horse_name 空率
    cur.execute("""
        SELECT COUNT(*) FROM race_log
        WHERE horse_id LIKE 'nar_%' AND (horse_name IS NULL OR horse_name = '')
    """)
    nar_empty_name = cur.fetchone()[0]

    # old_10digit で horse_name が空のもの
    cur.execute("""
        SELECT COUNT(*) FROM race_log
        WHERE horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
        AND (horse_name IS NULL OR horse_name = '')
    """)
    old_empty_name = cur.fetchone()[0]

    return {
        "total": total,
        "dist": dist,
        "by_jra": list(by_jra),
        "date_ranges": date_ranges,
        "nar_empty_name_cnt": nar_empty_name,
        "old_empty_name_cnt": old_empty_name,
    }


def analyze_mapping_feasibility(con: sqlite3.Connection) -> dict:
    """
    旧→新マッピングの可否を調査する。

    horse_name ベースのマッピング:
      - nar_prefix レコードに horse_name が補完された (backfill_nar_horse_name.py 実行後)
      - 同一馬名で old_10digit ↔ nar_prefix の対応付けが可能
    """
    cur = con.cursor()

    # horses テーブル等 master の存在確認
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    has_horse_master = any(t in tables for t in ["horses", "horse_master", "horse_profiles"])

    # race_id × horse_no でのクロス照合可能数
    # (同じ race_id + horse_no で old_10digit と nar_prefix が共存する行を探す)
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT race_id, horse_no FROM race_log
            WHERE horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
            INTERSECT
            SELECT race_id, horse_no FROM race_log
            WHERE horse_id LIKE 'nar_%'
        )
    """)
    cross_joinable = cur.fetchone()[0]

    # nar_prefix で horse_name が存在するもの（マッピングキーとして使える）
    cur.execute("""
        SELECT COUNT(*) FROM race_log
        WHERE horse_id LIKE 'nar_%' AND horse_name != ''
    """)
    nar_with_name = cur.fetchone()[0]

    # horse_name ベースのマッピング可能数を計算
    # (old_10digit NAR レコードと nar_prefix レコードで同じ horse_name を持つ馬の数)
    name_based_mappable = 0
    name_based_race_log_rows = 0
    if nar_with_name > 0:
        cur.execute("""
            SELECT COUNT(*) as cnt FROM (
                SELECT DISTINCT old_h.horse_name, old_h.horse_id AS old_id, nar_h.horse_id AS nar_id
                FROM (
                    SELECT DISTINCT horse_name, horse_id FROM race_log
                    WHERE horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                      AND horse_name != ''
                ) AS old_h
                JOIN (
                    SELECT DISTINCT horse_name, horse_id FROM race_log
                    WHERE horse_id LIKE 'nar_%'
                      AND horse_name != ''
                ) AS nar_h ON old_h.horse_name = nar_h.horse_name
            )
        """)
        name_based_mappable = cur.fetchone()["cnt"]

    return {
        "has_horse_master": has_horse_master,
        "tables": tables,
        "cross_joinable_pairs": cross_joinable,
        "nar_with_name": nar_with_name,
        "name_based_mappable": name_based_mappable,
        "name_based_race_log_rows": name_based_race_log_rows,
        "mapping_possible": cross_joinable > 0 or nar_with_name > 0,
    }


def build_horse_id_name_map(con: sqlite3.Connection) -> dict:
    """
    horse_name → nar_horse_id のマッピング辞書を構築する。

    nar_prefix レコードから horse_name → horse_id (nar形式) を抽出。
    同一馬名で複数の nar_id がある場合は最新を採用する。

    Returns:
        {horse_name: nar_horse_id, ...}
    """
    cur = con.cursor()
    cur.execute("""
        SELECT horse_name, horse_id, MAX(race_date) as latest_date
        FROM race_log
        WHERE horse_id LIKE 'nar_%'
          AND horse_name != ''
        GROUP BY horse_name
        HAVING COUNT(DISTINCT horse_id) = 1  -- 1対1対応の馬名のみ採用（同名異馬を除外）
    """)
    return {r["horse_name"]: r["horse_id"] for r in cur.fetchall()}


def apply_horse_id_mapping(
    con: sqlite3.Connection,
    name_to_nar_id: dict,
) -> dict:
    """
    old_10digit NAR レコードの horse_id を nar_prefix に置換する。

    horse_name を照合キーとして使用。
    JRA レコードは変更しない (is_jra = 0 のみ対象)。

    Returns:
        {"updated": int, "skipped_ambiguous": int, "skipped_no_map": int}
    """
    cur = con.cursor()

    # 更新対象: is_jra=0 かつ old_10digit かつ horse_name あり
    cur.execute("""
        SELECT race_id, horse_no, horse_name, horse_id
        FROM race_log
        WHERE horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
          AND is_jra = 0
          AND horse_name != ''
    """)
    rows = cur.fetchall()

    updates = []
    skipped_no_map = 0

    for row in rows:
        nar_id = name_to_nar_id.get(row["horse_name"])
        if nar_id:
            updates.append((nar_id, row["race_id"], row["horse_no"]))
        else:
            skipped_no_map += 1

    if updates:
        con.executemany(
            "UPDATE race_log SET horse_id = ? WHERE race_id = ? AND horse_no = ?",
            updates
        )
        con.commit()

    return {
        "updated": len(updates),
        "skipped_no_map": skipped_no_map,
    }


def analyze_empty_candidates(con: sqlite3.Connection) -> dict:
    """空 horse_id の復元可能性調査"""
    cur = con.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM race_log
        WHERE horse_id IS NULL OR horse_id = ''
    """)
    empty_cnt = cur.fetchone()[0]

    # 空 horse_id のサンプル
    cur.execute("""
        SELECT race_id, horse_no, horse_name, race_date, is_jra
        FROM race_log
        WHERE horse_id IS NULL OR horse_id = ''
        ORDER BY race_date DESC
        LIMIT 5
    """)
    samples = [dict(r) for r in cur.fetchall()]

    # 空 horse_id のうち horse_name がある行（名前ベース再検索が可能）
    cur.execute("""
        SELECT COUNT(*) FROM race_log
        WHERE (horse_id IS NULL OR horse_id = '')
        AND horse_name IS NOT NULL AND horse_name != ''
    """)
    empty_with_name = cur.fetchone()[0]

    return {
        "empty_total": empty_cnt,
        "empty_with_name": empty_with_name,
        "empty_no_name": empty_cnt - empty_with_name,
        "samples": samples,
    }


# ── 表示関数 ─────────────────────────────────────────────────────────────────

def print_report(state: dict, mapping: dict, empty: dict) -> None:
    """分析結果をコンソールに表示"""
    sep = "=" * 60

    print(sep)
    print("【1】 race_log.horse_id 現状分布")
    print(sep)
    print(f"  総件数: {state['total']:,}")
    for kind, cnt in sorted(state["dist"].items(), key=lambda x: -x[1]):
        pct = cnt / state["total"] * 100 if state["total"] else 0
        rng = state["date_ranges"].get(kind, ("?", "?"))
        print(f"  {kind:<15} {cnt:>8,} 件  ({pct:5.1f}%)  "
              f"[{rng[0]} 〜 {rng[1]}]")

    print()
    print("  ── is_jra 別内訳 ──")
    for row in state["by_jra"]:
        label = "JRA" if row["is_jra"] == 1 else "NAR/地方"
        print(f"  {label:<8} {row['kind']:<15} {row['cnt']:>8,} 件")

    print()
    print("  ── horse_name 充足率 ──")
    nar_cnt = state["dist"].get("nar_prefix", 0)
    old_cnt = state["dist"].get("old_10digit", 0)
    if nar_cnt:
        print(f"  nar_prefix の horse_name 空件数: "
              f"{state['nar_empty_name_cnt']:,} / {nar_cnt:,} "
              f"({state['nar_empty_name_cnt']/nar_cnt*100:.1f}%)")
    if old_cnt:
        print(f"  old_10digit の horse_name 空件数: "
              f"{state['old_empty_name_cnt']:,} / {old_cnt:,} "
              f"({state['old_empty_name_cnt']/old_cnt*100:.1f}%)")

    print()
    print(sep)
    print("【2】 旧→新マッピング可能性調査")
    print(sep)
    print(f"  horses マスターテーブル存在: {mapping['has_horse_master']}")
    print(f"  race_id × horse_no 直接照合可能ペア: {mapping['cross_joinable_pairs']:,} 件")
    print(f"  nar_prefix で horse_name あり:        {mapping['nar_with_name']:,} 件")
    print(f"  horse_name ベース マッピング可能 馬名: {mapping['name_based_mappable']:,} 件")
    print()
    if mapping['name_based_mappable'] > 0:
        print("  【判定】horse_name ベースマッピング 可能")
        print("  方針: nar_prefix の horse_name と old_10digit の horse_name を照合")
        print("        → old_10digit NAR レコードを nar_prefix 形式に置換")
        print("  ※ 同名異馬（1対1対応でない馬名）は安全のためスキップ")
    elif not mapping["mapping_possible"]:
        print("  【判定】自動マッピング 不可能")
        print("  理由:")
        print("    - horses マスターテーブルが DB に存在しない")
        print("    - nar_prefix レコードの horse_name が空（backfill_nar_horse_name.py 未実行）")
        print("    - 同一 race_id × horse_no の old_10digit ↔ nar_prefix 重複なし")
        print()
        print("  【対応方針】")
        print("    ① scripts/backfill_nar_horse_name.py --apply を先に実行する")
        print("    ② 再実行する")
    else:
        print(f"  【判定】マッピング可能: {mapping['cross_joinable_pairs']:,} ペア")

    print()
    print(sep)
    print("【3】 空 horse_id 復元可能性調査")
    print(sep)
    print(f"  空 horse_id 総件数: {empty['empty_total']:,}")
    print(f"  うち horse_name あり (名前ベース再検索可能): {empty['empty_with_name']:,} 件")
    print(f"  うち horse_name もなし (復元不可能):        {empty['empty_no_name']:,} 件")
    if empty["samples"]:
        print()
        print("  空 horse_id サンプル (最新5件):")
        for s in empty["samples"]:
            jra_label = "JRA" if s["is_jra"] else "NAR"
            print(f"    race_id={s['race_id']}  horse_no={s['horse_no']}  "
                  f"name={s['horse_name'] or '(空)'}  date={s['race_date']}  {jra_label}")


def print_dryrun_summary(state: dict, mapping: dict, empty: dict) -> None:
    """dry-run の要約を表示"""
    print()
    print("=" * 60)
    print("【dry-run サマリ】")
    print("=" * 60)
    print(f"  horse_name ベース マッピング可能 馬名: {mapping['name_based_mappable']:,} 件")
    print(f"  race_id × horse_no 直接照合可能:       {mapping['cross_joinable_pairs']:,} 件")
    print(f"  空 horse_id のうち復元可能:             {empty['empty_with_name']:,} 件")
    print()
    if mapping['name_based_mappable'] > 0:
        print("  → --apply で old_10digit NAR レコードを nar_prefix 形式に置換できます。")
        print("    (同名異馬は安全のためスキップ)")
    else:
        print("  → 現時点では apply モードでも DB 変更は行いません。")
        print("    先に scripts/backfill_nar_horse_name.py --apply を実行してください。")


# ── メイン ──────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="race_log.horse_id の正規化（現状分析 + 将来マッピング準備）"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="現状分析のみ（DB 変更なし）"
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="horse_name ベースで old_10digit NAR レコードを nar_prefix 形式に置換"
    )
    parser.add_argument(
        "--resolve-empty", action="store_true",
        help="空 horse_id の再取得試行（将来実装・現時点では未対応）"
    )
    parser.add_argument(
        "--db", default=DB_PATH,
        help=f"DB パス (デフォルト: {DB_PATH})"
    )
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("エラー: --dry-run または --apply を指定してください。")
        parser.print_help()
        sys.exit(1)

    db_path = os.path.normpath(args.db)
    if not os.path.exists(db_path):
        print(f"エラー: DB ファイルが見つかりません: {db_path}")
        sys.exit(1)

    print(f"対象 DB: {db_path}")
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # ── バックアップ（apply 時のみ） ──
    if args.apply:
        backup_path = db_path + "." + BACKUP_SUFFIX
        if not os.path.exists(backup_path):
            print(f"バックアップ作成中: {backup_path}")
            shutil.copy2(db_path, backup_path)
            print(f"バックアップ完了: {os.path.getsize(backup_path) / 1024 / 1024:.1f} MB")
        else:
            print(f"バックアップ既存のためスキップ: {backup_path}")
        print()

    # ── 分析実行 ──
    print("現状分析中...")
    con = _connect(db_path)
    try:
        state = analyze_current_state(con)
        mapping = analyze_mapping_feasibility(con)
        empty = analyze_empty_candidates(con)
    finally:
        con.close()

    # ── レポート出力 ──
    print_report(state, mapping, empty)
    print_dryrun_summary(state, mapping, empty)

    if args.apply:
        print()
        print("【apply モード】")
        if mapping["name_based_mappable"] == 0:
            print("  horse_name ベースのマッピング可能件数が 0 件のため、DB への変更は行いませんでした。")
            print("  先に scripts/backfill_nar_horse_name.py --apply を実行してください。")
        else:
            # horse_name ベースで old_10digit NAR レコードを nar_prefix 形式に置換
            print(f"  horse_name ベースマッピングを適用中... (対象: {mapping['name_based_mappable']:,} 馬名)")
            con2 = _connect(db_path)
            try:
                name_to_nar_id = build_horse_id_name_map(con2)
                result = apply_horse_id_mapping(con2, name_to_nar_id)
                print(f"  UPDATE 完了: {result['updated']:,} 件")
                print(f"  スキップ (マップなし): {result['skipped_no_map']:,} 件")

                # 適用後の内訳確認
                cur2 = con2.cursor()
                cur2.execute("""
                    SELECT
                      CASE
                        WHEN horse_id IS NULL OR horse_id = '' THEN 'empty'
                        WHEN horse_id LIKE 'nar_%'             THEN 'nar_prefix'
                        WHEN horse_id LIKE 'B%'                THEN 'B_prefix'
                        WHEN horse_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                                                               THEN 'old_10digit'
                        ELSE 'other'
                      END AS kind,
                      COUNT(*) AS cnt
                    FROM race_log
                    GROUP BY kind
                    ORDER BY cnt DESC
                """)
                print()
                print("  [apply 後の horse_id 分布]")
                total2 = con2.execute("SELECT COUNT(*) FROM race_log").fetchone()[0]
                for row in cur2.fetchall():
                    pct = row["cnt"] / total2 * 100 if total2 else 0
                    print(f"    {row['kind']:<15} {row['cnt']:>8,} 件  ({pct:5.1f}%)")
            finally:
                con2.close()

    if args.resolve_empty:
        print()
        print("【--resolve-empty モード】")
        print("  空 horse_id の HTML キャッシュからの再取得は未実装です。")
        print(f"  対象: {empty['empty_with_name']:,} 件 (horse_name あり)")


if __name__ == "__main__":
    main()
