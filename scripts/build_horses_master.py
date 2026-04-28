#!/usr/bin/env python3
"""
JRA horses マスターテーブル初期構築スクリプト

race_log の全 horse_id (old_10digit / B_prefix / nar_prefix) を集約して
horses テーブルに INSERT OR IGNORE する。

使い方:
    python scripts/build_horses_master.py --dry-run   # 集約結果のみ表示（DBへの書き込みなし）
    python scripts/build_horses_master.py --apply     # horses テーブルに INSERT
"""

import argparse
import os
import sys
import time

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("PYTHONIOENCODING", "utf-8")

from src.database import get_db, init_schema
from src.log import get_logger

logger = get_logger(__name__)

# JRA 会場コード（race_id[4:6] で判定）
JRA_VENUE_CODES = {
    "01",  # 札幌
    "02",  # 函館
    "03",  # 福島
    "04",  # 新潟
    "05",  # 東京
    "06",  # 中山
    "07",  # 中京
    "08",  # 京都
    "09",  # 阪神
    "10",  # 小倉
}


def detect_is_jra(horse_id: str, venue_codes: set) -> int:
    """
    horse_id と会場コードセットから JRA フラグを判定する。
    B_ プレフィックス は NAR、nar_ プレフィックスは NAR。
    old_10digit は venue_codes で判定。
    同一 horse_id で JRA / NAR 両方がある場合は JRA を優先 (1)。
    """
    if horse_id.startswith("B_") or horse_id.startswith("nar_"):
        return 0
    # JRA 会場コードが 1 つでも含まれていれば JRA
    if venue_codes & JRA_VENUE_CODES:
        return 1
    return 0


def run_dry_run(conn) -> None:
    """dry-run: 集約結果の統計を表示するのみ（INSERT しない）"""
    print("=" * 60)
    print("【dry-run】race_log → horses 集約プレビュー")
    print("=" * 60)

    # 1. 全体件数
    total_row = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE horse_id IS NOT NULL AND horse_id != ''"
    ).fetchone()
    print(f"\n対象行数 (race_log): {total_row[0]:,} 件")

    # 2. horse_id 種別分布
    print("\n--- horse_id 種別 ---")
    for label, cond in [
        ("old_10digit", "horse_id NOT LIKE 'B_%' AND horse_id NOT LIKE 'nar_%' AND length(horse_id) = 10"),
        ("B_prefix (NAR netkeiba)", "horse_id LIKE 'B_%'"),
        ("nar_prefix (公式 lineage)", "horse_id LIKE 'nar_%'"),
        ("その他", "horse_id NOT LIKE 'B_%' AND horse_id NOT LIKE 'nar_%' AND length(horse_id) != 10"),
    ]:
        cnt = conn.execute(
            f"SELECT COUNT(*) FROM race_log WHERE horse_id IS NOT NULL AND horse_id != '' AND ({cond})"
        ).fetchone()[0]
        print(f"  {label}: {cnt:,} 件")

    # 3. 集約後ユニーク数
    uniq_row = conn.execute(
        "SELECT COUNT(DISTINCT horse_id) FROM race_log WHERE horse_id IS NOT NULL AND horse_id != ''"
    ).fetchone()
    print(f"\nユニーク horse_id 数: {uniq_row[0]:,} 件")

    # 4. horse_name NULL 率
    null_name = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE (horse_name IS NULL OR horse_name = '') AND horse_id IS NOT NULL AND horse_id != ''"
    ).fetchone()[0]
    print(f"horse_name が空の行数: {null_name:,} 件 (これらは INSERT をスキップ)")

    # 5. スキップされるユニーク horse_id 数（horse_name が常に空）
    skip_ids = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT horse_id
            FROM race_log
            WHERE horse_id IS NOT NULL AND horse_id != ''
            GROUP BY horse_id
            HAVING MAX(CASE WHEN horse_name IS NOT NULL AND horse_name != '' THEN 1 ELSE 0 END) = 0
        )
        """
    ).fetchone()[0]
    print(f"  → スキップされるユニーク horse_id: {skip_ids:,} 件")

    # 6. 集約後 INSERT 候補数
    insert_cnt = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT horse_id
            FROM race_log
            WHERE horse_id IS NOT NULL AND horse_id != ''
            GROUP BY horse_id
            HAVING MAX(CASE WHEN horse_name IS NOT NULL AND horse_name != '' THEN 1 ELSE 0 END) = 1
        )
        """
    ).fetchone()[0]
    print(f"\nINSERT 候補数: {insert_cnt:,} 件")

    # 7. 期間確認
    period = conn.execute(
        "SELECT MIN(race_date), MAX(race_date) FROM race_log WHERE horse_id IS NOT NULL AND horse_id != ''"
    ).fetchone()
    print(f"\n出走期間: {period[0]} 〜 {period[1]}")

    # 8. NULL 率チェック（主要カラム）
    print("\n--- NULL 率（race_log 主要カラム）---")
    for col in ["sire_name", "bms_name", "sex", "horse_name"]:
        null_cnt = conn.execute(
            f"SELECT COUNT(*) FROM race_log WHERE ({col} IS NULL OR {col} = '') AND horse_id IS NOT NULL AND horse_id != ''"
        ).fetchone()[0]
        rate = null_cnt / max(total_row[0], 1) * 100
        print(f"  {col}: NULL/空 {null_cnt:,} 件 ({rate:.1f}%)")

    # 9. サンプル 5 件表示
    print("\n--- 集約サンプル (先頭5件) ---")
    rows = conn.execute(
        """
        SELECT
            horse_id,
            MAX(horse_name)      AS horse_name,
            MAX(sire_name)       AS sire_name,
            MAX(bms_name)        AS bms_name,
            MAX(sex)             AS sex,
            MIN(race_date)       AS first_seen,
            MAX(race_date)       AS last_seen,
            COUNT(*)             AS race_count,
            GROUP_CONCAT(DISTINCT venue_code) AS venues
        FROM race_log
        WHERE horse_id IS NOT NULL AND horse_id != ''
          AND horse_name IS NOT NULL AND horse_name != ''
        GROUP BY horse_id
        LIMIT 5
        """
    ).fetchall()
    for r in rows:
        venue_codes_set = set((r[8] or "").split(",")) if r[8] else set()
        is_jra = detect_is_jra(r[0], venue_codes_set)
        print(
            f"  horse_id={r[0]} name={r[1]} sire={r[2]} sex={r[4]} "
            f"期間={r[5]}〜{r[6]} count={r[7]} is_jra={is_jra}"
        )

    print("\n[dry-run 完了] --apply で実際に INSERT してください。")


def run_apply(conn) -> None:
    """horses テーブルに race_log から集約データを INSERT OR IGNORE する"""
    print("=" * 60)
    print("【apply】race_log → horses INSERT 開始")
    print("=" * 60)

    # 集約クエリで全行取得
    print("\n[1/4] 集約クエリ実行中...")
    t0 = time.time()
    rows = conn.execute(
        """
        SELECT
            horse_id,
            MAX(horse_name)      AS horse_name,
            MAX(sire_name)       AS sire_name,
            MAX(bms_name)        AS bms_name,
            MAX(sex)             AS sex,
            MIN(race_date)       AS first_seen_date,
            MAX(race_date)       AS last_seen_date,
            COUNT(*)             AS race_count,
            GROUP_CONCAT(DISTINCT venue_code) AS venue_codes_csv
        FROM race_log
        WHERE horse_id IS NOT NULL AND horse_id != ''
        GROUP BY horse_id
        HAVING MAX(CASE WHEN horse_name IS NOT NULL AND horse_name != '' THEN 1 ELSE 0 END) = 1
        """
    ).fetchall()
    elapsed_query = time.time() - t0
    print(f"  → 集約行数: {len(rows):,} 件 ({elapsed_query:.1f}秒)")

    # 既存 horses 行数確認
    existing_cnt = conn.execute("SELECT COUNT(*) FROM horses").fetchone()[0]
    print(f"\n[2/4] 現在の horses テーブル行数: {existing_cnt:,} 件")

    # INSERT OR IGNORE ループ
    print(f"\n[3/4] INSERT OR IGNORE 開始...")
    t1 = time.time()
    inserted = 0
    skipped_no_name = 0
    batch_size = 10000
    batch = []

    for r in rows:
        (
            horse_id, horse_name, sire_name, bms_name, sex,
            first_seen_date, last_seen_date, race_count, venue_codes_csv
        ) = r

        # horse_name 空チェック（HAVING で既に除外済みだが念のため）
        if not horse_name or not horse_name.strip():
            skipped_no_name += 1
            continue

        # is_jra 判定
        venue_codes_set = set(venue_codes_csv.split(",")) if venue_codes_csv else set()
        is_jra = detect_is_jra(horse_id, venue_codes_set)

        batch.append((
            horse_id,
            horse_name.strip(),
            sire_name.strip() if sire_name else None,
            None,          # dam_name: race_log に列なし → NULL
            bms_name.strip() if bms_name else None,
            None,          # birth_year: race_log に列なし → NULL
            sex.strip() if sex else None,
            None,          # color: race_log に列なし → NULL
            None,          # breeder: race_log に列なし → NULL
            None,          # owner: race_log に列なし → NULL
            is_jra,
            first_seen_date,
            last_seen_date,
            race_count,
        ))

        if len(batch) >= batch_size:
            conn.executemany(
                """
                INSERT OR IGNORE INTO horses (
                    horse_id, horse_name, sire_name, dam_name, bms_name,
                    birth_year, sex, color, breeder, owner,
                    is_jra, first_seen_date, last_seen_date, race_count
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                batch,
            )
            inserted += len(batch)
            elapsed_so_far = time.time() - t1
            pct = inserted / max(len(rows), 1) * 100
            print(f"  進捗: {inserted:,}/{len(rows):,} 件 ({pct:.1f}%) [{elapsed_so_far:.1f}秒]")
            batch = []

    # 残りをまとめて INSERT
    if batch:
        conn.executemany(
            """
            INSERT OR IGNORE INTO horses (
                horse_id, horse_name, sire_name, dam_name, bms_name,
                birth_year, sex, color, breeder, owner,
                is_jra, first_seen_date, last_seen_date, race_count
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            batch,
        )
        inserted += len(batch)

    conn.commit()
    elapsed_total = time.time() - t1
    print(f"  → INSERT 完了: {inserted:,} 件 ({elapsed_total:.1f}秒) / スキップ(horse_name 空): {skipped_no_name} 件")

    # 4. 検証
    print(f"\n[4/4] 検証...")
    after_cnt = conn.execute("SELECT COUNT(*) FROM horses").fetchone()[0]
    print(f"  horses 件数: {after_cnt:,} 件 (追加: {after_cnt - existing_cnt:,} 件)")

    jra_cnt = conn.execute("SELECT COUNT(*) FROM horses WHERE is_jra = 1").fetchone()[0]
    nar_cnt = conn.execute("SELECT COUNT(*) FROM horses WHERE is_jra = 0").fetchone()[0]
    print(f"  JRA: {jra_cnt:,} 件 / NAR: {nar_cnt:,} 件")

    # サンプル整合性確認（先頭 3 件）
    print("\n  --- サンプル整合性確認 ---")
    sample_horses = conn.execute(
        "SELECT horse_id, horse_name, is_jra, race_count, first_seen_date, last_seen_date FROM horses LIMIT 3"
    ).fetchall()
    for sh in sample_horses:
        h_id, h_name, is_jra, rc, fs, ls = sh
        actual_cnt = conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE horse_id = ?", (h_id,)
        ).fetchone()[0]
        match = "OK" if actual_cnt == rc else f"NG (race_log={actual_cnt} vs horses.race_count={rc})"
        print(f"  horse_id={h_id} name={h_name} is_jra={is_jra} race_count={rc} 期間={fs}〜{ls} → {match}")

    print("\n[apply 完了]")


def main() -> None:
    parser = argparse.ArgumentParser(description="horses マスターテーブル初期構築")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="集約結果のみ表示（DBへの書き込みなし）")
    group.add_argument("--apply", action="store_true", help="horses テーブルに INSERT OR IGNORE")
    args = parser.parse_args()

    print("horses マスターテーブル構築スクリプト起動")
    print(f"モード: {'dry-run' if args.dry_run else 'apply'}")

    # スキーマ初期化（horses テーブル含む）
    init_schema()
    conn = get_db()

    if args.dry_run:
        run_dry_run(conn)
    else:
        run_apply(conn)


if __name__ == "__main__":
    main()
