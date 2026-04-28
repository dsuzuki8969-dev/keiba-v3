"""
NAR course_db 完全補完スクリプト
==================================
全 NAR venue × 全距離 × 芝/ダ の std_time 補完に必要なレコードを course_db に追加する。

データソース: race_log（実走破タイム）
対象: NAR 14 場（venue_code >= 30）で未カバーの venue × surface × distance 組み合わせ
除外: 帯広（venue_code=52, 65）— ばんえい専用ロジックで処理されるため

判定ロジック:
- race_log で venue_code + surface + distance ごとに上位 3 着以内の走を収集
- 最低 5 走以上（全体）かつ上位 3 着レコードが 3 走以上ある組み合わせを対象
- 収集したレコードを course_db 形式（data_json: PastRun dict list）で INSERT/REPLACE
- 既存レコードは --force 指定時のみ上書き（デフォルトは INSERT ONLY = 既存スキップ）

課題: このスクリプトは course_db テーブルの course_key 単位で INSERT/REPLACE。
      calc_standard_time は course_db の top3 レコード avg_time を直接参照するため、
      race_log の実レコードを正確に渡すことで std_time が正しく算出される。

実行例:
  # ドライランで未カバー確認
  python scripts/backfill_nar_course_db.py --dry-run

  # 本実行
  python scripts/backfill_nar_course_db.py

  # 特定 venue のみ
  python scripts/backfill_nar_course_db.py --venue-codes 42,48

  # 既存も更新
  python scripts/backfill_nar_course_db.py --force
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime

# プロジェクトルートをパスに追加
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from config.settings import DATABASE_PATH

# ============================================================
# ログ設定
# ============================================================

LOG_DIR = os.path.join(_PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, "backfill_nar_course_db.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ============================================================
# NAR 定数
# ============================================================

# NAR 全 14 場の venue_code マッピング
NAR_VENUE_MAP = {
    "30": "門別",
    "35": "盛岡",
    "36": "水沢",
    "42": "浦和",
    "43": "船橋",
    "44": "大井",
    "45": "川崎",
    "46": "金沢",
    "47": "笠松",
    "48": "名古屋",
    "49": "園田",
    "50": "園田",  # 旧コード
    "51": "姫路",
    "54": "高知",
    "55": "佐賀",
}

# ばんえい競馬は別ロジック（_BANEI_TIME_COEFF）で処理するため除外
BANEI_CODES = {"52", "65"}


def _make_course_key(venue_code: str, surface: str, distance: int) -> str:
    """course_db のキー形式を生成: '{venue_code}_{surface}_{distance}'"""
    vc = venue_code.zfill(2)
    return f"{vc}_{surface}_{distance}"


def fetch_existing_nar_keys(conn: sqlite3.Connection) -> set:
    """course_db から NAR（venue_code >= 30）の既存キーを取得"""
    rows = conn.execute(
        """
        SELECT course_key FROM course_db
        WHERE CAST(SUBSTR(course_key, 1, INSTR(course_key,'_')-1) AS INTEGER) >= 30
        """
    ).fetchall()
    return {row[0] for row in rows}


def fetch_nar_race_log_stats(
    conn: sqlite3.Connection,
    venue_codes: list,
    min_runs: int,
) -> list:
    """
    race_log から NAR の venue_code × surface × distance 別集計を取得。
    Returns: [(venue_code, surface, distance, total_n, top3_n), ...]
    """
    placeholders = ",".join("?" * len(venue_codes))
    q = f"""
    SELECT
        venue_code,
        surface,
        distance,
        COUNT(*) AS total_n,
        SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS top3_n
    FROM race_log
    WHERE CAST(venue_code AS INTEGER) IN ({placeholders})
      AND finish_time_sec > 0
      AND surface IS NOT NULL
      AND distance > 0
      AND status IS NULL OR status = ''
    GROUP BY venue_code, surface, distance
    HAVING COUNT(*) >= ?
    ORDER BY venue_code, surface, distance
    """
    rows = conn.execute(q, venue_codes + [min_runs]).fetchall()
    return [
        (str(row[0]).zfill(2), row[1], row[2], row[3], row[4])
        for row in rows
    ]


def fetch_race_log_records(
    conn: sqlite3.Connection,
    venue_code: str,
    surface: str,
    distance: int,
) -> list:
    """
    指定 venue × surface × distance の race_log レコードを course_db 形式の dict に変換して返す。
    NAR は avg_time 直接使用のため、top3 レコードが重要。
    """
    q = """
    SELECT
        race_date,
        race_id,
        venue_code,
        surface,
        distance,
        condition,
        race_name,
        grade,
        field_count,
        gate_no,
        horse_no,
        jockey_id,
        jockey_name,
        trainer_id,
        weight_kg,
        position_4c,
        positions_corners,
        finish_pos,
        finish_time_sec,
        last_3f_sec,
        margin_ahead,
        margin_behind,
        first_3f_sec,
        race_first_3f,
        race_pace,
        course_id
    FROM race_log
    WHERE CAST(venue_code AS INTEGER) = ?
      AND surface = ?
      AND distance = ?
      AND finish_time_sec > 0
    ORDER BY race_date DESC, race_id, finish_pos
    """
    rows = conn.execute(q, [int(venue_code), surface, distance]).fetchall()

    records = []
    vc_zfill = venue_code.zfill(2)
    for row in rows:
        # course_id が空の場合は復元
        cid = row["course_id"] or f"{vc_zfill}_{surface}_{distance}"

        rec = {
            "race_date": row["race_date"] or "",
            "venue": vc_zfill,
            "course_id": cid,
            "distance": distance,
            "surface": surface,
            "condition": row["condition"] or "良",
            "class_name": row["race_name"] or "",
            "grade": row["grade"] or "",
            "field_count": row["field_count"] or 8,
            "gate_no": row["gate_no"] or 0,
            "horse_no": row["horse_no"] or 0,
            "jockey": row["jockey_name"] or "",
            "jockey_id": row["jockey_id"] or "",
            "trainer_id": row["trainer_id"] or "",
            "weight_kg": row["weight_kg"] or 55.0,
            "position_4c": row["position_4c"] or 0,
            "positions_corners": _parse_positions(row["positions_corners"]),
            "finish_pos": row["finish_pos"] or 0,
            "finish_time_sec": row["finish_time_sec"],
            "last_3f_sec": row["last_3f_sec"] or 0.0,
            "margin_behind": row["margin_behind"] or 0.0,
            "margin_ahead": row["margin_ahead"] or 0.0,
            "first_3f_sec": row["first_3f_sec"] or 0.0,
            "race_first_3f": row["race_first_3f"] or 0.0,
            "race_pace": row["race_pace"] or "",
        }
        records.append(rec)
    return records


def _parse_positions(val) -> list:
    """positions_corners JSON 文字列をリストに変換"""
    if not val:
        return []
    if isinstance(val, list):
        return val
    try:
        return json.loads(val)
    except Exception:
        return []


def run_backfill(
    venue_codes: list = None,
    min_runs: int = 5,
    dry_run: bool = False,
    force: bool = False,
):
    """
    メイン処理。

    venue_codes: 対象 venue コードリスト（None の場合は NAR 全場）
    min_runs: 最低必要走数（デフォルト 5）
    dry_run: True の場合 DB 更新せずログのみ
    force: True の場合、既存エントリも上書き
    """
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("NAR course_db 補完スクリプト 開始")
    logger.info(f"  dry_run={dry_run}, force={force}, min_runs={min_runs}")
    logger.info(f"  対象 venue: {venue_codes or 'NAR 全場'}")
    logger.info("=" * 60)

    # 対象 venue_code リスト作成
    if venue_codes:
        target_codes_int = [int(vc) for vc in venue_codes if vc not in BANEI_CODES]
    else:
        target_codes_int = [int(vc) for vc in NAR_VENUE_MAP.keys() if vc not in BANEI_CODES]

    if not target_codes_int:
        logger.warning("対象 venue が空です。処理を終了します。")
        return

    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.row_factory = sqlite3.Row

    # ① 既存 course_db キーを取得
    existing_keys = fetch_existing_nar_keys(conn)
    logger.info(f"既存 NAR course_db: {len(existing_keys)} エントリ")

    # ② race_log から組み合わせ集計
    stats = fetch_nar_race_log_stats(conn, target_codes_int, min_runs)
    logger.info(f"race_log 有効組み合わせ（{min_runs}走以上）: {len(stats)} 件")

    # ③ 未カバー / 既存 / スキップ に分類
    to_insert = []  # (course_key, venue_code, surface, distance, total_n, top3_n)
    to_skip_existing = []
    to_skip_low_top3 = []
    to_update_force = []

    for vc, surf, dist, total_n, top3_n in stats:
        course_key = _make_course_key(vc, surf, dist)
        venue_name = NAR_VENUE_MAP.get(vc, f"venue{vc}")

        if top3_n < 3:
            # top3 走が 3 件未満 → 精度不足でスキップ
            to_skip_low_top3.append((course_key, total_n, top3_n, venue_name))
            continue

        if course_key in existing_keys:
            if force:
                to_update_force.append((course_key, vc, surf, dist, total_n, top3_n, venue_name))
            else:
                to_skip_existing.append((course_key, total_n, venue_name))
        else:
            to_insert.append((course_key, vc, surf, dist, total_n, top3_n, venue_name))

    # ④ 結果表示
    logger.info("")
    logger.info("=== 分類結果 ===")
    logger.info(f"  新規 INSERT 対象:   {len(to_insert)} 件")
    logger.info(f"  既存スキップ:        {len(to_skip_existing)} 件（--force で上書き可能）")
    logger.info(f"  top3 不足スキップ:   {len(to_skip_low_top3)} 件（精度確保のため除外）")
    if force:
        logger.info(f"  強制更新対象:        {len(to_update_force)} 件")

    if to_insert:
        logger.info("")
        logger.info("=== 新規 INSERT 予定 ===")
        for course_key, vc, surf, dist, total_n, top3_n, venue_name in to_insert:
            logger.info(f"  {course_key!r:30} {venue_name} n={total_n} top3={top3_n}")

    if to_skip_low_top3:
        logger.info("")
        logger.info("=== top3 不足 SKIP（警告） ===")
        for course_key, total_n, top3_n, venue_name in to_skip_low_top3:
            logger.warning(f"  {course_key!r:30} {venue_name} n={total_n} top3={top3_n} ← 手動確認要")

    if dry_run:
        logger.info("")
        logger.info("【DRY RUN】DB 更新は行いません。--dry-run なしで本実行してください。")
        conn.close()
        return

    # ⑤ 本実行: INSERT
    insert_count = 0
    update_count = 0
    error_count = 0

    all_targets = to_insert + (to_update_force if force else [])

    for entry in all_targets:
        course_key, vc, surf, dist, total_n, top3_n, venue_name = entry
        try:
            records = fetch_race_log_records(conn, vc, surf, dist)
            if not records:
                logger.warning(f"  {course_key}: race_log からレコード取得できず → スキップ")
                error_count += 1
                continue

            data_json = json.dumps(records, ensure_ascii=False)
            conn.execute(
                """
                INSERT OR REPLACE INTO course_db (course_key, data_json, updated_at)
                VALUES (?, ?, datetime('now','localtime'))
                """,
                (course_key, data_json),
            )
            conn.commit()

            if course_key in existing_keys:
                update_count += 1
                logger.info(f"  UPDATE {course_key!r:30} {venue_name} → {len(records)} 走")
            else:
                insert_count += 1
                logger.info(f"  INSERT {course_key!r:30} {venue_name} → {len(records)} 走")

        except Exception as e:
            logger.error(f"  ERROR {course_key!r}: {e}")
            error_count += 1
            conn.rollback()

    # ⑥ 完了サマリ
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("")
    logger.info("=" * 60)
    logger.info("NAR course_db 補完 完了")
    logger.info(f"  INSERT: {insert_count} 件")
    logger.info(f"  UPDATE: {update_count} 件（--force）")
    logger.info(f"  ERROR:  {error_count} 件")
    logger.info(f"  経過:   {elapsed:.1f} 秒")
    logger.info("=" * 60)

    # ⑦ 補完後の NAR カバレッジ確認
    after_keys = fetch_existing_nar_keys(conn)
    logger.info(f"補完後 NAR course_db: {len(after_keys)} エントリ")

    # 残未カバー確認
    remaining_missing = []
    for vc, surf, dist, total_n, top3_n in stats:
        course_key = _make_course_key(vc, surf, dist)
        if course_key not in after_keys and top3_n >= 3:
            remaining_missing.append(course_key)

    if remaining_missing:
        logger.warning(f"補完後も未カバー({len(remaining_missing)} 件):")
        for k in remaining_missing:
            logger.warning(f"  {k!r}")
    else:
        logger.info("補完後 未カバーゼロ 達成。")

    conn.close()


def print_coverage_report(venue_codes: list = None, min_runs: int = 5):
    """補完前の NAR カバレッジレポートを出力"""
    if venue_codes:
        target_codes_int = [int(vc) for vc in venue_codes if vc not in BANEI_CODES]
    else:
        target_codes_int = [int(vc) for vc in NAR_VENUE_MAP.keys() if vc not in BANEI_CODES]

    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.row_factory = sqlite3.Row

    existing_keys = fetch_existing_nar_keys(conn)
    stats = fetch_nar_race_log_stats(conn, target_codes_int, min_runs)

    covered = sum(1 for vc, surf, dist, n, t3 in stats
                  if _make_course_key(vc, surf, dist) in existing_keys)
    uncovered = len(stats) - covered

    print()
    print("=== NAR course_db カバレッジ（補完前） ===")
    print(f"  race_log 有効組み合わせ（{min_runs}走以上）: {len(stats)} 件")
    print(f"  カバー済み: {covered} 件")
    print(f"  未カバー:   {uncovered} 件")
    print()

    if uncovered > 0:
        print("  未カバー詳細:")
        for vc, surf, dist, total_n, top3_n in stats:
            key = _make_course_key(vc, surf, dist)
            if key not in existing_keys:
                venue_name = NAR_VENUE_MAP.get(vc, f"venue{vc}")
                status = "OK" if top3_n >= 3 else "top3不足"
                print(f"    {key!r:30} {venue_name:4} n={total_n:5} top3={top3_n:5} [{status}]")
    print()
    conn.close()


# ============================================================
# CLI エントリーポイント
# ============================================================

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="NAR course_db 完全補完スクリプト"
    )
    ap.add_argument(
        "--venue-codes",
        type=str,
        default=None,
        help="対象 venue コードをカンマ区切りで指定（例: 42,48）。省略時は NAR 全場。",
    )
    ap.add_argument(
        "--min-runs",
        type=int,
        default=5,
        help="最低必要走数（デフォルト: 5）",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="DB 更新せずログのみ出力",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="既存エントリも更新（デフォルトは INSERT ONLY）",
    )
    ap.add_argument(
        "--report",
        action="store_true",
        help="カバレッジレポートのみ出力（DB 更新なし）",
    )

    args = ap.parse_args()

    # venue_codes パース
    vc_list = None
    if args.venue_codes:
        vc_list = [v.strip().zfill(2) for v in args.venue_codes.split(",")]
        # ばんえい除外チェック
        banei_filtered = [v for v in vc_list if v in BANEI_CODES]
        if banei_filtered:
            logger.warning(f"帯広（venue_code={banei_filtered}）はばんえい専用ロジックのため自動除外します。")
            vc_list = [v for v in vc_list if v not in BANEI_CODES]

    if args.report:
        print_coverage_report(venue_codes=vc_list, min_runs=args.min_runs)
        sys.exit(0)

    # カバレッジレポート表示後に実行
    print_coverage_report(venue_codes=vc_list, min_runs=args.min_runs)

    run_backfill(
        venue_codes=vc_list,
        min_runs=args.min_runs,
        dry_run=args.dry_run,
        force=args.force,
    )
