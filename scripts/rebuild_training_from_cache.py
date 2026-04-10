"""
HTMLキャッシュから調教データを再パースしてDB・JSON再構築するスクリプト

_normalize_course() 修正後のコース名 + 正しいsplits（坂路4F含む）で全データを再構築。
ネットワークアクセス不要（キャッシュHTML使用）。

処理:
  1. 既存JSONからnetkeiba race_idインデックスを構築
  2. HTMLキャッシュファイルからKB race_id → netkeiba race_id マッピング
  3. BeautifulSoupで全HTMLを再パース（_parse_training_table使用）
  4. training_records テーブルを再構築
  5. data/training_ml/ のJSONを再生成
"""

import json
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.log import get_logger

logger = get_logger(__name__)

CACHE_DIR = os.path.join("data", "cache", "keibabook")
DB_PATH = os.path.join("data", "keiba.db")
TRAINING_ML_DIR = os.path.join("data", "training_ml")

# JRA: netkeiba venue code → KB venue code
JRA_VENUE_TO_KB = {
    "05": "04", "06": "05", "08": "00", "09": "01", "10": "03",
    "07": "02", "01": "07", "02": "08", "03": "09", "04": "06",
}
# 逆引き
KB_TO_JRA_VENUE = {v: k for k, v in JRA_VENUE_TO_KB.items()}

# NAR: netkeiba venue code → KB venue code
NAR_VENUE_TO_KB = {
    "42": "13", "43": "12", "44": "10", "45": "11",
    "47": "19", "48": "34", "50": "37", "51": "39",
    "30": "14", "65": "58", "54": "26", "55": "23",
    "35": "15", "36": "29", "46": "20",
}
KB_TO_NAR_VENUE = {v: k for k, v in NAR_VENUE_TO_KB.items()}


def jra_kb_to_netkeiba(kb_id: str) -> str | None:
    """JRA KB race_id (12桁) → netkeiba race_id (12桁)"""
    if len(kb_id) != 12:
        return None
    year = kb_id[0:4]
    kaiko = kb_id[4:6]
    kb_venue = kb_id[6:8]
    day = kb_id[8:10]
    race = kb_id[10:12]
    ne_venue = KB_TO_JRA_VENUE.get(kb_venue)
    if ne_venue is None:
        return None
    return year + ne_venue + kaiko + day + race


def nar_kb_to_lookup_key(kb_id: str) -> tuple | None:
    """NAR KB race_id (16桁) → (date_YYYYMMDD, ne_venue, race_no) のルックアップキー"""
    if len(kb_id) != 16:
        return None
    year = kb_id[0:4]
    kb_venue = kb_id[6:8]
    race = kb_id[10:12]
    mmdd = kb_id[12:16]
    ne_venue = KB_TO_NAR_VENUE.get(kb_venue)
    if ne_venue is None:
        return None
    date_str = year + mmdd  # YYYYMMDD
    return (date_str, ne_venue, race)


def build_race_id_index():
    """既存JSON + race_log DBからインデックスを構築

    Returns:
        nar_index: {(date_YYYYMMDD, venue_code, race_no): (netkeiba_race_id, date_str)}
        race_meta: {netkeiba_race_id: {"date": str, "venue": str, "venue_code": str, "is_jra": bool}}
    """
    nar_index = {}  # (date_YYYYMMDD, venue_code, race_no) → (netkeiba_race_id, date_str)
    race_meta = {}  # race_id → meta info

    # --- Phase 1: 既存JSONからインデックス構築 ---
    json_files = sorted(
        f for f in os.listdir(TRAINING_ML_DIR)
        if f.endswith(".json") and not f.startswith("_")
    )
    logger.info(f"既存JSON: {len(json_files)}ファイルからインデックス構築中...")

    for jf in json_files:
        try:
            with open(os.path.join(TRAINING_ML_DIR, jf), "r", encoding="utf-8") as fp:
                data = json.load(fp)
        except Exception:
            continue

        date_str = data.get("date", "")
        date_compact = date_str.replace("-", "")

        for race in data.get("races", []):
            rid = str(race["race_id"])
            venue_code = race.get("venue_code", "")
            is_jra = race.get("is_jra", False)
            venue = race.get("venue", "")

            race_meta[rid] = {
                "date": date_str,
                "date_compact": date_compact,
                "venue": venue,
                "venue_code": venue_code,
                "is_jra": is_jra,
            }

            if not is_jra:
                race_no = rid[-2:]
                nar_index[(date_compact, venue_code, race_no)] = (rid, date_str)

    json_count = len(race_meta)
    logger.info(f"JSONインデックス: {json_count}レース (NAR lookup: {len(nar_index)}件)")

    # --- Phase 2: race_log DBから追加のNARレースをインデックス ---
    JRA_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
    try:
        from data.masters.venue_master import get_venue_name
    except ImportError:
        def get_venue_name(vc):
            return vc

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT race_id, race_date FROM race_log ORDER BY race_id"
    ).fetchall()
    conn.close()

    db_added = 0
    for rid, race_date in rows:
        rid = str(rid)
        if rid in race_meta:
            continue  # JSONで既にインデックス済み

        venue_code = rid[4:6]
        is_jra = venue_code in JRA_CODES
        race_no = rid[-2:]

        # race_dateフォーマット: "YYYY-MM-DD"
        date_str = race_date[:10] if race_date else ""
        date_compact = date_str.replace("-", "")

        race_meta[rid] = {
            "date": date_str,
            "date_compact": date_compact,
            "venue": get_venue_name(venue_code),
            "venue_code": venue_code,
            "is_jra": is_jra,
        }

        if not is_jra:
            key = (date_compact, venue_code, race_no)
            if key not in nar_index:
                nar_index[key] = (rid, date_str)
                db_added += 1

    logger.info(
        f"インデックス構築完了: {len(race_meta)}レース "
        f"(JSON: {json_count}, DB追加: {db_added}, NAR lookup: {len(nar_index)}件)"
    )
    return nar_index, race_meta


def parse_single_html(args):
    """1ファイルのHTMLをパースする（マルチプロセス用）"""
    from bs4 import BeautifulSoup
    from src.scraper.keibabook_training import KeibabookClient, KeibabookTrainingScraper
    from src.scraper.training_collector import _training_record_to_dict

    filepath, danwa_path = args
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as fp:
            html = fp.read()
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return None

    # ダミーClientでパーサーだけ使う
    client = KeibabookClient.__new__(KeibabookClient)
    scraper = KeibabookTrainingScraper(client)

    try:
        training_map = scraper._parse_training_table(soup)
    except Exception:
        return None

    if not training_map:
        return None

    # コメント取得（danwaキャッシュがあれば）
    if danwa_path and os.path.exists(danwa_path):
        try:
            with open(danwa_path, "r", encoding="utf-8", errors="replace") as fp:
                danwa_html = fp.read()
            danwa_soup = BeautifulSoup(danwa_html, "html.parser")
            comments = scraper._parse_danwa_table(danwa_soup)
            for name, recs in training_map.items():
                if name in comments and recs:
                    recs[0].stable_comment = comments[name]
        except Exception:
            pass

    # TrainingRecord → dict に変換
    result = {}
    for hname, records in training_map.items():
        result[hname] = [_training_record_to_dict(r) for r in records]

    return result


def rebuild_from_cache():
    """キャッシュHTMLから全調教データを再パース・DB再構築"""
    if not os.path.exists(CACHE_DIR):
        logger.error(f"キャッシュディレクトリなし: {CACHE_DIR}")
        return

    t0 = time.time()

    # ===== Step 1: インデックス構築 =====
    nar_index, race_meta = build_race_id_index()

    # ===== Step 2: HTMLキャッシュファイルの収集 & マッピング =====
    all_files = os.listdir(CACHE_DIR)
    cyokyo_files = [f for f in all_files if "_cyokyo_" in f and f.endswith(".html")]
    danwa_set = {f for f in all_files if "_danwa_" in f and f.endswith(".html")}

    logger.info(f"調教HTMLキャッシュ: {len(cyokyo_files)}件, コメント: {len(danwa_set)}件")

    # KB race_id → netkeiba race_id のマッピング構築
    # + パース対象ファイルリスト作成
    parse_tasks = []  # (filepath, danwa_path, netkeiba_race_id)
    mapped = 0
    unmapped = 0
    unmapped_venues = defaultdict(int)

    for fname in cyokyo_files:
        m = re.search(r"_cyokyo_(\d+)_(\d+)\.html$", fname)
        if not m:
            unmapped += 1
            continue

        prefix = m.group(1)  # 0=JRA, 1=NAR
        kb_id = m.group(2)
        is_jra = prefix == "0"

        netkeiba_rid = None

        if is_jra:
            netkeiba_rid = jra_kb_to_netkeiba(kb_id)
        else:
            key = nar_kb_to_lookup_key(kb_id)
            if key:
                date_yyyymmdd, ne_venue, race_no = key
                lookup = nar_index.get((date_yyyymmdd, ne_venue, race_no))
                if lookup:
                    netkeiba_rid = lookup[0]
                else:
                    unmapped_venues[ne_venue] += 1

        if not netkeiba_rid:
            unmapped += 1
            continue

        # danwaファイルのパス
        danwa_fname = fname.replace("_cyokyo_", "_danwa_")
        danwa_path = os.path.join(CACHE_DIR, danwa_fname) if danwa_fname in danwa_set else None

        filepath = os.path.join(CACHE_DIR, fname)
        parse_tasks.append((filepath, danwa_path, netkeiba_rid))
        mapped += 1

    logger.info(f"マッピング完了: {mapped}件マッチ, {unmapped}件未マッチ")
    if unmapped_venues:
        for vc, cnt in sorted(unmapped_venues.items(), key=lambda x: -x[1])[:10]:
            logger.info(f"  未マッチNAR venue={vc}: {cnt}件")

    # ===== Step 3: HTML一括パース（マルチプロセス） =====
    logger.info(f"調教HTML: {len(parse_tasks)}件をパース中...")

    all_records = []  # (race_id, horse_name, record_dict)
    parsed = 0
    errors = 0

    # race_id → {horse_name: [records]} を構築（JSON再生成用）
    race_training = defaultdict(dict)  # race_id → {horse: [rec_dict]}

    # マルチプロセスでパース
    mp_args = [(fp, dp) for fp, dp, _ in parse_tasks]
    rid_list = [rid for _, _, rid in parse_tasks]
    n_workers = min(6, os.cpu_count() or 4)

    logger.info(f"  ワーカー数: {n_workers}")
    batch_time = time.time()
    done_count = 0

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(parse_single_html, arg): idx for idx, arg in enumerate(mp_args)}

        for future in as_completed(futures):
            idx = futures[future]
            done_count += 1

            if done_count % 2000 == 0:
                elapsed = time.time() - t0
                speed = done_count / (time.time() - batch_time)
                remaining = (len(parse_tasks) - done_count) / speed if speed > 0 else 0
                logger.info(
                    f"  {done_count}/{len(parse_tasks)} ({100*done_count/len(parse_tasks):.1f}%) "
                    f"パース済={parsed} エラー={errors} "
                    f"経過{elapsed:.0f}秒 残り{remaining:.0f}秒"
                )

            try:
                result = future.result()
            except Exception:
                errors += 1
                continue

            if result is None:
                continue

            netkeiba_rid = rid_list[idx]
            for hname, rec_dicts in result.items():
                for rd in rec_dicts:
                    all_records.append((netkeiba_rid, hname, rd))
                race_training[netkeiba_rid][hname] = rec_dicts
            parsed += 1

    elapsed = time.time() - t0
    logger.info(
        f"パース完了: {parsed}レース / {len(all_records)}レコード / "
        f"エラー={errors} ({elapsed:.1f}秒)"
    )

    # ===== Step 4: DB再構築 =====
    logger.info("training_records テーブル再構築中...")
    conn = sqlite3.connect(DB_PATH)

    # 既存データ件数
    old_count = conn.execute("SELECT COUNT(*) FROM training_records").fetchone()[0]
    logger.info(f"  旧データ: {old_count:,}件")

    conn.execute("DELETE FROM training_records")
    conn.commit()

    batch = []
    for ne_rid, hname, rec in all_records:
        splits_json = json.dumps(rec.get("splits", {}), ensure_ascii=False)
        batch.append((
            ne_rid,
            hname,
            "",  # horse_id
            rec.get("date", ""),
            rec.get("course", ""),
            splits_json,
            rec.get("rider", ""),
            rec.get("track_condition", ""),
            rec.get("lap_count", ""),
            rec.get("intensity_label", ""),
            rec.get("sigma_from_mean", 0),
            rec.get("comment", ""),
            rec.get("stable_comment", ""),
            "keibabook",
        ))

    total_inserted = 0
    batch_size = 5000
    for start in range(0, len(batch), batch_size):
        chunk = batch[start:start + batch_size]
        conn.executemany(
            """INSERT OR IGNORE INTO training_records
                (race_id, horse_name, horse_id, date, course,
                 splits_json, rider, track_condition, lap_count,
                 intensity_label, sigma_from_mean, comment, stable_comment, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            chunk,
        )
        total_inserted += conn.execute("SELECT changes()").fetchone()[0]
        conn.commit()

        if (start + batch_size) % 50000 < batch_size:
            pct = 100 * (start + batch_size) / len(batch)
            logger.info(f"  DB挿入: {total_inserted:,}件 / {len(batch):,}件 ({pct:.0f}%)")

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM training_records").fetchone()[0]
    logger.info(f"DB再構築完了: {total:,}件 (旧: {old_count:,}件)")

    # コース名分布
    courses = conn.execute(
        "SELECT course, COUNT(*) as cnt FROM training_records GROUP BY course ORDER BY cnt DESC"
    ).fetchall()
    conn.close()

    logger.info("コース名分布:")
    for course, cnt in courses[:30]:
        logger.info(f"  {course}: {cnt:,}件")

    # ===== Step 5: JSON再生成 =====
    logger.info("training_ml JSON再生成中...")

    # race_meta から日付ごとにグループ化
    date_races = defaultdict(list)  # date_str → [race_id, ...]
    for rid, meta in race_meta.items():
        if rid in race_training:
            date_races[meta["date"]].append(rid)

    json_written = 0
    for date_str, rids in sorted(date_races.items()):
        races = []
        for rid in sorted(rids):
            meta = race_meta[rid]
            training = race_training.get(rid, {})
            if not training:
                continue
            races.append({
                "race_id": rid,
                "venue": meta.get("venue", ""),
                "venue_code": meta.get("venue_code", ""),
                "is_jra": meta.get("is_jra", False),
                "horse_count": len(training),
                "training": training,
            })

        if races:
            date_compact = date_str.replace("-", "")
            out_path = os.path.join(TRAINING_ML_DIR, f"{date_compact}.json")
            data = {"date": date_str, "race_count": len(races), "races": races}
            with open(out_path, "w", encoding="utf-8") as fp:
                json.dump(data, fp, ensure_ascii=False, indent=2)
            json_written += 1

    elapsed = time.time() - t0
    logger.info(f"JSON再生成完了: {json_written}ファイル")
    logger.info(f"全処理完了: {elapsed:.1f}秒 ({elapsed/60:.1f}分)")

    # ===== 最終サマリー =====
    # 坂路の4Fタイム有無を確認
    conn = sqlite3.connect(DB_PATH)
    sakamichi_total = conn.execute(
        "SELECT COUNT(*) FROM training_records WHERE course LIKE '%坂%'"
    ).fetchone()[0]

    # splits_jsonに800が含まれる（=4Fタイムあり）坂路レコード
    sakamichi_with_4f = conn.execute(
        """SELECT COUNT(*) FROM training_records
           WHERE course LIKE '%坂%' AND splits_json LIKE '%800%'"""
    ).fetchone()[0]
    conn.close()

    logger.info(f"坂路レコード: {sakamichi_total:,}件 (4Fタイムあり: {sakamichi_with_4f:,}件)")


if __name__ == "__main__":
    rebuild_from_cache()
