"""
調教データの一括コレクター（競馬ブックスマートプレミアム）

RaceListScraper で日別にレースIDを取得し、
KeibabookTrainingScraper で各レースの調教データを収集する。

特徴:
  - JRA + NAR 両対応（24場対応）
  - レジューム対応（中断→再開で重複なし）
  - 日単位でJSON保存（data/training_ml/{YYYYMMDD}.json）
  - KeibabookClient の HTML キャッシュも活用
"""

import json
import os
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from data.masters.venue_master import (
    JRA_CODES,
    get_venue_code_from_race_id,
    get_venue_name,
    is_banei,
)
from src.log import get_logger
from src.scraper.keibabook_training import (
    KeibabookClient,
    KeibabookTrainingScraper,
    is_kb_supported_venue,
)

logger = get_logger(__name__)

TRAINING_ML_DIR = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")),
    "data",
    "training_ml",
)
STATE_PATH = os.path.join(TRAINING_ML_DIR, "_training_state.json")


def _training_record_to_dict(rec) -> dict:
    """TrainingRecord を JSON シリアライズ可能な dict に変換"""
    d = asdict(rec)
    splits = d.get("splits", {})
    if splits:
        d["splits"] = {str(k): v for k, v in splits.items()}
    return d


def _load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _date_output_path(date_str: str) -> str:
    return os.path.join(TRAINING_ML_DIR, f"{date_str.replace('-', '')}.json")


def _save_day_data(date_str: str, races: list):
    os.makedirs(TRAINING_ML_DIR, exist_ok=True)
    path = _date_output_path(date_str)
    data = {"date": date_str, "race_count": len(races), "races": races}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _update_state(state: dict, date_str: str, races: int, horses: int, days: int):
    state["last_completed_date"] = date_str
    state["total_races"] = races
    state["total_horses"] = horses
    state["processed_days"] = days
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _save_state(state)


def collect_training_data(
    netkeiba_client,
    race_list_scraper,
    kb_client: KeibabookClient,
    start_date: str,
    end_date: str,
    jra_only: bool = False,
    nar_only: bool = False,
    resume: bool = True,
) -> dict:
    """
    指定期間のレース調教データを収集し、日別JSONに保存する。

    Args:
        netkeiba_client: NetkeibaClient (レース一覧取得用)
        race_list_scraper: RaceListScraper (レースID取得用)
        kb_client: KeibabookClient (競馬ブック認証済み)
        start_date: "YYYY-MM-DD"
        end_date: "YYYY-MM-DD"
        jra_only: JRAのみ
        nar_only: NARのみ
        resume: True=前回の続きから

    Returns:
        {"total_days": N, "total_races": N, "total_horses": N, "skipped_days": N}
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    state = _load_state() if resume else {}
    last_completed = state.get("last_completed_date")

    if resume and last_completed:
        resume_dt = datetime.strptime(last_completed, "%Y-%m-%d") + timedelta(days=1)
        if resume_dt > start_dt:
            start_dt = resume_dt
        if start_dt > end_dt:
            logger.info(f"既に完了済み (最終: {last_completed})")
            return {
                "total_days": 0,
                "total_races": state.get("total_races", 0),
                "total_horses": state.get("total_horses", 0),
                "skipped_days": 0,
            }

    if not kb_client.ensure_login():
        logger.warning("競馬ブックへのログインに失敗しました")
        return {"total_days": 0, "total_races": 0, "total_horses": 0, "skipped_days": 0}

    scraper = KeibabookTrainingScraper(kb_client)

    dates = []
    d = start_dt
    while d <= end_dt:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    total_races = state.get("total_races", 0)
    total_horses = state.get("total_horses", 0)
    processed_days = state.get("processed_days", 0)
    skipped_days = 0
    total_dates = len(dates)

    scope = "JRA" if jra_only else ("NAR" if nar_only else "JRA+NAR")
    logger.info(f"{dates[0]} 〜 {dates[-1]}  ({total_dates}日間)  対象: {scope}")
    if resume and last_completed:
        logger.info(f"レジューム: {last_completed} の翌日から再開 / 累計: {total_races}R / {total_horses}頭")

    try:
        for i, date_str in enumerate(dates):
            if os.path.exists(_date_output_path(date_str)):
                skipped_days += 1
                continue

            pct = 100 * (i + 1) // total_dates

            race_ids = race_list_scraper.get_race_ids(date_str)
            if not race_ids:
                logger.info(f"  [{i + 1}/{total_dates}] {date_str}  ({pct}%)  累計{total_races}R/{total_horses}頭 ... レースなし")
                _update_state(state, date_str, total_races, total_horses, processed_days)
                continue

            race_ids = [r for r in race_ids if not is_banei(get_venue_code_from_race_id(r))]
            if jra_only:
                race_ids = [r for r in race_ids if get_venue_code_from_race_id(r) in JRA_CODES]
            elif nar_only:
                race_ids = [r for r in race_ids if get_venue_code_from_race_id(r) not in JRA_CODES]

            if not race_ids:
                logger.info(f"  [{i + 1}/{total_dates}] {date_str}  ({pct}%)  累計{total_races}R/{total_horses}頭 ... 対象レースなし")
                _update_state(state, date_str, total_races, total_horses, processed_days)
                continue

            day_races = []
            day_horses = 0
            for rid in race_ids:
                vc = get_venue_code_from_race_id(rid)
                is_jra = vc in JRA_CODES
                if not is_kb_supported_venue(vc, is_jra):
                    continue

                try:
                    training_map = scraper.fetch(rid, race_date=date_str)
                except Exception as e:
                    logger.warning("training fetch failed %s: %s", rid, e, exc_info=True)
                    continue

                if not training_map:
                    continue

                training_dict = {}
                horse_count = 0
                for hname, records in training_map.items():
                    training_dict[hname] = [_training_record_to_dict(r) for r in records]
                    horse_count += 1

                day_races.append({
                    "race_id": rid,
                    "venue": get_venue_name(vc),
                    "venue_code": vc,
                    "is_jra": is_jra,
                    "horse_count": horse_count,
                    "training": training_dict,
                })
                day_horses += horse_count

            if day_races:
                _save_day_data(date_str, day_races)
                total_races += len(day_races)
                total_horses += day_horses
                processed_days += 1
                venues = sorted(set(r["venue"] or "不明" for r in day_races))
                logger.info(f"  [{i + 1}/{total_dates}] {date_str}  ({pct}%)  累計{total_races}R/{total_horses}頭 ... {len(day_races)}R / {day_horses}頭  [{', '.join(venues)}]")
            else:
                logger.info(f"  [{i + 1}/{total_dates}] {date_str}  ({pct}%)  累計{total_races}R/{total_horses}頭 ... 調教データなし")

            _update_state(state, date_str, total_races, total_horses, processed_days)

    except KeyboardInterrupt:
        logger.warning(
            f"Ctrl+C で停止しました。次回 --resume で再開できます。最終完了日: {state.get('last_completed_date', '未開始')} / 累計: {total_races}R / {total_horses}頭"
        )

    logger.info(f"完了: 保存先={TRAINING_ML_DIR}, 累計={total_races}R / {total_horses}頭 / {processed_days}日")
    if skipped_days:
        logger.info(f"スキップ: {skipped_days}日 (保存済み)")

    return {
        "total_days": processed_days,
        "total_races": total_races,
        "total_horses": total_horses,
        "skipped_days": skipped_days,
    }


def training_data_stats():
    """収集済み調教データの統計を表示"""
    if not os.path.exists(TRAINING_ML_DIR):
        logger.info("データなし")
        return

    state = _load_state()
    files = [f for f in os.listdir(TRAINING_ML_DIR) if f.endswith(".json") and not f.startswith("_")]

    if not files:
        logger.info("データなし")
        return

    total_races, total_horses = 0, 0
    jra_races, nar_races = 0, 0
    date_range = []

    for fname in sorted(files):
        fpath = os.path.join(TRAINING_ML_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            date_range.append(data.get("date", fname))
            for race in data.get("races", []):
                total_races += 1
                total_horses += race.get("horse_count", 0)
                if race.get("is_jra"):
                    jra_races += 1
                else:
                    nar_races += 1
        except Exception:
            logger.debug("training stats file read failed", exc_info=True)
            continue

    logger.info(
        f"統計: 期間={date_range[0]} 〜 {date_range[-1]}, ファイル数={len(files)}日分, "
        f"レース数={total_races} (JRA: {jra_races} / NAR: {nar_races}), 延べ出走数={total_horses}"
    )
    if state:
        logger.info(f"最終更新: {state.get('updated_at', '不明')}")
    size_mb = sum(
        os.path.getsize(os.path.join(TRAINING_ML_DIR, f))
        for f in files
    ) / (1024 * 1024)
    logger.info(f"データサイズ: {size_mb:.1f} MB")
