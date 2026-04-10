"""
レース結果スクレイパー・基準タイムDB構築
netkeibaからレース結果（1-3着、タイム、着差）を取得
"""

import json
import os
import re
import statistics
import time
from collections import defaultdict
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from src.log import get_logger
from src.models import Horse, PaceType, PastRun, TrainingRecord

logger = get_logger(__name__)

# ============================================================
# 基準タイムDB・上がり3F DB
# ============================================================


class StandardTimeDBBuilder:
    """
    course_db: {course_id: [PastRun]} を構築・管理
    初回は空、過去走データからオンザフライで補充
    """

    def __init__(self):
        self._course_db: Dict[str, List[PastRun]] = {}

    def get_course_db(self) -> Dict[str, List[PastRun]]:
        return self._course_db

    def stats(self) -> str:
        n_courses = len(self._course_db)
        n_runs = sum(len(v) for v in self._course_db.values())
        return f"コース数: {n_courses}, 総走数: {n_runs}"


class Last3FDBBuilder:
    """
    course_db から pace_last3f_db を構築
    pace_last3f_db: {course_id: {PaceType.value: [last3f_times]}}
    """

    def build(self, course_db: Dict[str, List[PastRun]]) -> Dict:
        result: Dict[str, Dict[str, List[float]]] = {}
        for cid, runs in course_db.items():
            is_dirt = "ダート" in cid
            lo = 34.0 if is_dirt else 32.0
            hi = 42.0 if is_dirt else 40.0
            by_pace: Dict[str, List[float]] = {}
            for r in runs:
                t = r.last_3f_sec
                if not (lo <= t <= hi):
                    continue
                pace_key = r.pace.value if r.pace else PaceType.M.value
                if pace_key not in by_pace:
                    by_pace[pace_key] = []
                by_pace[pace_key].append(t)
            result[cid] = by_pace
        return result


def _rolling_window_cutoff(target_date: Optional[str], window_days: int = 365) -> Optional[str]:
    """ローリングウィンドウのカットオフ日付を算出"""
    if not target_date:
        return None
    from datetime import datetime, timedelta
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    return (dt - timedelta(days=window_days)).strftime("%Y-%m-%d")


def _in_rolling_window(race_date: str, target_date: Optional[str], cutoff: Optional[str]) -> bool:
    """race_date がローリングウィンドウ内かチェック"""
    if not target_date or not cutoff:
        return True
    return cutoff <= race_date < target_date


def build_course_db_from_past_runs(
    horses: List[Horse],
    course_db: Dict[str, List[PastRun]],
    target_date: Optional[str] = None,
) -> Dict[str, List[PastRun]]:
    """出走馬の過去走を course_db に追加（ローリングウィンドウ対応）

    Args:
        target_date: 基準日 (YYYY-MM-DD)。指定時は1年前〜前日のデータのみ追加。
    """
    cutoff = _rolling_window_cutoff(target_date)
    for h in horses:
        for run in h.past_runs:
            if not _in_rolling_window(run.race_date, target_date, cutoff):
                continue
            cid = run.course_id
            if cid not in course_db:
                course_db[cid] = []
            course_db[cid].append(run)
    return course_db


def build_course_style_stats_db(
    course_db: Dict[str, List[PastRun]],
    target_date: Optional[str] = None,
) -> Dict[str, Dict[str, float]]:
    """
    コース×脚質グループ別複勝率を集計（ローリングウィンドウ対応）
    relative_position（positions_corners優先）から脚質を推定
    course_style_stats_db: {course_id: {"front": 0.45, "mid_front": 0.35, "mid": 0.30, "rear": 0.25, "average": 0.33}}

    Args:
        target_date: 基準日 (YYYY-MM-DD)。指定時は1年前〜前日のデータのみ使用。
    """
    cutoff = _rolling_window_cutoff(target_date)
    result: Dict[str, Dict[str, float]] = {}
    for cid, runs in course_db.items():
        filtered = [r for r in runs if _in_rolling_window(r.race_date, target_date, cutoff)]
        if len(filtered) < 5:
            continue
        by_group: Dict[str, List[int]] = {"front": [], "mid_front": [], "mid": [], "rear": []}
        for r in filtered:
            rp = r.relative_position
            if rp <= 0.2:
                g = "front"
            elif rp <= 0.45:
                g = "mid_front"
            elif rp <= 0.7:
                g = "mid"
            else:
                g = "rear"
            by_group[g].append(1 if r.finish_pos <= 3 else 0)
        total_place3 = sum(sum(v) for v in by_group.values())
        total_n = sum(len(v) for v in by_group.values())
        avg = total_place3 / total_n if total_n else 0.33
        rates = {g: sum(v) / len(v) if v else avg for g, v in by_group.items()}
        rates["average"] = avg
        result[cid] = rates
    return result


def build_gate_bias_db(
    course_db: Dict[str, List[PastRun]],
    target_date: Optional[str] = None,
) -> Dict[str, Dict[int, float]]:
    """
    競馬場×芝ダ×枠番別複勝率から枠順バイアスを算出（ローリングウィンドウ対応）
    gate_no（枠番 1-8）で集計
    gate_bias_db: {"venue_surface": {gate_no: bias_pt}}

    Args:
        target_date: 基準日 (YYYY-MM-DD)。指定時は1年前〜前日のデータのみ使用。
    """
    cutoff = _rolling_window_cutoff(target_date)
    result: Dict[str, Dict[int, float]] = {}
    by_venue_gate: Dict[str, Dict[int, List[int]]] = {}
    for cid, runs in course_db.items():
        parts = cid.split("_")
        venue = parts[0] if len(parts) >= 1 else ""
        if not venue:
            continue
        surface = parts[1] if len(parts) >= 2 else "芝"
        key = f"{venue}_{surface}"
        for r in [r for r in runs if _in_rolling_window(r.race_date, target_date, cutoff)]:
            if r.field_count <= 7:
                continue
            gate_no = getattr(r, "gate_no", 0) or 0
            if gate_no < 1 or gate_no > 8:
                continue
            if key not in by_venue_gate:
                by_venue_gate[key] = {}
            if gate_no not in by_venue_gate[key]:
                by_venue_gate[key][gate_no] = []
            by_venue_gate[key][gate_no].append(1 if r.finish_pos <= 3 else 0)
    for key, gates in by_venue_gate.items():
        total_p = sum(sum(v) for v in gates.values())
        total_n = sum(len(v) for v in gates.values())
        avg = total_p / total_n if total_n else 0.33
        biases = {}
        for g, flags in gates.items():
            if len(flags) >= 5:
                rate = sum(flags) / len(flags)
                diff = rate - avg
                biases[g] = max(-5.0, min(5.0, diff * 15.0))
            else:
                biases[g] = 0.0
        if biases:
            result[key] = biases
    return result


def build_position_sec_per_rank_db(
    course_db: Dict[str, List[PastRun]],
    target_date: Optional[str] = None,
) -> Dict[str, float]:
    """
    コース別に「位置取り1頭差あたりの秒差」を実データから推定。（ローリングウィンドウ対応）
    1〜3着の last_3f と4角通過順位の相関から算出。

    Args:
        target_date: 基準日 (YYYY-MM-DD)。指定時は1年前〜前日のデータのみ使用。
    """
    cutoff = _rolling_window_cutoff(target_date)
    ratios: Dict[str, List[float]] = defaultdict(list)
    for cid, runs in course_db.items():
        filtered = [r for r in runs if _in_rolling_window(r.race_date, target_date, cutoff)]
        if len(filtered) < 20:
            continue
        by_race: Dict[str, List[PastRun]] = defaultdict(list)
        for r in filtered:
            key = f"{r.race_date}_{r.venue}_{r.distance}"
            by_race[key].append(r)
        for race_runs in by_race.values():
            if len(race_runs) < 2:
                continue
            ordered = sorted(race_runs, key=lambda x: x.finish_pos)
            first = ordered[0]
            pos1 = first.positions_corners[-1] if first.positions_corners else first.position_4c
            l3f1 = first.last_3f_sec
            for r in ordered[1:]:
                pos = r.positions_corners[-1] if r.positions_corners else r.position_4c
                diff_pos_rank = max(1, abs(pos - pos1))
                diff_l3f = r.last_3f_sec - l3f1
                if diff_l3f > 0 and diff_pos_rank > 0:
                    ratios[cid].append(diff_l3f / diff_pos_rank)
    result = {}
    for cid, vals in ratios.items():
        if len(vals) >= 5:
            result[cid] = max(0.05, min(0.25, statistics.median(vals)))
    return result


def load_trainer_baseline_db(path: str) -> Dict[str, Dict[str, dict]]:
    """永続化された trainer_baseline_db を読み込む"""
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_trainer_baseline_db(path: str, db: Dict[str, Dict[str, dict]]):
    """trainer_baseline_db を保存"""
    if not path:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def merge_trainer_baseline(
    new_data: Dict[str, Dict[str, dict]], base: Dict[str, Dict[str, dict]]
) -> Dict[str, Dict[str, dict]]:
    """新規データを既存DBにマージ。同キーはサンプルを結合して平均・std再計算"""
    result = dict(base)
    for tid, courses in new_data.items():
        if tid not in result:
            result[tid] = {}
        for course, v in courses.items():
            if "mean_3f" not in v:
                continue
            existing = result[tid].get(course, {})
            if not existing:
                result[tid][course] = dict(v)
                continue
            # サンプル数で重み付き平均
            n1, n2 = existing.get("_n", 1), v.get("_n", 1)
            m1, m2 = existing["mean_3f"], v["mean_3f"]
            s1, s2 = existing.get("std_3f", 0.5), v.get("std_3f", 0.5)
            n = n1 + n2
            new_mean = (m1 * n1 + m2 * n2) / n
            new_std = max(0.1, (s1 + s2) / 2)  # 簡易マージ
            result[tid][course] = {"mean_3f": new_mean, "std_3f": new_std, "_n": int(n)}
    return result


def build_trainer_baseline_db(horses: List[Horse]) -> Dict[str, Dict[str, dict]]:
    """
    調教師×コース別の3F平均・標準偏差を調教記録から算出
    trainer_baseline_db: {trainer_id: {course: {"mean_3f": 35.5, "std_3f": 0.4}}}
    サンプルが2件未満の場合はスキップ
    """
    by_trainer_course: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))

    def _get_3f(rec: TrainingRecord) -> Optional[float]:
        for key in ("3F", "3f", 600, "600", "600m"):
            if key in rec.splits:
                try:
                    return float(rec.splits[key])
                except (TypeError, ValueError):
                    pass
        return None

    for h in horses:
        tid = getattr(h, "trainer_id", "") or ""
        if not tid:
            continue
        for rec in getattr(h, "training_records", []) or []:
            t3f = _get_3f(rec)
            if t3f is not None and rec.course:
                by_trainer_course[tid][rec.course].append(t3f)

    result = {}
    for tid, courses in by_trainer_course.items():
        result[tid] = {}
        for course, times in courses.items():
            if len(times) >= 2:
                result[tid][course] = {
                    "mean_3f": statistics.mean(times),
                    "std_3f": max(0.1, statistics.stdev(times)),
                    "_n": len(times),
                }
            elif len(times) == 1:
                result[tid][course] = {"mean_3f": times[0], "std_3f": 0.5, "_n": 1}
    return result


class RaceHistoryCollector:
    """
    指定期間のレース結果を収集し、基準タイムDBを構築
    """

    def __init__(self, client, db: StandardTimeDBBuilder):
        self.client = client
        self.db = db

    def collect_date_range(self, start: str, end: str):
        """期間内のレースを収集（簡易版: 既存DBがあればそのまま）"""
        logger.info("基準タイムDBは出走馬の過去走からオンザフライで補充されます")
        logger.info("事前収集はスキップしました（必要に応じて build_databases.py を実行）")


# ============================================================
# レース結果スクレイパー
# ============================================================


class RaceResultScraper:
    """レース結果スクレイパー"""

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )

    def scrape_result(self, race_id: str) -> Optional[Dict]:
        """
        レース結果を取得

        Args:
            race_id: レースID（例: "202506021011"）

        Returns:
            {
                'race_id': str,
                'date': str,
                'venue': str,
                'race_name': str,
                'distance': int,
                'surface': str,
                'track_condition': str,
                'horses': [
                    {
                        'finish': int,  # 着順
                        'gate_no': int,
                        'horse_no': int,
                        'horse_name': str,
                        'time': float,  # タイム（秒）
                        'margin': str,  # 着差
                        'odds': float,
                        'popularity': int,
                    },
                    ...
                ]
            }
        """
        url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            # レース情報を取得
            race_info = self._parse_race_info(soup, race_id)
            if not race_info:
                return None

            # 結果テーブルを取得
            horses = self._parse_result_table(soup)
            if not horses:
                return None

            race_info["horses"] = horses

            return race_info

        except Exception as e:
            logger.warning("scrape race %s failed: %s", race_id, e, exc_info=True)
            return None

    def _parse_race_info(self, soup: BeautifulSoup, race_id: str) -> Optional[Dict]:
        """レース情報をパース"""
        try:
            # レース名
            race_name_elem = soup.select_one(".RaceName")
            race_name = race_name_elem.text.strip() if race_name_elem else "不明"

            # 日付（race_idから推定）
            year = race_id[:4]
            month = race_id[4:6]
            day = race_id[6:8]
            date = f"{year}-{month}-{day}"

            # 距離・馬場状態
            data_intro = soup.select_one(".RaceData01")
            if not data_intro:
                return None

            data_text = data_intro.text

            # 距離
            distance_match = re.search(r"([芝ダ直])(\d+)m", data_text)
            if distance_match:
                surface = "芝" if distance_match.group(1) == "芝" else "ダート"
                distance = int(distance_match.group(2))
            else:
                surface = "芝"
                distance = 1600

            # 馬場状態
            condition_match = re.search(r"馬場：([良稍重不])", data_text)
            track_condition = condition_match.group(1) if condition_match else "良"

            # 場所
            venue_elem = soup.select_one(".RaceData02")
            venue_text = venue_elem.text if venue_elem else ""
            venue_match = re.search(r"(\d+)回([^\d]+)\d+日", venue_text)
            venue = venue_match.group(2).strip() if venue_match else "不明"

            return {
                "race_id": race_id,
                "date": date,
                "venue": venue,
                "race_name": race_name,
                "distance": distance,
                "surface": surface,
                "track_condition": track_condition,
            }

        except Exception as e:
            logger.warning("parse race info failed: %s", e, exc_info=True)
            return None

    def _parse_result_table(self, soup: BeautifulSoup) -> List[Dict]:
        """結果テーブルをパース"""
        horses = []

        try:
            result_table = soup.select_one(".ResultTableWrap table")
            if not result_table:
                return []

            rows = result_table.select("tbody tr")

            # ヘッダーからオッズ・人気カラムを動的検出（ばんえい対応）
            odds_col, pop_col = 12, 13  # デフォルト（JRA/NAR標準）
            header = result_table.select_one("thead tr")
            if header:
                for i, th in enumerate(header.select("th")):
                    t = th.get_text(strip=True)
                    if "単勝" in t:
                        odds_col = i
                    elif t == "人気":
                        pop_col = i

            for row in rows:
                try:
                    cells = row.select("td")
                    if len(cells) < 10:
                        continue

                    # 着順
                    finish_text = cells[0].text.strip()
                    if not finish_text.isdigit():
                        continue
                    finish = int(finish_text)

                    # 枠番・馬番
                    gate_no = int(cells[1].text.strip())
                    horse_no = int(cells[2].text.strip())

                    # 馬名
                    horse_name_elem = cells[3].select_one("a")
                    horse_name = (
                        horse_name_elem.text.strip() if horse_name_elem else cells[3].text.strip()
                    )

                    # タイム
                    time_text = cells[7].text.strip()
                    time_sec = self._parse_time(time_text)

                    # 着差
                    margin = cells[8].text.strip()

                    # オッズ（ヘッダー検出位置を使用）
                    odds_text = cells[odds_col].text.strip() if len(cells) > odds_col else ""
                    odds = float(odds_text) if odds_text.replace(".", "").isdigit() else 0.0

                    # 人気（ヘッダー検出位置を使用）
                    popularity_text = cells[pop_col].text.strip() if len(cells) > pop_col else ""
                    popularity = int(popularity_text) if popularity_text.isdigit() else 0

                    horses.append(
                        {
                            "finish": finish,
                            "gate_no": gate_no,
                            "horse_no": horse_no,
                            "horse_name": horse_name,
                            "time": time_sec,
                            "margin": margin,
                            "odds": odds,
                            "popularity": popularity,
                        }
                    )

                except Exception:
                    logger.debug("result row parse failed", exc_info=True)
                    continue

            return horses

        except Exception as e:
            logger.warning("parse result table failed: %s", e, exc_info=True)
            return []

    def _parse_time(self, time_str: str) -> float:
        """タイム文字列を秒に変換（例: "1:34.5" → 94.5）"""
        try:
            if ":" in time_str:
                parts = time_str.split(":")
                minutes = int(parts[0])
                seconds = float(parts[1])
                return minutes * 60 + seconds
            else:
                return float(time_str)
        except (ValueError, TypeError):
            return 0.0

    def scrape_multiple(self, race_ids: List[str], delay: float = 1.0) -> List[Dict]:
        """
        複数のレース結果を取得

        Args:
            race_ids: レースIDのリスト
            delay: リクエスト間隔（秒）

        Returns:
            レース結果のリスト
        """
        results = []

        for i, race_id in enumerate(race_ids):
            logger.info(f"Scraping {i + 1}/{len(race_ids)}: {race_id}")

            result = self.scrape_result(race_id)
            if result:
                results.append(result)

            if i < len(race_ids) - 1:
                time.sleep(delay)

        return results


if __name__ == "__main__":
    scraper = RaceResultScraper()

    # テスト: 2024年有馬記念
    race_id = "202406050811"
    result = scraper.scrape_result(race_id)

    if result:
        logger.info(f"レース: {result['race_name']}")
        logger.info(f"日付: {result['date']}")
        logger.info(f"場所: {result['venue']}")
        logger.info(f"距離: {result['surface']}{result['distance']}m")
        logger.info(f"馬場: {result['track_condition']}")
        logger.info("結果:")
        for h in result["horses"][:3]:
            logger.info(f"{h['finish']}着: {h['horse_name']} ({h['time']:.1f}秒)")
