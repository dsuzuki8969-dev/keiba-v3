"""
競馬解析マスターシステム v3.0 - 騎手・厩舎実成績スクレイパー
H-1: 騎手偏差値（人気別×期間 2×2象限）
H-2: 騎手×コース適性
J-1: 厩舎ランク・回収率プロファイル
J-2: 騎手×調教師コンビ成績

URL:
  騎手: https://db.netkeiba.com/jockey/result/recent/XXXXX/
  厩舎: https://db.netkeiba.com/trainer/result/recent/XXXXX/
"""

import json
import os
import re
import sys
import time as _time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from bs4 import BeautifulSoup

    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

from config.settings import PERSONNEL_DB_PATH
from data.masters.venue_master import JRA_VENUE_CODES, VENUE_NAME_TO_CODE
from src.models import JockeyStats, JushaRank, KaisyuType, TrainerStats
from src.log import get_logger
from src.scraper.netkeiba import BASE_URL, NetkeibaClient

logger = get_logger(__name__)

# ============================================================
# 騎手成績スクレイパー
# ============================================================


class JockeyScraper:
    """
    netkeiba の騎手成績ページから
    H-1用の人気別×期間の偏差値データを取得する

    URL例:
      全成績: https://db.netkeiba.com/jockey/result/recent/01012/
      コース別: https://db.netkeiba.com/jockey/01012/?pid=jockey_leading
    """

    def __init__(self, client: NetkeibaClient):
        self.client = client

    def fetch(self, jockey_id: str, jockey_name: str = "") -> JockeyStats:
        stats = JockeyStats(jockey_id=jockey_id, jockey_name=jockey_name)

        # result/recent ページからレース個別データを取得し、
        # 人気別勝率とコース別成績を同時に構築する
        url = f"{BASE_URL}/jockey/result/recent/{jockey_id}/"
        soup = self.client.get(url)
        if not soup:
            return stats

        # 所属・フルネーム検出
        stats.location = self._detect_affiliation(soup)
        full_name = self._extract_full_name(soup)
        if full_name:
            stats.jockey_name = full_name

        # レース一覧テーブルから人気別・コース別データを構築
        races = self._parse_race_log(soup)
        if not races:
            return stats

        # 人気別集計（直近20走 = 短期 / 全データ = 長期として使う）
        long_data = self._aggregate_by_popularity(races)  # 全20走
        short_data = long_data  # 同一データ（20走しかないため）

        if long_data:
            stats.upper_long_dev = self._calc_deviation(
                long_data.get("upper_wins", 0), long_data.get("upper_runs", 0), "upper"
            )
            stats.lower_long_dev = self._calc_deviation(
                long_data.get("lower_wins", 0), long_data.get("lower_runs", 0), "lower"
            )
            stats.upper_short_dev = stats.upper_long_dev
            stats.lower_short_dev = stats.lower_long_dev
            stats.kaisyu_type = self._classify_kaisyu(long_data)

        # プロフィールページの年度別データから勝率ベース偏差値を算出
        # recent 20走は短期変動が大きいため、キャリア全体の勝率も考慮する
        prof_url = f"{BASE_URL}/jockey/{jockey_id}/"
        prof_soup = self.client.get(prof_url)
        if prof_soup:
            yearly = self._parse_yearly_stats(prof_soup)
            if yearly:
                total_wins = yearly.get("total_wins", 0)
                total_runs = yearly.get("total_runs", 0)
                if total_runs >= 50:
                    overall_wr = total_wins / total_runs
                    # 全体勝率から直接偏差値を算出
                    # JRA平均勝率 ≈ 0.08, σ ≈ 0.04
                    # 偏差値50 = 8%勝率、60 = 12%、70 = 16%、40 = 4%
                    career_dev = max(30.0, min(70.0, 50.0 + (overall_wr - 0.08) / 0.04 * 10))
                    _upper_runs = long_data.get("upper_runs", 0) if long_data else 0
                    _lower_runs = long_data.get("lower_runs", 0) if long_data else 0
                    # recent データが不十分（5走未満）→ career_dev で上書き
                    # recent データが十分 → career_dev を下限として保証
                    if _upper_runs < 5:
                        stats.upper_long_dev = career_dev
                        stats.upper_short_dev = career_dev
                    else:
                        stats.upper_long_dev = max(stats.upper_long_dev, career_dev)
                        stats.upper_short_dev = max(stats.upper_short_dev, career_dev)
                    if _lower_runs < 5:
                        stats.lower_long_dev = career_dev
                        stats.lower_short_dev = career_dev
                    else:
                        stats.lower_long_dev = max(stats.lower_long_dev, career_dev)
                        stats.lower_short_dev = max(stats.lower_short_dev, career_dev)

        # コース別成績を構築
        stats.course_records = self._build_course_records_from_races(races)

        return stats

    def _parse_race_log(self, soup: BeautifulSoup) -> list:
        """result/recent ページのレース一覧テーブルを解析する"""
        table = soup.select_one("table")
        if not table:
            return []
        rows = table.select("tr")
        if len(rows) < 2:
            return []

        headers = [h.get_text(strip=True) for h in rows[0].select("td, th")]
        races = []
        for row in rows[1:]:
            cells = row.select("td")
            if len(cells) < len(headers):
                continue
            data = {}
            for h, c in zip(headers, cells):
                data[h] = c.get_text(strip=True)
            # 着順と人気を数値化
            try:
                finish = data.get("着順", "")
                popularity = data.get("人気", "")
                distance_str = data.get("距離", "")
                venue_str = data.get("開催", "")
                if not finish or not finish.isdigit():
                    continue
                data["_finish"] = int(finish)
                data["_popularity"] = int(popularity) if popularity.isdigit() else 0
                # 距離パース: "芝1800" or "ダ1200"
                m = re.match(r"(芝|ダ)(\d{3,4})", distance_str)
                if m:
                    data["_surface"] = "芝" if m.group(1) == "芝" else "ダート"
                    data["_distance"] = int(m.group(2))
                # 開催パース: "2中山2" → "中山"
                vm = re.match(r"\d*(.+?)\d*$", venue_str)
                if vm:
                    data["_venue"] = vm.group(1)
                races.append(data)
            except (ValueError, AttributeError):
                continue
        return races

    def _aggregate_by_popularity(self, races: list) -> dict:
        """レースリストから人気別の勝率を集計"""
        result = {
            "upper_wins": 0, "upper_runs": 0,
            "lower_wins": 0, "lower_runs": 0,
            "upper_recovery": 0.0, "lower_recovery": 0.0,
        }
        for r in races:
            pop = r.get("_popularity", 0)
            fin = r.get("_finish", 99)
            if pop == 0:
                continue
            if pop <= 3:
                result["upper_runs"] += 1
                if fin == 1:
                    result["upper_wins"] += 1
            else:
                result["lower_runs"] += 1
                if fin == 1:
                    result["lower_wins"] += 1
        return result if (result["upper_runs"] + result["lower_runs"]) > 0 else None

    def _parse_yearly_stats(self, soup: BeautifulSoup) -> Optional[dict]:
        """プロフィールページの ResultsByYears テーブルから勝利数・騎乗回数を取得"""
        tables = soup.select(".ResultsByYears")
        if not tables:
            return None
        # 最初のテーブル（中央成績）の累計行を取得
        table = tables[0]
        for row in table.select("tr"):
            cells = row.select("td, th")
            if len(cells) < 7:
                continue
            text = cells[0].get_text(strip=True)
            if text == "累計":
                try:
                    wins = int(cells[2].get_text(strip=True).replace(",", ""))
                    runs = int(cells[6].get_text(strip=True).replace(",", ""))
                    return {"total_wins": wins, "total_runs": runs}
                except (ValueError, IndexError):
                    pass
        return None

    def _build_course_records_from_races(self, races: list) -> dict:
        """レースリストからコース別成績を構築"""
        from data.masters.venue_master import VENUE_NAME_TO_CODE
        course_agg = {}
        for r in races:
            venue = r.get("_venue", "")
            surface = r.get("_surface", "")
            distance = r.get("_distance", 0)
            fin = r.get("_finish", 99)
            if not venue or not surface or not distance:
                continue
            vc = VENUE_NAME_TO_CODE.get(venue)
            if not vc:
                continue
            cid = f"{vc}_{surface}_{distance}"
            if cid not in course_agg:
                course_agg[cid] = {"wins": 0, "runs": 0}
            course_agg[cid]["runs"] += 1
            if fin == 1:
                course_agg[cid]["wins"] += 1

        course_records = {}
        for cid, agg in course_agg.items():
            if agg["runs"] >= 2:  # 2走以上
                dev = self._calc_deviation(agg["wins"], agg["runs"], "upper")
                course_records[cid] = {
                    "all_dev": dev,
                    "upper_dev": dev,
                    "lower_dev": dev,
                    "sample_n": agg["runs"],
                }
        return course_records

    @staticmethod
    def _extract_full_name(soup: BeautifulSoup) -> str:
        """ページタイトルからフルネームを抽出"""
        title = soup.find("title")
        if title:
            m = re.match(r"^(.+?)の騎手成績", title.get_text())
            if m:
                return m.group(1).strip()
        return ""

    def _detect_affiliation(self, soup: BeautifulSoup) -> str:
        """HTML から騎手/調教師の所属（美浦・栗東・NAR場名）を検出する"""
        _NAR_VENUES = [
            "大井", "船橋", "川崎", "浦和", "門別",
            "盛岡", "水沢", "金沢", "笠松", "名古屋",
            "園田", "姫路", "高知", "佐賀", "帯広",
        ]
        text = soup.get_text()
        if "(美)" in text or "美浦" in text:
            return "美浦"
        if "(栗)" in text or "栗東" in text:
            return "栗東"
        for v in _NAR_VENUES:
            if f"({v})" in text:
                return v
        return ""

    # _fetch_period_stats と _parse_stats_table は fetch() に統合済み

    def _calc_deviation(self, wins: int, runs: int, ninki_type: str) -> float:
        """
        H-1: 勝率から偏差値を算出
        基準: 上位人気平均勝率=0.30, 下位人気=0.06
        σ: 上位=0.08, 下位=0.04
        """
        if runs == 0:
            return 50.0

        win_rate = wins / runs

        if ninki_type == "upper":
            base_rate, sigma = 0.30, 0.08
        else:
            base_rate, sigma = 0.06, 0.04

        deviation = 50 + (win_rate - base_rate) / sigma * 10
        return max(30.0, min(70.0, deviation))

    def _classify_kaisyu(self, data: dict) -> KaisyuType:
        """H-1: 回収率タイプ分類"""
        upper_r = data.get("upper_recovery", 80)
        lower_r = data.get("lower_recovery", 80)
        upper_wr = data["upper_wins"] / max(data["upper_runs"], 1)

        if upper_wr >= 0.30 and upper_r >= 85:
            return KaisyuType.SHINRAITYPE
        if lower_r >= 100:
            return KaisyuType.ANA_TYPE
        if upper_wr >= 0.30 and upper_r < 75:
            return KaisyuType.KAJOHYOKA
        return KaisyuType.HEIBONTYPE

    # _fetch_course_records は fetch() 内の _build_course_records_from_races に統合済み


# ============================================================
# 厩舎成績スクレイパー
# ============================================================


class TrainerScraper:
    """
    netkeiba の厩舎成績ページから J-1 データを取得する
    中央・地方24場対応。good_venues/bad_venues/location を近走成績から算出
    """

    MIN_RUNS_FOR_VENUE = 20  # 得意/苦手判定の最低出走数

    def __init__(self, client: NetkeibaClient):
        self.client = client

    def fetch(self, trainer_id: str, trainer_name: str = "") -> TrainerStats:
        stats = TrainerStats(
            trainer_id=trainer_id,
            trainer_name=trainer_name,
            stable_name=trainer_name,
            location="JRA",
        )

        # プロフィールページから年度別集計データを取得
        prof_url = f"{BASE_URL}/trainer/{trainer_id}/"
        prof_soup = self.client.get(prof_url)

        # result/recent ページからレース一覧を取得
        url = f"{BASE_URL}/trainer/result/recent/{trainer_id}/"
        soup = self.client.get(url)
        if not soup and not prof_soup:
            return stats

        # 厩舎ランクと回収率を算出（プロフィールページ優先、なければレース一覧）
        data = self._parse_trainer_stats(prof_soup or soup)

        # 休み明け回収率
        stats.recovery_break = data.get("break_recovery", 0.0)

        # 短期勢い (2ヶ月の勝率が長期より高ければ好調)
        long_wr = data.get("long_win_rate", 0.0)
        short_wr = data.get("short_win_rate", 0.0)
        if short_wr - long_wr >= 0.03:
            stats.short_momentum = "好調"
        elif long_wr - short_wr >= 0.03:
            stats.short_momentum = "不調"

        # 近走成績ページから競馬場別・得意/苦手・location を取得
        venue_data = self._fetch_venue_stats_from_races(trainer_id)
        if venue_data:
            stats.good_venues = venue_data.get("good_venues", [])
            stats.bad_venues = venue_data.get("bad_venues", [])
            stats.location = venue_data.get("location", stats.location)

        # プロフィールページまたはレースページから美浦/栗東を検出
        page_for_affil = prof_soup or soup
        if page_for_affil and (not stats.location or stats.location in ("JRA", "地方")):
            affil = self._detect_affiliation_trainer(page_for_affil)
            if affil:
                stats.location = affil

        # NAR/JRA判定後にランク分類（NAR調教師は閾値が異なるため）
        is_nar = stats.location not in ("JRA", "美浦", "栗東")
        stats.rank = self._classify_rank(data, is_nar=is_nar)
        stats.kaisyu_type = self._classify_kaisyu(data)

        # 勝率から偏差値を算出
        # JRA実測: 平均≈0.076, σ≈0.031（15名サンプル）
        # NAR実測: 平均≈0.10, σ≈0.04（race_log集計）
        wr = data.get("long_win_rate", 0.0)
        tr = data.get("total_runs", 0)
        if tr >= 20 and wr > 0:
            if is_nar:
                base_rate, sigma = 0.10, 0.04  # NAR
            else:
                base_rate, sigma = 0.07, 0.03  # JRA
            stats.deviation = max(30.0, min(70.0, 50.0 + (wr - base_rate) / sigma * 10))

        # ページタイトルからフルネームを取得
        for s in [prof_soup, soup]:
            if s:
                full_name = self._extract_full_name(s)
                if full_name:
                    stats.trainer_name = full_name
                    stats.stable_name = full_name
                    break

        return stats

    @staticmethod
    def _extract_full_name(soup: BeautifulSoup) -> str:
        """ページタイトルから調教師フルネームを抽出"""
        title = soup.find("title")
        if title:
            m = re.match(r"^(.+?)の調教師成績", title.get_text())
            if m:
                return m.group(1).strip()
        return ""

    def _detect_affiliation_trainer(self, soup: BeautifulSoup) -> str:
        """調教師ページHTML から所属（美浦・栗東・NAR場名）を検出する"""
        _NAR_VENUES = [
            "大井", "船橋", "川崎", "浦和", "門別",
            "盛岡", "水沢", "金沢", "笠松", "名古屋",
            "園田", "姫路", "高知", "佐賀", "帯広",
        ]
        text = soup.get_text()
        if "(美)" in text or "美浦" in text:
            return "美浦"
        if "(栗)" in text or "栗東" in text:
            return "栗東"
        for v in _NAR_VENUES:
            if f"({v})" in text:
                return v
        return ""

    def _fetch_venue_stats_from_races(self, trainer_id: str) -> Optional[dict]:
        """近走成績ページをパースし、競馬場別成績・得意/苦手・location を算出"""
        url = f"{BASE_URL}/trainer/race.html?id={trainer_id}"
        soup = self.client.get(url)
        if not soup:
            return None

        # 表ヘッダ: 日付(0), 開催(1), 天気(2), R(3), レース名(4), 映像(5), 頭数(6), 枠番(7), 馬番(8), 単勝(9), 人気(10), 着順(11), ...
        venue_wins: Dict[str, int] = {}
        venue_runs: Dict[str, int] = {}
        jra_count = 0
        chiho_count = 0

        for row in soup.select("table tr"):
            cells = row.select("td")
            if len(cells) < 12:
                continue

            kaikai = cells[1].get_text(strip=True)  # 開催列
            venue_name = self._parse_venue_from_kaikai(kaikai)
            if not venue_name:
                continue

            chakujun_text = cells[11].get_text(strip=True)  # 着順列
            try:
                chakujun = int(chakujun_text) if chakujun_text.isdigit() else 0
            except ValueError:
                chakujun = 0

            venue_wins[venue_name] = venue_wins.get(venue_name, 0) + (1 if chakujun == 1 else 0)
            venue_runs[venue_name] = venue_runs.get(venue_name, 0) + 1

            vc = VENUE_NAME_TO_CODE.get(venue_name)
            if vc:
                if vc in JRA_VENUE_CODES:
                    jra_count += 1
                else:
                    chiho_count += 1

        if not venue_runs:
            return None

        total_wins = sum(venue_wins.values())
        total_runs = sum(venue_runs.values())
        avg_wr = total_wins / total_runs if total_runs else 0

        good, bad = [], []
        for vname, runs in venue_runs.items():
            if runs < self.MIN_RUNS_FOR_VENUE:
                continue
            wr = venue_wins.get(vname, 0) / runs
            if wr >= avg_wr * 1.15:
                good.append(vname)
            elif wr <= avg_wr * 0.85 and avg_wr > 0.02:
                bad.append(vname)

        if jra_count >= chiho_count:
            location = "JRA"
        else:
            # NAR の場合、最も多く出走した競馬場を所属地として返す
            nar_venue_runs = {
                v: venue_runs[v]
                for v in venue_runs
                if VENUE_NAME_TO_CODE.get(v) not in JRA_VENUE_CODES
            }
            if nar_venue_runs:
                location = max(nar_venue_runs, key=nar_venue_runs.get)
            else:
                location = "地方"

        return {
            "good_venues": good[:5],
            "bad_venues": bad[:5],
            "location": location,
        }

    def _parse_venue_from_kaikai(self, kaikai: str) -> Optional[str]:
        """開催文字列から競馬場名を抽出。例: 1小倉8→小倉, 名古屋→名古屋"""
        if not kaikai:
            return None
        for vname in VENUE_NAME_TO_CODE.keys():
            if vname in kaikai:
                return vname
        return None

    def _parse_trainer_stats(self, soup: BeautifulSoup) -> dict:
        """近走レース一覧テーブルから勝率・トレンドを算出する"""
        data = {
            "total_wins": 0,
            "total_runs": 0,
            "long_win_rate": 0.0,
            "short_win_rate": 0.0,
            "upper_recovery": 80.0,
            "lower_recovery": 80.0,
            "break_recovery": 0.0,
        }

        # 方法1: ResultsByYears テーブルから年度別データを取得
        # 累計（キャリア全体）ではなく直近3年の勝率を使用
        # （長期キャリア調教師の初期低勝率に引きずられるのを防止）
        rby = soup.select(".ResultsByYears")
        if rby:
            yearly_data = []  # [(year, wins, runs), ...]
            cumulative_wins, cumulative_runs = 0, 0
            for row in rby[0].select("tr"):
                cells = row.select("td, th")
                if len(cells) >= 7:
                    text = cells[0].get_text(strip=True)
                    if text == "累計":
                        try:
                            cumulative_wins = int(cells[2].get_text(strip=True).replace(",", ""))
                            cumulative_runs = int(cells[6].get_text(strip=True).replace(",", ""))
                        except (ValueError, IndexError):
                            pass
                    elif text.isdigit() and len(text) == 4:
                        # 年度行（例: 2026, 2025, ...）
                        try:
                            y_wins = int(cells[2].get_text(strip=True).replace(",", ""))
                            y_runs = int(cells[6].get_text(strip=True).replace(",", ""))
                            if y_runs > 0:
                                yearly_data.append((int(text), y_wins, y_runs))
                        except (ValueError, IndexError):
                            pass

            # 直近3年 vs 累計の高い方を採用
            # （現役上昇中 → 直近、引退/ベテラン → 累計キャリアピーク）
            recent_wins, recent_runs = 0, 0
            for _y, _w, _r in sorted(yearly_data, reverse=True)[:3]:
                recent_wins += _w
                recent_runs += _r

            recent_wr = recent_wins / recent_runs if recent_runs >= 30 else 0.0
            cumulative_wr = cumulative_wins / cumulative_runs if cumulative_runs > 0 else 0.0

            if recent_wr >= cumulative_wr and recent_runs >= 30:
                # 直近が累計以上 → 直近を使用（現役で好調/安定）
                data["total_wins"] = recent_wins
                data["total_runs"] = recent_runs
                data["long_win_rate"] = recent_wr
            elif cumulative_runs > 0:
                # 累計の方が高い or 直近サンプル不足 → 累計を使用
                data["total_wins"] = cumulative_wins
                data["total_runs"] = cumulative_runs
                data["long_win_rate"] = cumulative_wr

            # 短期勝率: 直近1年
            if yearly_data:
                latest = sorted(yearly_data, reverse=True)[0]
                if latest[2] >= 10:
                    data["short_win_rate"] = latest[1] / latest[2]

        # 方法2: ResultsByYears がなければレース一覧テーブルから集計
        if data["total_runs"] == 0:
            table = soup.select_one("table")
            if table:
                headers = [h.get_text(strip=True) for h in table.select("tr:first-child td, tr:first-child th")]
                fin_idx = -1
                for i, h in enumerate(headers):
                    if h == "着順":
                        fin_idx = i
                        break
                if fin_idx >= 0:
                    for row in table.select("tr")[1:]:
                        cells = row.select("td")
                        if len(cells) > fin_idx:
                            finish_text = cells[fin_idx].get_text(strip=True)
                            if finish_text.isdigit():
                                data["total_runs"] += 1
                                if int(finish_text) == 1:
                                    data["total_wins"] += 1
                    if data["total_runs"] > 0:
                        data["long_win_rate"] = data["total_wins"] / data["total_runs"]
                        # 直近5走で短期勝率を推定
                        short_wins = 0
                        short_runs = min(5, data["total_runs"])
                        count = 0
                        for row in table.select("tr")[1:6]:
                            cells = row.select("td")
                            if len(cells) > fin_idx:
                                ft = cells[fin_idx].get_text(strip=True)
                                if ft.isdigit():
                                    count += 1
                                    if int(ft) == 1:
                                        short_wins += 1
                        if count > 0:
                            data["short_win_rate"] = short_wins / count

        return data

    def _classify_rank(self, data: dict, is_nar: bool = False) -> JushaRank:
        """
        調教師の勝率からランクを分類する。
        NAR調教師はJRAより平均勝率が高い傾向にあるため、
        閾値を引き上げて相対的に同等の評価基準にする。
        """
        wr = data.get("long_win_rate", 0.0)
        tr = data.get("total_runs", 0)
        if is_nar:
            # NAR調教師の閾値（JRAより高め）
            # NAR: A ≥ 0.22, B ≥ 0.15, C ≥ 0.10
            if wr >= 0.22 and tr >= 100:
                return JushaRank.A
            if wr >= 0.15 and tr >= 50:
                return JushaRank.B
            if wr >= 0.10:
                return JushaRank.C
            return JushaRank.D
        else:
            # JRA調教師の閾値
            # JRA: A ≥ 0.18, B ≥ 0.12, C ≥ 0.07
            if wr >= 0.18 and tr >= 100:
                return JushaRank.A
            if wr >= 0.12 and tr >= 50:
                return JushaRank.B
            if wr >= 0.07:
                return JushaRank.C
            return JushaRank.D

    def _classify_kaisyu(self, data: dict) -> KaisyuType:
        upper = data.get("upper_recovery", 80)
        lower = data.get("lower_recovery", 80)
        wr = data.get("long_win_rate", 0.0)
        if wr >= 0.15 and upper >= 85:
            return KaisyuType.SHINRAITYPE
        if lower >= 100:
            return KaisyuType.ANA_TYPE
        if wr >= 0.15 and upper < 75:
            return KaisyuType.KAJOHYOKA
        return KaisyuType.HEIBONTYPE


# ============================================================
# 馬場状態別集計（course_db から）
# ============================================================


def aggregate_condition_from_course_db(
    course_db: Dict[str, List],
) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """
    course_db の PastRun から騎手・調教師の馬場状態別（良/稍重/重/不良）実績を集計。

    Returns:
        (jockey_condition_records, trainer_condition_records)
        各要素: {id: {"良": {wins, runs}, "稍重": {...}, ...}}
    """
    from src.models import PastRun

    jockey_cond: Dict[str, Dict[str, Dict]] = {}
    trainer_cond: Dict[str, Dict[str, Dict]] = {}

    def _ensure(d: Dict, key: str, subkey: str):
        if key not in d:
            d[key] = {}
        if subkey not in d[key]:
            d[key][subkey] = {"wins": 0, "runs": 0}

    for cid, runs in course_db.items():
        for r in runs:
            if not isinstance(r, PastRun):
                continue
            cond = r.condition if r.condition in ("良", "稍重", "重", "不良") else "良"
            is_win = r.finish_pos == 1

            if r.jockey_id:
                _ensure(jockey_cond, r.jockey_id, cond)
                jockey_cond[r.jockey_id][cond]["runs"] += 1
                if is_win:
                    jockey_cond[r.jockey_id][cond]["wins"] += 1

            if r.trainer_id:
                _ensure(trainer_cond, r.trainer_id, cond)
                trainer_cond[r.trainer_id][cond]["runs"] += 1
                if is_win:
                    trainer_cond[r.trainer_id][cond]["wins"] += 1

    return jockey_cond, trainer_cond


def _rget(rec, key, default=""):
    """course_db レコードの値取得（dict/object 両対応）"""
    if isinstance(rec, dict):
        return rec.get(key, default)
    return getattr(rec, key, default)


# 異体字→正規化マップ（騎手・調教師名マッチング用）
_KANJI_NORMALIZE = str.maketrans({
    "邊": "辺", "邉": "辺", "齋": "斎", "齊": "斎", "斉": "斎",
    "髙": "高", "﨑": "崎", "德": "徳", "惠": "恵", "廣": "広",
    "國": "国", "圀": "国", "條": "条", "澤": "沢", "濱": "浜",
    "櫻": "桜", "實": "実", "壽": "寿", "與": "与", "龍": "竜",
    "驒": "騨", "轟": "轟", "藝": "芸", "萬": "万", "學": "学",
    "應": "応", "會": "会", "辯": "弁", "瀧": "滝", "島": "島",
    "嶋": "島", "嶌": "島", "黑": "黒", "禮": "礼", "靜": "静",
    "眞": "真", "遙": "遥", "亮": "亮", "晃": "晃",
})


def _normalize_name(name: str) -> str:
    """異体字正規化 + 表記揺れ統一（ソース間の名前突合用）

    対応パターン:
    - 異体字: 龍→竜、邊→辺 等
    - 所属プレフィックス: (美)矢作芳人 → 矢作芳人
    - 全角/半角スペース: 田島 寿一 → 田島寿一
    - 外国人騎手: C.ルメール → ルメール
    - マーカー記号: △長浜 → 長浜
    """
    import re as _re
    s = name.translate(_KANJI_NORMALIZE)
    # 所属サフィックス/プレフィックス除去: (美)矢作、赤津和（浦和）、（大井）森泰斗 等
    s = _re.sub(r"[（(][^）)]+[）)]", "", s)
    # 全角/半角スペース除去
    s = s.replace("\u3000", "").replace(" ", "")
    # 外国人騎手: ドット表記統一（全角．→半角. , 中点·→.）
    # ※ C.デムーロとM.デムーロは別人なのでプレフィックス自体は保持
    s = _re.sub(r"^([A-Za-zＡ-Ｚａ-ｚ]+)[．·]", lambda m: m.group(1) + ".", s)
    # 全角英字→半角英字（Ｃ.ルメール → C.ルメール）
    s = s.translate(str.maketrans(
        "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    ))
    # マーカー記号除去
    s = _re.sub(r"^[△▲★☆◉◎○×◇●▼\s]+", "", s)
    return s.strip()


def build_jockey_stats_from_course_db(
    jockey_id: str,
    jockey_name: str,
    course_db: Dict[str, List],
) -> JockeyStats:
    """
    course_db から騎手偏差値を算出する（ウェブ取得失敗時のフォールバック）。
    全騎手の勝率を相対ランキングし、パーセンタイル → 偏差値化する。
    同一データセット内での相対評価のため、G1偏重バイアスも打ち消せる。

    ★ course_records も構築する（NAR騎手の詳細グレード対応）
    """
    # ── 全騎手のコース別成績を集計 ──
    # jid → {course_id → {wins, runs}}
    jockey_course_agg: Dict[str, Dict[str, list]] = {}
    # jid → {condition → {wins, runs}}
    jockey_cond_agg: Dict[str, Dict[str, list]] = {}
    # jid → [wins, runs] (全体)
    jockey_total: Dict[str, list] = {}
    # 名前→IDマップ（ID不一致時の逆引き用）
    name_to_cdb_id: Dict[str, str] = {}

    for cid, runs in course_db.items():
        for r in runs:
            jid = _rget(r, "jockey_id", "") or ""
            jname = _rget(r, "jockey", "") or ""
            # NAR新聞HTMLにはjockey_idが無い → 名前をキーとして使用
            if not jid:
                if not jname:
                    continue
                jid = f"_name_{jname}"
            if jname:
                name_to_cdb_id[jname] = jid
                # 異体字正規化名も登録
                _norm = _normalize_name(jname)
                if _norm != jname:
                    name_to_cdb_id[_norm] = jid
            fp = _rget(r, "finish_pos", 0)
            if isinstance(fp, str):
                try:
                    fp = int(fp)
                except ValueError:
                    continue
            # 全体集計
            if jid not in jockey_total:
                jockey_total[jid] = [0, 0]
            jockey_total[jid][1] += 1
            if fp == 1:
                jockey_total[jid][0] += 1
            # コース別集計
            if jid not in jockey_course_agg:
                jockey_course_agg[jid] = {}
            if cid not in jockey_course_agg[jid]:
                jockey_course_agg[jid][cid] = [0, 0]
            jockey_course_agg[jid][cid][1] += 1
            if fp == 1:
                jockey_course_agg[jid][cid][0] += 1
            # 馬場状態別集計
            cond = _rget(r, "condition", "") or ""
            if cond:
                if jid not in jockey_cond_agg:
                    jockey_cond_agg[jid] = {}
                if cond not in jockey_cond_agg[jid]:
                    jockey_cond_agg[jid][cond] = [0, 0]
                jockey_cond_agg[jid][cond][1] += 1
                if fp == 1:
                    jockey_cond_agg[jid][cond][0] += 1

    # jockey_id が course_db の ID と形式が違う場合、名前で逆引き
    our = jockey_total.get(jockey_id)
    resolved_jid = jockey_id
    if not our and jockey_name:
        # 名前から course_db 内の ID を探す（NAR公式ID ↔ netkeiba ID の不一致対策）
        # pred 側: "篠谷葵（船橋）" → course_db 側: "篠谷葵" or "▲篠谷葵"
        import re
        _clean = re.sub(r"[（(].+?[）)]", "", jockey_name).replace("　", "").strip()
        _clean_no_mark = re.sub(r"^[△▲☆★◇]", "", _clean)
        # 異体字正規化も試す（渡邊→渡辺, 齋藤→斎藤 等）
        _norm_clean = _normalize_name(_clean_no_mark)
        cdb_jid = (
            name_to_cdb_id.get(_clean)
            or name_to_cdb_id.get(_clean_no_mark)
            or name_to_cdb_id.get(_norm_clean)
        )
        if not cdb_jid:
            # 柔軟マッチ（省略名対策: "笠野雄" → "笠野雄大", "木間龍" → "木間塚龍"）
            # 姓（先頭2文字）＋名の最後の文字で候補を絞り込む
            for cname, cid in name_to_cdb_id.items():
                cn = re.sub(r"^[△▲☆★◇]", "", cname)
                cn_norm = _normalize_name(cn)
                if cn.startswith(_clean_no_mark) or _clean_no_mark.startswith(cn):
                    cdb_jid = cid
                    break
                if cn_norm.startswith(_norm_clean) or _norm_clean.startswith(cn_norm):
                    cdb_jid = cid
                    break
                # 姓一致＋名末尾一致（"木間龍"→"木間塚龍": 先頭2文字"木間"＋末尾"龍"）
                if len(_norm_clean) >= 3 and len(cn_norm) >= 3:
                    if cn_norm[:2] == _norm_clean[:2] and cn_norm[-1] == _norm_clean[-1]:
                        cdb_jid = cid
                        break
        if cdb_jid:
            our = jockey_total.get(cdb_jid)
            resolved_jid = cdb_jid
    if not our or our[1] < 5:
        return JockeyStats(jockey_id=jockey_id, jockey_name=jockey_name)

    our_wins, our_runs = our[0], our[1]

    # ── ベイズ補正付きz-scoreで偏差値を算出 ──
    # 母集団の平均勝率を算出（20走以上の騎手のみ、信頼性確保）
    _MIN_RUNS_POP = 20
    pop_wins = sum(v[0] for v in jockey_total.values() if v[1] >= _MIN_RUNS_POP)
    pop_runs = sum(v[1] for v in jockey_total.values() if v[1] >= _MIN_RUNS_POP)
    pop_wr = pop_wins / pop_runs if pop_runs > 0 else 0.08

    # ベイズ収縮: 少数走の騎手は母集団平均に引き寄せる
    # 収縮強度 k=30 → 30走で実測値と母集団平均が半々
    _SHRINK_K = 30
    adj_wr = (our_wins + _SHRINK_K * pop_wr) / (our_runs + _SHRINK_K)

    # 母集団の標準偏差を算出
    import math
    valid_entries = [(v[0], v[1]) for v in jockey_total.values() if v[1] >= _MIN_RUNS_POP]
    if len(valid_entries) >= 5:
        adj_wrs = [(w + _SHRINK_K * pop_wr) / (r + _SHRINK_K) for w, r in valid_entries]
        _mean = sum(adj_wrs) / len(adj_wrs)
        _var = sum((x - _mean) ** 2 for x in adj_wrs) / len(adj_wrs)
        pop_sigma = math.sqrt(_var) if _var > 0 else 0.03
    else:
        pop_sigma = 0.03

    # z-score → 偏差値（50中心、σ=10）
    z = (adj_wr - pop_wr) / pop_sigma if pop_sigma > 0 else 0
    dev = round(max(30.0, min(70.0, 50.0 + z * 10.0)), 1)

    # ── course_records を構築（コース別偏差値、ベイズ補正付き） ──
    course_records: Dict[str, Dict[str, Any]] = {}
    our_courses = jockey_course_agg.get(resolved_jid, {})
    for cid, (wins, runs) in our_courses.items():
        if runs < 2:
            continue
        # コース別の母集団平均・標準偏差を算出
        cid_entries = []
        for jid, courses in jockey_course_agg.items():
            if cid in courses and courses[cid][1] >= 3:
                cw, cr = courses[cid]
                cid_entries.append((cw, cr))
        if not cid_entries:
            course_dev = dev  # フォールバック: 全体偏差値を使用
        else:
            cid_pop_w = sum(e[0] for e in cid_entries)
            cid_pop_r = sum(e[1] for e in cid_entries)
            cid_pop_wr = cid_pop_w / cid_pop_r if cid_pop_r > 0 else pop_wr
            # ベイズ補正（コース別は走数が少ないので収縮強度高め）
            _CID_K = 15
            cid_adj_wr = (wins + _CID_K * cid_pop_wr) / (runs + _CID_K)
            # コース別σ
            cid_adj_wrs = [(w + _CID_K * cid_pop_wr) / (r + _CID_K) for w, r in cid_entries]
            cid_mean = sum(cid_adj_wrs) / len(cid_adj_wrs)
            cid_var = sum((x - cid_mean) ** 2 for x in cid_adj_wrs) / len(cid_adj_wrs)
            cid_sigma = math.sqrt(cid_var) if cid_var > 0 else 0.03
            cid_z = (cid_adj_wr - cid_pop_wr) / cid_sigma if cid_sigma > 0 else 0
            course_dev = round(max(30.0, min(70.0, 50.0 + cid_z * 10.0)), 1)
        course_records[cid] = {
            "all_dev": course_dev,
            "upper_dev": course_dev,
            "lower_dev": course_dev,
            "sample_n": runs,
        }

    # ── condition_records を構築（馬場状態別成績） ──
    condition_records: Dict[str, Dict[str, Any]] = {}
    our_conds = jockey_cond_agg.get(resolved_jid, {})
    for cond, (wins, runs) in our_conds.items():
        if runs >= 2:
            condition_records[cond] = {"wins": wins, "runs": runs}

    stats = JockeyStats(
        jockey_id=jockey_id,
        jockey_name=jockey_name,
        upper_long_dev=dev,
        upper_short_dev=dev,
        lower_long_dev=dev,
        lower_short_dev=dev,
        course_records=course_records,
        condition_records=condition_records,
    )
    return stats


def build_trainer_stats_from_course_db(
    trainer_id: str,
    trainer_name: str,
    course_db: Dict[str, List],
    nk_name_to_id: Optional[Dict[str, str]] = None,
) -> TrainerStats:
    """
    course_db から調教師のランクを算出する（ウェブ取得失敗時のフォールバック）。
    全調教師の勝率を相対ランキングし、パーセンタイルでA/B/C/Dに分類する。
    nk_name_to_id: personnel_db のnetkeiba trainer名→IDマップ（NAR ID不一致対策）
    """
    import datetime

    cutoff = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")

    # データセット内全調教師の集計 [wins, runs, recent_wins, recent_runs, places]
    trainer_stats_raw: Dict[str, list] = {}
    # 名前→IDマップ（ID不一致時の逆引き用）
    name_to_cdb_tid: Dict[str, str] = {}
    for cid, runs in course_db.items():
        for r in runs:
            tid = _rget(r, "trainer_id", "") or ""
            if not tid:
                continue
            # 調教師名のマップは course_db に trainer フィールドがないため cid から取れないが、
            # trainer_id は集計に使えるのでそのまま
            if tid not in trainer_stats_raw:
                trainer_stats_raw[tid] = [0, 0, 0, 0, 0]
            fp = _rget(r, "finish_pos", 0)
            if isinstance(fp, str):
                try:
                    fp = int(fp)
                except ValueError:
                    continue
            rd = _rget(r, "race_date", "")
            trainer_stats_raw[tid][1] += 1
            if fp == 1:
                trainer_stats_raw[tid][0] += 1
            if fp <= 3:
                trainer_stats_raw[tid][4] += 1
            if rd >= cutoff:
                trainer_stats_raw[tid][3] += 1
                if fp == 1:
                    trainer_stats_raw[tid][2] += 1

    our = trainer_stats_raw.get(trainer_id)
    resolved_tid = trainer_id
    if not our and trainer_name and nk_name_to_id:
        # NAR公式ID（11xxx）がcourse_dbに無い場合、名前からnetkeiba IDを逆引き
        import re
        _clean = re.sub(r"[（(].+?[）)]", "", trainer_name).replace("　", "").strip()
        _norm_clean = _normalize_name(_clean)
        for nk_name, nk_id in nk_name_to_id.items():
            nk_norm = _normalize_name(nk_name)
            # 前方一致 or 姓＋名末尾一致（省略名対策）
            if nk_name.startswith(_clean) or _clean.startswith(nk_name):
                cand = trainer_stats_raw.get(nk_id)
                if cand:
                    our = cand
                    resolved_tid = nk_id
                    break
            # 異体字正規化後の一致
            if nk_norm.startswith(_norm_clean) or _norm_clean.startswith(nk_norm):
                cand = trainer_stats_raw.get(nk_id)
                if cand:
                    our = cand
                    resolved_tid = nk_id
                    break
            if len(_norm_clean) >= 3 and len(nk_norm) >= 3:
                if nk_norm[:2] == _norm_clean[:2] and nk_norm[-1] == _norm_clean[-1]:
                    cand = trainer_stats_raw.get(nk_id)
                    if cand:
                        our = cand
                        resolved_tid = nk_id
                        break
    total_runs = our[1] if our else 0
    total_wins = our[0] if our else 0
    recent_runs = our[3] if our else 0
    recent_wins = our[2] if our else 0
    total_places = our[4] if our else 0

    wr = total_wins / max(total_runs, 1)
    pr = total_places / max(total_runs, 1)

    # NAR/JRA判定: course_db 内のレースの venue_code から主要競馬場を特定
    from data.masters.venue_master import JRA_VENUE_CODES as _JRA_CODES
    _nar_venue_codes = {"30", "35", "36", "42", "43", "44", "45",
                        "46", "47", "48", "50", "51", "54", "55"}
    jra_runs_count = 0
    nar_runs_count = 0
    for cid, cid_runs in course_db.items():
        for r in cid_runs:
            tid = _rget(r, "trainer_id", "") or ""
            if tid != resolved_tid:
                continue
            vc = _rget(r, "venue", "") or (cid.split("_")[0] if "_" in cid else "")
            if vc in _JRA_CODES:
                jra_runs_count += 1
            elif vc in _nar_venue_codes:
                nar_runs_count += 1
    is_nar_trainer = nar_runs_count > jra_runs_count

    # 5走以上の調教師の勝率を相対ランキング
    valid_wrs = sorted(v[0] / v[1] for v in trainer_stats_raw.values() if v[1] >= 5)
    n = len(valid_wrs)
    if n > 0 and total_runs >= 5:
        rank_below = sum(1 for w in valid_wrs if w < wr)
        pct = rank_below / n
        # 上位25% → A, 25-50% → B, 50-75% → C, 75%以下 → D
        rank = (
            JushaRank.A
            if pct >= 0.75
            else JushaRank.B
            if pct >= 0.50
            else JushaRank.C
            if pct >= 0.25
            else JushaRank.D
        )
    else:
        rank = JushaRank.D
        pct = 0.0

    # NAR調教師は平均勝率が高いため、相対ランキングに加えて
    # 絶対閾値でのチェックも実施（相対ランキングで過大評価されるのを防止）
    if is_nar_trainer and total_runs >= 5:
        # NAR: A ≥ 0.22, B ≥ 0.15, C ≥ 0.10（JRAより高い閾値）
        if wr < 0.10:
            rank = JushaRank.D
        elif wr < 0.15 and rank in (JushaRank.A, JushaRank.B):
            rank = JushaRank.C
        elif wr < 0.22 and rank == JushaRank.A:
            rank = JushaRank.B

    # KaisyuType: 勝率と複勝率の関係から分類
    # 複勝率に占める勝利割合（win_ratio_in_place）が高い → 1着が多い → 信頼型
    # 勝率は高いが複勝率が平均より低め → 過大評価型（1着か大敗が多い）
    # 勝率は低いが複勝率は標準 → 穴型
    avg_wr = valid_wrs[n // 2] if n > 0 else 0.1
    avg_pr_list = sorted(v[4] / v[1] for v in trainer_stats_raw.values() if v[1] >= 5)
    avg_pr = avg_pr_list[len(avg_pr_list) // 2] if avg_pr_list else 0.33
    win_in_place = wr / max(pr, 0.01)
    if wr >= avg_wr * 1.3 and pr >= avg_pr * 0.9:
        kaisyu = KaisyuType.SHINRAITYPE  # 信頼型：勝率高く複勝率も安定
    elif wr < avg_wr * 0.8 and pr >= avg_pr * 0.9:
        kaisyu = KaisyuType.ANA_TYPE  # 穴型：勝率は低いが複勝率は普通
    elif wr >= avg_wr * 1.1 and pr < avg_pr * 0.85:
        kaisyu = KaisyuType.KAJOHYOKA  # 過大評価型：勝率は高いが複勝率が低い
    else:
        kaisyu = KaisyuType.HEIBONTYPE  # 平凡型

    recent_wr = recent_wins / max(recent_runs, 1)
    momentum = "好調" if recent_runs >= 10 and recent_wr >= wr * 1.3 else ""

    # 偏差値を算出（パーセンタイル → 偏差値 40-75）
    deviation = round(40.0 + pct * 35.0, 1) if n > 0 and total_runs >= 5 else 50.0

    # ── 得意/苦手競馬場を集計 ──
    good_venues: list = []
    bad_venues: list = []
    if total_runs >= 5:
        from data.masters.venue_master import VENUE_CODE_TO_NAME
        venue_agg: Dict[str, list] = {}  # venue_code → [wins, runs]
        for cid, cid_runs in course_db.items():
            for r in cid_runs:
                tid = _rget(r, "trainer_id", "") or ""
                if tid != resolved_tid:
                    continue
                vc = _rget(r, "venue", "") or (cid.split("_")[0] if "_" in cid else "")
                if not vc:
                    continue
                if vc not in venue_agg:
                    venue_agg[vc] = [0, 0]
                venue_agg[vc][1] += 1
                fp = _rget(r, "finish_pos", 0)
                if isinstance(fp, str):
                    try:
                        fp = int(fp)
                    except ValueError:
                        continue
                if fp == 1:
                    venue_agg[vc][0] += 1
        for vc, (vw, vr) in venue_agg.items():
            if vr < 3:
                continue
            venue_wr = vw / vr
            if venue_wr >= wr * 1.5:
                vname = VENUE_CODE_TO_NAME.get(vc, vc) if hasattr(VENUE_CODE_TO_NAME, "get") else vc
                good_venues.append(vname)
            elif venue_wr <= wr * 0.5:
                vname = VENUE_CODE_TO_NAME.get(vc, vc) if hasattr(VENUE_CODE_TO_NAME, "get") else vc
                bad_venues.append(vname)

    # ── condition_records を構築 ──
    condition_records: Dict[str, Dict[str, Any]] = {}
    for cid, cid_runs in course_db.items():
        for r in cid_runs:
            tid = getattr(r, "trainer_id", "") or ""
            if tid != resolved_tid:
                continue
            cond = getattr(r, "condition", "") or ""
            if not cond:
                continue
            if cond not in condition_records:
                condition_records[cond] = {"wins": 0, "runs": 0}
            condition_records[cond]["runs"] += 1
            if r.finish_pos == 1:
                condition_records[cond]["wins"] += 1
    # 2走未満を除外
    condition_records = {k: v for k, v in condition_records.items() if v["runs"] >= 2}

    return TrainerStats(
        trainer_id=trainer_id,
        trainer_name=trainer_name,
        stable_name=trainer_name,
        location="地方" if is_nar_trainer else "JRA",
        rank=rank,
        kaisyu_type=kaisyu,
        recovery_break=0.0,
        short_momentum=momentum,
        deviation=deviation,
        good_venues=good_venues,
        bad_venues=bad_venues,
        condition_records=condition_records,
    )


def build_nar_jockey_stats_from_race_log(
    jockey_id: str,
    jockey_name: str,
    db_path: str = "data/keiba.db",
    min_runs: int = 5,
) -> Optional[JockeyStats]:
    """
    race_log DB からNAR騎手の偏差値を算出する。
    course_db にデータが不足している場合のフォールバック。
    min_runs: 最小NAR出走数（クロスチェック用途では30推奨）。
    """
    import re
    import sqlite3

    _clean = re.sub(r"[（(].+?[）)]", "", jockey_name).replace("　", "").strip()
    _norm = _normalize_name(_clean)
    if len(_norm) < 2:
        return None

    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return None

    try:
        # jockey_nameの前方一致でrace_logのjockey_idを特定
        rows = conn.execute(
            "SELECT DISTINCT jockey_id, jockey_name FROM race_log "
            "WHERE jockey_name LIKE ? AND jockey_id != ''",
            (f"%{_norm}%",),
        ).fetchall()

        if not rows:
            rows = conn.execute(
                "SELECT DISTINCT jockey_id, jockey_name FROM race_log "
                "WHERE jockey_name LIKE ? AND jockey_id != ''",
                (f"%{_clean}%",),
            ).fetchall()

        if not rows:
            return None

        # 最も出走数が多いIDを採用
        best_jid = rows[0][0]
        if len(rows) > 1:
            jid_counts = {}
            for jid, _ in rows:
                cnt = conn.execute(
                    "SELECT COUNT(*) FROM race_log WHERE jockey_id = ?", (jid,)
                ).fetchone()[0]
                jid_counts[jid] = cnt
            best_jid = max(jid_counts, key=jid_counts.get)

        # NAR競馬場コード
        _nar_venues = ("30", "35", "36", "42", "43", "44", "45", "46", "47", "48", "50", "51", "54", "55")
        _nar_in = ",".join(f"'{v}'" for v in _nar_venues)

        # 成績集計（NAR競馬場限定 — 比較対象と条件を揃える）
        stats_row = conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) "
            "FROM race_log WHERE jockey_id = ? AND venue_code IN ({})".format(_nar_in),
            (best_jid,),
        ).fetchone()
        total_runs, total_wins = stats_row
        total_wins = total_wins or 0

        if total_runs < min_runs:
            return None

        # NAR競馬場全騎手の勝率を相対ランキング
        all_jockeys = conn.execute(
            "SELECT jockey_id, COUNT(*) as runs, "
            "SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) as wins "
            "FROM race_log WHERE venue_code IN ({}) AND jockey_id != '' "
            "GROUP BY jockey_id HAVING runs >= 5".format(_nar_in),
        ).fetchall()

        valid_wrs = sorted(w / r for _, r, w in all_jockeys if r >= 5)
        n = len(valid_wrs)
        wr = total_wins / total_runs

        if n > 0:
            rank_below = sum(1 for w in valid_wrs if w < wr)
            percentile = rank_below / n
            dev = round(40.0 + percentile * 35.0, 1)
        else:
            dev = 50.0

        # コース別集計
        course_rows = conn.execute(
            "SELECT venue_code || '_' || surface || '_' || distance as cid, "
            "COUNT(*) as runs, SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) as wins "
            "FROM race_log WHERE jockey_id = ? "
            "GROUP BY cid HAVING runs >= 2",
            (best_jid,),
        ).fetchall()
        course_records = {}
        for cid, runs, wins in course_rows:
            course_records[cid] = {
                "all_dev": dev,
                "upper_dev": dev,
                "lower_dev": dev,
                "sample_n": runs,
            }

        # 馬場状態別
        cond_rows = conn.execute(
            "SELECT condition, COUNT(*), SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) "
            "FROM race_log WHERE jockey_id = ? AND condition != '' "
            "GROUP BY condition HAVING COUNT(*) >= 2",
            (best_jid,),
        ).fetchall()
        condition_records = {c: {"wins": w, "runs": r} for c, r, w in cond_rows}

        logger.info("RACE_LOG 騎手 %s: dev=%.1f, %d走%d勝",
                     jockey_name, dev, total_runs, total_wins)

        return JockeyStats(
            jockey_id=jockey_id,
            jockey_name=jockey_name,
            upper_long_dev=dev,
            upper_short_dev=dev,
            lower_long_dev=dev,
            lower_short_dev=dev,
            course_records=course_records,
            condition_records=condition_records,
        )
    except Exception as e:
        logger.warning("race_log 騎手統計計算エラー %s: %s", jockey_name, e)
        return None
    finally:
        conn.close()


def build_nar_trainer_stats_from_race_log(
    trainer_id: str,
    trainer_name: str,
    db_path: str = "data/keiba.db",
    min_runs: int = 5,
) -> Optional[TrainerStats]:
    """
    race_log DB からNAR調教師の成績を算出する。
    course_db にNAR調教師のデータがない場合のフォールバック。
    trainer_name は "山中尊（船橋）" 形式 → "山中尊" に正規化して前方一致検索。
    min_runs: 最小NAR出走数（クロスチェック用途では30推奨）。
    """
    import re
    import sqlite3
    import datetime

    _clean = re.sub(r"[（(].+?[）)]", "", trainer_name).replace("　", "").strip()
    _norm = _normalize_name(_clean)
    if len(_norm) < 2:
        return None

    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return None

    try:
        # trainer_nameの前方一致でrace_logのtrainer_idを特定
        rows = conn.execute(
            "SELECT DISTINCT trainer_id, trainer_name FROM race_log "
            "WHERE trainer_name LIKE ? AND trainer_id != ''",
            (f"%{_norm}%",),
        ).fetchall()

        if not rows:
            # 異体字正規化前の名前でも試す
            rows = conn.execute(
                "SELECT DISTINCT trainer_id, trainer_name FROM race_log "
                "WHERE trainer_name LIKE ? AND trainer_id != ''",
                (f"%{_clean}%",),
            ).fetchall()

        if not rows:
            return None

        # 複数ヒット時は最もID出現回数が多いものを採用
        best_tid = rows[0][0]
        if len(rows) > 1:
            tid_counts = {}
            for tid, _ in rows:
                cnt = conn.execute(
                    "SELECT COUNT(*) FROM race_log WHERE trainer_id = ?", (tid,)
                ).fetchone()[0]
                tid_counts[tid] = cnt
            best_tid = max(tid_counts, key=tid_counts.get)

        # NAR競馬場コード
        _nar_venues = ("30", "35", "36", "42", "43", "44", "45", "46", "47", "48", "50", "51", "54", "55")
        _nar_in = ",".join(f"'{v}'" for v in _nar_venues)

        # 成績集計（NAR競馬場限定 — 比較対象と条件を揃える）
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
        stats_row = conn.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) "
            "FROM race_log WHERE trainer_id = ? AND venue_code IN ({})".format(_nar_in),
            (best_tid,),
        ).fetchone()
        total_runs, total_wins, total_places = stats_row
        total_wins = total_wins or 0
        total_places = total_places or 0

        recent = conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) "
            "FROM race_log WHERE trainer_id = ? AND race_date >= ? AND venue_code IN ({})".format(_nar_in),
            (best_tid, cutoff),
        ).fetchone()
        recent_runs, recent_wins = recent
        recent_wins = recent_wins or 0

        if total_runs < min_runs:
            return None

        # 全調教師の勝率を相対ランキング（NAR競馬場限定）
        all_trainers = conn.execute(
            "SELECT trainer_id, COUNT(*) as runs, "
            "SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) as wins "
            "FROM race_log WHERE venue_code IN ({}) AND trainer_id != '' "
            "GROUP BY trainer_id HAVING runs >= 5".format(_nar_in),
        ).fetchall()

        valid_wrs = sorted(w / r for _, r, w in all_trainers if r >= 5)
        n = len(valid_wrs)
        wr = total_wins / total_runs
        pr = total_places / total_runs

        if n > 0:
            rank_below = sum(1 for w in valid_wrs if w < wr)
            pct = rank_below / n
            rank = (
                JushaRank.A if pct >= 0.75
                else JushaRank.B if pct >= 0.50
                else JushaRank.C if pct >= 0.25
                else JushaRank.D
            )
            # NAR絶対閾値チェック
            if wr < 0.10:
                rank = JushaRank.D
            elif wr < 0.15 and rank in (JushaRank.A, JushaRank.B):
                rank = JushaRank.C
            elif wr < 0.22 and rank == JushaRank.A:
                rank = JushaRank.B
            deviation = round(40.0 + pct * 35.0, 1)
        else:
            rank = JushaRank.C
            deviation = 50.0
            pct = 0.5

        # 好調度
        recent_wr = recent_wins / max(recent_runs, 1)
        momentum = "好調" if recent_runs >= 10 and recent_wr >= wr * 1.3 else ""

        # 馬場状態別
        cond_rows = conn.execute(
            "SELECT condition, COUNT(*), SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) "
            "FROM race_log WHERE trainer_id = ? AND condition != '' "
            "GROUP BY condition HAVING COUNT(*) >= 2",
            (best_tid,),
        ).fetchall()
        condition_records = {c: {"wins": w, "runs": r} for c, r, w in cond_rows}

        logger.info("RACE_LOG 厩舎 %s: rank=%s, dev=%.1f, %d走%d勝",
                     trainer_name, rank.value, deviation, total_runs, total_wins)

        return TrainerStats(
            trainer_id=trainer_id,
            trainer_name=trainer_name,
            stable_name=trainer_name,
            location="地方",
            rank=rank,
            kaisyu_type=KaisyuType.HEIBONTYPE,
            recovery_break=0.0,
            short_momentum=momentum,
            deviation=deviation,
            condition_records=condition_records,
        )
    except Exception as e:
        logger.warning("race_log 厩舎統計計算エラー %s: %s", trainer_name, e)
        return None
    finally:
        conn.close()


def enrich_personnel_with_condition_records(
    jockey_db: Dict[str, JockeyStats],
    trainer_db: Dict[str, TrainerStats],
    course_db: Dict[str, List],
) -> None:
    """course_db から馬場状態別を集計し、jockey_db/trainer_db にマージ（in-place）"""
    jockey_cond, trainer_cond = aggregate_condition_from_course_db(course_db)
    for jid, rec in jockey_cond.items():
        if jid in jockey_db:
            jockey_db[jid].condition_records = rec
    for tid, rec in trainer_cond.items():
        if tid in trainer_db:
            trainer_db[tid].condition_records = rec


# ============================================================
# race_log 一括キャッシュ（バッチ版 — 個別SQL 334回→一括8クエリ）
# ============================================================


class _BatchNarRaceLogCache:
    """
    race_log テーブルからNAR騎手・調教師の統計情報を一括プリロードする。
    個別クエリ ~334回 → 一括8クエリに削減し、~549s → ~3s に高速化。
    """

    _NAR_VENUES = ("30", "35", "36", "42", "43", "44", "45", "46", "47", "48", "50", "51", "54", "55")

    def __init__(self, db_path: str = "data/keiba.db"):
        import sqlite3

        t0 = _time.time()
        conn = sqlite3.connect(db_path)
        _nar_in = ",".join(f"'{v}'" for v in self._NAR_VENUES)

        # ---- 1. 名前→ID マッピング（全件一括取得）----
        self._jockey_name_to_ids = self._build_name_map(conn, "jockey_id", "jockey_name")
        self._trainer_name_to_ids = self._build_name_map(conn, "trainer_id", "trainer_name")

        # IDごとの正規名（最多件数の名前）の長さを事前計算
        # 短縮名の偽マッチ防止に使用（例: 「渡辺竜」3文字 → 「渡辺竜也」4文字）
        self._id_canonical_name_len: Dict[str, int] = {}
        for name_map in (self._jockey_name_to_ids, self._trainer_name_to_ids):
            # id → (max_count, name_len)
            _id_best: Dict[str, tuple] = {}
            for name_key, id_counts in name_map.items():
                for pid, cnt in id_counts.items():
                    prev = _id_best.get(pid, (0, 0))
                    if cnt > prev[0]:
                        _id_best[pid] = (cnt, len(name_key))
            for pid, (_, nlen) in _id_best.items():
                self._id_canonical_name_len[pid] = nlen

        # ---- 2. NAR全騎手の勝率ランキング（パーセンタイル用）----
        all_j = conn.execute(
            "SELECT jockey_id, COUNT(*) as runs, "
            "SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) as wins "
            "FROM race_log WHERE venue_code IN ({}) AND jockey_id != '' "
            "GROUP BY jockey_id HAVING runs >= 5".format(_nar_in),
        ).fetchall()
        self._jockey_nar_stats = {jid: (r, w or 0) for jid, r, w in all_j}
        self._jockey_nar_wrs = sorted(w / r for _, r, w in all_j if r >= 5 and w is not None)

        # ---- 3. NAR全調教師の勝率ランキング ----
        all_t = conn.execute(
            "SELECT trainer_id, COUNT(*) as runs, "
            "SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) as wins, "
            "SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) as places "
            "FROM race_log WHERE venue_code IN ({}) AND trainer_id != '' "
            "GROUP BY trainer_id HAVING runs >= 5".format(_nar_in),
        ).fetchall()
        self._trainer_nar_stats = {tid: (r, w or 0, p or 0) for tid, r, w, p in all_t}
        self._trainer_nar_wrs = sorted(
            (w or 0) / r for _, r, w, _ in all_t if r >= 5 and w is not None
        )

        # ---- 4. 調教師の直近60日成績 ----
        cutoff = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
        recent = conn.execute(
            "SELECT trainer_id, COUNT(*), "
            "SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) "
            "FROM race_log WHERE race_date >= ? AND venue_code IN ({}) "
            "AND trainer_id != '' GROUP BY trainer_id".format(_nar_in),
            (cutoff,),
        ).fetchall()
        self._trainer_recent = {tid: (r, w or 0) for tid, r, w in recent}

        # ---- 5. 騎手コース別成績 ----
        jcourse = conn.execute(
            "SELECT jockey_id, venue_code || '_' || surface || '_' || distance, "
            "COUNT(*), SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) "
            "FROM race_log WHERE jockey_id != '' "
            "GROUP BY jockey_id, venue_code || '_' || surface || '_' || distance "
            "HAVING COUNT(*) >= 2",
        ).fetchall()
        self._jockey_course: Dict[str, Dict] = {}
        for jid, cid, runs, wins in jcourse:
            self._jockey_course.setdefault(jid, {})[cid] = (runs, wins or 0)

        # ---- 6. 馬場状態別（騎手・調教師）----
        self._jockey_cond = self._load_condition(conn, "jockey_id")
        self._trainer_cond = self._load_condition(conn, "trainer_id")

        conn.close()
        logger.info(
            "BatchNarRaceLogCache 構築完了: %.1fs (騎手%d / 調教師%d)",
            _time.time() - t0,
            len(self._jockey_nar_stats),
            len(self._trainer_nar_stats),
        )

    @staticmethod
    def _build_name_map(conn, id_col: str, name_col: str) -> Dict[str, Dict[str, int]]:
        """name → {id: count} マッピングを一括構築"""
        rows = conn.execute(
            "SELECT {}, {}, COUNT(*) as cnt "
            "FROM race_log WHERE {} != '' AND {} != '' "
            "GROUP BY {}, {}".format(id_col, name_col, id_col, name_col, id_col, name_col),
        ).fetchall()
        result: Dict[str, Dict[str, int]] = {}
        for pid, pname, cnt in rows:
            clean = pname.replace("\u3000", "").strip()
            norm = _normalize_name(clean)
            if len(norm) >= 2:
                result.setdefault(norm, {})[pid] = result.setdefault(norm, {}).get(pid, 0) + cnt
                if clean != norm:
                    result.setdefault(clean, {})[pid] = result.setdefault(clean, {}).get(pid, 0) + cnt
        return result

    @staticmethod
    def _load_condition(conn, id_col: str) -> Dict[str, Dict]:
        """馬場状態別成績を一括ロード"""
        rows = conn.execute(
            "SELECT {}, condition, COUNT(*), "
            "SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) "
            "FROM race_log WHERE {} != '' AND condition != '' "
            "GROUP BY {}, condition HAVING COUNT(*) >= 2".format(id_col, id_col, id_col),
        ).fetchall()
        result: Dict[str, Dict] = {}
        for pid, cond, runs, wins in rows:
            result.setdefault(pid, {})[cond] = {"wins": wins or 0, "runs": runs}
        return result

    def _resolve_id(self, name: str, name_map: Dict[str, Dict[str, int]]) -> Optional[str]:
        """名前からrace_log上のIDを解決（完全一致のみ。部分一致は誤マッチが多いため廃止）"""
        clean = re.sub(r"[（(].+?[）)]", "", name).replace("\u3000", "").strip()
        norm = _normalize_name(clean)
        if len(norm) < 2:
            return None

        # 完全一致を優先
        # 正規化で短縮名にマッチするケースを防止
        # 例: 「渡邊竜」→正規化→「渡辺竜」→ race_logの短縮名「渡辺竜」(=渡辺竜也の別名)にヒット
        # 対策: マッチしたIDの正規名長と比較し、2文字以上差がある場合はスキップ
        def _validate(key: str) -> Optional[str]:
            if key not in name_map:
                return None
            best_id = max(name_map[key], key=name_map[key].get)
            # 正規名長チェック: IDの正規名をcanonical_name_lenで取得
            canonical_len = self._id_canonical_name_len.get(best_id, len(key))
            if len(key) != canonical_len:
                return None  # 名前長不一致 → 短縮名や別人の偽マッチ
            return best_id

        result = _validate(norm)
        if result:
            return result
        if clean != norm:
            result = _validate(clean)
            if result:
                return result

        return None

    def get_jockey_stats(
        self, jockey_id: str, jockey_name: str, min_runs: int = 5
    ) -> Optional[JockeyStats]:
        """キャッシュからNAR騎手統計を取得"""
        best_jid = self._resolve_id(jockey_name, self._jockey_name_to_ids)
        if not best_jid:
            return None

        stats = self._jockey_nar_stats.get(best_jid)
        if not stats:
            return None
        total_runs, total_wins = stats
        if total_runs < min_runs:
            return None

        wr = total_wins / total_runs
        wrs = self._jockey_nar_wrs
        n = len(wrs)
        if n > 0:
            rank_below = sum(1 for w in wrs if w < wr)
            percentile = rank_below / n
            dev = round(40.0 + percentile * 35.0, 1)
        else:
            dev = 50.0

        # コース別
        course_records = {}
        for cid, (runs, wins) in self._jockey_course.get(best_jid, {}).items():
            course_records[cid] = {
                "all_dev": dev, "upper_dev": dev, "lower_dev": dev, "sample_n": runs,
            }

        # 馬場状態別
        condition_records = self._jockey_cond.get(best_jid, {})

        logger.info("RACE_LOG 騎手 %s: dev=%.1f, %d走%d勝", jockey_name, dev, total_runs, total_wins)

        return JockeyStats(
            jockey_id=jockey_id,
            jockey_name=jockey_name,
            upper_long_dev=dev,
            upper_short_dev=dev,
            lower_long_dev=dev,
            lower_short_dev=dev,
            course_records=course_records,
            condition_records=condition_records,
        )

    def get_trainer_stats(
        self, trainer_id: str, trainer_name: str, min_runs: int = 5
    ) -> Optional[TrainerStats]:
        """キャッシュからNAR調教師統計を取得"""
        best_tid = self._resolve_id(trainer_name, self._trainer_name_to_ids)
        if not best_tid:
            return None

        stats = self._trainer_nar_stats.get(best_tid)
        if not stats:
            return None
        total_runs, total_wins, total_places = stats
        if total_runs < min_runs:
            return None

        wr = total_wins / total_runs
        pr = total_places / total_runs
        wrs = self._trainer_nar_wrs
        n = len(wrs)

        if n > 0:
            rank_below = sum(1 for w in wrs if w < wr)
            pct = rank_below / n
            rank = (
                JushaRank.A if pct >= 0.75
                else JushaRank.B if pct >= 0.50
                else JushaRank.C if pct >= 0.25
                else JushaRank.D
            )
            # NAR絶対閾値チェック
            if wr < 0.10:
                rank = JushaRank.D
            elif wr < 0.15 and rank in (JushaRank.A, JushaRank.B):
                rank = JushaRank.C
            elif wr < 0.22 and rank == JushaRank.A:
                rank = JushaRank.B
            deviation = round(40.0 + pct * 35.0, 1)
        else:
            rank = JushaRank.C
            deviation = 50.0

        # 好調度
        recent_runs, recent_wins = self._trainer_recent.get(best_tid, (0, 0))
        recent_wr = recent_wins / max(recent_runs, 1)
        momentum = "好調" if recent_runs >= 10 and recent_wr >= wr * 1.3 else ""

        # 馬場状態別
        condition_records = self._trainer_cond.get(best_tid, {})

        logger.info(
            "RACE_LOG 厩舎 %s: rank=%s, dev=%.1f, %d走%d勝",
            trainer_name, rank.value, deviation, total_runs, total_wins,
        )

        return TrainerStats(
            trainer_id=trainer_id,
            trainer_name=trainer_name,
            stable_name=trainer_name,
            location="地方",
            rank=rank,
            kaisyu_type=KaisyuType.HEIBONTYPE,
            recovery_break=0.0,
            short_momentum=momentum,
            deviation=deviation,
            condition_records=condition_records,
        )


# ============================================================
# 騎手・厩舎DBキャッシュマネージャー
# ============================================================


class PersonnelDBManager:
    """
    騎手・厩舎の成績をJSONにキャッシュし、
    エンジンが使える形式で提供する
    """

    def __init__(
        self,
        db_path: str = None,
        cache_days: int = 7,  # 7日間キャッシュ
    ):
        if db_path is None:
            db_path = PERSONNEL_DB_PATH
        self.db_path = db_path
        self.cache_days = cache_days
        self._data: dict = {"jockeys": {}, "trainers": {}, "updated": {}}
        self._load()

    def _load(self):
        if os.path.exists(self.db_path):
            with open(self.db_path, "r", encoding="utf-8") as f:
                self._data = json.load(f)

    def save(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with open(self.db_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)

    def is_stale(self, key: str) -> bool:
        """キャッシュが古いか判定"""
        updated = self._data.get("updated", {}).get(key)
        if not updated:
            return True
        age = (datetime.now() - datetime.fromisoformat(updated)).days
        return age >= self.cache_days

    def get_jockey(self, jockey_id: str) -> Optional[JockeyStats]:
        d = self._data["jockeys"].get(jockey_id)
        if not d:
            return None
        return JockeyStats(
            jockey_id=d["jockey_id"],
            jockey_name=d["jockey_name"],
            upper_long_dev=d.get("upper_long_dev", 50.0),
            upper_short_dev=d.get("upper_short_dev", 50.0),
            lower_long_dev=d.get("lower_long_dev", 50.0),
            lower_short_dev=d.get("lower_short_dev", 50.0),
            kaisyu_type=KaisyuType[d.get("kaisyu_type", "HEIBONTYPE")],
            course_records=d.get("course_records", {}),
            condition_records=d.get("condition_records", {}),
            location=d.get("location", ""),
        )

    def get_trainer(self, trainer_id: str) -> Optional[TrainerStats]:
        d = self._data["trainers"].get(trainer_id)
        if not d:
            return None
        return TrainerStats(
            trainer_id=d["trainer_id"],
            trainer_name=d["trainer_name"],
            stable_name=d.get("stable_name", d["trainer_name"]),
            location=d.get("location", "JRA"),
            rank=JushaRank[d.get("rank", "C")],
            kaisyu_type=KaisyuType[d.get("kaisyu_type", "HEIBONTYPE")],
            recovery_break=d.get("recovery_break", 0.0),
            recovery_by_class=d.get("recovery_by_class", {}),
            recovery_distance_change=d.get("recovery_distance_change", 0.0),
            recovery_jockey_change=d.get("recovery_jockey_change", 0.0),
            rotation_type=d.get("rotation_type", "標準"),
            break_type=d.get("break_type", "初戦型"),
            short_momentum=d.get("short_momentum", ""),
            good_venues=d.get("good_venues", []),
            bad_venues=d.get("bad_venues", []),
            condition_records=d.get("condition_records", {}),
            deviation=d.get("deviation", 50.0),
            jockey_combo=d.get("jockey_combo", {}),
        )

    def store_jockey(self, stats: JockeyStats):
        self._data["jockeys"][stats.jockey_id] = {
            "jockey_id": stats.jockey_id,
            "jockey_name": stats.jockey_name,
            "upper_long_dev": stats.upper_long_dev,
            "upper_short_dev": stats.upper_short_dev,
            "lower_long_dev": stats.lower_long_dev,
            "lower_short_dev": stats.lower_short_dev,
            "kaisyu_type": stats.kaisyu_type.name,
            "course_records": stats.course_records,
            "condition_records": stats.condition_records,
            "location": stats.location,
        }
        self._data.setdefault("updated", {})[f"jockey_{stats.jockey_id}"] = (
            datetime.now().isoformat()
        )

    def store_trainer(self, stats: TrainerStats):
        self._data["trainers"][stats.trainer_id] = {
            "trainer_id": stats.trainer_id,
            "trainer_name": stats.trainer_name,
            "stable_name": stats.stable_name,
            "location": stats.location,
            "rank": stats.rank.name,
            "kaisyu_type": stats.kaisyu_type.name,
            "recovery_break": stats.recovery_break,
            "recovery_by_class": stats.recovery_by_class,
            "recovery_distance_change": stats.recovery_distance_change,
            "recovery_jockey_change": stats.recovery_jockey_change,
            "rotation_type": stats.rotation_type,
            "break_type": stats.break_type,
            "short_momentum": stats.short_momentum,
            "good_venues": stats.good_venues,
            "bad_venues": stats.bad_venues,
            "condition_records": stats.condition_records,
            "deviation": stats.deviation,
            "jockey_combo": stats.jockey_combo,
        }
        self._data.setdefault("updated", {})[f"trainer_{stats.trainer_id}"] = (
            datetime.now().isoformat()
        )

    def build_from_horses(
        self,
        horses,
        client: NetkeibaClient,
        force: bool = False,
        course_db: Dict = None,
        save: bool = True,
    ) -> Tuple[Dict[str, JockeyStats], Dict[str, TrainerStats]]:
        """
        出走馬リストから騎手・厩舎DBを自動構築する
        キャッシュが新鮮ならスキップ。ウェブ取得失敗時は course_db でフォールバック。

        race_log クエリは _BatchNarRaceLogCache で一括プリロードし、
        個別SQL ~334回 → 一括8クエリに削減（~549s → ~3s）。
        """
        j_scraper = JockeyScraper(client)
        t_scraper = TrainerScraper(client)

        jockey_ids = {h.jockey_id: h.jockey for h in horses if h.jockey_id}
        trainer_ids = {h.trainer_id: h.trainer for h in horses if h.trainer_id}

        # NAR騎手・調教師のIDセットを馬のvenueから構築（IDプレフィックスは不統一のため）
        # JRA所属ID（00xxx/01xxx）はNAR出走があってもJRA扱いを維持（race_log汚染防止）
        # 05xxxはnetkeiba共通ID（NAR調教師も使用）なのでvenue判定に従う
        _JRA_VENUES = {"札幌","函館","福島","新潟","東京","中山","中京","京都","阪神","小倉"}
        _JRA_ID_PREFIXES = {"00", "01"}
        _nar_jockey_ids = {h.jockey_id for h in horses
                           if h.jockey_id and h.jockey_id[:2] not in _JRA_ID_PREFIXES
                           and getattr(h, "venue", "") not in _JRA_VENUES and getattr(h, "venue", "")}
        _nar_trainer_ids = {h.trainer_id for h in horses
                            if h.trainer_id and h.trainer_id[:2] not in _JRA_ID_PREFIXES
                            and getattr(h, "venue", "") not in _JRA_VENUES and getattr(h, "venue", "")}
        logger.info("騎手%d名 / 厩舎%dの成績取得 (NAR騎手%d, NAR厩舎%d)",
                     len(jockey_ids), len(trainer_ids), len(_nar_jockey_ids), len(_nar_trainer_ids))

        # ---- race_log 一括プリロード ----
        _nar_cache = _BatchNarRaceLogCache()

        for jid, jname in jockey_ids.items():
            # NAR騎手のみ race_log クロスチェック対象
            # JRA騎手は少数NAR出走記録で偏差値が歪むため対象外
            _is_nar_jockey = jid in _nar_jockey_ids
            cached = self.get_jockey(jid)
            if not force and not self.is_stale(f"jockey_{jid}"):
                if cached and cached.upper_long_dev != 50.0:
                    # キャッシュ済み — NAR騎手のみrace_logでクロスチェック
                    if _is_nar_jockey:
                        rl = _nar_cache.get_jockey_stats(jid, jname, min_runs=30)
                        if rl and rl.upper_long_dev != 50.0:
                            diff = abs(rl.upper_long_dev - cached.upper_long_dev)
                            if diff >= 15.0:
                                logger.info("RACE_LOG 補正 騎手 %s: %.1f → %.1f",
                                            jname, cached.upper_long_dev, rl.upper_long_dev)
                                self.store_jockey(rl)
                    else:
                        logger.debug("CACHE 騎手 %s", jname)
                    continue
                elif cached and cached.upper_long_dev == 50.0 and course_db:
                    # キャッシュが50.0（未取得相当）→ NAR騎手はrace_log優先、なければcourse_db
                    if _is_nar_jockey:
                        rl = _nar_cache.get_jockey_stats(jid, jname)
                        if rl and rl.upper_long_dev != 50.0:
                            logger.debug("RACE_LOG 騎手 %s: upper=%.1f", jname, rl.upper_long_dev)
                            self.store_jockey(rl)
                            continue
                    stats = build_jockey_stats_from_course_db(jid, jname, course_db)
                    if stats.upper_long_dev not in (50.0, 40.0):
                        logger.debug("COURSE_DB 騎手 %s: upper=%.1f", jname, stats.upper_long_dev)
                        self.store_jockey(stats)
                        continue
                # course_dbフォールバック失敗 → NAR騎手のみrace_logで再計算
                if _is_nar_jockey:
                    _cached_dev = getattr(cached, "upper_long_dev", 50.0) if cached else 50.0
                    if _cached_dev in (50.0, 40.0):
                        fb = _nar_cache.get_jockey_stats(jid, jname)
                        if fb and fb.upper_long_dev != 50.0:
                            self.store_jockey(fb)
                            continue
            # NAR公式IDはnetkeibaIDと体系が異なるためフェッチすると別人データ取得
            # NAR騎手はrace_log/course_dbのみで算出
            if _is_nar_jockey:
                stats = None
                # race_log優先
                rl = _nar_cache.get_jockey_stats(jid, jname, min_runs=5)
                if rl and rl.upper_long_dev != 50.0:
                    stats = rl
                    logger.debug("RACE_LOG 騎手 %s: %.1f", jname, rl.upper_long_dev)
                # course_dbフォールバック
                if (not stats or stats.upper_long_dev in (50.0, 40.0)) and course_db:
                    fb = build_jockey_stats_from_course_db(jid, jname, course_db)
                    if fb.upper_long_dev not in (50.0, 40.0):
                        stats = fb
                        logger.debug("COURSE_DB 騎手 %s: %.1f", jname, fb.upper_long_dev)
                if stats:
                    self.store_jockey(stats)
                else:
                    # データなし → デフォルト50.0で保存
                    self.store_jockey(JockeyStats(
                        jockey_id=jid, jockey_name=jname,
                        upper_long_dev=50.0, upper_short_dev=50.0,
                        lower_long_dev=50.0, lower_short_dev=50.0,
                    ))
                continue
            logger.debug("FETCH 騎手 %s", jname)
            stats = j_scraper.fetch(jid, jname)
            if stats.upper_long_dev == 50.0 and course_db:
                # ウェブ取得失敗 → course_db フォールバック
                fb = build_jockey_stats_from_course_db(jid, jname, course_db)
                if fb.upper_long_dev not in (50.0, 40.0):
                    stats = fb
                    logger.debug("→ course_db フォールバック: %.1f", stats.upper_long_dev)
            self.store_jockey(stats)

        # NAR調教師ID（11xxx）→ netkeiba ID（05xxx）の名前逆引き用マップ構築
        _nk_name_to_tid: Dict[str, str] = {}
        if course_db:
            for _tid, _tdata in self._data.get("trainers", {}).items():
                if not _tid.startswith("1"):  # netkeiba IDのみ（11xxx以外）
                    _nk_name_to_tid[_tdata.get("trainer_name", "")] = _tid

        for tid, tname in trainer_ids.items():
            # NAR調教師のみ race_log クロスチェック対象
            # JRA調教師は少数NAR出走記録で偏差値が歪むため対象外
            _is_nar_trainer = tid in _nar_trainer_ids
            cached = self.get_trainer(tid)
            # C/D はウェブ取得失敗時のデフォルト値なのでcourse_dbで再計算
            if not force and not self.is_stale(f"trainer_{tid}"):
                if cached and cached.rank not in (JushaRank.C, JushaRank.D):
                    # キャッシュ済み — NAR調教師のみrace_logでクロスチェック
                    if _is_nar_trainer:
                        rl = _nar_cache.get_trainer_stats(tid, tname, min_runs=30)
                        if rl and rl.deviation != 50.0:
                            diff = abs(rl.deviation - cached.deviation)
                            if diff >= 15.0:
                                logger.info("RACE_LOG 補正 厩舎 %s: %.1f → %.1f",
                                            tname, cached.deviation, rl.deviation)
                                self.store_trainer(rl)
                    else:
                        logger.debug("CACHE 厩舎 %s", tname)
                    continue
                elif cached and course_db:
                    # キャッシュがC/D → NAR調教師はrace_log優先、なければcourse_db
                    if _is_nar_trainer:
                        rl = _nar_cache.get_trainer_stats(tid, tname)
                        if rl and rl.deviation != 50.0:
                            logger.debug("RACE_LOG 厩舎 %s: rank=%s, dev=%.1f",
                                         tname, rl.rank.value, rl.deviation)
                            self.store_trainer(rl)
                            continue
                    stats = build_trainer_stats_from_course_db(tid, tname, course_db, _nk_name_to_tid)
                    if stats.rank not in (JushaRank.C, JushaRank.D) or stats.short_momentum:
                        logger.debug("COURSE_DB 厩舎 %s: rank=%s", tname, stats.rank.value)
                        self.store_trainer(stats)
                        continue
                # course_dbフォールバック失敗 → NAR調教師のみrace_logで再計算
                if _is_nar_trainer and cached:
                    fb = _nar_cache.get_trainer_stats(tid, tname)
                    if fb and fb.deviation != 50.0:
                        self.store_trainer(fb)
                        continue
            # NAR公式IDはnetkeibaIDと体系が異なるためフェッチすると別人データ取得
            # NAR調教師はrace_log/course_dbのみで算出
            if _is_nar_trainer:
                stats = None
                # race_log優先
                rl = _nar_cache.get_trainer_stats(tid, tname, min_runs=5)
                if rl and rl.deviation != 50.0:
                    stats = rl
                    logger.debug("RACE_LOG 厩舎 %s: dev=%.1f", tname, rl.deviation)
                # course_dbフォールバック
                if (not stats or stats.deviation == 50.0) and course_db:
                    fb = build_trainer_stats_from_course_db(tid, tname, course_db, _nk_name_to_tid)
                    if fb.rank not in (JushaRank.C, JushaRank.D):
                        stats = fb
                        logger.debug("COURSE_DB 厩舎 %s: rank=%s", tname, fb.rank.value)
                if stats:
                    self.store_trainer(stats)
                else:
                    # データなし → デフォルトで保存
                    self.store_trainer(TrainerStats(
                        trainer_id=tid, trainer_name=tname,
                        stable_name="", location="地方",
                    ))
                continue
            logger.debug("FETCH 厩舎 %s", tname)
            stats = t_scraper.fetch(tid, tname)
            if stats.rank in (JushaRank.C, JushaRank.D) and course_db:
                fb = build_trainer_stats_from_course_db(tid, tname, course_db, _nk_name_to_tid)
                if fb.rank not in (JushaRank.C, JushaRank.D):
                    stats = fb
                    logger.debug("→ course_db フォールバック: rank=%s", stats.rank.value)
            self.store_trainer(stats)

        if save:
            self.save()

        # DictとしてReturn
        jockey_db = {jid: self.get_jockey(jid) for jid in jockey_ids if self.get_jockey(jid)}
        trainer_db = {tid: self.get_trainer(tid) for tid in trainer_ids if self.get_trainer(tid)}

        logger.info("取得完了: 騎手%d / 厩舎%d", len(jockey_db), len(trainer_db))
        return jockey_db, trainer_db
