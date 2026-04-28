"""
NAR公式サイトスクレイパー (keiba.go.jp)

認証不要。DebaTable（出馬表）、HorseMarkInfo（馬詳細）、
RaceList（レース一覧）から完全なレースデータを取得する。
"""

import re
import threading
import time
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from src.log import get_logger

logger = get_logger(__name__)


# ================================================================
# 共通: コーナー通過順パーサ（括弧対応）
# ================================================================
# NAR公式・netkeiba・競馬ブック等で共通利用される
# 「(14,7),(5,9,12),(1,6,8),13,4,10,(11,3),2」形式を
# 各馬番 → 通過順位リストに変換する

def _parse_bracketed_corner_sequence(text: str) -> List[List[int]]:
    """括弧付き馬番シーケンスをパース

    例: "(14,7),(5,9,12),13,4" → [[14,7], [5,9,12], [13], [4]]
    括弧内は同通過位置の馬群（同順位タイ）を表す。

    Args:
        text: 括弧付き馬番文字列
    Returns:
        グループリスト。各グループは同通過位置の馬番リスト。
    """
    groups: List[List[int]] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "(" or ch == "（":
            # 括弧内の馬番を収集
            close_paren = ")" if ch == "(" else "）"
            j = text.find(close_paren, i)
            if j < 0:
                break
            inner = text[i + 1:j]
            hnos = []
            for part in re.split(r"[,\uFF0C\s\-]+", inner):
                part = part.strip()
                if part.isdigit():
                    hnos.append(int(part))
            if hnos:
                groups.append(hnos)
            i = j + 1
        elif ch.isdigit():
            # 単独馬番
            j = i
            while j < n and text[j].isdigit():
                j += 1
            try:
                num = int(text[i:j])
                if 1 <= num <= 30:  # 馬番妥当性チェック
                    groups.append([num])
            except ValueError:
                pass
            i = j
        else:
            i += 1
    return groups


def parse_corner_passing_from_text(full_text: str) -> Dict[int, List[int]]:
    """フルテキストから各コーナー通過順をパース

    「3角 (14,7),(5,9,12),...」「4コーナー: 14,7,(5,9)...」等を検知し、
    同通過位置の馬群には同順位を付与する。

    Returns:
        {馬番: [コーナー1順位, コーナー2順位, ...], ...}
        同通過位置（括弧内）の馬は全員同じ順位になる。
    """
    corners_map: Dict[int, List[int]] = {}

    # コーナー番号 → シーケンス文字列 を抽出
    # 対応形式:
    #   "3角 (14,7),..."
    #   "3コーナー (14,7),..."
    #   "3角: (14,7),..."
    # 各マッチごとに「次のN角/末尾」までを捕獲
    corner_pattern = re.compile(
        r"(\d)\s*(?:角|コーナー)[：:\s　]*([0-9\(\)（）,\uFF0C\-\s]+?)(?=\d\s*(?:角|コーナー)|ハロンタイム|払戻|$)",
        re.DOTALL,
    )

    corner_data: List[Tuple[int, List[List[int]]]] = []
    for m in corner_pattern.finditer(full_text):
        corner_num = int(m.group(1))
        raw = m.group(2).strip()
        # 改行・全角カンマ除去
        raw = raw.replace("\u3000", " ").replace("\n", " ")
        groups = _parse_bracketed_corner_sequence(raw)
        if groups:
            corner_data.append((corner_num, groups))

    if not corner_data:
        return corners_map

    # コーナー番号順にソート（3角→4角の順）
    corner_data.sort(key=lambda x: x[0])

    # 各馬番に通過順位を付与: 同位置馬群は同順位、次位置は「現位置 + グループサイズ」
    for corner_num, groups in corner_data:
        pos = 1
        for grp in groups:
            for hno in grp:
                corners_map.setdefault(hno, []).append(pos)
            pos += len(grp)

    return corners_map


_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
}
_REQ_INTERVAL = 2.0

_BASE = "https://www.keiba.go.jp/KeibaWeb"

# ── NAR baba_code → netkeiba venue_code 逆引き ──
_NAR_BABA_TO_NETKEIBA = {
    "36": "30",   # 門別
    "10": "35",   # 盛岡
    "11": "36",   # 水沢
    "18": "42",   # 浦和
    "19": "43",   # 船橋
    "20": "44",   # 大井
    "21": "45",   # 川崎
    "22": "46",   # 金沢
    "23": "47",   # 笠松
    "24": "48",   # 名古屋
    "27": "50",   # 園田（netkeiba race_id 50 に統一。旧コード 49 は互換のみ）
    "28": "51",   # 姫路
    "31": "54",   # 高知
    "32": "55",   # 佐賀
    "3":  "65",   # 帯広
}

# ── netkeiba venue_code → NAR baba_code ──
_NETKEIBA_TO_NAR_BABA = {
    "30": "36",   # 門別
    "35": "10",   # 盛岡
    "36": "11",   # 水沢
    "42": "18",   # 浦和
    "43": "19",   # 船橋
    "44": "20",   # 大井
    "45": "21",   # 川崎
    "46": "22",   # 金沢
    "47": "23",   # 笠松
    "48": "24",   # 名古屋
    "49": "27",   # 園田 旧コード互換（正規は50→27）
    "50": "27",   # 園田（netkeiba race_id 50 正規）
    "51": "28",   # 姫路
    "54": "31",   # 高知
    "55": "32",   # 佐賀
    "65": "3",    # 帯広
}

# ── NAR baba_code → 場名 ──
_NAR_VENUE_NAMES = {
    "36": "門別", "10": "盛岡", "11": "水沢", "18": "浦和",
    "19": "船橋", "20": "大井", "21": "川崎", "22": "金沢",
    "23": "笠松", "24": "名古屋", "27": "園田", "28": "姫路",
    "31": "高知", "32": "佐賀", "3": "帯広",
}


class OfficialNARScraper:
    """NAR 公式サイトからレースデータを取得"""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)
        self._last_req = 0.0
        self._lock = threading.Lock()

    def _wait(self):
        """リクエスト間隔を確保（スレッドセーフ）"""
        with self._lock:
            elapsed = time.time() - self._last_req
            if elapsed < _REQ_INTERVAL:
                time.sleep(_REQ_INTERVAL - elapsed)
            self._last_req = time.time()

    # ================================================================
    # 公開 API
    # ================================================================

    def get_full_entry_from_race_id(self, race_id: str):
        """netkeiba形式 race_id から NAR公式出馬表を取得

        Returns: (RaceInfo, List[Horse]) or (None, [])
        """
        venue_code = race_id[4:6]
        baba_code = _NETKEIBA_TO_NAR_BABA.get(venue_code)
        if not baba_code:
            logger.debug("NAR: Unknown venue code %s", venue_code)
            return None, []

        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        if not race_no:
            return None, []

        # 日付: race_idから抽出（YYYY + VV + MMDD + RR）
        year = race_id[0:4]
        month = race_id[6:8]
        day = race_id[8:10]
        today = f"{year}/{month}/{day}"
        race_date = f"{year}-{month}-{day}"

        return self.get_full_entry(
            race_date=today, race_no=race_no,
            baba_code=baba_code, race_id_override=race_id,
        )

    def get_full_entry(self, race_date: str, race_no: int,
                       baba_code: str, race_id_override: str = ""):
        """NAR公式 DebaTable から完全なレースデータを取得

        Args:
            race_date: 日付 "YYYY/MM/DD" 形式
            race_no: レース番号
            baba_code: NAR baba_code (e.g., "20" for 大井)
            race_id_override: 指定されていればそのrace_idを使用

        Returns: (RaceInfo, List[Horse]) or (None, [])
        """

        url = f"{_BASE}/TodayRaceInfo/DebaTable"
        params = {
            "k_raceDate": race_date,
            "k_raceNo": str(race_no),
            "k_babaCode": baba_code,
        }

        try:
            self._wait()
            resp = self._session.get(url, params=params, headers=_HEADERS,
                                     timeout=15)
            if resp.status_code != 200:
                logger.debug("NAR DebaTable %d: %s R%d",
                             resp.status_code, baba_code, race_no)
                return None, []
        except Exception as e:
            logger.debug("NAR DebaTable fetch failed: %s", e)
            return None, []

        return self._parse_debatable(
            resp.text, race_date, race_no, baba_code, race_id_override,
        )

    def get_race_ids(self, date: str) -> List[str]:
        """指定日のNARレースID一覧を取得

        Args:
            date: "YYYY-MM-DD" 形式

        Returns: List[race_id] (netkeiba形式)
        """
        race_date = date.replace("-", "/")
        race_ids = []

        year = date[:4]
        mmdd = date[5:7] + date[8:10]  # "YYYY-MM-DD" → "MMDD"

        # 今日のレース開催場一覧を取得
        venues = self._get_today_venues(race_date)
        for baba_code, venue_name, race_count in venues:
            netkeiba_vc = _NAR_BABA_TO_NETKEIBA.get(baba_code)
            if not netkeiba_vc:
                continue
            for rno in range(1, race_count + 1):
                # race_id: YYYY + VV + MMDD + RR (netkeiba NAR形式)
                race_id = f"{year}{netkeiba_vc}{mmdd}{rno:02d}"
                race_ids.append(race_id)

        # 岩手競馬（水沢・盛岡）補完
        # nar.netkeiba.com に岩手競馬は掲載されないため、keiba.go.jp で直接確認
        for iwate_baba, iwate_vc in [("11", "36"), ("10", "35")]:  # 水沢, 盛岡
            if not any(rid[4:6] == iwate_vc for rid in race_ids):
                iwate_ids = self._get_iwate_race_ids(race_date, year, mmdd, iwate_baba, iwate_vc)
                if iwate_ids:
                    race_ids.extend(iwate_ids)
                    logger.info("岩手補完(%s): %dR", "水沢" if iwate_baba == "11" else "盛岡", len(iwate_ids))

        # ばんえい(帯広)補完: keiba.go.jpはばんえいを含まないため別途取得
        if not any(rid[4:6] == "65" for rid in race_ids):
            banei_ids = self._get_banei_race_ids(year, mmdd)
            race_ids.extend(banei_ids)

        race_ids.sort()
        logger.info("NAR race list: %d レース (%d 開催場%s)",
                     len(race_ids), len(venues),
                     " +ばんえい" if any(r[4:6] == "65" for r in race_ids) else "")
        return race_ids

    def _get_iwate_race_ids(self, race_date: str, year: str, mmdd: str,
                             baba_code: str, netkeiba_vc: str) -> List[str]:
        """岩手競馬（水沢・盛岡）のレースIDを取得

        nar.netkeiba.comに岩手競馬のレース一覧が掲載されないため:
        1. keiba.go.jp の RaceListページを直接プローブ
        2. 失敗時は nar.netkeiba.com の出馬表(shutuba)を直接プローブ
        """
        venue_name = "水沢" if baba_code == "11" else "盛岡"

        # ── 1. keiba.go.jp プローブ ──
        try:
            url = f"{_BASE}/TodayRaceInfo/RaceList"
            params = {"k_raceDate": race_date, "k_babaCode": baba_code}
            self._wait()
            resp = self._session.get(url, params=params, headers=_HEADERS, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                race_links = soup.select("a[href*='DebaTable']")
                race_count = len(race_links) if race_links else 0
                if race_count == 0:
                    # R付きテキストでフォールバック
                    for el in soup.select("td, a, span"):
                        text = el.get_text(strip=True)
                        if re.match(r"^\d{1,2}R$", text):
                            race_count += 1
                if race_count > 0:
                    return [f"{year}{netkeiba_vc}{mmdd}{rno:02d}" for rno in range(1, race_count + 1)]
            logger.warning("岩手keiba.go.jpプローブ失敗(%s): status=%s, レース検出=0",
                           venue_name, resp.status_code if resp else "N/A")
        except Exception as e:
            logger.warning("岩手keiba.go.jpプローブ例外(%s): %s", venue_name, e)

        # ── 2. nar.netkeiba.com 出馬表プローブ（フォールバック） ──
        try:
            probe_id = f"{year}{netkeiba_vc}{mmdd}01"
            self._wait()
            resp2 = self._session.get(
                "https://nar.netkeiba.com/race/shutuba.html",
                params={"race_id": probe_id}, headers=_HEADERS, timeout=10
            )
            if resp2.status_code == 200:
                soup2 = BeautifulSoup(resp2.text, "html.parser")
                horse_links = soup2.select("a[href*='/horse/']")
                if len(horse_links) >= 3:
                    # 出走馬が確認できた → 開催とみなしデフォルト12R生成
                    logger.info("岩手netkeibaプローブ成功(%s): 出走馬%d頭検出 → 12R生成",
                                venue_name, len(horse_links))
                    return [f"{year}{netkeiba_vc}{mmdd}{rno:02d}" for rno in range(1, 13)]
        except Exception as e:
            logger.debug("岩手netkeibaプローブ失敗(%s): %s", venue_name, e)

        return []

    def _get_banei_race_ids(self, year: str, mmdd: str) -> List[str]:
        """ばんえい(帯広)のレースIDを取得

        keiba.go.jpはばんえいを含まないため、nar.netkeiba.comで
        レース一覧を確認し、存在すればID一覧を返す。
        """
        try:
            # nar.netkeiba.comの出馬表で1R目をプローブ
            probe_id = f"{year}65{mmdd}01"
            probe_url = "https://nar.netkeiba.com/race/shutuba.html"
            resp = self._session.get(
                probe_url,
                params={"race_id": probe_id},
                headers=_HEADERS,
                timeout=10,
            )
            if resp.status_code != 200:
                return []
            # 出馬表テーブルが存在すれば開催日
            soup = BeautifulSoup(resp.text, "html.parser")
            # ばんえいの出走表には馬名リンクが含まれる
            horse_links = soup.select("a[href*='/horse/']")
            if len(horse_links) < 3:
                return []
            # レース数を推定: nar.netkeiba.comのレース一覧から取得を試みる
            race_count = self._probe_banei_race_count(year, mmdd)
            ids = [f"{year}65{mmdd}{rno:02d}" for rno in range(1, race_count + 1)]
            logger.info("ばんえい補完: %dR検出", race_count)
            return ids
        except Exception as e:
            logger.debug("ばんえい補完失敗: %s", e)
            return []

    def _probe_banei_race_count(self, year: str, mmdd: str) -> int:
        """nar.netkeiba.comでばんえいのレース数を推定"""
        try:
            date_key = f"{year}{mmdd}"
            url = "https://nar.netkeiba.com/top/race_list_sub.html"
            # まず日付タブからkaisai_idを取得
            date_url = "https://nar.netkeiba.com/top/race_list_get_date_list.html"
            resp = self._session.get(
                date_url,
                params={"kaisai_date": date_key, "encoding": "UTF-8"},
                headers=_HEADERS,
                timeout=10,
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                # 帯広のkaisai_idを探す
                for a in soup.select("a[href*='kaisai_date=']"):
                    href = a.get("href", "")
                    if date_key not in href:
                        continue
                    mi = re.search(r"kaisai_id=(\d+)", href)
                    if not mi:
                        continue
                    kid = mi.group(1)
                    # kaisai_idが65で始まるものがばんえい
                    if not kid.startswith("65"):
                        continue
                    # このkaisai_idでレース一覧を取得
                    sub_resp = self._session.get(
                        url,
                        params={"kaisai_date": date_key, "kaisai_id": kid, "encoding": "UTF-8"},
                        headers=_HEADERS,
                        timeout=10,
                    )
                    if sub_resp.status_code == 200:
                        sub_soup = BeautifulSoup(sub_resp.text, "html.parser")
                        race_links = sub_soup.select("a[href*='race_id=']")
                        count = 0
                        for a_tag in race_links:
                            m = re.search(r"race_id=(\d{12})", a_tag.get("href", ""))
                            if m and m.group(1)[4:6] == "65":
                                count += 1
                        if count > 0:
                            return count
        except Exception:
            pass
        # デフォルト: ばんえいは通常10-12R
        return 12

    def fetch_horse_history(self, lineage_code: str, horse_name: str = ""):
        """NAR公式 HorseMarkInfo から過去走を取得

        Args:
            lineage_code: k_lineageLoginCode (11桁)
            horse_name: 馬名 (ログ用)

        Returns: (List[PastRun], pedigree_dict)
        """

        if not lineage_code:
            return [], {}

        url = f"{_BASE}/DataRoom/HorseMarkInfo"
        params = {"k_lineageLoginCode": lineage_code}

        try:
            self._wait()
            resp = self._session.get(url, params=params, headers=_HEADERS,
                                     timeout=15)
            if resp.status_code != 200:
                logger.debug("NAR HorseMarkInfo %d: %s",
                             resp.status_code, lineage_code)
                return [], {}
        except Exception as e:
            logger.debug("NAR HorseMarkInfo failed: %s", e)
            return [], {}

        past_runs, pedigree = self._parse_horse_mark_info(resp.text)
        if past_runs:
            logger.info("NAR history: %s %d走", horse_name, len(past_runs))
        return past_runs, pedigree

    # ================================================================
    # DebaTable パーサー
    # ================================================================

    def _parse_debatable(self, html: str, race_date: str, race_no: int,
                         baba_code: str, race_id_override: str = ""):
        """NAR DebaTable HTMLから RaceInfo + Horse[] をフル構築

        NAR DebaTable 実構造 (11行/馬):
        Row  0: 枠(rowspan=5), 馬番(rowspan=5), 馬名, 騎手, オッズ, 着別成績, 過去5走概要
        Row 1-5: 着別成績 サブ行 (全/左/右/場/距)
        Row  6: 最高タイム
        Row  7: 性齢, 毛色, 生年月日, 斤量+騎手成績
        Row  8: 父, 調教師, 馬体重(rowspan=2)
        Row  9: 母, 馬主
        Row 10: 母父, 生産牧場
        """
        from data.masters.course_master import ALL_COURSES
        from src.models import CourseMaster, Horse, RaceInfo

        soup = BeautifulSoup(html, "html.parser")

        venue_name = _NAR_VENUE_NAMES.get(baba_code, "")
        netkeiba_vc = _NAR_BABA_TO_NETKEIBA.get(baba_code, "")
        iso_date = race_date.replace("/", "-")

        # 盛岡(10)と水沢(11)は別コードなので自動判別不要

        # ── レースヘッダ解析 ──
        race_name = ""
        distance = 0
        surface = ""
        direction = "右"
        grade = ""
        post_time = ""

        # <h4>: 発走時刻 "2026年3月8日（日）　高　知　第4競走　18:15発走"
        h4 = soup.select_one("h4")
        if h4:
            h4_text = h4.get_text(strip=True)
            m_pt = re.search(r"(\d{1,2}:\d{2})発走", h4_text)
            if m_pt:
                post_time = m_pt.group(1)

        # <h3> in section.raceTitle: レース名
        race_title_h3 = soup.select_one("section.raceTitle h3")
        if race_title_h3:
            race_name = race_title_h3.get_text(strip=True)
        else:
            # フォールバック: 最初の h3
            h3 = soup.select_one("h3")
            if h3:
                raw = h3.get_text(strip=True)
                # 距離・天候情報を含む場合は除去
                for sep in ["ダート", "芝"]:
                    idx = raw.find(sep)
                    if idx > 0:
                        after = raw[idx + len(sep):].lstrip(" \u3000")
                        if after and (after[0].isdigit()
                                      or after[0] in "０１２３４５６７８９"):
                            raw = raw[:idx]
                            break
                race_name = raw.strip() or h3.get_text(strip=True)

        # <ul class="dataArea"><li>: 距離・馬場・天候
        data_area = soup.select_one("ul.dataArea li")
        if data_area:
            da_text = data_area.get_text(strip=True)
            m_dist = re.search(
                r"(ダ(?:ート)?|芝)\s*(\d{3,4})\s*[mMｍ]?", da_text
            )
            if m_dist:
                s = m_dist.group(1)
                surface = "ダート" if "ダ" in s else "芝"
                distance = int(m_dist.group(2))
            m_dir = re.search(r"[（(]([左右])[）)]", da_text)
            if m_dir:
                direction = m_dir.group(1)

        # フォールバック: ページ全体から距離を探す
        if not distance:
            for el in soup.select("span, div, p, td, li"):
                text = el.get_text(strip=True)
                m = re.search(
                    r"(ダ(?:ート)?|芝)\s*(\d{3,4})\s*[mMｍ]?", text
                )
                if m:
                    s = m.group(1)
                    surface = "ダート" if "ダ" in s else "芝"
                    distance = int(m.group(2))
                    m_dir = re.search(r"[（(]?([左右])[）)]?", text)
                    if m_dir:
                        direction = m_dir.group(1)
                    break
        if not distance:
            full_text = soup.get_text()
            m = re.search(r"(\d{3,4})\s*[mMメートルｍ]", full_text)
            if m:
                distance = int(m.group(1))
        if not surface:
            surface = "ダート"

        # グレード判定
        for keyword, g in [
            ("Jpn1", "G1"), ("JpnI", "G1"), ("Jpn2", "G2"),
            ("JpnII", "G2"), ("Jpn3", "G3"), ("JpnIII", "G3"),
            ("GI", "G1"), ("GII", "G2"), ("GIII", "G3"),
            ("重賞", "重賞"),
        ]:
            if keyword in (race_name or ""):
                grade = g
                break

        # CourseMaster 検索
        course = None
        if netkeiba_vc and surface and distance:
            course_id = f"{netkeiba_vc}_{surface}_{distance}"
            for c in ALL_COURSES:
                if c.course_id == course_id:
                    course = c
                    break
        if not course and netkeiba_vc and distance:
            best, best_diff = None, 9999
            for c in ALL_COURSES:
                if c.venue_code == netkeiba_vc:
                    d = abs(c.distance - distance)
                    if d < best_diff:
                        best, best_diff = c, d
            if best:
                course = best
        if not course:
            course = CourseMaster(
                venue=venue_name, venue_code=netkeiba_vc,
                distance=distance or 1400, surface=surface or "ダート",
                direction=direction, straight_m=300,
                corner_count=4, corner_type="小回り",
                _first_corner="平均", slope_type="坂なし",
                inside_outside="なし", is_jra=False,
            )

        # ── 馬リスト解析 (11行/馬) ──
        horses = []
        tables = soup.select("table")
        if not tables:
            return None, []

        # メインテーブル = 最大行数のテーブル
        main_table = max(tables, key=lambda t: len(t.select("tr")))
        all_rows = main_table.select("tr")

        # horseNum クラスを持つ行 = 各馬のメイン行
        horse_start_indices = []
        for idx, row in enumerate(all_rows):
            if row.select_one("td.horseNum"):
                horse_start_indices.append(idx)

        # フォールバック: courseNum で検索
        if not horse_start_indices:
            for idx, row in enumerate(all_rows):
                for cell in row.select("td"):
                    cls = cell.get("class", [])
                    if isinstance(cls, str):
                        cls = cls.split()
                    if "courseNum" in cls or "horseNum" in cls:
                        horse_start_indices.append(idx)
                        break

        last_gate_no = 0

        for si in horse_start_indices:
            main_cells = all_rows[si].select("td")
            if not main_cells:
                continue

            # ── Row 0: 枠, 馬番, 馬名, 騎手, オッズ ──
            gate_no = 0
            horse_no = 0
            horse_name = ""
            jockey = ""
            jockey_id = ""
            odds_val = None
            popularity = None
            horse_id = ""
            lineage_code = ""

            for cell in main_cells:
                cls = cell.get("class", [])
                if isinstance(cls, str):
                    cls = cls.split()
                text = cell.get_text(strip=True)

                # 枠番 (courseNum class)
                if any(c.startswith("courseNum") or c.startswith("course_")
                       for c in cls) or "courseNum" in cls:
                    if text.isdigit():
                        gate_no = int(text)
                        last_gate_no = gate_no

                # 馬番 (horseNum class)
                if "horseNum" in cls:
                    if text.isdigit():
                        horse_no = int(text)

                # 馬名 (HorseMarkInfo リンク)
                a_h = cell.select_one("a[href*='HorseMarkInfo']")
                if a_h:
                    horse_name = a_h.get_text(strip=True)
                    href = a_h.get("href", "")
                    m_lc = re.search(r"k_lineageLoginCode=(\d+)", href)
                    if m_lc:
                        lineage_code = m_lc.group(1)
                        horse_id = f"nar_{lineage_code}"

                # 騎手 (RiderMark リンク)
                a_j = cell.select_one("a[href*='RiderMark']")
                if a_j and not jockey:
                    jockey = a_j.get_text(strip=True)
                    m_jid = re.search(
                        r"k_riderLicenseNo=(\d+)", a_j.get("href", "")
                    )
                    if m_jid:
                        jockey_id = m_jid.group(1)

                # オッズ (odds_weight class 内の span)
                if "odds_weight" in cls:
                    span = cell.select_one(
                        "span.odds_Black, span.odds_Red, span.odds_Blue"
                    )
                    if span:
                        try:
                            odds_val = float(span.get_text(strip=True))
                        except ValueError:
                            pass
                    m_pop = re.search(r"\((\d+)人気\)", text)
                    if m_pop:
                        popularity = int(m_pop.group(1))

            if not gate_no:
                gate_no = last_gate_no
            if not horse_no or not horse_name:
                continue

            # ── Row 7 (offset +7): 性齢, 毛色, 斤量 ──
            sex = ""
            age = 0
            color = ""
            weight_kg = 55.0

            if si + 7 < len(all_rows):
                cells7 = all_rows[si + 7].select("td")
                for ci, cell in enumerate(cells7):
                    cls = cell.get("class", [])
                    if isinstance(cls, str):
                        cls = cls.split()
                    text = cell.get_text(strip=True)

                    # 性齢 (最初の noBorder セル): "牡3", "牝5", "セン7"
                    if ci == 0 or "noBorder" in cls:
                        m_sa = re.search(r"(牡|牝|セン?)\s*(\d+)", text)
                        if m_sa and not sex:
                            sex = m_sa.group(1)
                            if sex == "セ":
                                sex = "セン"
                            age = int(m_sa.group(2))

                    # 毛色 (2番目 noBorder)
                    if ci == 1 and "noBorder" in cls and not color:
                        if text and not text[0].isdigit():
                            color = text

                    # 斤量: "57.0　3-6-0-1" or "△ 54.0　0-0-0-2"
                    # 斤量は XX.0 形式（整数+.0）で40-70の範囲
                    # 最初に見つかった値を採用し、後続セルで上書きしない
                    if weight_kg == 55.0 and ci >= 2:
                        m_wk = re.search(r"△?\s*(\d{2}\.\d)", text)
                        if m_wk:
                            try:
                                wk = float(m_wk.group(1))
                                if 40 <= wk <= 70:
                                    weight_kg = wk
                            except ValueError:
                                pass

            # ── Row 8 (offset +8): 父, 調教師, 馬体重 ──
            sire = ""
            trainer = ""
            trainer_id = ""
            horse_weight = None
            weight_change = None

            if si + 8 < len(all_rows):
                cells8 = all_rows[si + 8].select("td")
                sire_set = False
                for ci, cell in enumerate(cells8):
                    cls = cell.get("class", [])
                    if isinstance(cls, str):
                        cls = cls.split()
                    text = cell.get_text(strip=True)

                    # 父 (最初のセル, colspan=3)
                    if ci == 0 and not sire_set:
                        a = cell.select_one("a")
                        if (a and "TrainerMark" not in a.get("href", "")) or (not a and text):
                            sire = text
                        sire_set = True

                    # 調教師 (TrainerMark リンク)
                    a_tr = cell.select_one("a[href*='TrainerMark']")
                    if a_tr:
                        trainer = a_tr.get_text(strip=True)
                        m_tid = re.search(
                            r"k_trainerLicenseNo=(\d+)",
                            a_tr.get("href", ""),
                        )
                        if m_tid:
                            trainer_id = m_tid.group(1)

                    # 馬体重 (odds_weight class or rowspan=2)
                    rs = cell.get("rowspan", "")
                    if "odds_weight" in cls or str(rs) == "2":
                        m_hw = re.match(
                            r"(\d{3,4})\s*[\(（]([+-]?\d+)[\)）]", text
                        )
                        if m_hw:
                            horse_weight = int(m_hw.group(1))
                            weight_change = int(m_hw.group(2))
                        elif re.match(r"^\d{3,4}$", text):
                            v = int(text)
                            if 300 <= v <= 700:
                                horse_weight = v

            # ── Row 9 (offset +9): 母, 馬主 ──
            dam = ""
            owner = ""

            if si + 9 < len(all_rows):
                cells9 = all_rows[si + 9].select("td")
                for ci, cell in enumerate(cells9):
                    text = cell.get_text(strip=True)

                    # 母 (最初のセル)
                    if ci == 0 and not dam:
                        dam = text

                    # 馬主 (2番目のセル or Owner リンク)
                    if ci == 1 and not owner:
                        owner = text

            # ── Row 10 (offset +10): 母父, 生産牧場 ──
            mgs = ""
            breeder = ""

            if si + 10 < len(all_rows):
                cells10 = all_rows[si + 10].select("td")
                for ci, cell in enumerate(cells10):
                    text = cell.get_text(strip=True)

                    # 母父 (最初のセル, 括弧付き): "（Consolidator）"
                    if ci == 0 and not mgs:
                        mgs = text.strip("（）()").strip()

                    # 生産牧場 (2番目のセル)
                    if ci == 1 and not breeder:
                        breeder = text

            # ── 過去5走データ解析 ──
            past_runs = []
            # Row 0 末尾5セル: 過去走概要
            # "126.02.08　良　12頭高知　右1400　6番"
            # → finish_pos, date, condition, field+venue, dir+dist, horse_no
            past_summaries = []
            if len(main_cells) > 10:
                for cell in main_cells[-5:]:
                    text = cell.get_text(strip=True)
                    if text and re.search(r"\d{2}\.\d{2}\.\d{2}", text):
                        past_summaries.append(text)

            # Row 8,9,10 のオフセット3以降: 過去走詳細
            past_details_r8 = []  # "2人　474　永森大 57.0"
            past_details_r9 = []  # "1:31.9　1-1-1-1　38.1"
            past_details_r10 = []  # "0.2　カツテナイオイシサ"
            if si + 8 < len(all_rows):
                c8 = all_rows[si + 8].select("td")
                # 先頭セル(父・調教師・馬体重)をスキップして残りを取得
                skip = 0
                for cell in c8:
                    cls = cell.get("class", [])
                    if isinstance(cls, str):
                        cls = cls.split()
                    if "odds_weight" in cls or skip < 2:
                        skip += 1
                        continue
                    past_details_r8.append(cell.get_text(strip=True))
            if si + 9 < len(all_rows):
                c9 = all_rows[si + 9].select("td")
                # 先頭2セル(母・馬主)をスキップ
                past_details_r9 = [
                    c.get_text(strip=True) for c in c9[2:]
                ]
            if si + 10 < len(all_rows):
                c10 = all_rows[si + 10].select("td")
                # 先頭2-3セル(母父・牧場・info)をスキップ
                skip = 0
                for cell in c10:
                    cls = cell.get("class", [])
                    if isinstance(cls, str):
                        cls = cls.split()
                    if "info" in cls or skip < 2:
                        skip += 1
                        continue
                    past_details_r10.append(cell.get_text(strip=True))

            for pi, summary in enumerate(past_summaries):
                try:
                    pr = self._parse_past_race_cells(
                        summary,
                        past_details_r8[pi] if pi < len(past_details_r8) else "",
                        past_details_r9[pi] if pi < len(past_details_r9) else "",
                        past_details_r10[pi] if pi < len(past_details_r10) else "",
                    )
                    if pr:
                        past_runs.append(pr)
                except Exception:
                    pass

            horse = Horse(
                horse_id=horse_id or f"nar_{baba_code}_{race_no}_{horse_no}",
                horse_name=horse_name,
                sex=sex or "不明",
                age=age,
                color=color,
                trainer=trainer,
                trainer_id=trainer_id,
                owner=owner,
                breeder=breeder,
                sire=sire,
                dam=dam,
                maternal_grandsire=mgs,
                race_date=iso_date,
                venue=venue_name,
                race_no=race_no,
                gate_no=gate_no,
                horse_no=horse_no,
                jockey=jockey,
                jockey_id=jockey_id,
                weight_kg=weight_kg,
                odds=odds_val,
                popularity=popularity,
                horse_weight=horse_weight,
                weight_change=weight_change,
            )
            if lineage_code:
                horse._lineage_code = lineage_code
            if past_runs:
                horse.past_runs = past_runs

            horses.append(horse)

        if not horses:
            logger.warning("NAR DebaTable: No horses for %s R%d",
                           baba_code, race_no)
            return None, []

        # race_id の構築
        if race_id_override:
            rid = race_id_override
        else:
            year = iso_date[:4]
            mmdd = iso_date[5:7] + iso_date[8:10]  # "YYYY-MM-DD" → "MMDD"
            rid = f"{year}{netkeiba_vc}{mmdd}{race_no:02d}"

        race_info = RaceInfo(
            race_id=rid,
            race_date=iso_date,
            venue=venue_name,
            race_no=race_no,
            race_name=race_name or f"{venue_name}{race_no}R",
            grade=grade,
            condition="",
            course=course,
            field_count=len(horses),
            post_time=post_time,
            is_jra=False,
        )

        logger.info(
            "NAR full entry: %s %dR %s %s%dm %d頭",
            venue_name, race_no, race_name,
            surface or "ダート", distance, len(horses),
        )
        return race_info, horses

    # ================================================================
    # DebaTable 過去走セル パーサー
    # ================================================================

    @staticmethod
    def _parse_past_race_cells(summary: str, detail_r8: str,
                               detail_r9: str, detail_r10: str):
        """DebaTable 内の過去走4セルからPastRunを構築

        Args:
            summary:  Row 0 末尾セル "126.02.08　良　12頭高知　右1400　6番"
            detail_r8: Row 8 セル "2人　474　永森大 57.0"
            detail_r9: Row 9 セル "1:31.9　1-1-1-1　38.1"
            detail_r10: Row 10 セル "0.2　カツテナイオイシサ"

        Returns: PastRun or None
        """
        from src.models import PastRun

        if not summary:
            return None

        # ── summary 解析 ──
        # "126.02.08　良　12頭高知　右1400　6番"
        #  ^ finish_pos (1着)
        #    ^^^^^^^^ date (26.02.08 = 2026-02-08)
        #              ^^ condition
        #                  ^^^^^ field_count + venue
        #                         ^^^^^^^^ direction + distance
        #                                  ^^^ horse_no
        finish_pos = 0
        race_date = ""
        condition = "良"
        field_count = 0
        p_venue = ""
        p_distance = 0
        p_surface = "ダート"
        p_horse_no = 0

        # 着順 + 日付: "1  26.02.08" → pos=1, date=2026-02-08
        m_sd = re.match(r"(\d{1,2})\s*(\d{2})\.(\d{2})\.(\d{2})", summary)
        if m_sd:
            finish_pos = int(m_sd.group(1))
            y = int(m_sd.group(2))
            year = 2000 + y if y < 50 else 1900 + y
            race_date = f"{year}-{m_sd.group(3)}-{m_sd.group(4)}"

        # 馬場: "良", "稍重", "重", "不良"
        m_cond = re.search(r"[　\s](良|稍重|重|不良)[　\s]", summary)
        if m_cond:
            condition = m_cond.group(1)

        # 頭数 + 場名: "12頭高知"
        m_fc = re.search(r"(\d{1,2})頭([^\s　]+)", summary)
        if m_fc:
            field_count = int(m_fc.group(1))
            p_venue = m_fc.group(2)

        # 距離: "右1400" or "左1200" — 左右の後の数字のみマッチ
        m_dist = re.search(r"[左右](\d{3,4})", summary)
        if m_dist:
            p_distance = int(m_dist.group(1))
        else:
            # フォールバック: 頭数の後の距離
            m_dist2 = re.search(r"頭.+?(\d{3,4})", summary)
            if m_dist2:
                p_distance = int(m_dist2.group(1))

        # 馬番: "6番"
        m_hn = re.search(r"(\d{1,2})番", summary)
        if m_hn:
            p_horse_no = int(m_hn.group(1))

        # ── detail_r8 解析: "2人　474　永森大 57.0" ──
        p_popularity = None
        p_horse_weight = None
        p_jockey = ""
        p_weight_kg = 55.0

        if detail_r8:
            m_pop = re.search(r"(\d{1,2})人", detail_r8)
            if m_pop:
                p_popularity = int(m_pop.group(1))
            m_hw = re.search(r"\b(\d{3,4})\b", detail_r8)
            if m_hw:
                hw = int(m_hw.group(1))
                if 300 <= hw <= 700:
                    p_horse_weight = hw
            m_wk = re.search(r"([\d.]+)$", detail_r8.strip())
            if m_wk:
                try:
                    wk = float(m_wk.group(1))
                    if 40 <= wk <= 70:
                        p_weight_kg = wk
                except ValueError:
                    pass

        # ── detail_r9 解析: "1:31.9　1-1-1-1　38.1" ──
        finish_time_sec = 0.0
        positions_corners = []
        last_3f_sec = 0.0

        if detail_r9:
            # タイム: "1:31.9" → 91.9秒
            m_t = re.search(r"(\d):(\d{2}\.\d)", detail_r9)
            if m_t:
                finish_time_sec = int(m_t.group(1)) * 60 + float(m_t.group(2))

            # 通過順: "1-1-1-1"
            m_pos = re.search(r"(\d+-\d+(?:-\d+)*)", detail_r9)
            if m_pos:
                positions_corners = [int(x) for x in m_pos.group(1).split("-")]

            # 上がり3F: 末尾の "38.1"
            m_3f = re.search(r"[\s　](3\d\.\d|4\d\.\d)\s*$", detail_r9)
            if m_3f:
                last_3f_sec = float(m_3f.group(1))

        position_4c = (
            positions_corners[-1] if positions_corners else finish_pos
        )

        return PastRun(
            race_date=race_date,
            venue=p_venue,
            course_id="",
            distance=p_distance,
            surface=p_surface,
            condition=condition,
            class_name="",
            grade="",
            field_count=field_count,
            gate_no=0,
            horse_no=p_horse_no,
            jockey=p_jockey,
            weight_kg=p_weight_kg,
            position_4c=position_4c,
            finish_pos=finish_pos,
            finish_time_sec=finish_time_sec,
            last_3f_sec=last_3f_sec,
            margin_behind=0.0,
            margin_ahead=0.0,
            horse_weight=p_horse_weight,
            positions_corners=positions_corners,
            popularity_at_race=p_popularity,
        )

    # ================================================================
    # HorseMarkInfo パーサー（過去走）
    # ================================================================

    def _parse_horse_mark_info(self, html: str):
        """NAR HorseMarkInfo ページから過去走 + 血統を取得

        Returns: (List[PastRun], pedigree_dict)
        """
        from src.models import PastRun

        soup = BeautifulSoup(html, "html.parser")
        past_runs = []
        pedigree = {
            "sire": "", "dam": "", "maternal_grandsire": "",
            "sire_id": "", "dam_id": "", "mgs_id": "",
        }

        # 血統情報: プロフィール部分
        full_text = soup.get_text()
        m_sire = re.search(r'(?:父|サイアー)[：:\s]+([^\s/（(、,]+)', full_text)
        if m_sire:
            pedigree["sire"] = m_sire.group(1).strip()
        m_dam = re.search(r'(?:母)[：:\s]+([^\s/（(、,]+)', full_text)
        if m_dam:
            pedigree["dam"] = m_dam.group(1).strip()
        m_mgs = re.search(
            r'(?:母の父|母父|BMS)[：:\s]+([^\s/（(、,]+)', full_text
        )
        if m_mgs:
            pedigree["maternal_grandsire"] = m_mgs.group(1).strip()

        # 過去走テーブルを探す
        date_pat = re.compile(r"(\d{4})[/\-.](\d{1,2})[/\-.](\d{1,2})")
        tables = soup.select("table")
        race_table = None
        max_date_rows = 0

        for tbl in tables:
            dr = 0
            for row in tbl.select("tr"):
                cells = row.select("td")
                if cells:
                    for cell in cells[:3]:
                        if date_pat.search(cell.get_text(strip=True)):
                            dr += 1
                            break
            if dr > max_date_rows:
                max_date_rows = dr
                race_table = tbl

        if not race_table or max_date_rows == 0:
            return past_runs, pedigree

        # 過去走テーブル解析 (列位置ベース)
        #
        # HorseMarkInfo テーブル構造 (23セル/行):
        #   ci  0: 年月日        ci 12: 人気
        #   ci  1: 競馬場        ci 13: 着順
        #   ci  2: R             ci 14: タイム
        #   ci  3: 競走名        ci 15: 差
        #   ci  4: 格組          ci 16: 上3F
        #   ci  5: 距離          ci 17: 体重
        #   ci  6: 天候          ci 18: 騎手(所属)
        #   ci  7: 馬場状態      ci 19: 重量(斤量)
        #   ci  8: (空セル)      ci 20: 調教師
        #   ci  9: 頭数          ci 21: 収得賞金
        #   ci 10: 枠            ci 22: 1着馬
        #   ci 11: 馬番
        #
        # ※ ヘッダ行では「天候・馬場」が colspan=3 で ci6-8 を占める

        def _safe_int(s, default=0):
            try:
                return int(s)
            except (ValueError, TypeError):
                return default

        def _safe_float(s, default=0.0):
            try:
                return float(s)
            except (ValueError, TypeError):
                return default

        for row in race_table.select("tr"):
            cells = row.select("td")
            if len(cells) < 15:
                continue

            # 日付を含む行を特定
            c0_text = cells[0].get_text(strip=True)
            m_d = date_pat.search(c0_text)
            if not m_d:
                continue
            date_str = (
                f"{m_d.group(1)}-"
                f"{int(m_d.group(2)):02d}-"
                f"{int(m_d.group(3)):02d}"
            )

            try:
                # 列位置ベースで安全に取得 (23セル想定)
                n = len(cells)
                texts = [c.get_text(strip=True) for c in cells]

                venue = texts[1] if n > 1 else ""
                # NAR公式はJRA会場に「Ｊ」プレフィックスを付ける → 除去
                venue = venue.lstrip("Ｊ").lstrip("J").strip()
                # ci 3: 競走名（ユングフラウ賞、C3三組 等）
                race_name = texts[3] if n > 3 else ""
                # ci 4: 格組（３歳、２歳 等）
                kakugumi = texts[4] if n > 4 else ""
                # 表示用class_nameは競走名を優先
                class_name = race_name or kakugumi

                # 距離: 数字のみ ("1300") または "ダ1300" 等
                raw_dist = texts[5] if n > 5 else ""
                m_dist = re.search(r"(芝|ダ|ダート)\s*(\d{3,4})", raw_dist)
                if m_dist:
                    surface_val = "芝" if m_dist.group(1) == "芝" else "ダート"
                    distance = int(m_dist.group(2))
                elif raw_dist.isdigit() and 100 <= int(raw_dist) <= 3600:
                    surface_val = "ダート"
                    distance = int(raw_dist)
                else:
                    surface_val = "ダート"
                    distance = 0

                # 馬場状態
                condition = texts[7] if n > 7 else "良"
                if condition not in ("良", "稍重", "重", "不良"):
                    condition = "良"

                # 頭数
                field_count = _safe_int(texts[9]) if n > 9 else 0

                # 枠・馬番
                gate_no = _safe_int(texts[10]) if n > 10 else 0
                horse_no = _safe_int(texts[11]) if n > 11 else 0

                # 人気
                popularity = _safe_int(texts[12]) if n > 12 else 0

                # 着順 (除外・中止の場合は0)
                raw_pos = texts[13] if n > 13 else ""
                if raw_pos.isdigit():
                    finish_pos = int(raw_pos)
                else:
                    finish_pos = 0

                # タイム ("1:26.0" or "58.3")
                raw_time = texts[14] if n > 14 else ""
                m_time = re.match(r"^(\d+):(\d+)\.(\d)$", raw_time)
                if m_time:
                    finish_time = (
                        int(m_time.group(1)) * 60
                        + int(m_time.group(2))
                        + int(m_time.group(3)) / 10.0
                    )
                else:
                    m_time2 = re.match(r"^(\d{2,3})\.(\d)$", raw_time)
                    if m_time2:
                        finish_time = (
                            int(m_time2.group(1))
                            + int(m_time2.group(2)) / 10.0
                        )
                    else:
                        finish_time = 0.0

                # 差(着差)
                raw_margin = texts[15] if n > 15 else ""
                margin = _safe_float(raw_margin)

                # 上がり3F
                raw_3f = texts[16] if n > 16 else ""
                last_3f = _safe_float(raw_3f)

                # 馬体重
                raw_hw = texts[17] if n > 17 else ""
                hw = _safe_int(raw_hw) if raw_hw.isdigit() else None
                if hw and not (200 <= hw <= 800):
                    hw = None

                # 騎手 (テキストベース、リンクなしの場合あり)
                jockey = ""
                if n > 18:
                    a_j = cells[18].select_one("a")
                    if a_j:
                        jockey = a_j.get_text(strip=True)
                    else:
                        # テキストから騎手名を抽出 (改行・タブ・所属除去)
                        raw_j = cells[18].get_text()
                        raw_j = re.sub(r"[\n\r\t\u3000]+", "", raw_j).strip()
                        # (所属) を除去
                        raw_j = re.sub(r"[（(].+?[）)]$", "", raw_j).strip()
                        jockey = raw_j

                # 斤量
                raw_wk = texts[19] if n > 19 else ""
                weight_kg = _safe_float(raw_wk) or 55.0

                # レース番号 (ci 2)
                race_no_val = _safe_int(texts[2]) if n > 2 else 0

                # 通過順位 — 着差列(15)にはないので、パターンで全行探索
                positions_corners = []
                for ci2 in range(14, min(n, 18)):
                    t2 = texts[ci2]
                    if re.match(r"^\d+-\d+(-\d+)*$", t2):
                        positions_corners = [
                            int(x) for x in t2.split("-")
                        ]
                        break

                position_4c = (
                    positions_corners[-1] if positions_corners
                    else finish_pos
                )

                # course_idを生成（venue名→コード変換）
                from data.masters.venue_master import VENUE_NAME_TO_CODE
                _vc = VENUE_NAME_TO_CODE.get(venue, "")
                _course_id = f"{_vc}_{surface_val}_{distance}" if _vc and distance else ""

                # gradeを競走名→格組の順で推定
                _grade = ""
                try:
                    from src.calculator.ability import StandardTimeCalculator as _STC
                    _grade = _STC._infer_grade_from_class_name(race_name)
                    if not _grade and kakugumi:
                        _grade = _STC._infer_grade_from_class_name(kakugumi)
                except Exception:
                    pass

                # race_idを構築（会場コード + 日付 + レース番号）
                _race_id = ""
                if _vc and race_no_val > 0:
                    _mmdd = date_str.replace("-", "")[4:8]
                    _race_id = f"{date_str[:4]}{_vc}{_mmdd}{race_no_val:02d}"

                pr = PastRun(
                    race_date=date_str,
                    venue=venue,
                    course_id=_course_id,
                    distance=distance,
                    surface=surface_val,
                    condition=condition,
                    class_name=class_name,
                    grade=_grade,
                    field_count=field_count,
                    gate_no=gate_no,
                    horse_no=horse_no,
                    jockey=jockey,
                    weight_kg=weight_kg,
                    position_4c=position_4c,
                    finish_pos=finish_pos,
                    finish_time_sec=finish_time,
                    last_3f_sec=last_3f,
                    margin_behind=margin,
                    margin_ahead=0.0,
                    horse_weight=hw,
                    positions_corners=positions_corners,
                    popularity_at_race=popularity or None,
                    race_no=race_no_val,
                    race_id=_race_id,
                )
                past_runs.append(pr)

            except Exception as e:
                logger.debug("NAR past run parse error: %s", e)
                continue

        return past_runs, pedigree

    # ================================================================
    # レース一覧
    # ================================================================

    def _get_today_venues(self, race_date: str) -> List[Tuple[str, str, int]]:
        """NAR公式から指定日の開催場一覧を取得

        2段階で確実に取得:
        1. TodayRaceInfoTop ページからリンク抽出（高速だが翌日分は不確実）
        2. 未発見の会場を RaceList で直接プローブ（確実）

        Args:
            race_date: "YYYY/MM/DD" 形式

        Returns: [(baba_code, venue_name, race_count), ...]
        """
        venues = []
        seen_baba = set()

        # ── 1. TodayRaceInfoTop からリンク抽出（高速パス） ──
        try:
            url = f"{_BASE}/TodayRaceInfo/TodayRaceInfoTop"
            self._wait()
            resp = self._session.get(url, headers=_HEADERS, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for a in soup.select("a[href*='RaceList']"):
                    href = a.get("href", "")
                    m_bc = re.search(r"k_babaCode=(\d+)", href)
                    if not m_bc:
                        continue
                    baba_code = m_bc.group(1)
                    if race_date.replace("/", "%2f") not in href \
                            and race_date not in href:
                        continue
                    if baba_code in seen_baba:
                        continue
                    seen_baba.add(baba_code)
                    venue_name = _NAR_VENUE_NAMES.get(
                        baba_code, a.get_text(strip=True)
                    )
                    race_count = self._get_race_count(race_date, baba_code)
                    if race_count > 0:
                        venues.append((baba_code, venue_name, race_count))
        except Exception as e:
            logger.debug("TodayRaceInfoTop failed: %s", e)

        # ── 2. 未発見の会場を RaceList で直接プローブ（翌日・過去日対応） ──
        # TodayRaceInfoTop は日付パラメータを受け付けないため、
        # 翌日分のリンクが含まれない場合がある。全NAR会場を個別確認する。
        remaining = [bc for bc in _NAR_BABA_TO_NETKEIBA if bc not in seen_baba]
        if remaining:
            logger.debug("RaceListプローブ: %d会場を直接確認", len(remaining))
            for baba_code in remaining:
                race_count = self._get_race_count(race_date, baba_code)
                if race_count > 0:
                    venue_name = _NAR_VENUE_NAMES.get(baba_code, f"NAR{baba_code}")
                    venues.append((baba_code, venue_name, race_count))
                    seen_baba.add(baba_code)
                    logger.info("RaceListプローブ発見: %s %dR", venue_name, race_count)

        return venues

    def _get_race_count(self, race_date: str, baba_code: str) -> int:
        """指定開催場のレース数を取得（0 = 非開催）"""
        url = f"{_BASE}/TodayRaceInfo/RaceList"
        params = {"k_raceDate": race_date, "k_babaCode": baba_code}
        try:
            self._wait()
            resp = self._session.get(url, params=params, headers=_HEADERS,
                                     timeout=15)
            if resp.status_code != 200:
                return 0
        except Exception:
            return 0

        soup = BeautifulSoup(resp.text, "html.parser")
        # DebaTableリンクでレース数を数える（最も確実）
        race_links = soup.select("a[href*='DebaTable']")
        if race_links:
            return len(race_links)
        # フォールバック: R付きテキストを数える
        count = 0
        for el in soup.select("td, a, span"):
            text = el.get_text(strip=True)
            if re.match(r"^\d{1,2}R$", text):
                count += 1
        return count  # 0 = 非開催

    # ================================================================
    # NAR公式 成績（結果）取得
    # ================================================================

    def get_result(self, race_id: str, race_date: str) -> Optional[dict]:
        """NAR公式 RaceMarkTable からレース結果を取得

        Args:
            race_id: netkeiba形式 race_id (例: "202644030806")
            race_date: 日付 "YYYY-MM-DD" 形式

        Returns:
            {
                "order": [{"horse_no": int, "finish": int, "last_3f": float,
                           "time_sec": float, "weight_kg": float,
                           "horse_weight": int, "corners": [int, ...]}, ...],
                "payouts": {"tansho": [...], "fukusho": [...], ...}
            }
            または None（取得/パース失敗時）
        """
        # race_id からパラメータを抽出
        venue_code = race_id[4:6]
        baba_code = _NETKEIBA_TO_NAR_BABA.get(venue_code)
        if not baba_code:
            logger.debug("NAR get_result: 不明な venue_code %s", venue_code)
            return None

        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        if not race_no:
            logger.debug("NAR get_result: race_no 取得失敗 race_id=%s", race_id)
            return None

        # 日付を YYYY/MM/DD 形式に変換
        date_slash = race_date.replace("-", "/")

        url = f"{_BASE}/TodayRaceInfo/RaceMarkTable"
        params = {
            "k_raceDate": date_slash,
            "k_raceNo": str(race_no),
            "k_babaCode": baba_code,
        }

        try:
            self._wait()
            resp = self._session.get(url, params=params, headers=_HEADERS,
                                     timeout=15)
            if resp.status_code != 200:
                logger.debug("NAR RaceMarkTable %d: %s R%d",
                             resp.status_code, baba_code, race_no)
                return None
        except Exception as e:
            logger.debug("NAR RaceMarkTable 取得失敗: %s", e)
            return None

        try:
            soup = BeautifulSoup(resp.text, "html.parser")

            # 着順テーブルをパース
            order = self._parse_race_mark_table(soup)
            if not order:
                logger.debug("NAR RaceMarkTable: 着順データなし %s R%d",
                             baba_code, race_no)
                return None

            # コーナー通過順をパース
            corners_map = self._parse_nar_corners(soup)

            # 払戻をパース
            payouts = self._parse_nar_payouts(soup)

            # 着順リストにコーナー通過順をマージ
            for entry in order:
                hno = entry["horse_no"]
                entry["corners"] = corners_map.get(hno, [])

            logger.info("NAR result: %s R%d %d頭 払戻%d券種",
                        _NAR_VENUE_NAMES.get(baba_code, baba_code),
                        race_no, len(order), len(payouts))

            return {"order": order, "payouts": payouts}

        except Exception as e:
            logger.warning("NAR RaceMarkTable パース失敗: %s", e)
            return None

    def _parse_race_mark_table(self, soup: BeautifulSoup) -> list:
        """成績テーブルをパースして着順リストを返す

        列順: 着順, 枠番, 馬番, 馬名, 所属, 性齢, 負担重量, 騎手, 調教師,
              馬体重, 差, タイム, 着差, 上り3F, 人気

        Returns:
            [{"horse_no": int, "finish": int, "last_3f": float,
              "time_sec": float, "weight_kg": float, "horse_weight": int}, ...]
        """
        results = []
        tables = soup.select("table")
        if not tables:
            return results

        # 成績テーブルを特定: 「着順」ヘッダを持つテーブル
        result_table = None
        for tbl in tables:
            header_text = tbl.get_text()
            if "着順" in header_text and "馬番" in header_text:
                result_table = tbl
                break

        if not result_table:
            return results

        for row in result_table.select("tr"):
            cells = row.select("td")
            if len(cells) < 12:
                continue

            texts = [c.get_text(strip=True) for c in cells]

            # 着順列（先頭）が数字でなければスキップ（取消・除外・ヘッダ行）
            if not texts[0].isdigit():
                continue

            finish = int(texts[0])

            # 馬番 (3列目, index=2)
            horse_no = int(texts[2]) if texts[2].isdigit() else 0
            if not horse_no:
                continue

            # 負担重量 (7列目, index=6)
            weight_kg = 55.0
            try:
                wk = float(texts[6])
                if 40 <= wk <= 70:
                    weight_kg = wk
            except (ValueError, IndexError):
                pass

            # 馬体重 (10列目, index=9): "480" or "480(+4)"
            horse_weight = None
            if len(texts) > 9:
                m_hw = re.match(r"(\d{3,4})", texts[9])
                if m_hw:
                    hw = int(m_hw.group(1))
                    if 200 <= hw <= 800:
                        horse_weight = hw

            # タイム (12列目, index=11): "M:SS.S" or "SS.S"
            time_sec = 0.0
            if len(texts) > 11:
                m_t = re.match(r"(\d+):(\d{2}\.\d)", texts[11])
                if m_t:
                    time_sec = int(m_t.group(1)) * 60 + float(m_t.group(2))
                else:
                    m_t2 = re.match(r"(\d{2,3}\.\d)", texts[11])
                    if m_t2:
                        time_sec = float(m_t2.group(1))

            # 上り3F (14列目, index=13)
            last_3f = 0.0
            if len(texts) > 13:
                try:
                    val = float(texts[13])
                    if 30 <= val <= 50:
                        last_3f = val
                except ValueError:
                    pass

            results.append({
                "horse_no": horse_no,
                "finish": finish,
                "last_3f": last_3f,
                "time_sec": time_sec,
                "weight_kg": weight_kg,
                "horse_weight": horse_weight,
            })

        return results

    def _parse_nar_corners(self, soup: BeautifulSoup) -> dict:
        """コーナー通過順テーブルをパース

        対応形式:
          "3角 (14,7),(5,9,12),(1,6,8),13,4,10,(11,3),2"
          "3コーナー: (14,7),(5,9,12),..."
          "1角: 3,5,1,2,7"
          "1角 3-5-1-2-7"

        括弧 () は同通過位置の馬群を表し、同じ順位を付与する。

        Returns:
            {馬番: [各コーナーの通過順位], ...}
            例: 3角 (14,7),(5,9,12) → {14:[1], 7:[1], 5:[3], 9:[3], 12:[3]}
        """
        return parse_corner_passing_from_text(soup.get_text())

    def _parse_nar_payouts(self, soup: BeautifulSoup) -> dict:
        """払戻テーブルをパースしてnetkeiba互換形式で返す

        券種: 単勝, 複勝, 枠連複, 馬連複, 馬連単, ワイド, 三連複, 三連単

        Returns:
            {
                "tansho": [{"combo": str, "payout": int, "popularity": int}],
                "fukusho": [...],
                "wakuren": [...],
                "umaren": [...],
                "umatan": [...],
                "wide": [...],
                "sanrenpuku": [...],
                "sanrentan": [...],
            }
        """
        # 券種名 → キーのマッピング
        bet_type_map = {
            "単勝": "tansho",
            "複勝": "fukusho",
            "枠連複": "wakuren",
            "馬連複": "umaren",
            "馬連単": "umatan",
            "ワイド": "wide",
            "三連複": "sanrenpuku",
            "三連単": "sanrentan",
        }

        payouts: dict = {}

        # 払戻テーブルを特定: 「払戻」や券種名を含むテーブル
        tables = soup.select("table")
        payout_table = None
        for tbl in tables:
            tbl_text = tbl.get_text()
            # 「単勝」と「払戻」両方を含むテーブルを探す
            if "単勝" in tbl_text and ("払戻" in tbl_text or "複勝" in tbl_text):
                payout_table = tbl
                break

        if not payout_table:
            return payouts

        for row in payout_table.select("tr"):
            cells = row.select("td, th")
            if len(cells) < 2:
                continue

            # 最初のセルから券種名を取得
            bet_name = cells[0].get_text(strip=True)

            # 券種マッピングに一致するか確認
            bet_key = None
            for name, key in bet_type_map.items():
                if name in bet_name:
                    bet_key = key
                    break

            if not bet_key:
                continue

            # 複勝・ワイドは1セル内に改行区切りで複数値の場合がある
            # 各行: (組番, 払戻金, 人気)
            # cells[1]=組番, cells[2]=払戻金, cells[3]=人気（の場合もある）

            entries = []

            if len(cells) >= 4:
                # 標準形式: 組番 | 払戻金 | 人気
                combos_raw = cells[1].get_text("\n", strip=True).split("\n")
                payouts_raw = cells[2].get_text("\n", strip=True).split("\n")
                pops_raw = cells[3].get_text("\n", strip=True).split("\n") \
                    if len(cells) > 3 else []

                for i in range(len(combos_raw)):
                    combo = combos_raw[i].strip()
                    if not combo:
                        continue

                    # 払戻金: 円記号・カンマ除去
                    payout_val = 0
                    if i < len(payouts_raw):
                        raw_p = payouts_raw[i].strip()
                        raw_p = raw_p.replace("円", "").replace(",", "") \
                                     .replace("￥", "").replace("¥", "") \
                                     .replace("、", "").strip()
                        try:
                            payout_val = int(raw_p)
                        except ValueError:
                            pass

                    # 人気: "1番人気" → 1 or 数字のみ
                    pop_val = 0
                    if i < len(pops_raw):
                        raw_pop = pops_raw[i].strip()
                        m_pop = re.search(r"(\d+)", raw_pop)
                        if m_pop:
                            pop_val = int(m_pop.group(1))

                    if payout_val > 0:
                        entries.append({
                            "combo": combo,
                            "payout": payout_val,
                            "popularity": pop_val,
                        })

            elif len(cells) >= 3:
                # 2セル形式: 組番+払戻金 | 人気
                combo_payout = cells[1].get_text("\n", strip=True).split("\n")
                pops_raw = cells[2].get_text("\n", strip=True).split("\n")

                for i, cp in enumerate(combo_payout):
                    parts = re.split(r"\s+", cp.strip())
                    if len(parts) >= 2:
                        combo = parts[0]
                        raw_p = parts[-1].replace("円", "").replace(",", "") \
                                         .replace("￥", "").replace("¥", "") \
                                         .replace("、", "").strip()
                        try:
                            payout_val = int(raw_p)
                        except ValueError:
                            continue

                        pop_val = 0
                        if i < len(pops_raw):
                            m_pop = re.search(r"(\d+)", pops_raw[i].strip())
                            if m_pop:
                                pop_val = int(m_pop.group(1))

                        entries.append({
                            "combo": combo,
                            "payout": payout_val,
                            "popularity": pop_val,
                        })

            if entries:
                payouts[bet_key] = entries

        return payouts

    # ================================================================
    # 騎手・調教師 公式成績取得
    # ================================================================

    def fetch_rider_stats(self, license_no: str) -> Optional[Dict]:
        """
        keiba.go.jp RiderMark ページから騎手成績を取得する。

        Args:
            license_no: NAR公式ライセンスNo（例: "31266"）

        Returns:
            {
                "name": "赤津和希",
                "affiliation": "浦和",
                "lifetime": {"nar": {"runs": 1400, "wins": 46, ...}},
                "yearly": {"2026": {...}, "2025": {...}},
            }
            取得失敗時は None
        """
        url = f"{_BASE}/DataRoom/RiderMark?k_riderLicenseNo={license_no}"
        self._wait()
        try:
            resp = self._session.get(url, timeout=15)
            resp.encoding = "utf-8"
            if resp.status_code != 200:
                logger.warning("RiderMark %s: HTTP %d", license_no, resp.status_code)
                return None
        except Exception as e:
            logger.warning("RiderMark %s: %s", license_no, e)
            return None

        return self._parse_rider_or_trainer_mark(resp.text, license_no, "rider")

    def fetch_trainer_stats(self, license_no: str) -> Optional[Dict]:
        """
        keiba.go.jp TrainerMark ページから調教師成績を取得する。

        Args:
            license_no: NAR公式ライセンスNo

        Returns:
            fetch_rider_stats と同形式。取得失敗時は None
        """
        url = f"{_BASE}/DataRoom/TrainerMark?k_trainerLicenseNo={license_no}"
        self._wait()
        try:
            resp = self._session.get(url, timeout=15)
            resp.encoding = "utf-8"
            if resp.status_code != 200:
                logger.warning("TrainerMark %s: HTTP %d", license_no, resp.status_code)
                return None
        except Exception as e:
            logger.warning("TrainerMark %s: %s", license_no, e)
            return None

        return self._parse_rider_or_trainer_mark(resp.text, license_no, "trainer")

    def _parse_rider_or_trainer_mark(
        self, html: str, license_no: str, role: str
    ) -> Optional[Dict]:
        """RiderMark/TrainerMark ページの共通パーサー

        テーブル構造:
          Table 0 (class='trainerinfo'): 所属、生年月日等
          Table 1: 成績テーブル
            - ヘッダー: 着別回数, 1着, 2着, 3着, 4着, 5着, 着外, 合計, 勝率, 連対率
            - 生涯成績行: [地方競馬, 46, 69, 74, ..., 1400, 3.3%, 8.2%]
            - 年度別セクション: [cs=10]2026年 地方競馬成績 → 人気別行 → 合計行
        """
        soup = BeautifulSoup(html, "html.parser")

        result: Dict = {
            "license_no": license_no,
            "role": role,
            "name": "",
            "affiliation": "",
            "lifetime": {},
            "yearly": {},
        }

        # 名前: h4タグから取得（全角スペース入りなので除去）
        h4_tags = soup.find_all("h4")
        if h4_tags:
            raw_name = h4_tags[0].get_text(strip=True)
            result["name"] = raw_name.replace("\u3000", "").replace(" ", "")

        # 所属: trainerinfo テーブルの「所属」行
        info_table = soup.find("table", class_="trainerinfo")
        if info_table:
            for tr in info_table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    label = tds[0].get_text(strip=True)
                    value = tds[1].get_text(strip=True).replace("\u3000", "").replace(" ", "")
                    if label == "所属":
                        result["affiliation"] = value

        # 成績テーブル: 2番目のtable
        tables = soup.find_all("table")
        if len(tables) < 2:
            logger.warning("%sMark %s: 成績テーブルが見つかりません", role, license_no)
            return result

        stats_table = tables[1]
        rows = stats_table.find_all("tr")

        def _safe_int(s):
            try:
                return int(s)
            except (ValueError, TypeError):
                return 0

        def _safe_pct(s):
            try:
                return float(s.replace("%", "")) / 100.0
            except (ValueError, TypeError):
                return 0.0

        current_year = None
        for tr in rows:
            tds = tr.find_all(["th", "td"])
            if not tds:
                continue

            # セクションヘッダー（colspan=10）: "生涯成績" or "2026年 地方競馬成績"
            if tds[0].get("colspan"):
                section_text = tds[0].get_text(strip=True)
                if "生涯" in section_text:
                    current_year = "lifetime"
                else:
                    m = re.search(r"(\d{4})年", section_text)
                    current_year = m.group(1) if m else None
                continue

            # ヘッダー行（th）はスキップ
            if tds[0].name == "th":
                continue

            # データ行: [ラベル, 1着, 2着, 3着, 4着, 5着, 着外, 合計, 勝率, 連対率]
            if len(tds) < 10:
                continue

            label = tds[0].get_text(strip=True)
            values = [td.get_text(strip=True) for td in tds[1:]]

            row_data = {
                "label": label,
                "win": _safe_int(values[0]),
                "place2": _safe_int(values[1]),
                "place3": _safe_int(values[2]),
                "p4": _safe_int(values[3]),
                "p5": _safe_int(values[4]),
                "unplaced": _safe_int(values[5]),
                "runs": _safe_int(values[6]),
                "win_rate": _safe_pct(values[7]),
                "rentai_rate": _safe_pct(values[8]),
            }

            if current_year == "lifetime":
                if "地方" in label:
                    result["lifetime"]["nar"] = row_data
                elif "JRA" in label:
                    result["lifetime"]["jra"] = row_data
            elif current_year:
                if current_year not in result["yearly"]:
                    result["yearly"][current_year] = {"by_popularity": [], "total": None}
                if label == "合計":
                    result["yearly"][current_year]["total"] = row_data
                else:
                    result["yearly"][current_year]["by_popularity"].append(row_data)

        # 複勝率を計算（公式ページにはないが、1着+2着+3着から算出）
        nar = result["lifetime"].get("nar", {})
        if nar and nar.get("runs", 0) > 0:
            top3 = nar.get("win", 0) + nar.get("place2", 0) + nar.get("place3", 0)
            nar["place3_rate"] = round(top3 / nar["runs"], 4)

        return result
