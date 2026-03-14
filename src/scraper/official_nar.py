"""
NAR公式サイトスクレイパー (keiba.go.jp)

認証不要。DebaTable（出馬表）、HorseMarkInfo（馬詳細）、
RaceList（レース一覧）から完全なレースデータを取得する。
"""

import logging
import re
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("keiba.scraper.official_nar")

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
    "27": "49",   # 園田
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
    "49": "27",   # 園田
    "50": "27",   # 園田 (netkeiba旧コード)
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

        # 日付: 今日を使用（ライブデータ用途）
        today = datetime.now().strftime("%Y/%m/%d")
        race_date = datetime.now().strftime("%Y-%m-%d")

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
        from src.models import CourseMaster, Horse, RaceInfo

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

        # 今日のレース開催場一覧を取得
        venues = self._get_today_venues(race_date)
        if not venues:
            return []

        year = date[:4]
        mmdd = date[5:7] + date[8:10]  # "YYYY-MM-DD" → "MMDD"
        for baba_code, venue_name, race_count in venues:
            netkeiba_vc = _NAR_BABA_TO_NETKEIBA.get(baba_code)
            if not netkeiba_vc:
                continue
            for rno in range(1, race_count + 1):
                # race_id: YYYY + VV + MMDD + RR (netkeiba NAR形式)
                race_id = f"{year}{netkeiba_vc}{mmdd}{rno:02d}"
                race_ids.append(race_id)

        race_ids.sort()
        logger.info("NAR race list: %d レース (%d 開催場)",
                     len(race_ids), len(venues))
        return race_ids

    def fetch_horse_history(self, lineage_code: str, horse_name: str = ""):
        """NAR公式 HorseMarkInfo から過去走を取得

        Args:
            lineage_code: k_lineageLoginCode (11桁)
            horse_name: 馬名 (ログ用)

        Returns: (List[PastRun], pedigree_dict)
        """
        from src.models import PastRun

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
        from src.models import CourseMaster, Horse, PastRun, RaceInfo
        from data.masters.course_master import ALL_COURSES

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
                first_corner="平均", slope_type="坂なし",
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
                    m_wk = re.search(r"△?\s*([\d.]+)", text)
                    if m_wk and ci >= 2:
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
                        if a and "TrainerMark" not in a.get("href", ""):
                            sire = text
                        elif not a and text:
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
                class_name = texts[4] if n > 4 else ""

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

                pr = PastRun(
                    race_date=date_str,
                    venue=venue,
                    course_id="",
                    distance=distance,
                    surface=surface_val,
                    condition=condition,
                    class_name=class_name,
                    grade="",
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
        """NAR公式から本日の開催場一覧を取得

        Args:
            race_date: "YYYY/MM/DD" 形式

        Returns: [(baba_code, venue_name, race_count), ...]
        """
        url = f"{_BASE}/TodayRaceInfo/TodayRaceInfoTop"
        try:
            self._wait()
            resp = self._session.get(url, headers=_HEADERS, timeout=15)
            if resp.status_code != 200:
                logger.debug("NAR TodayRaceInfoTop %d", resp.status_code)
                return []
        except Exception as e:
            logger.debug("NAR TodayRaceInfoTop failed: %s", e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        venues = []
        seen_baba = set()  # baba_code 重複排除

        # RaceList リンクから開催場を抽出
        # ページには複数日付のリンクが含まれるため、
        # 対象日付(race_date)のリンクのみ抽出し baba_code で重複排除する
        for a in soup.select("a[href*='RaceList']"):
            href = a.get("href", "")
            m_bc = re.search(r"k_babaCode=(\d+)", href)
            if not m_bc:
                continue
            baba_code = m_bc.group(1)

            # 対象日付のリンクのみ処理（他日付のリンクは無視）
            if race_date.replace("/", "%2f") not in href \
                    and race_date not in href:
                continue

            # 同一 baba_code は1回だけ処理
            if baba_code in seen_baba:
                continue
            seen_baba.add(baba_code)

            venue_name = _NAR_VENUE_NAMES.get(
                baba_code, a.get_text(strip=True)
            )
            # レース数を取得するためRaceListページをfetch
            race_count = self._get_race_count(race_date, baba_code)
            if race_count > 0:
                venues.append((baba_code, venue_name, race_count))

        return venues

    def _get_race_count(self, race_date: str, baba_code: str) -> int:
        """指定開催場のレース数を取得"""
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
        # レースリンクを数える
        race_links = soup.select("a[href*='DebaTable']")
        if race_links:
            return len(race_links)
        # フォールバック: R付きテキストを数える
        count = 0
        for el in soup.select("td, a, span"):
            text = el.get_text(strip=True)
            if re.match(r"^\d{1,2}R$", text):
                count += 1
        return max(count, 12)  # デフォルト12レース
