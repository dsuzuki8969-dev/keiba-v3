"""
競馬解析マスターシステム v3.0 - netkeibaスクレイパー
Phase 1 データソース: netkeiba.com

取得対象:
  1. レース一覧 (日付 → レースID一覧)
  2. 出走表 (レースID → 出走馬リスト + 枠順・騎手・斤量)
  3. 馬の過去走データ (horse_id → PastRun[])
  4. オッズ (レースID → {馬番: オッズ})
  5. 騎手成績 (jockey_id → JockeyStats)
  6. 調教師成績 (trainer_id → TrainerStats)

注意:
  - スクレイピングはrobots.txtと利用規約を遵守すること
  - 本番運用時はリクエスト間隔を1-3秒空けること
  - JRA公式データ(JRA-VAN)への移行はPhase 2で実施
"""

import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

try:
    import requests
    from bs4 import BeautifulSoup

    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

from data.masters.venue_master import (
    JRA_CODES,
    VENUE_MAP,
    get_venue_code_from_race_id,
    get_venue_name,
    is_banei,
    is_jra,
)
from src.log import get_logger
from src.models import CourseMaster, Horse, PastRun, RaceInfo, TrainingRecord

logger = get_logger(__name__)

from config.settings import CACHE_DIR as _DEFAULT_CACHE_DIR
from config.settings import CACHE_MAX_AGE_SEC

try:
    import lz4.frame as _lz4

    _HAS_LZ4 = True
except ImportError:
    _HAS_LZ4 = False


# ============================================================
# 定数・設定
# ============================================================

BASE_URL = "https://db.netkeiba.com"
RACE_URL = "https://race.netkeiba.com"
NAR_URL = "https://nar.netkeiba.com"  # 地方競馬
ODDS_URL = "https://race.netkeiba.com/odds"
REQUEST_INTERVAL = 1.5  # 秒 (礼儀あるスクレイピング)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja-JP,ja;q=0.9",
}

# コース文字列 → CourseMasterキー変換
SURFACE_MAP = {"芝": "芝", "ダ": "ダート", "障": "障害", "直": "ダート"}
DIRECTION_MAP = {"右": "右", "左": "左"}

# 出馬表の調教師セルに含まれる所属プレフィックス
# netkeiba は "(美)矢作芳人" "(栗)池江泰寿" のように表示する
# NAR交流競走では "(大井)三村仁司" "(船橋)川島正行" など
_AFFILIATION_MAP = {
    "(美)": "美浦",
    "(栗)": "栗東",
    "(地)": "地方",  # 旧表記フォールバック
}
# NAR 競馬場名 → そのまま所属表示（交流競走で (大井) などが出る場合）
_NAR_VENUES_FOR_AFFILIATION = {
    "大井",
    "船橋",
    "川崎",
    "浦和",
    "門別",
    "盛岡",
    "水沢",
    "金沢",
    "笠松",
    "名古屋",
    "園田",
    "姫路",
    "高知",
    "佐賀",
    "帯広",
}


def _parse_trainer_affiliation(trainer_td_text: str) -> str:
    """調教師セルのテキストから所属（美浦・栗東・NAR場名）を抽出する。
    例: '(美)矢作芳人' → '美浦',  '(大井)三村' → '大井'
    """
    if not trainer_td_text:
        return ""
    # 既知プレフィックスを先にチェック
    for prefix, label in _AFFILIATION_MAP.items():
        if prefix in trainer_td_text:
            return label
    # (大井) (船橋) etc. パターン
    import re as _re

    m = _re.search(r"\(([^\)]{1,6})\)", trainer_td_text)
    if m:
        cand = m.group(1)
        if cand in _NAR_VENUES_FOR_AFFILIATION:
            return cand
    return ""


# ============================================================
# HTTPクライアント (レート制限付き)
# ============================================================


class NetkeibaClient:
    def __init__(self, cache_dir: str = None, no_cache: bool = False, request_interval: float = None,
                 ignore_ttl: bool = False):
        if not HAS_DEPS:
            raise ImportError(
                "requests と beautifulsoup4 が必要です: pip install requests beautifulsoup4 lxml"
            )
        if cache_dir is None:
            cache_dir = _DEFAULT_CACHE_DIR
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.cache_dir = cache_dir
        self.force_no_cache = no_cache
        self.ignore_ttl = ignore_ttl  # True なら TTL 無視でキャッシュを常に使う
        self.request_interval = request_interval if request_interval is not None else REQUEST_INTERVAL
        os.makedirs(cache_dir, exist_ok=True)
        self._last_request = 0.0
        self._stats_cache = 0
        self._stats_fetch = 0
        self._stats_skip = 0
        self._race_top_fetched = False
        self._db_top_fetched = False
        self._known_404: set = set()  # 400/404 を返したURLを記憶してスキップ

    def clone(self) -> "NetkeibaClient":
        """並列リクエスト用にクライアントを複製する。
        セッションCookieを引き継ぐが、レートリミットは独立（各クライアントが1.5s守る）。
        キャッシュディレクトリと known_404 セットは共有。
        """
        c = object.__new__(NetkeibaClient)
        c.session = requests.Session()
        c.session.headers.update(HEADERS)
        c.session.cookies.update(self.session.cookies)  # 認証Cookieを引き継ぐ
        c.cache_dir = self.cache_dir
        c.force_no_cache = self.force_no_cache
        c.ignore_ttl = self.ignore_ttl
        c.request_interval = self.request_interval
        c._last_request = 0.0  # 独立したレートリミット
        c._stats_cache = 0
        c._stats_fetch = 0
        c._stats_skip = 0
        c._race_top_fetched = self._race_top_fetched
        c._db_top_fetched = self._db_top_fetched
        c._known_404 = self._known_404  # 共有（読み取り大半、書き込みは稀）
        return c

    def _ensure_race_top_cookie(self) -> None:
        """race.netkeiba.com のトップを1回だけ取得し、セッションCookieを貰う（400回避のため）"""
        if self._race_top_fetched:
            return
        try:
            h = dict(HEADERS)
            h["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            h["Referer"] = "https://www.google.com/"
            self.session.get(f"{RACE_URL}/", timeout=10, headers=h)
        except Exception:
            pass
        self._race_top_fetched = True

    def _ensure_db_top_cookie(self) -> None:
        """db.netkeiba.com のトップを1回だけ取得し、セッションCookieを貰う（400回避のため）"""
        if self._db_top_fetched:
            return
        try:
            h = dict(HEADERS)
            h["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            h["Referer"] = "https://www.google.com/"
            self.session.get(f"{BASE_URL}/", timeout=10, headers=h)
        except Exception:
            pass
        self._db_top_fetched = True

    def _cache_ttl(self, url: str) -> float:
        """URLパターン別のキャッシュ有効期限（秒）を返す。
        過去レース結果・馬履歴は長期保持、当日データは短期とする。"""
        # 確定済みの過去レース結果（不変データ）: 30日
        if "/race/result.html" in url:
            return 30 * 24 * 3600
        # 馬の過去走履歴（直近レース後にのみ更新）: 7日
        if "/horse/result/" in url or ("/horse/" in url and "/horse/result" not in url and url.rstrip("/").split("/")[-1].isdigit()):
            return 7 * 24 * 3600
        # 当日出走表・馬柱（取消・変更あり）: 2時間
        if "/race/shutuba.html" in url or "/race/newspaper.html" in url:
            return 2 * 3600
        # オッズ（頻繁に更新）: 30分
        if "/odds/" in url or "odds" in url.lower():
            return 30 * 60
        # その他はデフォルト（settings.py の値）
        return CACHE_MAX_AGE_SEC

    def _read_cache(self, cache_path: str, url: str = "") -> Optional[str]:
        """lz4 → plain HTML の順でキャッシュを読む。破損ファイルは検出して削除し再取得を促す。"""
        if self.ignore_ttl:
            max_age = float("inf")  # TTL 無視: キャッシュが存在すれば常に使う
        else:
            max_age = self._cache_ttl(url) if url else CACHE_MAX_AGE_SEC

        def _is_valid_html(text: str) -> bool:
            """内容が有効なHTMLか確認（空でなく、`<` `>` または `<html` または `<!DOCTYPE` を含む）"""
            if not text or not text.strip():
                return False
            stripped = text.strip()
            return (
                ("<" in stripped and ">" in stripped)
                or "<html" in stripped.lower()
                or "<!DOCTYPE" in stripped.upper()
            )

        def _validate_and_clean(content: str, file_path: str, desc: str) -> Optional[str]:
            """HTML検証。無効なら警告ログを出してファイルを削除し None を返す"""
            if _is_valid_html(content):
                return content
            logger.warning(
                "キャッシュ破損を検出（%s）: %s → 削除して再取得します",
                desc,
                file_path,
            )
            try:
                os.remove(file_path)
            except OSError:
                pass
            return None

        lz4_path = cache_path + ".lz4"
        if _HAS_LZ4 and os.path.exists(lz4_path):
            try:
                age = time.time() - os.path.getmtime(lz4_path)
                if age <= max_age:
                    with open(lz4_path, "rb") as f:
                        raw = f.read()
                    content = _lz4.decompress(raw).decode("utf-8")
                    result = _validate_and_clean(content, lz4_path, "lz4")
                    if result is not None:
                        return result
            except OSError:
                pass
            except Exception as e:
                logger.warning(
                    "lz4解凍失敗（破損の可能性）: %s → 削除して再取得します: %s",
                    lz4_path,
                    e,
                    exc_info=True,
                )
                try:
                    os.remove(lz4_path)
                except OSError:
                    pass
        if os.path.exists(cache_path):
            try:
                age = time.time() - os.path.getmtime(cache_path)
                if age <= max_age:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        content = f.read()
                    result = _validate_and_clean(content, cache_path, "plain")
                    if result is not None:
                        return result
            except OSError:
                pass
        return None

    def _write_cache(self, cache_path: str, text: str) -> None:
        """lz4 が利用可能なら圧縮保存、なければ plain 保存"""
        if _HAS_LZ4:
            lz4_path = cache_path + ".lz4"
            with open(lz4_path, "wb") as f:
                f.write(_lz4.compress(text.encode("utf-8")))
        else:
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(text)

    def get(
        self, url: str, params: dict = None, use_cache: bool = True, encoding: str = None
    ) -> Optional[BeautifulSoup]:
        cache_key = self._cache_key(url, params)
        cache_path = os.path.join(self.cache_dir, cache_key + ".html")

        if use_cache and not self.force_no_cache:
            cached = self._read_cache(cache_path, url)
            if cached is not None:
                self._stats_cache += 1
                return BeautifulSoup(cached, "lxml")

        # 過去に400/404だったURLはHTTPリクエストを省略
        if cache_key in self._known_404:
            self._stats_skip += 1
            return None

        self._stats_fetch += 1
        elapsed = time.time() - self._last_request
        if elapsed < self.request_interval:
            time.sleep(self.request_interval - elapsed)

        # クエリを自前でエンコードし、Referer を付与して 400 を防ぐ
        full_url = url
        if params:
            sep = "&" if "?" in url else "?"
            full_url = url + sep + urlencode(params)

        parsed = urlparse(full_url)
        if "race.netkeiba.com" in parsed.netloc:
            self._ensure_race_top_cookie()
        elif "db.netkeiba.com" in parsed.netloc:
            self._ensure_db_top_cookie()
        headers = dict(HEADERS)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        if "nar.netkeiba.com" in parsed.netloc:
            headers["Referer"] = "https://nar.netkeiba.com/"
        elif "race.netkeiba.com" in parsed.netloc:
            # 出馬表・馬柱・結果は「一覧から遷移」に見せる
            if "/race/shutuba.html" in full_url or "/race/newspaper.html" in full_url or "/race/result.html" in full_url:
                headers["Referer"] = "https://race.netkeiba.com/top/race_list.html"
            else:
                headers["Referer"] = "https://race.netkeiba.com/"
        elif "db.netkeiba.com" in parsed.netloc:
            # 馬結果ページは馬トップから遷移に見せる
            if "/horse/result/" in full_url:
                horse_id = full_url.rstrip("/").split("/")[-1]
                headers["Referer"] = f"{BASE_URL}/horse/{horse_id}/"
            else:
                headers["Referer"] = f"{BASE_URL}/"

        try:
            resp = self.session.get(full_url, timeout=15, headers=headers)
            resp.raise_for_status()
            if encoding:
                resp.encoding = encoding
            elif params and params.get("encoding") == "UTF-8":
                resp.encoding = "utf-8"
            elif "race.netkeiba.com" in url or "nar.netkeiba.com" in url:
                resp.encoding = "utf-8" if "newspaper" in url else "euc-jp"
            else:
                resp.encoding = "euc-jp"
            self._last_request = time.time()

            self._write_cache(cache_path, resp.text)

            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            # 400/404 は恒久的に存在しないページなのでスキップリストに登録
            if hasattr(e, "response") and getattr(e.response, "status_code", 0) in (400, 404):
                self._known_404.add(cache_key)
                logger.debug("404/400スキップ登録: %s", full_url)
            else:
                logger.warning("GET failed: %s → %s: %s", full_url, type(e).__name__, e, exc_info=True)
            return None

    def _cache_key(self, url: str, params: dict = None) -> str:
        key = url.replace("https://", "").replace("/", "_").replace("?", "_")
        if params:
            key += "_" + "_".join(f"{k}={v}" for k, v in sorted(params.items()))
        return key[:200]


# ============================================================
# レース一覧スクレイパー
# ============================================================


class RaceListScraper:
    """
    指定日付のJRA/地方レース一覧を取得する
    URL: https://race.netkeiba.com/top/race_list.html?kaisai_date=YYYYMMDD
    """

    def __init__(self, client: NetkeibaClient):
        self.client = client

    def get_race_ids(self, date: str) -> List[str]:
        """
        date: "YYYY-MM-DD"
        Returns: race_id のリスト (例: "202501050101")

        netkeibaは2段階: ①日付タブ取得 ② race_list_sub でレース一覧取得
        JRA取得失敗時もNAR（ばんえい含む）は常に試行する
        """
        date_key = date.replace("-", "")
        race_ids = []

        # ── JRA（race.netkeiba.com）──
        # ① 日付タブから current_group を取得
        date_url = f"{RACE_URL}/top/race_list_get_date_list.html"
        date_soup = self.client.get(date_url, params={"kaisai_date": date_key, "encoding": "UTF-8"})

        group = ""
        if date_soup:
            for li in date_soup.select("li[date][group]"):
                if li.get("date") == date_key:
                    group = li.get("group", "")
                    break
            if not group:
                # Active な日付の group を拾う
                active = date_soup.select_one("li.Active[group]")
                if active and active.get("date") == date_key:
                    group = active.get("group", "")

            # ② race_list_sub でレース一覧取得（groupがなくても取得を試す）
            sub_url = f"{RACE_URL}/top/race_list_sub.html"
            params = {"kaisai_date": date_key, "encoding": "UTF-8"}
            if group:
                params["current_group"] = group
            sub_soup = self.client.get(sub_url, params=params)
            if sub_soup:
                for a_tag in sub_soup.select("a[href*='race_id=']"):
                    href = a_tag.get("href", "")
                    m = re.search(r"race_id=(\d{12})", href)
                    if m:
                        race_ids.append(m.group(1))
        else:
            logger.warning("JRA race list取得失敗（レート制限の可能性）、NARのみ取得")

        # JRAレースが0件の場合、current_group付きキャッシュを検索してフォールバック
        if not race_ids and not group:
            import glob as _glob
            _pattern = os.path.join(
                self.client.cache_dir,
                f"race.netkeiba.com_top_race_list_sub.html_current_group=*_*kaisai_date={date_key}*"
            )
            for _cached in _glob.glob(_pattern):
                try:
                    import lz4.frame as _lz4
                    with open(_cached, "rb") as _cf:
                        _html = _lz4.decompress(_cf.read()).decode("utf-8", errors="replace")
                    _cached_soup = BeautifulSoup(_html, "html.parser")
                    for _a in _cached_soup.select("a[href*='race_id=']"):
                        _m = re.search(r"race_id=(\d{12})", _a.get("href", ""))
                        if _m:
                            race_ids.append(_m.group(1))
                    if race_ids:
                        logger.info("JRAキャッシュフォールバック: %s から %d件取得", os.path.basename(_cached), len(race_ids))
                        break
                except Exception:
                    continue

        # ── NAR地方（nar.netkeiba.com）── JRA成否に関わらず常に取得
        nar_ids = self._get_nar_race_ids(date_key)
        race_ids.extend(nar_ids)

        return list(dict.fromkeys(race_ids))  # 重複除去・順序保持

    def _get_nar_race_ids(self, date_key: str) -> List[str]:
        """nar.netkeiba.com から地方競馬（金沢・高知・佐賀等）のレースIDを取得"""
        ids = []
        try:
            date_url = f"{NAR_URL}/top/race_list_get_date_list.html"
            date_soup = self.client.get(
                date_url, params={"kaisai_date": date_key, "encoding": "UTF-8"}
            )
            if not date_soup:
                return ids

            # 当日開催の全場（金沢・高知・佐賀等）のkaisai_idを収集
            kaisai_ids = []
            for a in date_soup.select("a[href*='kaisai_date=']"):
                href = a.get("href", "")
                if date_key not in href:
                    continue
                mi = re.search(r"kaisai_id=(\d+)", href)
                if mi:
                    kid = mi.group(1)
                    if kid not in kaisai_ids:
                        kaisai_ids.append(kid)

            sub_url = f"{NAR_URL}/top/race_list_sub.html"
            to_fetch = kaisai_ids if kaisai_ids else [None]
            for kaisai_id in to_fetch:
                params = {"kaisai_date": date_key, "encoding": "UTF-8"}
                if kaisai_id:
                    params["kaisai_id"] = kaisai_id
                sub_soup = self.client.get(sub_url, params=params)
                if not sub_soup:
                    continue
                # 地方race_id: YYYY+場(2)+MM+DD+R → 日付は [6:10] が MMDD、場は [4:6]
                mmdd = date_key[4:8] if len(date_key) >= 8 else ""
                for a_tag in sub_soup.select("a[href*='race_id=']"):
                    href = a_tag.get("href", "")
                    m = re.search(r"race_id=(\d{12})", href)
                    if m:
                        rid = m.group(1)
                        if mmdd and rid[6:10] != mmdd:
                            continue
                        ids.append(rid)
        except Exception:
            logger.debug("NAR race list fetch failed", exc_info=True)
        return ids


# ============================================================
# 馬柱（出走表）スクレイパー
# ============================================================
# 馬柱 = newspaper.html（競馬新聞形式）から取得。shutuba.html は使用しない。
# JRA: race.netkeiba.com/race/newspaper.html
# NAR: nar.netkeiba.com/race/newspaper.html
# 過去走: db.netkeiba.com/horse/{id}/


class RaceEntryParser:
    """
    馬柱（競馬新聞）から RaceInfo + Horse[] を構築する。
    URL: race.netkeiba.com/race/newspaper.html?race_id=XXXX
    """

    def __init__(self, client: NetkeibaClient, all_courses: Dict[str, CourseMaster]):
        self.client = client
        self.courses = all_courses

    def parse(
        self, race_id: str
    ) -> Tuple[Optional[RaceInfo], List[Horse], Dict[str, List[TrainingRecord]]]:
        is_nar = get_venue_code_from_race_id(race_id) not in JRA_CODES
        if is_nar:
            # 地方: nar.netkeiba.com を優先（HorseList 行が存在するため）。
            # 失敗時は race.netkeiba.com にフォールバック。
            shutuba_soup = self.client.get(
                f"{NAR_URL}/race/shutuba.html", params={"race_id": race_id}
            )
            if not shutuba_soup:
                shutuba_soup = self.client.get(
                    f"{RACE_URL}/race/shutuba.html", params={"race_id": race_id}
                )
            if not shutuba_soup:
                return None, [], {}
            race_info = self._parse_race_info(race_id, shutuba_soup)
            horses = self._parse_horses(race_id, shutuba_soup)
            # HorseList が取れなかった場合は race.netkeiba.com でリトライ
            if not horses:
                fallback_soup = self.client.get(
                    f"{RACE_URL}/race/shutuba.html", params={"race_id": race_id}
                )
                if fallback_soup:
                    if not race_info:
                        race_info = self._parse_race_info(race_id, fallback_soup)
                    horses = self._parse_horses(race_id, fallback_soup)
            if race_info and horses:
                race_info.field_count = len(horses)
            return race_info or None, horses, {}

        # JRA: race.netkeiba.com で取得
        shutuba_soup = self.client.get(f"{RACE_URL}/race/shutuba.html", params={"race_id": race_id})
        if not shutuba_soup:
            return None, [], {}

        # JRA: newspaper で馬柱を取得し、shutuba で補完
        soup = self.client.get(f"{RACE_URL}/race/newspaper.html", params={"race_id": race_id})
        if not soup:
            race_info = self._parse_race_info(race_id, shutuba_soup)
            horses = self._parse_horses(race_id, shutuba_soup)
            if race_info and horses:
                race_info.field_count = len(horses)
            return race_info or None, horses, {}

        race_info = self._parse_race_info(race_id, soup)
        horses = self._parse_horses_from_newspaper(race_id, soup)
        training_from_newspaper = self._parse_training_from_oikiri_table(soup)
        if shutuba_soup:
            if len(horses) < 5:
                shutuba_horses = self._parse_horses(race_id, shutuba_soup)
                if len(shutuba_horses) > len(horses):
                    horses = shutuba_horses
            if horses:
                self._supplement_jockey_weight(horses, shutuba_soup)
        return race_info, horses, training_from_newspaper

    def _supplement_jockey_weight(self, horses: List[Horse], soup) -> None:
        """騎手・斤量は newspaper にないため shutuba から補完"""
        if not soup:
            return
        for row in soup.select("table.ShutubaTable tr.HorseList"):
            horse_link = row.select_one("a[href*='/horse/']")
            if not horse_link:
                continue
            m = re.search(r"/horse/([A-Za-z]?\d+)", horse_link.get("href", ""))
            if not m:
                continue
            hid = m.group(1)
            for h in horses:
                if h.horse_id == hid:
                    jockey_a = row.select_one("a[href*='/jockey/']")
                    if jockey_a:
                        h.jockey = jockey_a.get_text(strip=True)
                        jm = re.search(r"/jockey/result/recent/(\w+)", jockey_a.get("href", ""))
                        if jm:
                            h.jockey_id = jm.group(1)
                    cells = row.select("td")
                    for i, c in enumerate(cells):
                        if "Barei" in (c.get("class") or []):
                            # 性齢を補完（例: "牡4", "牝3", "セ5"）
                            barei_text = c.get_text(strip=True)
                            bm = re.match(r"([牡牝セ])(\d+)", barei_text)
                            if bm:
                                h.sex = bm.group(1)
                                h.age = int(bm.group(2))
                            if i + 1 < len(cells):
                                wtext = cells[i + 1].get_text(strip=True)
                                if re.match(r"\d+\.?\d*", wtext):
                                    h.weight_kg = h.base_weight_kg = float(wtext)
                            break
                    # 馬体重・増減（前日予想では無い場合あり。ある時のみ設定）
                    wh_cell = row.select_one("td.Weight")
                    if wh_cell:
                        wh_text = wh_cell.get_text(strip=True)
                        wh_m = re.search(r"(\d{3,4})\(([+-]?\d+)\)", wh_text)
                        if wh_m:
                            h.horse_weight = int(wh_m.group(1))
                            h.weight_change = int(wh_m.group(2))
                    # 馬主
                    owner_cell = row.select_one("td.Owner a, td.Owner span")
                    if owner_cell:
                        owner_text = owner_cell.get_text(strip=True)
                        if owner_text:
                            h.owner = owner_text
                    break

    def _parse_race_info(self, race_id: str, soup: BeautifulSoup) -> Optional[RaceInfo]:
        try:
            # レース名
            race_name = ""
            name_el = soup.select_one(".RaceName")
            if name_el:
                race_name = name_el.get_text(strip=True)

            # コース情報・馬場状態 (例: "09:50発走 / ダ1200m (右) / 天候:晴 / 馬場:良")
            race_data_el = soup.select_one(".RaceData01")
            surface, direction, distance = "芝", "右", 2000
            track_turf, track_dirt = "", ""
            post_time_str = ""
            _banei_water_content = None
            if race_data_el:
                text = race_data_el.get_text()
                pt_m = re.search(r"(\d{1,2}):(\d{2})発走", text)
                if pt_m:
                    post_time_str = f"{int(pt_m.group(1)):02d}:{pt_m.group(2)}"
                sm = re.search(r"(芝|ダ|障|直)", text)
                dm = re.search(r"(右|左)", text)
                nm = re.search(r"(\d{3,4})m", text)
                if sm:
                    surface = SURFACE_MAP.get(sm.group(1), "芝")
                if dm:
                    direction = dm.group(1)
                elif sm and sm.group(1) == "直":
                    direction = "直"  # ばんえい（直線コース）
                if nm:
                    distance = int(nm.group(1))
                baba_m = re.search(r"馬場[：:]\s*([良稍重不]+)", text)
                if baba_m:
                    cond = baba_m.group(1)
                    if surface == "芝":
                        track_turf = cond
                    elif surface == "ダート":
                        track_dirt = cond
                # 芝/ダ両方の馬場が出ている場合 (例: "芝:良 ダ:稍")
                baba_both = re.search(r"芝[：:]?\s*([良稍重不]+)", text)
                if baba_both:
                    track_turf = baba_both.group(1)
                baba_both_d = re.search(r"ダ[：:]?\s*([良稍重不]+)", text)
                if baba_both_d:
                    track_dirt = baba_both_d.group(1)
                # ばんえい: 水分量からcondition推定
                if is_banei(get_venue_code_from_race_id(race_id)):
                    water_m = re.search(r"水分量[：:]\s*([\d.]+)", text)
                    if water_m:
                        wc = float(water_m.group(1))
                        _banei_water_content = wc
                        if wc <= 1.5:
                            track_dirt = "良"
                        elif wc <= 2.5:
                            track_dirt = "稍重"
                        elif wc <= 3.5:
                            track_dirt = "重"
                        else:
                            track_dirt = "不良"
                    else:
                        track_dirt = "良"

            # 競馬場: RaceData02 の span から取得（race_id より優先）
            venue_code = get_venue_code_from_race_id(race_id)
            venue = get_venue_name(venue_code)
            race_data2 = soup.select_one(".RaceData02")
            if race_data2:
                for span in race_data2.select("span"):
                    vn = span.get_text(strip=True)
                    # JRA競馬場名と一致する span を採用
                    if vn in VENUE_MAP:
                        venue = vn
                        # 競馬場名→venue_code の逆引き
                        rev = {v: k for k, v in VENUE_MAP.items()}
                        if vn in rev:
                            venue_code = rev[vn]
                        break

            # 内外
            inside_outside = "なし"
            if "内" in (race_data_el.get_text() if race_data_el else ""):
                inside_outside = "内"
            elif "外" in (race_data_el.get_text() if race_data_el else ""):
                inside_outside = "外"

            # 日付・場名・R番（venue は上記 RaceData02 で上書き済み）
            race_no = int(race_id[10:12])

            # 日付: HTML から取得（JRA は race_id に日付が入らない）
            race_date_str = self._parse_race_date_from_html(soup)
            if not race_date_str:
                # 地方競馬: race_id が YYYY+場(2)+MM+DD+R 形式
                if venue_code not in JRA_CODES and len(race_id) >= 10:
                    mm, dd = race_id[6:8], race_id[8:10]
                    if mm.isdigit() and dd.isdigit() and 1 <= int(mm) <= 12 and 1 <= int(dd) <= 31:
                        race_date_str = f"{race_id[:4]}-{mm}-{dd}"
                if not race_date_str:
                    race_date_str = f"{race_id[:4]}-01-01"  # 最終フォールバック

            # コースマスタ検索
            course_id = f"{venue_code}_{surface}_{distance}"
            course = self.courses.get(course_id)
            if not course:
                # フォールバック: 同場・同面の近い距離
                candidates = [
                    c
                    for c in self.courses.values()
                    if c.venue_code == venue_code and c.surface == surface
                ]
                if candidates:
                    course = min(candidates, key=lambda c: abs(c.distance - distance))
                else:
                    _is_banei_course = is_banei(venue_code)
                    course = CourseMaster(
                        venue=venue,
                        venue_code=venue_code,
                        distance=distance,
                        surface=surface,
                        direction=direction,
                        straight_m=200 if _is_banei_course else 350,
                        corner_count=0 if _is_banei_course else 4,
                        corner_type="" if _is_banei_course else "大回り",
                        first_corner="" if _is_banei_course else "平均",
                        slope_type="坂あり" if _is_banei_course else "坂なし",
                        inside_outside="なし" if _is_banei_course else inside_outside,
                        is_jra=False if _is_banei_course else True,
                    )

            # 条件・グレード
            race_data2 = soup.select_one(".RaceData02")
            condition = race_data2.get_text(strip=True) if race_data2 else ""
            grade = self._extract_grade(race_name + condition)

            # 頭数（newspaper: 調教テーブル OikiriTable の馬行数）
            horses_rows = soup.select("table.OikiriTable tr.HorseList td[class*='Waku']")
            field_count = len(horses_rows)

            ri = RaceInfo(
                race_id=race_id,
                race_date=race_date_str,
                venue=venue,
                race_no=race_no,
                race_name=race_name,
                grade=grade,
                condition=condition,
                course=course,
                field_count=field_count,
                is_jra=is_jra(venue_code),
            )
            ri.track_condition_turf = track_turf
            ri.track_condition_dirt = track_dirt
            ri.post_time = post_time_str
            # ばんえい水分量を moisture_dirt に設定（特徴量として利用）
            if _banei_water_content is not None:
                ri.moisture_dirt = _banei_water_content
            return ri
        except Exception as e:
            logger.warning("race info parse error %s: %s", race_id, e, exc_info=True)
            return None

    def _parse_race_date_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        """
        HTML の meta og:description / og:title から開催日を取得。
        例: "2025年12月28日 中山1R" → "2025-12-28"
        """
        for selector in [
            'meta[property="og:description"]',
            'meta[property="og:title"]',
            'meta[name="description"]',
        ]:
            el = soup.select_one(selector)
            if el and el.get("content"):
                content = el.get("content", "")
                m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", content)
                if m:
                    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return None

    def _parse_horses_from_newspaper(self, race_id: str, soup: BeautifulSoup) -> List[Horse]:
        """
        newspaper.html の調教テーブル (OikiriTable) から馬柱を取得。
        枠・馬番・馬名・horse_id を取得。性齢・斤量・騎手・調教師・馬体重は
        db.netkeiba.com/horse の馬ページから HorseHistoryParser で補完。
        """
        horses = []
        for row in soup.select("table.OikiriTable tr.HorseList"):
            waku_td = row.select_one("td[class*='Waku']")  # Waku1, Waku2, ...
            umaban_td = row.select_one("td.Umaban")
            if not waku_td or not umaban_td:
                continue

            try:
                gate_no = int(waku_td.get_text(strip=True))
                horse_no = int(umaban_td.get_text(strip=True))

                horse_link = row.select_one("a[href*='/horse/']")
                horse_name = horse_link.get_text(strip=True) if horse_link else ""
                horse_id = ""
                if horse_link:
                    m = re.search(r"/horse/([A-Za-z]?\d+)", horse_link.get("href", ""))
                    if m:
                        horse_id = m.group(1)

                if not horse_id:
                    continue

                # 性齢・斤量・騎手・調教師・馬体重は db.netkeiba.com/horse で後補完
                sex, age = "牡", 4
                weight_kg = 55.0
                jockey_name, jockey_id = "", ""
                trainer_name, trainer_id = "", ""
                trainer_affiliation = ""
                weight_horse, weight_change = None, None

                race_date_str = self._parse_race_date_from_html(soup)
                venue_code = get_venue_code_from_race_id(race_id)
                if not race_date_str and venue_code not in JRA_CODES and len(race_id) >= 10:
                    mm, dd = race_id[6:8], race_id[8:10]
                    if mm.isdigit() and dd.isdigit():
                        race_date_str = f"{race_id[:4]}-{mm}-{dd}"
                if not race_date_str:
                    race_date_str = f"{race_id[:4]}-01-01"
                race_no = int(race_id[10:12])

                horses.append(
                    Horse(
                        horse_id=horse_id,
                        horse_name=horse_name,
                        sex=sex,
                        age=age,
                        color="",
                        trainer=trainer_name,
                        trainer_id=trainer_id,
                        owner="",
                        breeder="",
                        sire="",
                        dam="",
                        past_runs=[],
                        race_date=race_date_str,
                        venue=venue_code,
                        race_no=race_no,
                        gate_no=gate_no,
                        horse_no=horse_no,
                        jockey=jockey_name,
                        jockey_id=jockey_id,
                        weight_kg=weight_kg,
                        base_weight_kg=weight_kg,
                        horse_weight=weight_horse,
                        weight_change=weight_change,
                        trainer_affiliation=trainer_affiliation,
                    )
                )
            except Exception as e:
                logger.warning(
                    "horse parse error (newspaper): race_id=%s → %s: %s", race_id, type(e).__name__, e, exc_info=True
                )
                continue

        return horses

    def _parse_training_from_oikiri_table(
        self, soup: BeautifulSoup
    ) -> Dict[str, List[TrainingRecord]]:
        """
        newspaper の OikiriTable から調教タイム・強度・コメントを取得。
        競馬ブックが使えない場合のフォールバック用。
        """
        result: Dict[str, List[TrainingRecord]] = {}
        _INTENSITY_MAP = {
            "一杯": "一杯",
            "強め": "強め",
            "本調教": "通常",
            "仕上": "やや速い",
            "馬也": "馬なり",
            "馬なり": "馬なり",
            "軽め": "軽め",
            "極軽め": "極軽め",
        }
        for row in soup.select("table.OikiriTable tr.HorseList"):
            horse_link = row.select_one("td.Horse_Info a[href*='/horse/']")
            if not horse_link:
                continue
            horse_name = horse_link.get_text(strip=True)
            if not horse_name:
                continue

            day_td = row.select_one("td.Training_Day")
            course_td = row.select_one("td.Training_Day + td")  # コース列（美Ｗ等）
            if not day_td:
                continue
            day_text = day_td.get_text(strip=True)
            m = re.match(r"(\d{4})/(\d{1,2})/(\d{1,2})", day_text)
            date_str = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else ""
            course_raw = course_td.get_text(strip=True) if course_td else ""
            course = course_raw.replace("Ｗ", "ウッド") or course_raw or "調教"

            lis = row.select("ul.TrainingTimeDataList li")
            splits = {}
            keys = [1000, 800, 600, 400, 200]
            for i, li in enumerate(lis):
                if i >= len(keys):
                    break
                t = li.get_text(strip=True)
                num = re.search(r"^([\d.]+)", t) or re.search(r"([\d.]+)", t)
                if num:
                    try:
                        splits[str(keys[i])] = float(num.group(1))
                    except ValueError:
                        pass

            load_td = row.select_one("td.TrainingLoad")
            intensity_raw = load_td.get_text(strip=True) if load_td else ""
            intensity = _INTENSITY_MAP.get(intensity_raw, "馬なり")

            comment_div = row.select_one("div.Comment_Cell")
            comment = ""
            if comment_div:
                ps = comment_div.select("p")
                comment = " ".join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))

            rec = TrainingRecord(
                date=date_str,
                venue="",
                course=course,
                splits=splits,
                intensity_label=intensity,
                comment=comment,
            )
            result.setdefault(horse_name, []).append(rec)

        return result

    def _parse_horses(self, race_id: str, soup: BeautifulSoup) -> List[Horse]:
        """shutuba.html 用（レガシー・フォールバック）"""
        horses = []
        rows = soup.select("table.ShutubaTable tr.HorseList")

        for row in rows:
            try:
                cells = row.select("td")
                if len(cells) < 10:
                    continue

                g0 = cells[0].get_text(strip=True)
                g1 = cells[1].get_text(strip=True)
                # 枠番・馬番は数字のみ（ヘッダ行・別テーブル行をスキップ）
                if not g0.isdigit() or not g1.isdigit():
                    continue
                gate_no = int(g0)
                horse_no = int(g1)

                # 馬名・horse_id
                horse_link = row.select_one("a[href*='/horse/']")
                horse_name = horse_link.get_text(strip=True) if horse_link else ""
                horse_id = ""
                if horse_link:
                    m = re.search(r"/horse/([A-Za-z]?\d+)", horse_link.get("href", ""))
                    if m:
                        horse_id = m.group(1)

                # 性齢（印列あり: cells[4]、なし: cells[3］）。例: 牡4, 牝3
                sex_age = ""
                sex_age_idx = -1
                for idx in (4, 3):
                    if idx < len(cells):
                        t = cells[idx].get_text(strip=True)
                        if re.match(r"^[牡牝セ][0-9]+$", t):
                            sex_age = t
                            sex_age_idx = idx
                            break
                if not sex_age:
                    continue
                sex = sex_age[0]
                age = int(sex_age[1:])

                # 斤量（性齢の次のセル）
                weight_idx = sex_age_idx + 1
                weight_text = (
                    cells[weight_idx].get_text(strip=True) if weight_idx < len(cells) else ""
                )
                weight_kg = float(weight_text) if weight_text else 55.0

                # 騎手
                jockey_link = row.select_one("a[href*='/jockey/']")
                jockey_name = jockey_link.get_text(strip=True) if jockey_link else ""
                jockey_id = ""
                if jockey_link:
                    m = re.search(r"/jockey/result/recent/(\w+)", jockey_link.get("href", ""))
                    if m:
                        jockey_id = m.group(1)

                # 調教師（所属プレフィックス含むセルテキストから affiliation も抽出）
                trainer_link = row.select_one("a[href*='/trainer/']")
                trainer_name = trainer_link.get_text(strip=True) if trainer_link else ""
                trainer_id = ""
                trainer_affiliation = ""
                if trainer_link:
                    m = re.search(r"/trainer/result/recent/(\w+)", trainer_link.get("href", ""))
                    if m:
                        trainer_id = m.group(1)
                    # 親要素のテキスト全体から所属を抽出
                    td_text = (
                        trainer_link.parent.get_text(strip=True) if trainer_link.parent else ""
                    )
                    trainer_affiliation = _parse_trainer_affiliation(td_text)

                # 馬体重
                weight_horse = None
                weight_change = None
                wh_cell = row.select_one("td.Weight")
                if wh_cell:
                    wh_text = wh_cell.get_text(strip=True)
                    wh_m = re.search(r"(\d{3,4})\(([+-]?\d+)\)", wh_text)
                    if wh_m:
                        weight_horse = int(wh_m.group(1))
                        weight_change = int(wh_m.group(2))

                # 出走日付・場・R番（日付はHTMLまたはrace_idから）
                race_date_str = self._parse_race_date_from_html(soup)
                vc = get_venue_code_from_race_id(race_id)
                if not race_date_str and vc not in JRA_CODES and len(race_id) >= 10:
                    mm, dd = race_id[6:8], race_id[8:10]
                    if mm.isdigit() and dd.isdigit():
                        race_date_str = f"{race_id[:4]}-{mm}-{dd}"
                if not race_date_str:
                    race_date_str = f"{race_id[:4]}-01-01"
                venue_code = vc
                race_no = int(race_id[10:12])

                horses.append(
                    Horse(
                        horse_id=horse_id,
                        horse_name=horse_name,
                        sex=sex,
                        age=age,
                        color="",
                        trainer=trainer_name,
                        trainer_id=trainer_id,
                        owner="",
                        breeder="",
                        sire="",
                        dam="",
                        past_runs=[],  # 後でHorseHistoryParserで補充
                        race_date=race_date_str,
                        venue=venue_code,
                        race_no=race_no,
                        gate_no=gate_no,
                        horse_no=horse_no,
                        jockey=jockey_name,
                        jockey_id=jockey_id,
                        weight_kg=weight_kg,
                        base_weight_kg=weight_kg,
                        horse_weight=weight_horse,
                        weight_change=weight_change,
                        trainer_affiliation=trainer_affiliation,
                    )
                )
            except Exception as e:
                logger.warning("horse parse error: race_id=%s → %s: %s", race_id, type(e).__name__, e, exc_info=True)
                continue

        return horses

    def _extract_grade(self, text: str) -> str:
        for g in ["G1", "G2", "G3", "(G1)", "(G2)", "(G3)"]:
            if g.replace("(", "").replace(")", "") in text.upper():
                return g.replace("(", "").replace(")", "")
        if "新馬" in text:
            return "新馬"
        if "未勝利" in text:
            return "未勝利"
        if "1勝" in text or "500万" in text:
            return "1勝"
        if "2勝" in text or "1000万" in text:
            return "2勝"
        if "3勝" in text or "1600万" in text:
            return "3勝"
        if "オープン" in text or "OP" in text:
            return "OP"
        # NAR（地方）のクラス名: A1, A2, B1〜B3, C1〜C3 など
        import re as _re
        nar_m = _re.search(r"\b([A-C][1-3])\b", text.upper())
        if nar_m:
            return nar_m.group(1)
        # 地方の条件名（重賞/特別/一般）
        if "重賞" in text:
            return "重賞"
        if "特別" in text:
            return "特別"
        # NAR世代限定戦 (2歳/3歳, "以上"を除く) → C3相当
        if _re.search(r"[23]歳(?!以上|上)", text):
            return "C3"
        return ""


# ============================================================
# 血統パーサー（父馬・母馬・母父馬）
# ============================================================


def clean_horse_name(name: str) -> str:
    """カタカナ+英語(国)のダブル表記からカタカナ部分のみ抽出
    例: 'マジェスティックウォリアーMajestic Warrior(米)' → 'マジェスティックウォリアー'
    """
    if not name:
        return name
    # カタカナ部分 + 英字が続くパターン
    m = re.match(r'^([ァ-ヴー・ッ\u30FC]+)[A-Z]', name)
    if m:
        return m.group(1)
    return name


class PedigreeParser:
    """
    血統ページ（horse/ped/{id}/）から父馬・母馬・母父馬を取得
    5代血統表: 1行目1列=父、母行1列=母、同行2列=母父
    """

    def __init__(self, client: "NetkeibaClient"):
        self.client = client

    def parse(
        self, horse_id: str, horse: Optional[Horse] = None
    ) -> Tuple[str, str, str, str, str, str]:
        """
        血統ページをパース
        Returns: (sire_id, sire_name, dam_id, dam_name, maternal_grandsire_id, maternal_grandsire_name)
        """
        url = f"{BASE_URL}/horse/ped/{horse_id}/"
        soup = self.client.get(url)
        if not soup:
            return ("", "", "", "", "", "")

        sire_id, sire_name = "", ""
        dam_id, dam_name = "", ""
        mgs_id, mgs_name = "", ""

        # 5代血統表: table を探す（db_heredity or 最初の大きいtable）
        table = (
            soup.select_one("table.db_heredity")
            or soup.select_one("table.pedigree_table")
            or soup.select_one("div#db-main-column table")
            or soup.select_one("table")
        )
        if not table:
            return ("", "", "", "", "", "")

        # 5代血統表: rowspanで世代を判定。父=最初のrowspan最大[SIRE]、
        # 母=同rowspanの[MARE]（父方の小rowspan mareを誤検出しないよう注意）
        tds = table.select("td")

        # ヘルパー: tdからhorse IDと名前を抽出（ダブル表記をクリーニング）
        def _extract_horse_link(td_elem):
            for a in td_elem.select("a[href*='/horse/']"):
                h = a.get("href", "")
                if any(x in h for x in ("/horse/ped/", "/horse/sire/", "/horse/mare/")):
                    continue
                lm = re.search(r"/horse/([0-9a-zA-Z]+)/?", h)
                if lm and a.get_text(strip=True):
                    return lm.group(1), clean_horse_name(a.get_text(strip=True))
            return None, None

        # 父: 最初の[SIRE]リンクを持つtd
        for td in tds:
            sire_link = td.select_one("a[href*='/horse/sire/']")
            if not sire_link:
                continue
            lid, lname = _extract_horse_link(td)
            if lid:
                sire_id, sire_name = lid, lname
                break

        # 父のrowspanを基準値として取得（5代=16, 4代=8 等）
        father_rowspan = int(tds[0].get("rowspan", "1")) if tds else 1

        # 母: 父と同じrowspanを持つ[MARE]リンクtd（＝第1世代の母）
        for i, td in enumerate(tds):
            rs = int(td.get("rowspan", "1"))
            if rs != father_rowspan:
                continue
            mare_link = td.select_one("a[href*='/horse/mare/']")
            if not mare_link:
                continue
            lid, lname = _extract_horse_link(td)
            if lid:
                dam_id, dam_name = lid, lname
                # 次のセル = 母父(BMS)
                if i + 1 < len(tds):
                    bms_lid, bms_lname = _extract_horse_link(tds[i + 1])
                    if bms_lid:
                        mgs_id, mgs_name = bms_lid, bms_lname
                break

        # 父が取れない場合、1行目1列目を父とする
        if not sire_id and tds:
            for a in tds[0].select("a[href*='/horse/']"):
                h = a.get("href", "")
                if "/horse/ped/" in h or "/horse/sire/" in h or "/horse/mare/" in h:
                    continue
                fm = re.search(r"/horse/([0-9a-zA-Z]+)/?", h)
                if fm:
                    sire_id, sire_name = fm.group(1), a.get_text(strip=True)
                    break

        if horse:
            if sire_id or sire_name:
                horse.sire = sire_name
                horse.sire_id = sire_id
            if dam_id or dam_name:
                horse.dam = dam_name
                horse.dam_id = dam_id
            if mgs_id or mgs_name:
                horse.maternal_grandsire_id = mgs_id
                if mgs_name:
                    horse.maternal_grandsire = mgs_name

        return (sire_id, sire_name, dam_id, dam_name, mgs_id, mgs_name)


# ============================================================
# 馬の過去走スクレイパー
# ============================================================


class HorseHistoryParser:
    """
    馬の過去走データを取得する（db.netkeiba.com/horse/{id}/）
    同一ページから性齢・調教師を抽出して Horse を補完する。
    """

    def __init__(self, client: NetkeibaClient):
        self.client = client

    def parse(self, horse_id: str, horse: Optional[Horse] = None) -> List[PastRun]:
        # 過去走: horse/result/{id}/ に戦績テーブル。horse/{id}/ はAjax読み込みのため不可
        result_url = f"{BASE_URL}/horse/result/{horse_id}/"
        result_soup = self.client.get(result_url)
        if not result_soup:
            return []

        if horse:
            self._enrich_horse_from_page(horse, result_soup)
            top_soup = self.client.get(f"{BASE_URL}/horse/{horse_id}/")
            if top_soup:
                self._enrich_horse_profile_from_top(horse, top_soup)
            # 血統（父馬・母馬・母父馬）を取得
            ped_parser = PedigreeParser(self.client)
            ped_parser.parse(horse_id, horse)

        runs = []
        # 戦績テーブル: db_h_race_results (horse/result/)
        for row in result_soup.select("table.db_h_race_results tr")[1:]:
            try:
                run = self._parse_row(row)
                if run:
                    runs.append(run)
            except Exception:
                logger.debug("past run row parse failed", exc_info=True)
                continue

        return runs

    def _extract_venue_from_kaisai(self, kaisai: str) -> str:
        """開催文字列「5阪神8」「1京都3」などから場名を抽出"""
        for v in (
            # 中央10場
            "東京",
            "中山",
            "阪神",
            "京都",
            "中京",
            "小倉",
            "福島",
            "新潟",
            "札幌",
            "函館",
            # 地方14場
            "大井",
            "川崎",
            "船橋",
            "浦和",
            "名古屋",
            "金沢",
            "笠松",
            "園田",
            "姫路",
            "門別",
            "盛岡",
            "水沢",
            "高知",
            "佐賀",
            "帯広",
        ):
            if v in kaisai:
                return v
        return "東京"

    def _enrich_horse_from_page(self, horse: Horse, soup: BeautifulSoup) -> None:
        """戦績ページから性を抽出（p.txt_01: 抹消　牡　栗毛）"""
        try:
            txt01 = soup.select_one("p.txt_01")
            if txt01:
                text = txt01.get_text()
                for s in ("牡", "牝", "セ"):
                    if s in text:
                        horse.sex = s
                        break
        except Exception:
            logger.debug("enrich horse sex failed", exc_info=True)

    def _enrich_horse_profile_from_top(self, horse: Horse, soup: BeautifulSoup) -> None:
        """馬TOPページから調教師・年齢を抽出（db_prof_table）"""
        try:
            for th in soup.select("table.db_prof_table th"):
                if "調教師" in th.get_text():
                    tr = th.find_parent("tr")
                    if tr:
                        td = tr.select_one("td")
                        if td:
                            a = td.select_one("a[href*='/trainer/']")
                            if a:
                                horse.trainer = a.get_text(strip=True)
                                m = re.search(r"/trainer/([^/]+)/", a.get("href", ""))
                                if m:
                                    horse.trainer_id = m.group(1)
                    break
            for th in soup.select("table.db_prof_table th"):
                if "生年月日" in th.get_text():
                    tr = th.find_parent("tr")
                    if tr:
                        td = tr.select_one("td")
                        if td:
                            bm = re.search(r"(\d{4})年(\d{1,2})月", td.get_text())
                            if bm and horse.race_date:
                                by, bm_m = int(bm.group(1)), int(bm.group(2))
                                rm = re.search(r"(\d{4})-(\d{2})", horse.race_date)
                                if rm:
                                    ry, rm_m = int(rm.group(1)), int(rm.group(2))
                                    age = ry - by
                                    if rm_m < bm_m:  # 誕生日前
                                        age -= 1
                                    horse.age = max(2, min(age, 10))
                    break
            txt01 = soup.select_one("p.txt_01")
            if txt01:
                m = re.search(r"([牡牝セ])(\d)", txt01.get_text())
                if m:
                    horse.age = int(m.group(2))
        except Exception:
            logger.debug("enrich horse profile failed", exc_info=True)

    def _parse_row(self, row) -> Optional[PastRun]:
        """horse/result/{id}/ の戦績テーブル行をパース"""
        cells = row.select("td")
        if len(cells) < 18:
            return None

        # 日付 (0)
        date_text = cells[0].get_text(strip=True)
        m = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", date_text)
        if not m:
            return None
        race_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # race_id: cells[4]のレース名リンクから抽出
        race_id = ""
        if len(cells) > 4:
            race_link = cells[4].select_one("a[href*='/race/']")
            if race_link:
                rid_m = re.search(r"/race/(\d{12})/", race_link.get("href", ""))
                if rid_m:
                    race_id = rid_m.group(1)

        # 開催 (1) 例 "5阪神8" → 阪神
        kaisai = cells[1].get_text(strip=True)
        venue = self._extract_venue_from_kaisai(kaisai)

        # ばんえい判定（開催に「帯広」含む）
        _is_banei = "帯広" in kaisai

        # 距離 (14) 例 "ダ1400" or "芝2000"、ばんえい: "200"
        course_text = cells[14].get_text(strip=True) if len(cells) > 14 else ""
        if _is_banei:
            # ばんえいは surface 文字なし、常にダート扱い
            surface = "ダート"
            dist_m_match = re.search(r"(\d+)", course_text)
            distance = int(dist_m_match.group(1)) if dist_m_match else 200
        else:
            surface_key = course_text[0] if course_text else "芝"
            surface = SURFACE_MAP.get(surface_key, "芝")
            dist_m_match = re.search(r"(\d{3,4})", course_text)
            distance = int(dist_m_match.group(1)) if dist_m_match else 2000

        # venue_master.py の VENUE_NAME_TO_CODE と完全一致させる
        venue_code_map = {
            # 中央10場
            "札幌": "03",
            "函館": "04",
            "福島": "01",
            "新潟": "02",
            "東京": "05",
            "中山": "06",
            "中京": "07",
            "京都": "08",
            "阪神": "09",
            "小倉": "10",
            # 地方14場（venue_master.py の正式コード）
            "帯広": "65",
            "門別": "30",
            "盛岡": "35",
            "水沢": "36",
            "浦和": "42",
            "船橋": "43",
            "大井": "44",
            "川崎": "45",
            "金沢": "46",
            "笠松": "47",
            "名古屋": "48",
            "園田": "50",
            "姫路": "51",
            "高知": "54",
            "佐賀": "55",
        }
        vc = venue_code_map.get(venue, "05")
        course_id = f"{vc}_{surface}_{distance}"

        # 馬場 (16)。ばんえい: cells[15]に水分量（数値）→ 馬場状態を推定
        if _is_banei:
            water_text = cells[15].get_text(strip=True) if len(cells) > 15 else ""
            try:
                wc = float(water_text)
                if wc <= 1.5:
                    condition = "良"
                elif wc <= 2.5:
                    condition = "稍重"
                elif wc <= 3.5:
                    condition = "重"
                else:
                    condition = "不良"
            except (ValueError, TypeError):
                condition = ""
        else:
            condition = cells[16].get_text(strip=True) if len(cells) > 16 else ""
        class_text = cells[4].get_text(strip=True) if len(cells) > 4 else ""
        grade = self._infer_grade(class_text)

        # 頭数(6) 枠(7) 馬番(8)
        field_count_text = cells[6].get_text(strip=True) if len(cells) > 6 else ""
        field_count = int(field_count_text) if field_count_text.isdigit() else 10
        gate_no = (
            int(cells[7].get_text(strip=True))
            if len(cells) > 7 and cells[7].get_text(strip=True).isdigit()
            else 1
        )
        horse_no = (
            int(cells[8].get_text(strip=True))
            if len(cells) > 8 and cells[8].get_text(strip=True).isdigit()
            else 1
        )

        # オッズ(9) 人気(10)
        odds_text = cells[9].get_text(strip=True) if len(cells) > 9 else ""
        try:
            tansho_odds = float(odds_text) if odds_text and re.match(r"[\d.]+", odds_text) else None
        except ValueError:
            tansho_odds = None
        pop_text = cells[10].get_text(strip=True) if len(cells) > 10 else ""
        popularity_at_race = int(pop_text) if pop_text.isdigit() else None

        # 着順(11) 騎手(12) 斤量(13)
        finish_text = cells[11].get_text(strip=True) if len(cells) > 11 else ""
        finish_pos = int(finish_text) if finish_text.isdigit() else 99
        jockey_cell = cells[12] if len(cells) > 12 else None
        jockey = jockey_cell.get_text(strip=True) if jockey_cell else ""
        jockey_id = ""
        if jockey_cell:
            a = jockey_cell.select_one("a[href*='/jockey/']")
            if a:
                jm = re.search(r"/jockey/result/recent/([^/]+)/", a.get("href", ""))
                if not jm:
                    jm = re.search(r"/jockey/([^/]+)/", a.get("href", ""))
                if jm:
                    jockey_id = jm.group(1)
        weight_text = cells[13].get_text(strip=True) if len(cells) > 13 else ""
        weight_kg = float(weight_text) if re.match(r"\d+\.?\d*", weight_text) else 55.0

        # 馬体重: 通常[24]、ばんえいはカラムずれで[27]付近
        horse_weight, weight_change = None, None
        _hw_candidates = [24] if not _is_banei else range(24, min(len(cells), 30))
        for _hw_idx in _hw_candidates:
            if _hw_idx >= len(cells):
                break
            wt_text = cells[_hw_idx].get_text(strip=True)
            wh_m = re.match(r"(\d+)\s*\(([+-]?\d+)\)\s*$", wt_text)
            if wh_m:
                horse_weight = int(wh_m.group(1))
                weight_change = int(wh_m.group(2))
                break
            elif re.match(r"^\d{3,4}$", wt_text):
                horse_weight = int(wt_text)
                break

        # タイム(18) 着差(19) 通過(21) ペース(22) 上り(23)
        time_text = cells[18].get_text(strip=True) if len(cells) > 18 else ""
        finish_time = self._parse_time(time_text)
        margin_text = cells[19].get_text(strip=True) if len(cells) > 19 else ""
        margin_ahead = self._parse_margin(margin_text)
        if margin_ahead is None:
            margin_ahead = 0.0
        # 取消・除外馬（finish_pos=99）は通過データなし
        if finish_pos == 99:
            pos_text = ""
        else:
            pos_text = cells[21].get_text(strip=True) if len(cells) > 21 else ""
        corners = self._parse_corners(pos_text)
        pos_4c = corners[-1] if corners else self._parse_4c(pos_text)
        pace_text = cells[22].get_text(strip=True) if len(cells) > 22 else ""
        first_3f = self._parse_first3f(pace_text)
        last3f_text = cells[23].get_text(strip=True) if len(cells) > 23 else ""
        last3f = float(last3f_text) if re.match(r"\d+\.?\d*", last3f_text) else (0.0 if _is_banei else 35.5)
        # ペース: 戦績テーブルにはH/M/Sが無いため first_3f から推定
        pace = None
        if first_3f is not None:
            try:
                from src.utils.pace_inference import infer_pace_from_first3f

                pace = infer_pace_from_first3f(distance, surface, first_3f)
            except Exception:
                logger.debug("pace inference failed", exc_info=True)

        return PastRun(
            race_date=race_date,
            venue=venue,
            course_id=course_id,
            distance=distance,
            surface=surface,
            condition=condition,
            class_name=class_text,
            grade=grade,
            field_count=field_count,
            gate_no=gate_no,
            horse_no=horse_no,
            jockey=jockey,
            weight_kg=weight_kg,
            position_4c=pos_4c,
            positions_corners=corners,
            first_3f_sec=first_3f,
            finish_pos=finish_pos,
            finish_time_sec=finish_time,
            last_3f_sec=last3f,
            margin_behind=0.0,  # 後着差は別途算出
            margin_ahead=margin_ahead,
            jockey_id=jockey_id,
            horse_weight=horse_weight,
            weight_change=weight_change,
            pace=pace,
            tansho_odds=tansho_odds,
            popularity_at_race=popularity_at_race,
            race_id=race_id,
        )

    def _parse_time(self, text: str) -> float:
        """例: "1:59.8" → 119.8。パース不可時は0.0（非完走・取消等）"""
        m = re.match(r"(\d+):(\d+)\.(\d+)", text)
        if m:
            return int(m.group(1)) * 60 + int(m.group(2)) + int(m.group(3)) / 10
        m2 = re.match(r"(\d+)\.(\d+)", text)
        if m2:
            return int(m2.group(1)) + int(m2.group(2)) / 10
        return 0.0

    def _parse_margin(self, text: str) -> Optional[float]:
        """着差テキストを秒換算。
        horse/result ページでは秒単位の数値 ("0.4", "-0.1", "1.2") が来る。
        race result ページでは日本語表現 ("クビ", "ハナ") が来る。
        パース不能時は None を返す。
        """
        if not text or text.strip() in ("", "同着", "---", "------"):
            return None
        text = text.strip()
        # 秒単位の小数（horse/result ページ形式）: "0.4", "-0.1", "1.2" 等
        m_sec = re.match(r"^(-?\d+\.\d+)$", text)
        if m_sec:
            return float(m_sec.group(1))
        # 複合表現: "1.3/4" "2.1/2" "1.1/4" 等 → 数字+分数
        m_compound = re.match(r"(\d+)\.(\d)/(\d)", text)
        if m_compound:
            whole = int(m_compound.group(1))
            numer = int(m_compound.group(2))
            denom = int(m_compound.group(3))
            return (whole + numer / denom) * 0.2
        margin_map = {
            "ハナ": 0.05,
            "クビ": 0.1,
            "アタマ": 0.15,
            "1/2": 0.1,
            "3/4": 0.15,
            "大": 2.0,      # "大差" = 10馬身以上
        }
        for key, val in margin_map.items():
            if key in text:
                return val
        # 純粋な整数: "1" "2" "3" "4" "5" "10" 等（馬身数→秒換算）
        m = re.match(r"^(\d+)$", text)
        if m:
            return float(m.group(1)) * 0.2
        return None

    def _parse_4c(self, text: str) -> int:
        """例: "05-05-04-03" → 3"""
        parts = text.split("-")
        if parts:
            last = parts[-1].strip()
            if last.isdigit():
                return int(last)
        return 5

    def _parse_corners(self, text: str) -> List[int]:
        """例: "05-05-04-03" → [5,5,4,3], "8-6" → [8,6]。全コーナー通過順位を返す"""
        result = []
        for part in text.split("-"):
            p = part.strip()
            if p.isdigit():
                result.append(int(p))
        return result

    def _parse_first3f(self, text: str) -> Optional[float]:
        """ペース列 "35.7-36.0" → 前半3F 35.7。前半-後半の形式"""
        if not text or "-" not in text:
            return None
        parts = text.split("-")
        for p in parts[:1]:
            m = re.match(r"(\d+)\.?(\d*)", p.strip())
            if m:
                try:
                    return float(m.group(1) + "." + (m.group(2) or "0"))
                except ValueError:
                    pass
        return None

    def _infer_grade(self, text: str) -> str:
        if "G1" in text.upper():
            return "G1"
        if "G2" in text.upper():
            return "G2"
        if "G3" in text.upper():
            return "G3"
        if "新馬" in text:
            return "新馬"
        if "未勝利" in text:
            return "未勝利"
        if "1勝" in text or "500万" in text:
            return "1勝"
        if "2勝" in text or "1000万" in text:
            return "2勝"
        if "3勝" in text or "1600万" in text:
            return "3勝"
        return "OP"


# ============================================================
# オッズスクレイパー
# ============================================================


class OddsScraper:
    """
    単勝オッズ・人気を取得する
    URL: https://race.netkeiba.com/odds/index.html?race_id=XXXX
    """

    def __init__(self, client: NetkeibaClient):
        self.client = client

    def get_tansho(self, race_id: str) -> Dict[int, Tuple[float, int]]:
        """
        Returns: {馬番: (オッズ, 人気)}
        優先順: 1) AJAX API (認証済みセッション) → 2) result page → 3) HTML odds page
        """
        vc = get_venue_code_from_race_id(race_id)

        # 1) AJAX API (JRA専用 - リアルタイムオッズ)
        if vc in JRA_CODES:
            result = self._get_tansho_from_api(race_id)
            if result:
                return result

        # 2) 結果ページから確定オッズ取得（HTML parse — 完了レース用）
        from_result = self._get_tansho_from_result_page(race_id)
        if from_result:
            return from_result

        # 3) HTML odds page (NAR対応 + 最終フォールバック)
        result = self._get_tansho_from_odds_page(race_id)
        if result and any(0.1 < v[0] < 9999 for v in result.values()):
            return result

        return {}

    def _get_tansho_from_api(self, race_id: str) -> Dict[int, Tuple[float, int]]:
        """JRA AJAX API から単勝オッズを取得（認証済みセッション必要）"""
        try:
            session = getattr(self.client, "session", None)
            if not session:
                return {}
            api_url = f"{RACE_URL}/api/api_get_jra_odds.html"
            resp = session.get(
                api_url,
                params={"race_id": race_id, "type": "1"},
                headers={
                    **HEADERS,
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"{RACE_URL}/odds/index.html?race_id={race_id}&type=b1",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                return {}
            data = resp.json()
            odds_data = data.get("data", {})
            if not isinstance(odds_data, dict):
                return {}
            tansho = odds_data.get("odds", {})
            if not isinstance(tansho, dict) or "1" not in tansho:
                return {}
            result = {}
            for horse_no_str, vals in tansho["1"].items():
                try:
                    no = int(horse_no_str)
                    odds_val = float(vals[0]) if vals and vals[0] else 0.0
                    pop = int(vals[2]) if len(vals) > 2 and vals[2] else 0
                    if 0.1 < odds_val < 9999:
                        result[no] = (odds_val, pop)
                except (ValueError, TypeError, IndexError):
                    continue
            return result
        except Exception:
            return {}

    def _get_tansho_from_odds_page(self, race_id: str) -> Dict[int, Tuple[float, int]]:
        """odds/index.html から単勝オッズを取得"""
        base = NAR_URL if get_venue_code_from_race_id(race_id) not in JRA_CODES else RACE_URL
        url = f"{base}/odds/index.html"
        soup = self.client.get(url, params={"race_id": race_id, "type": "b1"}, use_cache=False)
        if not soup:
            return {}

        result = {}
        # JRA: tr.HorseList or tr[id^='odds-']
        rows = soup.select("tr.HorseList, tr[id^='odds-']")
        if not rows:
            # JRA fallback with tbody
            rows = soup.select("table.RaceOdds_HorseList_Table tbody tr")
        if not rows:
            # NAR: no tbody tag, first table is tansho
            tables = soup.select("table.RaceOdds_HorseList_Table")
            if tables:
                rows = [r for r in tables[0].select("tr") if r.select("td")]
        for row in rows:
            try:
                no_el = row.select_one(".Num, td.W31, td:nth-child(2)")
                odds_el = row.select_one("td.Odds span, td.Odds")
                pop_el = row.select_one(".Popular")
                if not no_el:
                    continue
                no = int(no_el.get_text(strip=True))
                if not odds_el:
                    continue
                odds_text = odds_el.get_text(strip=True)
                # NAR odds may contain range like "2.2 - 3.5" for fukusho; skip
                if " - " in odds_text:
                    continue
                if odds_text in ("---.-", "-", ""):
                    continue
                odds = float(odds_text)
                pop = (
                    int(pop_el.get_text(strip=True))
                    if pop_el and pop_el.get_text(strip=True).isdigit()
                    else 0
                )
                result[no] = (odds, pop)
            except (ValueError, TypeError):
                continue
        # If popularity not set (NAR), calculate from odds ranking
        if result and all(v[1] == 0 for v in result.values()):
            sorted_by_odds = sorted(result.items(), key=lambda x: x[1][0])
            for rank, (no, (odds, _)) in enumerate(sorted_by_odds, 1):
                result[no] = (odds, rank)
        return result

    def _get_tansho_from_result_page(self, race_id: str) -> Dict[int, Tuple[float, int]]:
        """result.html から確定オッズ・人気を取得（過去レース用）"""
        base = NAR_URL if get_venue_code_from_race_id(race_id) not in JRA_CODES else RACE_URL
        url = f"{base}/race/result.html"
        soup = self.client.get(url, params={"race_id": race_id})
        if not soup:
            return {}

        result = {}
        table = soup.select_one(".ResultTableWrap table")
        if not table:
            return {}
        rows = table.select("tbody tr")
        header = table.select_one("thead tr")
        odds_col, pop_col = None, None
        if header:
            for i, th in enumerate(header.select("th")):
                t = th.get_text(strip=True)
                if "単勝" in t and "オッズ" in t:
                    odds_col = i
                elif t == "人気":
                    pop_col = i
        if odds_col is None or pop_col is None:
            odds_col, pop_col = 9, 8
        for row in rows:
            try:
                cells = row.select("td")
                if len(cells) <= max(odds_col, pop_col):
                    continue
                no_el = cells[2] if len(cells) > 2 else None
                if not no_el:
                    continue
                no = int(no_el.get_text(strip=True))
                odds_text = cells[odds_col].get_text(strip=True)
                pop_text = cells[pop_col].get_text(strip=True)
                odds = (
                    float(odds_text)
                    if odds_text.replace(".", "").replace("-", "").isdigit()
                    else None
                )
                pop = int(pop_text) if pop_text.isdigit() else 99
                if odds and 0.1 < odds < 9999:
                    result[no] = (odds, pop)
            except (ValueError, TypeError, IndexError):
                continue
        return result

    def get_sanrenpuku_odds(self, race_id: str) -> Dict[Tuple[int, ...], float]:
        """三連複オッズを取得。
        Returns: {(馬番1, 馬番2, 馬番3): オッズ値, ...}  キーは昇順ソート済み
        """
        vc = get_venue_code_from_race_id(race_id)

        # 1) JRA API (リアルタイム)
        if vc in JRA_CODES:
            result = self._get_sanrenpuku_from_api(race_id)
            if result:
                return result

        # 2) HTML odds page (JRA + NAR)
        result = self._get_sanrenpuku_from_html(race_id)
        return result

    def _get_sanrenpuku_from_api(self, race_id: str) -> Dict[Tuple[int, ...], float]:
        """JRA AJAX API から三連複オッズを取得"""
        try:
            session = getattr(self.client, "session", None)
            if not session:
                return {}
            api_url = f"{RACE_URL}/api/api_get_jra_odds.html"
            resp = session.get(
                api_url,
                params={"race_id": race_id, "type": "7"},
                headers={
                    **HEADERS,
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"{RACE_URL}/odds/index.html?race_id={race_id}&type=b7",
                },
                timeout=20,
            )
            if resp.status_code != 200:
                return {}
            data = resp.json()
            odds_data = data.get("data", {})
            if not isinstance(odds_data, dict):
                return {}
            # API応答: {"odds": {"7": {"0102": [["15.3"]], ...}}} or similar
            trio = odds_data.get("odds", {})
            # type=7 のキーを探す
            trio_dict = trio.get("7", trio.get("b7", {}))
            if not isinstance(trio_dict, dict):
                return {}
            result: Dict[Tuple[int, ...], float] = {}
            for combo_key, vals in trio_dict.items():
                try:
                    # コンボキー: "010203" (2桁ずつ) or "1-2-3"
                    nums = []
                    if "-" in combo_key:
                        nums = [int(x) for x in combo_key.split("-")]
                    else:
                        # 2桁ずつ分割
                        nums = [int(combo_key[i:i+2]) for i in range(0, len(combo_key), 2)]
                    if len(nums) != 3:
                        continue
                    # オッズ値取得
                    if isinstance(vals, list):
                        # [[odds_str]] or [odds_str]
                        v = vals[0] if vals else "0"
                        if isinstance(v, list):
                            v = v[0] if v else "0"
                        odds_val = float(v)
                    elif isinstance(vals, (int, float)):
                        odds_val = float(vals)
                    else:
                        odds_val = float(str(vals))
                    if 1.0 < odds_val < 999999:
                        result[tuple(sorted(nums))] = odds_val
                except (ValueError, TypeError, IndexError):
                    continue
            return result
        except Exception:
            return {}

    def _get_sanrenpuku_from_html(self, race_id: str) -> Dict[Tuple[int, ...], float]:
        """HTML odds page から三連複オッズを取得（JRA/NAR共通）"""
        base = NAR_URL if get_venue_code_from_race_id(race_id) not in JRA_CODES else RACE_URL
        url = f"{base}/odds/index.html"
        soup = self.client.get(url, params={"race_id": race_id, "type": "b7"}, use_cache=False)
        if not soup:
            return {}

        result: Dict[Tuple[int, ...], float] = {}

        # パターン1: 組み合わせテーブル（各行にコンボ+オッズ）
        for tr in soup.select("tr"):
            cells = tr.select("td")
            if len(cells) < 2:
                continue
            # 組み合わせセル: "1 - 3 - 5" or "01-03-05" 等のパターン
            combo_text = cells[0].get_text(strip=True)
            # 数字を3つ抽出
            nums = re.findall(r'\d+', combo_text)
            if len(nums) != 3:
                # 2番目のセルが組み合わせの場合もある
                combo_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                nums = re.findall(r'\d+', combo_text)
            if len(nums) != 3:
                continue
            horse_nos = tuple(sorted(int(x) for x in nums))
            if max(horse_nos) > 18 or min(horse_nos) < 1:
                continue

            # オッズセル: 最後のセルまたは特定クラスのセル
            odds_text = ""
            for cell in reversed(cells):
                text = cell.get_text(strip=True).replace(",", "")
                if re.match(r'[\d.]+$', text):
                    odds_text = text
                    break
            if not odds_text:
                continue
            try:
                odds_val = float(odds_text)
                if 1.0 < odds_val < 999999:
                    result[horse_nos] = odds_val
            except ValueError:
                continue

        return result

    def get_weights(self, race_id: str) -> Dict[int, Tuple[Optional[int], Optional[int]]]:
        """shutuba.html から馬体重を取得。
        Returns: {馬番: (horse_weight, weight_change)}  拾えなければ空dict
        """
        base = NAR_URL if get_venue_code_from_race_id(race_id) not in JRA_CODES else RACE_URL
        url = f"{base}/race/shutuba.html"
        soup = self.client.get(url, params={"race_id": race_id}, use_cache=False)
        if not soup:
            return {}
        result: Dict[int, Tuple[Optional[int], Optional[int]]] = {}
        for row in soup.select("tr.HorseList"):
            try:
                no_el = row.select_one("td.Umaban, td:nth-child(2)")
                if not no_el:
                    continue
                no_text = no_el.get_text(strip=True)
                if not no_text.isdigit():
                    continue
                no = int(no_text)
                wh_cell = row.select_one("td.Weight")
                if not wh_cell:
                    continue
                wh_text = wh_cell.get_text(strip=True)
                m = re.search(r"(\d{3,4})\(([+-]?\d+)\)", wh_text)
                if m:
                    result[no] = (int(m.group(1)), int(m.group(2)))
            except (ValueError, TypeError):
                continue
        return result


# ============================================================
# メインスクレイパーファサード
# ============================================================


class NetkeibaScraper:
    """
    全スクレイパーを統合するファサードクラス
    """

    def __init__(
        self,
        all_courses: Dict[str, CourseMaster],
        cache_dir: str = None,
    ):
        if not HAS_DEPS:
            raise ImportError("pip install requests beautifulsoup4 lxml")
        if cache_dir is None:
            cache_dir = _DEFAULT_CACHE_DIR
        self.client = NetkeibaClient(cache_dir)
        self.races = RaceListScraper(self.client)
        self.entry = RaceEntryParser(self.client, all_courses)
        self.history = HorseHistoryParser(self.client)
        self.odds = OddsScraper(self.client)

    def fetch_race(
        self,
        race_id: str,
        fetch_history: bool = True,
        fetch_odds: bool = True,
        use_cache: bool = True,
    ) -> Tuple[Optional[RaceInfo], List[Horse]]:
        """
        1レース分のデータを完全取得する

        Args:
            race_id: netkeibaのrace_id (12桁)
            fetch_history: 過去走データを取得するか
            fetch_odds: オッズを取得するか
            use_cache: キャッシュを使用するか（再生成時の高速化）
        """
        # ── キャッシュ読み込み ──
        if use_cache and fetch_history:
            try:
                from src.scraper.race_cache import load_race_cache
                cached = load_race_cache(race_id)
                if cached:
                    ri, hs = cached
                    # オッズは変動するので、キャッシュ後に公式から再取得
                    if fetch_odds:
                        odds_data = self.odds.get_tansho(race_id)
                        for horse in hs:
                            if horse.horse_no in odds_data:
                                horse.odds, horse.popularity = odds_data[horse.horse_no]
                    logger.info("キャッシュ復元: %s %d頭", ri.race_name, len(hs))
                    return ri, hs
            except Exception:
                logger.debug("キャッシュ読み込みスキップ", exc_info=True)

        logger.info("出走表取得: %s", race_id)
        race_info, horses, _ = self.entry.parse(race_id)

        if not race_info:
            logger.warning("レース情報取得失敗: %s", race_id)
            return None, []

        if fetch_history:
            for i, horse in enumerate(horses):
                logger.debug("馬過去走 %d/%d: %s", i + 1, len(horses), horse.horse_name)
                past_runs = self.history.parse(horse.horse_id, horse=horse)
                horse.past_runs = past_runs
                if past_runs:
                    horse.prev_jockey = past_runs[0].jockey

        if fetch_odds:
            logger.debug("オッズ取得: %s", race_id)
            odds_data = self.odds.get_tansho(race_id)
            for horse in horses:
                if horse.horse_no in odds_data:
                    horse.odds, horse.popularity = odds_data[horse.horse_no]

        race_info.field_count = len(horses)

        # ── キャッシュ保存（過去走を取得した場合のみ） ──
        if fetch_history and use_cache:
            try:
                from src.scraper.race_cache import save_race_cache
                save_race_cache(race_id, race_info, horses)
            except Exception:
                logger.debug("キャッシュ保存スキップ", exc_info=True)

        logger.info("完了: %s %d頭", race_info.race_name, len(horses))
        return race_info, horses

    def fetch_date(self, date: str) -> List[str]:
        """指定日のレースID一覧を返す"""
        return self.races.get_race_ids(date)


# ============================================================
# キャッシュユーティリティ（文字化けしたrace系キャッシュの削除）
# ============================================================


def purge_old_cache(cache_dir: str = None, max_age_days: int = 30) -> dict:
    """指定日数より古いキャッシュファイルを削除する。
    Returns: {"removed": 件数, "freed_mb": 解放容量MB}
    """
    if cache_dir is None:
        cache_dir = _DEFAULT_CACHE_DIR
    cache_dir = os.path.abspath(cache_dir)
    if not os.path.isdir(cache_dir):
        return {"removed": 0, "freed_mb": 0.0}

    cutoff = time.time() - max_age_days * 86400
    removed = 0
    freed = 0
    for name in os.listdir(cache_dir):
        fp = os.path.join(cache_dir, name)
        if not os.path.isfile(fp):
            continue
        try:
            if os.path.getmtime(fp) < cutoff:
                sz = os.path.getsize(fp)
                os.remove(fp)
                removed += 1
                freed += sz
        except OSError:
            pass
    return {"removed": removed, "freed_mb": round(freed / 1024 / 1024, 1)}


def clear_race_cache(cache_dir: str = None) -> int:
    """
    race/nar 系で encoding 指定なしで取得したキャッシュを削除。
    文字化けしたキャッシュの修復用。再取得時に正しい UTF-8 で保存される。
    Returns: 削除したファイル数
    """
    if cache_dir is None:
        cache_dir = _DEFAULT_CACHE_DIR
    cache_dir = os.path.abspath(cache_dir)
    removed = 0
    for name in os.listdir(cache_dir):
        if not name.endswith(".html"):
            continue
        # encoding=UTF-8 が含まれる = 正しく取得済み → スキップ
        if "encoding=UTF-8" in name:
            continue
        # race/nar 系のみ削除対象
        if name.startswith("race.netkeiba.com_") or name.startswith("nar.netkeiba.com_"):
            path = os.path.join(cache_dir, name)
            try:
                os.remove(path)
                removed += 1
            except OSError:
                pass
    return removed


# ============================================================
# 動作確認用 (ネットワーク無効時はスキップ)
# ============================================================

if __name__ == "__main__":
    import sys

    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if _root not in sys.path:
        sys.path.insert(0, _root)

    if len(sys.argv) > 1 and sys.argv[1] == "--clear-race-cache":
        n = clear_race_cache()
        logger.info(f"[cache] race/nar 系キャッシュ {n} 件削除（再取得時に正しいencodingで保存）")
        sys.exit(0)

    from data.masters.course_master import get_all_courses

    if not HAS_DEPS:
        logger.warning("依存ライブラリをインストールしてください: pip install requests beautifulsoup4 lxml")
    else:
        scraper = NetkeibaScraper(get_all_courses())
        # 今日のレース一覧
        today = datetime.now().strftime("%Y-%m-%d")
        ids = scraper.fetch_date(today)
        logger.info(f"本日のレース数: {len(ids)}")
        if ids:
            logger.info(f"先頭5件: {ids[:5]}")
