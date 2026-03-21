"""
競馬解析マスターシステム v3.0 - 競馬ブックスマートプレミアム 調教スクレイパー
中央競馬 + 地方競馬 両対応（南関東・門別・園田 等 NAR 全対応）

URL形式:
  ●調教（race_idに /0/ [JRA] / /1/ [NAR] プレフィックスが必要）
    中央: https://s.keibabook.co.jp/cyuou/cyokyo/0/{KB_JRA_12桁ID}
    地方: https://s.keibabook.co.jp/chihou/cyokyo/1/{KB_NAR_16桁ID}
  ●厩舎コメント
    中央: https://s.keibabook.co.jp/cyuou/danwa/0/{KB_JRA_12桁ID}
    地方: https://s.keibabook.co.jp/chihou/danwa/1/{KB_NAR_16桁ID}

KB race_id形式:
  JRA (12桁): YYYY + kaiko(2) + KB_venue(2) + day(2) + R(2)
              ※ netkeiba形式とはkaiko/venueの位置が逆
  NAR (16桁): YYYY + kaiko(2) + KB_venue(2) + day(2) + R(2) + MMDD(4)
              ※ netkeiba IDから直接変換不可。nitteiページから取得。

対応NAR競馬場（KB調教提供確認済み）:
  南関東: 大井・川崎・船橋・浦和
  その他: 笠松・名古屋・姫路・高知・佐賀・帯広
  ユーザー確認済だがコード未確定（動的発見対象）: 園田・門別

認証: KEIBABOOK_ID / KEIBABOOK_PASS または ~/.keiba_credentials.json
"""

import json
import os
import re
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

try:
    import requests
    from bs4 import BeautifulSoup

    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

from src.log import get_logger
from src.models import TrainingRecord

logger = get_logger(__name__)

try:
    from data.masters.venue_master import (
        get_venue_code_from_race_id,
    )
    from data.masters.venue_master import (
        is_jra as _is_jra_venue,
    )
except Exception:
    get_venue_code_from_race_id = lambda rid: rid[4:6] if len(rid) >= 6 else "00"
    _is_jra_venue = lambda vc: vc in ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10")

CREDENTIALS_FILE = Path.home() / ".keiba_credentials.json"

try:
    from config.settings import (
        CACHE_MAX_AGE_SEC as _KB_CACHE_MAX_AGE,
    )
    from config.settings import (
        KEIBABOOK_CACHE_DIR as _KB_DEFAULT_CACHE,
    )
except Exception:
    _KB_DEFAULT_CACHE = str(Path(__file__).resolve().parents[2] / "data" / "cache" / "keibabook")
    _KB_CACHE_MAX_AGE = 24 * 3600

# ============================================================
# 競馬ブック URL 定数（中央/地方でパスが異なる）
# ============================================================

KB_BASE = "https://s.keibabook.co.jp"
KB_LOGIN = "https://s.keibabook.co.jp/login/login"

# 調教  ← /0/ (JRA) / /1/ (NAR) が必須
KB_CYUOU_CYOKYO = "https://s.keibabook.co.jp/cyuou/cyokyo/0"  # 中央: /cyuou/cyokyo/0/{12桁ID}
KB_CHIHOU_CYOKYO = "https://s.keibabook.co.jp/chihou/cyokyo/1"  # 地方: /chihou/cyokyo/1/{16桁ID}

# 厩舎コメント（「厩舎の話」ページ）
KB_CYUOU_DANWA = "https://s.keibabook.co.jp/cyuou/danwa/0"  # 中央: /cyuou/danwa/0/{12桁ID}
KB_CHIHOU_DANWA = "https://s.keibabook.co.jp/chihou/danwa/1"  # 地方: /chihou/danwa/1/{16桁ID}

# nittei（日程）
KB_CYUOU_NITTEI = "https://s.keibabook.co.jp/cyuou/nittei"  # 中央: /{YYYYMMDD}{KB_venue}
KB_CHIHOU_NITTEI = "https://s.keibabook.co.jp/chihou/nittei"  # 地方: /{YYYYMMDD}{KB_venue}

# 動的発見済みKB venue codeのキャッシュファイル
KB_VENUE_CACHE_PATH = Path(__file__).resolve().parents[2] / "data" / "kb_venue_cache.json"

# ============================================================
# 場コード変換テーブル（netkeiba → keibabook）
# ============================================================

# JRA: netkeiba場コード → KB場コード
# KB JRA race_id形式: YYYY + kaiko(2) + KB_venue(2) + day(2) + R(2) = 12桁
# ※ netkeiba形式: YYYY + ne_venue(2) + kaiko(2) + day(2) + R(2) = 12桁
# ※ kaiko と venue の順序が逆になることに注意
JRA_VENUE_TO_KB: Dict[str, str] = {
    "05": "04",  # 東京  ✓ nittei 2026022104 で確認
    "06": "05",  # 中山  ✓ nittei 2026022805 + syutuba 202602050211 で確認
    "08": "00",  # 京都  ✓ nittei 2026020100 で確認
    "09": "01",  # 阪神  ✓ nittei 2026022101 + syutuba 202601010411 で確認
    "10": "03",  # 小倉  ✓ nittei 2026022203 で確認
    # 以下は未確認（春〜夏開催時に要確認）
    "07": "02",  # 中京  ← 暫定（3月開催時に確認予定）
    "01": "07",  # 福島  ← 暫定（春・夏開催時）
    "02": "08",  # 新潟  ← 暫定（夏開催時）
    "03": "09",  # 札幌  ← 暫定（夏開催時）
    "04": "06",  # 函館  ← 暫定（夏開催時）
}

# NAR: netkeiba場コード → KB場コード
# KB NAR race_id形式: YYYY + kaiko(2) + KB_venue(2) + day(2) + R(2) + MMDD(4) = 16桁
# ※ netkeiba NAR IDからは直接変換不可。nitteiページ参照で取得。
# ※ None のトラックはコード未確定 or 非提供。動的発見機能で自動補完する。
NAR_VENUE_TO_KB: Dict[str, Optional[str]] = {
    "42": "13",  # 浦和  ✓
    "43": "12",  # 船橋  ✓
    "44": "10",  # 大井  ✓
    "45": "11",  # 川崎  ✓
    "47": "19",  # 笠松  ✓
    "48": "34",  # 名古屋 ✓
    "50": "37",  # 園田  ✓
    "51": "39",  # 姫路  ✓ (netkeibaコード51)
    "30": None,  # 門別  ← KB提供あり・コード未確定（動的発見対象）
    "65": "58",  # 帯広  ✓
    "54": "26",  # 高知  ✓
    "55": "23",  # 佐賀  ✓
    "35": None,  # 盛岡  ← 非対応/未確認
    "36": "29",  # 水沢  ✓
    "46": "20",  # 金沢  ✓
}

# NAR venue name lookup: netkeiba venue code → KB nittei に表示される競馬場名の先頭文字
# 動的発見時にnitteiリンクテキストとマッチングするために使用
NAR_VENUE_NAMES: Dict[str, str] = {
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
    "50": "園田",
    "51": "姫路",
    "65": "帯広",
    "54": "高知",
    "55": "佐賀",
}


def _load_kb_venue_cache() -> Dict[str, str]:
    """動的発見済みNAR KB venue codeを読み込む"""
    try:
        if KB_VENUE_CACHE_PATH.exists():
            with open(KB_VENUE_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        logger.debug("KB venue cache load failed", exc_info=True)
    return {}


def _save_kb_venue_cache(cache: Dict[str, str]) -> None:
    """動的発見済みNAR KB venue codeを保存する"""
    try:
        KB_VENUE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(KB_VENUE_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.debug("KB venue cache save failed", exc_info=True)


def _apply_kb_venue_cache() -> None:
    """起動時にキャッシュから NAR_VENUE_TO_KB を補完する"""
    cache = _load_kb_venue_cache()
    for ne_code, kb_code in cache.items():
        if ne_code not in NAR_VENUE_TO_KB or NAR_VENUE_TO_KB[ne_code] is None:
            NAR_VENUE_TO_KB[ne_code] = kb_code


# 起動時に適用
_apply_kb_venue_cache()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja-JP,ja;q=0.9",
}

REQUEST_INTERVAL = 1.5


# ============================================================
# 認証情報マネージャー (競馬ブック用)
# ============================================================


class KeibabookCredentials:
    @staticmethod
    def load() -> Tuple[str, str]:
        """(keibabook_id, password) を返す"""
        # 環境変数優先
        env_id = os.environ.get("KEIBABOOK_ID", "")
        env_pass = os.environ.get("KEIBABOOK_PASS", "")
        if env_id and env_pass:
            return env_id, env_pass

        # ~/.keiba_credentials.json
        if CREDENTIALS_FILE.exists():
            try:
                with open(CREDENTIALS_FILE, "r") as f:
                    creds = json.load(f)
                uid = creds.get("keibabook_id", "")
                pwd = creds.get("keibabook_pass", "")
                if uid and pwd:
                    return uid, pwd
            except Exception:
                logger.debug("KB credentials load failed", exc_info=True)

        return "", ""

    @staticmethod
    def save(keibabook_id: str, password: str):
        """既存の credentials.json に競馬ブックの情報を追記する"""
        data = {}
        if CREDENTIALS_FILE.exists():
            try:
                with open(CREDENTIALS_FILE, "r") as f:
                    data = json.load(f)
            except Exception:
                logger.debug("KB credentials file read failed", exc_info=True)

        data["keibabook_id"] = keibabook_id
        data["keibabook_pass"] = password

        with open(CREDENTIALS_FILE, "w") as f:
            json.dump(data, f, indent=2)
        try:
            os.chmod(CREDENTIALS_FILE, 0o600)
        except Exception:
            logger.debug("chmod credentials failed", exc_info=True)
        logger.info(f"認証情報を保存: {CREDENTIALS_FILE}")


# ============================================================
# 競馬ブック 認証クライアント
# ============================================================


class KeibabookClient:
    """
    競馬ブックスマートプレミアム認証付きHTTPクライアント
    """

    def __init__(
        self,
        keibabook_id: str = "",
        password: str = "",
        cache_dir: str = None,
    ):
        if not HAS_DEPS:
            raise ImportError("pip install requests beautifulsoup4 lxml")

        if cache_dir is None:
            cache_dir = _KB_DEFAULT_CACHE
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.cache_dir = cache_dir
        self._logged_in = False
        self._is_premium = False
        self._last_req = 0.0
        self._stats_cache = 0
        self._stats_fetch = 0
        os.makedirs(cache_dir, exist_ok=True)

        self._id = keibabook_id or KeibabookCredentials.load()[0]
        self._pwd = password or KeibabookCredentials.load()[1]

    # ---------- ログイン ----------

    def login(self) -> bool:
        if self._logged_in:
            return True
        if not (self._id and self._pwd):
            logger.warning("認証情報が見つかりません。セットアップ: python -m src.scraper.keibabook_training --setup")
            return False

        try:
            # ログインページ取得
            resp = self.session.get(KB_LOGIN, timeout=15)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")

            # CSRFトークン
            csrf = ""
            for inp in soup.select("input[name=_token]"):
                csrf = inp.get("value", "")
                break

            time.sleep(REQUEST_INTERVAL)

            # POST（フィールド名はフォームの実際の name 属性に合わせる）
            payload = {
                "login_id": self._id,
                "pswd": self._pwd,
                "autologin": "1",
                "service": "keibabook",
                "referer": "",
            }
            if csrf:
                payload["_token"] = csrf

            post = self.session.post(
                KB_LOGIN,
                data=payload,
                headers={**HEADERS, "Referer": KB_LOGIN},
                timeout=15,
            )
            post.encoding = "utf-8"

            if self._check_success(post):
                self._logged_in = True
                self._is_premium = self._check_premium(post)
                grade = "スマートプレミアム" if self._is_premium else "一般会員"
                logger.info(f"ログイン成功: {self._id} ({grade})")
                return True
            else:
                logger.warning("ログイン失敗")
                return False

        except Exception as e:
            logger.warning("keibabook login error: %s", e, exc_info=True)
            return False

    def _check_success(self, resp) -> bool:
        text = resp.text
        fails = ["ログインに失敗", "パスワードが違", "ID が違", "login_error"]
        for f in fails:
            if f in text:
                return False
        if resp.status_code >= 400:
            return False
        # ログイン後に付与される tk cookie の存在で判定
        cookie_names = [c.name for c in self.session.cookies]
        return "tk" in cookie_names

    def _check_premium(self, resp) -> bool:
        # 調教ページを1つ取得してキャッシュヘッダーで判定
        time.sleep(REQUEST_INTERVAL)
        try:
            test_resp = self.session.get(
                f"{KB_CYUOU_CYOKYO}/202601040811", timeout=15
            )
            if test_resp and "use:smartpremium" in test_resp.text[:300]:
                return True
        except Exception:
            logger.debug("premium check failed", exc_info=True)
        # フォールバック: レスポンス文字列に含まれる会員種別表記
        text = resp.text
        return "スマートプレミアム" in text or "プレミアム会員" in text

    # ---------- HTTP ----------

    def get(
        self,
        url: str,
        params: dict = None,
        use_cache: bool = True,
    ) -> Optional[BeautifulSoup]:
        cache_key = self._cache_key(url, params)
        cache_path = os.path.join(self.cache_dir, cache_key + ".html")

        if use_cache and os.path.exists(cache_path):
            try:
                age = time.time() - os.path.getmtime(cache_path)
                if age <= _KB_CACHE_MAX_AGE:
                    self._stats_cache += 1
                    with open(cache_path, "r", encoding="utf-8") as f:
                        return BeautifulSoup(f.read(), "lxml")
            except OSError:
                pass

        self._stats_fetch += 1
        elapsed = time.time() - self._last_req
        if elapsed < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - elapsed)

        for attempt in range(2):
            try:
                resp = self.session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                resp.encoding = "utf-8"
                self._last_req = time.time()
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(resp.text)
                return BeautifulSoup(resp.text, "lxml")
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, OSError) as e:
                if attempt == 0:
                    time.sleep(REQUEST_INTERVAL)
                    continue
                logger.warning(f"GET失敗(リトライ後): {url} → {type(e).__name__}: {e}")
                return None
            except Exception as e:
                logger.warning("keibabook GET failed: %s → %s: %s", url, type(e).__name__, e, exc_info=True)
                return None

    def _cache_key(self, url: str, params: dict = None) -> str:
        key = url.replace("https://", "").replace("/", "_").replace("?", "_")
        if params:
            key += "_" + "_".join(f"{k}={v}" for k, v in sorted(params.items()))
        return key[:200]

    @property
    def is_logged_in(self) -> bool:
        return self._logged_in

    @property
    def is_premium(self) -> bool:
        return self._is_premium

    def ensure_login(self) -> bool:
        if not self._logged_in:
            return self.login()
        return True


# ============================================================
# 競馬ブック レースID変換
# ============================================================


def jra_netkeiba_to_kb_id(netkeiba_race_id: str) -> Optional[str]:
    """
    JRA の netkeiba race_id → KB JRA race_id に変換する。

    netkeiba JRA 形式: YYYY + ne_venue(2) + kaiko(2) + day(2) + R(2) = 12桁
    KB JRA 形式:       YYYY + kaiko(2) + KB_venue(2) + day(2) + R(2) = 12桁
    ※ kaiko と venue の位置が逆になる

    Returns None if venue is unknown (unconfirmed tracks).
    """
    if len(netkeiba_race_id) != 12:
        return None

    year = netkeiba_race_id[0:4]
    ne_venue = netkeiba_race_id[4:6]
    kaiko = netkeiba_race_id[6:8]
    day = netkeiba_race_id[8:10]
    race = netkeiba_race_id[10:12]

    kb_venue = JRA_VENUE_TO_KB.get(ne_venue)
    if kb_venue is None:
        return None  # 未対応場コード

    return year + kaiko + kb_venue + day + race


def netkeiba_to_keibabook_id(netkeiba_race_id: str) -> str:
    """
    後方互換ラッパー。JRA のみ対応。NAR は別途 get_nar_kb_race_id() を使用。
    変換不可の場合はそのまま返す。
    """
    kb_id = jra_netkeiba_to_kb_id(netkeiba_race_id)
    return kb_id if kb_id is not None else netkeiba_race_id


def is_kb_supported_venue(venue_code: str, is_jra: bool) -> bool:
    """
    KB が調教データを提供している場コードか確認する。
    JRA: JRA_VENUE_TO_KB に存在すれば対応
    NAR: NAR_VENUE_TO_KB に存在し、かつ KB コードが None でなければ対応
    """
    if is_jra:
        return venue_code in JRA_VENUE_TO_KB
    else:
        return NAR_VENUE_TO_KB.get(venue_code) is not None


# ============================================================
# 調教データスクレイパー
# ============================================================


class KeibabookTrainingScraper:
    """
    競馬ブックスマートプレミアムから調教データ・厩舎コメントを取得する。

    中央/地方でURLが異なる:
      調教: cyuou/cyokyo or chihou/cyokyo + /{race_id}
      コメント: cyuou/danwa or chihou/danwa + /{race_id}（別ページ）

    地方競馬の一部競馬場は調教・コメント非提供 → 取得スキップ、表示は「－」
    """

    def __init__(self, client: KeibabookClient):
        self.client = client

    def fetch(
        self,
        race_id: str,
        race_date: Optional[Union[date, str]] = None,
    ) -> Dict[str, List[TrainingRecord]]:
        """
        1レース分の全出走馬の調教データ＋厩舎コメントを取得する。

        Args:
            race_id:    netkeiba の race_id (12桁)
            race_date:  レース日 (date / "YYYY-MM-DD" / None)
                        NAR の場合は必須（KB NAR race_id の取得に使用）

        Returns:
            {horse_name: [TrainingRecord, ...]}
        """
        if not self.client.ensure_login():
            logger.warning("未ログイン。調教データ取得をスキップ")
            return {}

        venue_code = get_venue_code_from_race_id(race_id)
        jra = _is_jra_venue(venue_code)

        if not is_kb_supported_venue(venue_code, jra):
            logger.debug(f"場コード {venue_code} は KB調教データ非対応。スキップ")
            return {}

        if jra:
            return self._fetch_jra(race_id)
        else:
            return self._fetch_nar(race_id, race_date)

    # ------------------------------------------------------------------
    # JRA 取得
    # ------------------------------------------------------------------

    def _fetch_jra(self, race_id: str) -> Dict[str, List[TrainingRecord]]:
        """JRA レースの調教データを取得する（12桁KB ID使用）"""
        kb_id = jra_netkeiba_to_kb_id(race_id)
        if not kb_id:
            logger.warning(f"JRA race_id変換失敗: {race_id}")
            return {}

        cyokyo_url = f"{KB_CYUOU_CYOKYO}/{kb_id}"
        soup = self.client.get(cyokyo_url)
        if not soup:
            return {}

        text = soup.get_text() if hasattr(soup, "get_text") else str(soup)
        if "指定されたページは存在しません" in text or "提供外" in text:
            return {}
        if self._is_premium_wall(soup):
            logger.warning("プレミアムコンテンツ壁を検出")
            return {}

        training_map = self._parse_training_table(soup)

        danwa_soup = self.client.get(f"{KB_CYUOU_DANWA}/{kb_id}")
        if danwa_soup:
            dtext = danwa_soup.get_text() if hasattr(danwa_soup, "get_text") else ""
            if "指定されたページは存在しません" not in dtext and "提供外" not in dtext:
                comments = self._parse_danwa_table(danwa_soup)
                for name, recs in training_map.items():
                    if name in comments and recs:
                        recs[0].comment = comments[name]

        return training_map

    # ------------------------------------------------------------------
    # NAR 取得（nitteiページからKB 16桁IDをルックアップ）
    # ------------------------------------------------------------------

    def _fetch_nar(
        self,
        netkeiba_race_id: str,
        race_date: Optional[Union[date, str]],
    ) -> Dict[str, List[TrainingRecord]]:
        """
        NAR レースの調教データを取得する（16桁KB ID使用）。

        KB NAR race_id形式: YYYY + kaiko(2) + KB_venue(2) + day(2) + R(2) + MMDD(4) = 16桁
        netkeiba IDからは直接変換不可なので nittei ページから取得する。
        """
        venue_code = get_venue_code_from_race_id(netkeiba_race_id)
        kb_venue = NAR_VENUE_TO_KB.get(venue_code)

        # KB venue code が未確定の場合は動的発見を試みる
        if kb_venue is None:
            kb_venue = self._try_discover_kb_venue(venue_code)
        if not kb_venue:
            venue_name = NAR_VENUE_NAMES.get(venue_code, venue_code)
            logger.debug(f"{venue_name}(ne={venue_code}) は KB調教非対応 or コード未確認。スキップ")
            return {}

        race_no = netkeiba_race_id[10:12]  # レース番号 (2桁)

        # レース日を YYYYMMDD に変換
        if race_date is None:
            logger.warning(f"NAR調教取得にはrace_dateが必要です: {netkeiba_race_id}")
            return {}
        if isinstance(race_date, str):
            # "YYYY-MM-DD" → date オブジェクト
            try:
                race_date = date.fromisoformat(race_date[:10])
            except ValueError:
                logger.warning(f"race_date の形式が不正: {race_date}")
                return {}

        date_str = race_date.strftime("%Y%m%d")
        nittei_url = f"{KB_CHIHOU_NITTEI}/{date_str}{kb_venue}"

        soup = self.client.get(nittei_url)
        if not soup:
            return {}

        text = soup.get_text() if hasattr(soup, "get_text") else ""
        if "指定されたページは存在しません" in text:
            logger.warning(f"NAR nittei ページなし: {nittei_url}")
            return {}

        kb_nar_id = self._find_nar_kb_race_id(soup, race_no)
        if not kb_nar_id:
            logger.warning(f"NAR KB race_id が見つかりません: {netkeiba_race_id} R{race_no} on {date_str}")
            return {}

        cyokyo_url = f"{KB_CHIHOU_CYOKYO}/{kb_nar_id}"
        soup = self.client.get(cyokyo_url)
        if not soup:
            return {}

        text = soup.get_text() if hasattr(soup, "get_text") else ""
        if "指定されたページは存在しません" in text or "提供外" in text:
            return {}
        if self._is_premium_wall(soup):
            logger.warning("プレミアムコンテンツ壁を検出")
            return {}

        training_map = self._parse_training_table(soup)

        danwa_soup = self.client.get(f"{KB_CHIHOU_DANWA}/{kb_nar_id}")
        if danwa_soup:
            dtext = danwa_soup.get_text() if hasattr(danwa_soup, "get_text") else ""
            if "指定されたページは存在しません" not in dtext and "提供外" not in dtext:
                comments = self._parse_danwa_table(danwa_soup)
                for name, recs in training_map.items():
                    if name in comments and recs:
                        recs[0].comment = comments[name]

        return training_map

    def _find_nar_kb_race_id(self, nittei_soup: "BeautifulSoup", race_no: str) -> Optional[str]:
        """
        nittei ページの syutuba リンクから対象レース番号の KB race_id (16桁) を探す。
        syutuba URL: /chihou/syutuba/{16桁ID} → KB race_id[10:12] がレース番号
        """
        for a in nittei_soup.select('a[href*="syutuba"]'):
            href = a.get("href", "")
            m = re.search(r"chihou/syutuba/(\d{16})", href)
            if m:
                cand = m.group(1)
                if cand[10:12] == race_no:
                    return cand
        return None

    def discover_nar_venue_codes(self) -> Dict[str, str]:
        """
        KB chihou/nittei/top ページから未知の NAR 会場コードを動的に発見する。

        Returns:
            {ne_venue_code: kb_venue_code} の発見マップ
        """
        if not self.client.ensure_login():
            return {}

        soup = self.client.get(f"{KB_CHIHOU_NITTEI}/top")
        if not soup:
            return {}

        cache = _load_kb_venue_cache()
        updated = {}

        for a in soup.select("a[href]"):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            m = re.search(r"chihou/nittei/(\d{8})(\d{2})", href)
            if not m or not text:
                continue
            kb_code = m.group(2)
            # venue name にマッチする ne_code を探す
            for ne_code, name in NAR_VENUE_NAMES.items():
                if text.startswith(name):
                    if NAR_VENUE_TO_KB.get(ne_code) is None:
                        NAR_VENUE_TO_KB[ne_code] = kb_code
                        cache[ne_code] = kb_code
                        updated[ne_code] = kb_code
                        logger.info(f"NAR venue code 発見: {name}(ne={ne_code}) = KB={kb_code}")

        if updated:
            _save_kb_venue_cache(cache)

        return updated

    def _try_discover_kb_venue(self, ne_venue_code: str) -> Optional[str]:
        """未知の NAR 会場コードを nittei/top から探す（フォールバック）"""
        # キャッシュ確認
        cache = _load_kb_venue_cache()
        if ne_venue_code in cache:
            kb_code = cache[ne_venue_code]
            NAR_VENUE_TO_KB[ne_venue_code] = kb_code
            return kb_code
        # 動的発見試行
        found = self.discover_nar_venue_codes()
        return found.get(ne_venue_code)

    def _parse_danwa_table(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        danwa（厩舎の話）ページから馬名→コメントの辞書を取得。
        列構成: 枠番・馬番・馬名・性齢・騎手・厩舎の話 等。ヘッダーから動的検出。
        """
        result: Dict[str, str] = {}
        table = (
            soup.select_one("table.danwa")
            or soup.select_one("table[class*='danwa']")
            or soup.select_one("table[class*='comment']")
            or soup.select_one("table[class*='bamei']")
            or soup.select_one("table.race_detail")
            or max(soup.select("table"), key=lambda t: len(t.select("tr")), default=None)
        )
        if not table:
            return result

        header_row = table.select_one("tr")
        cells_all = header_row.select("th, td") if header_row else []
        headers = self._detect_danwa_headers(cells_all)

        rows = table.select("tr")[1:]
        for row in rows:
            cells = row.select("td")
            if len(cells) < 2:
                continue
            name_idx = headers.get("horse_name", 0)
            comm_idx = headers.get("comment", -1)
            name_el = row.select_one("a[href*='horse']")
            name = ""
            if name_el:
                name = name_el.get_text(strip=True)
            if not name and 0 <= name_idx < len(cells):
                name = cells[name_idx].get_text(strip=True)
            comm = ""
            if 0 <= comm_idx < len(cells):
                comm = cells[comm_idx].get_text(strip=True)
            elif comm_idx < 0 and len(cells) > 1:
                comm = cells[-1].get_text(strip=True)
            if name:
                result[name] = comm or ""
        return result

    def _detect_danwa_headers(self, cells: list) -> dict:
        """danwa テーブルのヘッダーから馬名・厩舎の話のインデックスを検出"""
        headers = {"horse_name": 0, "comment": -1}
        for i, cell in enumerate(cells):
            text = cell.get_text(strip=True)
            if any(k in text for k in ["馬名", "horse", "ウマ"]):
                headers["horse_name"] = i
            elif any(k in text for k in ["厩舎の話", "厩舎", "コメント", "comment", "ダンワ"]):
                headers["comment"] = i
        return headers

    def _is_premium_wall(self, soup: BeautifulSoup) -> bool:
        """プレミアム会員専用ページの壁が出ているか確認"""
        text = soup.get_text()
        walls = ["プレミアム会員限定", "会員登録が必要", "ログインしてください", "premium_wall"]
        return any(w in text for w in walls) and len(soup.select("table")) == 0

    def _parse_training_table(self, soup: BeautifulSoup) -> Dict[str, List[TrainingRecord]]:
        """競馬ブック調教ページの実際のHTML構造に対応したパーサー。

        HTML構造（中央・地方共通）:
          <table class="default cyokyo">
            各馬 = 2行1組:
              行1: <td class="kbamei">馬名</td> <td class="tanpyo">短評</td>
              行2: <td colspan="5"> の中に、子要素が順番に並ぶ:
                   パターンA (JRA): <dl>(前回) + 日付 + 強度</dl> <table>タイム</table>
                   パターンB (NAR): <dl>(前回)</dl> <table>コメント</table>
                                    <dl>日付+強度</dl> <table>タイム</table> ...
                   → 子要素を順次走査して (dl, table) ペアを構築

        注: スマートプレミアム版HTMLでは、2頭目以降の行が1頭目の
        colspan <td> 内にネストされるため、再帰検索で全 td.kbamei を拾う。
        """
        result: Dict[str, List[TrainingRecord]] = {}

        table = soup.select_one("table.cyokyo")
        if not table:
            return result

        # 全 td.kbamei を再帰的に検索し、各馬名行→次のtr(データ行) をペアで処理
        for name_cell in table.select("td.kbamei"):
            try:
                link = name_cell.select_one("a")
                current_name = (
                    link.get_text(strip=True) if link else name_cell.get_text(strip=True)
                )
                if not current_name:
                    continue

                name_row = name_cell.find_parent("tr")
                if not name_row:
                    continue

                tp = name_row.select_one("td.tanpyo")
                current_tanpyo = tp.get_text(strip=True) if tp else ""

                data_row = name_row.find_next_sibling("tr")
                if not data_row:
                    continue

                colspan_td = data_row.select_one("td[colspan]")
                if not colspan_td:
                    continue

                entries, gen_comment = self._extract_training_entries(colspan_td, current_tanpyo)
                if entries:
                    if gen_comment:
                        entries[0].comment = gen_comment
                    elif current_tanpyo and not entries[0].comment:
                        entries[0].comment = current_tanpyo
                    result.setdefault(current_name, []).extend(entries)

            except Exception:
                logger.debug("parse training row failed", exc_info=True)
                continue

        return result

    def _extract_training_entries(self, td, tanpyo: str) -> Tuple[list, str]:
        """colspan td 内の子要素を順次走査し、(TrainingRecord リスト, 一般コメント) を返す。"""
        entries: list = []
        pending_dl_data: Optional[dict] = None
        general_comment = ""

        for child in td.children:
            if not hasattr(child, "name") or not child.name:
                continue

            if child.name == "dl" and "dl-table" in (child.get("class") or []):
                dl_data = self._parse_dl(child)
                if dl_data.get("date") or pending_dl_data is None:
                    pending_dl_data = dl_data

            elif child.name == "table" and "cyokyodata" in (child.get("class") or []):
                splits, lap_count = self._parse_cyokyodata(child)
                awase_row = child.select_one("tr.awase")
                awase = awase_row.get_text(strip=True) if awase_row else ""

                if not splits and not awase:
                    txt = child.get_text(strip=True)
                    if txt and pending_dl_data:
                        # 「中間軽め」等タイムなしの調教記録もレコード化
                        rec = TrainingRecord(
                            date=pending_dl_data.get("date", ""),
                            venue="",
                            course=pending_dl_data.get("course", ""),
                            rider=pending_dl_data.get("rider", ""),
                            track_condition=pending_dl_data.get("track_condition", ""),
                            intensity_label=txt,
                            splits={},
                            comment="",
                        )
                        entries.append(rec)
                        pending_dl_data = None
                    elif txt:
                        general_comment = txt
                    continue

                if pending_dl_data and pending_dl_data.get("date"):
                    course = self._normalize_course(pending_dl_data.get("course", ""))
                    # 強度: 競馬ブックの生テキストをそのまま使用
                    raw_intensity = pending_dl_data.get("intensity", "")
                    intensity = raw_intensity if raw_intensity else self._normalize_intensity(
                        raw_intensity, splits, course
                    )
                    rec = TrainingRecord(
                        date=pending_dl_data["date"],
                        venue="",
                        course=course,
                        splits=splits,
                        rider=pending_dl_data.get("rider", ""),
                        track_condition=pending_dl_data.get("track_condition", ""),
                        lap_count=lap_count,
                        intensity_label=intensity,
                        comment=awase,
                    )
                    entries.append(rec)
                    pending_dl_data = None

        return entries, general_comment

    def _parse_dl(self, dl) -> dict:
        """<dl class="dl-table"> から乗り手・日付・コース・馬場状態・強度を抽出。

        HTML構造:
          <dt>           → 乗り手 ("(前回)", "助手", "石橋脩" 等)
          <dt class="left"> → "日付 コース 馬場" ("2/25 美Ｗ 重")
          <dt class="right"> → 強度 ("馬なり余力", "一杯に追う" 等)
        """
        data: dict = {"date": "", "course": "", "intensity": "", "rider": "", "track_condition": ""}
        for dt in dl.select("dt"):
            cls = dt.get("class", [])
            text = dt.get_text(strip=True).replace("\xa0", " ")
            if "left" in cls:
                parts = text.split()
                if len(parts) >= 2:
                    data["date"] = parts[0]
                    data["course"] = parts[1]
                if len(parts) >= 3:
                    data["track_condition"] = parts[2]
            elif "right" in cls:
                data["intensity"] = text
            else:
                # クラスなし = 乗り手（"(前回)" は除外）
                if text and text != "(前回)":
                    data["rider"] = text
        return data

    def _pick_best_entry(self, entries: list):
        """最終追切（最も直近の日付）を選択。タイム有りを優先。"""
        with_splits = [e for e in entries if e.splits and any(e.splits.values())]
        if with_splits:
            return with_splits[-1]
        return entries[-1]

    def _parse_date_course(self, text: str) -> Tuple[str, str]:
        """'2/4 美Ｗ 良' → ('2/4', '美Ｗ')"""
        parts = text.split()
        if len(parts) >= 2:
            return parts[0], parts[1]
        elif len(parts) == 1:
            return parts[0], ""
        return "", ""

    def _parse_cyokyodata(self, table) -> tuple:
        """<table class="cyokyodata"> からタイムと周回数を抽出。
        列: 6F(roku_furlong), 5F, 4F, 3F, 1F, 周回(mawariiti)
        距離: 1200m, 1000m, 800m, 600m, 200m

        Returns:
            (splits_dict, lap_count_str)
        """
        splits = {}
        lap_count = ""
        time_row = table.select_one("tr.time")
        if not time_row:
            return splits, lap_count

        cells = time_row.select("td")
        dist_map = [1200, 1000, 800, 600, 200]

        for i, cell in enumerate(cells):
            text = cell.get_text(strip=True)
            if i < len(dist_map):
                # 数値以外（空, "1回"等）はスキップ
                try:
                    val = float(text)
                    splits[dist_map[i]] = val
                except (ValueError, TypeError):
                    # "1回" 等はスキップ（周回数ではない）
                    continue
            else:
                # 最後のセル = 周回数 "［7］" 等
                if text:
                    lap_count = text

        return splits, lap_count

    def _normalize_course(self, text: str) -> str:
        """コース表記を正規化"""
        map_ = {
            "坂": "坂路",
            "坂路": "坂路",
            "ウッド": "ウッド",
            "W": "ウッド",
            "CW": "CW",
            "C.W": "CW",
            "ポリ": "ポリトラック",
            "P": "ポリトラック",
            "芝": "芝コース",
            "ダ": "ダートコース",
            "南W": "南ウッド",
            "北W": "北ウッド",
            "障": "障害コース",
        }
        for key, val in map_.items():
            if key in text:
                return val
        return text or "不明"

    def _parse_laptime(self, text: str) -> dict:
        """
        ラップタイム文字列を分解する。
        例: "52.3-38.1-24.8-12.5" → {800: 52.3, 600: 38.1, 400: 24.8, 200: 12.5}
        例: "65.0-50.0-36.5-23.8-12.2" → {1000: 65.0, 800: 50.0, 600: 36.5, 400: 23.8, 200: 12.2}
        """
        nums = re.findall(r"\d+\.\d+", text)
        if not nums:
            # 整数形式 "523-381-248-125" の場合
            nums_int = re.findall(r"\d{2,3}", text)
            nums = [f"{int(n) / 10:.1f}" for n in nums_int if 90 <= int(n) <= 999]

        splits = {}
        n = len(nums)
        if n == 0:
            return splits

        # 最後のラップから逆算して200m刻みで割り当て
        for i, val in enumerate(reversed(nums)):
            m_key = (i + 1) * 200
            try:
                splits[m_key] = float(val)
            except ValueError:
                pass

        return splits

    def _normalize_intensity(self, text: str, splits: dict, course: str) -> str:
        """
        競馬ブックの強度表記 or ラップから強度を推定する。
        競馬ブックは "一杯" "強め" "馬なり" "軽め" を直接記載することが多い。
        """
        # 直接記載がある場合
        for label in ["一杯", "強め", "馬なり", "軽め", "極軽め"]:
            if label in text:
                return label

        # なければラップから推定 (calibration.py と同ロジック)
        if not splits:
            return "馬なり"

        last = list(splits.values())[0]  # 最短距離（200m）のタイム

        if "坂路" in course:
            if last <= 11.5:
                return "一杯"
            if last <= 12.0:
                return "強め"
            if last <= 12.5:
                return "馬なり"
            if last <= 13.0:
                return "軽め"
            return "極軽め"
        else:
            if last <= 11.2:
                return "一杯"
            if last <= 11.8:
                return "強め"
            if last <= 12.3:
                return "馬なり"
            if last <= 13.0:
                return "軽め"
            return "極軽め"


# ============================================================
# エンジン統合用ラッパー
# ============================================================


def fetch_training_for_race(
    race_id: str,
    horses,
    scraper: KeibabookTrainingScraper,
    race_date: Optional[Union["date", str]] = None,
) -> int:
    """
    レースの調教データを取得して Horse.training_records に格納する。

    Args:
        race_id:    netkeiba の race_id (12桁)
        horses:     Horse オブジェクトのリスト
        scraper:    KeibabookTrainingScraper インスタンス
        race_date:  レース日（NAR の場合は必須）

    Returns:
        取得成功した馬の数
    """
    training_map = scraper.fetch(race_id, race_date=race_date)
    if not training_map:
        return 0

    count = 0
    for horse in horses:
        records = training_map.get(horse.horse_name, [])
        if records:
            horse.training_records = records
            count += 1
            best = records[0]
            logger.debug(
                f"{horse.horse_name}: {best.intensity_label} {best.course}"
                f"{' 「' + best.comment + '」' if best.comment else ''}"
            )

    return count


# ============================================================
# auth.py の TrainingScraper を競馬ブック版に差し替えるアダプター
# ============================================================


class KbTrainingAdapter:
    """
    auth.py の TrainingScraper と同じインターフェースを提供する。
    main.py / engine.py を変更せずに差し替えられる。
    """

    def __init__(self, keibabook_id: str = "", password: str = ""):
        self._client = KeibabookClient(keibabook_id, password)
        self._scraper = KeibabookTrainingScraper(self._client)

    def login(self) -> bool:
        return self._client.login()

    def fetch(
        self,
        race_id: str,
        race_date: Optional[Union["date", str]] = None,
    ) -> Dict[str, List[TrainingRecord]]:
        """horse_name → [TrainingRecord] の辞書を返す（main.pyとの互換）"""
        return self._scraper.fetch(race_id, race_date=race_date)

    @property
    def is_logged_in(self) -> bool:
        return self._client.is_logged_in

    @property
    def is_premium(self) -> bool:
        return self._client.is_premium


# ============================================================
# 競馬ブック 結果取得 + 過去走取得
# ============================================================

# 結果ページURL
KB_CYUOU_SEISEKI = "https://s.keibabook.co.jp/cyuou/seiseki"   # JRA: /{KB_JRA_12桁ID}
KB_CHIHOU_SEISEKI = "https://s.keibabook.co.jp/chihou/seiseki"  # NAR: /{KB_NAR_16桁ID}
# 馬詳細ページURL
KB_UMA_DB = "https://s.keibabook.co.jp/db/uma"  # /{KB_HORSE_ID}/top

# 券種名 → netkeiba互換キー
_KB_TICKET_MAP = {
    "単勝": "tansho",
    "複勝": "fukusho",
    "枠連": "wakuren",
    "馬連": "umaren",
    "馬単": "umatan",
    "ワイド": "wide",
    "3連複": "sanrenpuku",
    "三連複": "sanrenpuku",
    "3連単": "sanrentan",
    "三連単": "sanrentan",
}


class KeibabookResultScraper:
    """競馬ブック 結果取得 + 過去走取得スクレイパー"""

    def __init__(self, client: KeibabookClient):
        self.client = client

    # ----------------------------------------------------------
    # 結果取得（Phase 3-A）
    # ----------------------------------------------------------

    def fetch_result(
        self,
        netkeiba_race_id: str,
        race_date: Optional[Union[date, str]] = None,
    ) -> Optional[dict]:
        """
        netkeiba race_id からレース結果（着順・払戻・通過順）を取得する。

        Returns:
            {
                "order": [{"horse_no":1, "finish":1, "corners":[...], "last_3f":34.0, "time_sec":96.5},...],
                "payouts": {"tansho":[...], "fukusho":[...], ...}
            }
            or None（取得失敗時）
        """
        venue_code = get_venue_code_from_race_id(netkeiba_race_id)
        is_jra = _is_jra_venue(venue_code)

        if is_jra:
            kb_id = jra_netkeiba_to_kb_id(netkeiba_race_id)
            if not kb_id:
                logger.debug(f"KB変換失敗(JRA): {netkeiba_race_id}")
                return None
            url = f"{KB_CYUOU_SEISEKI}/{kb_id}"
        else:
            # NAR: nittei ページから KB race_id を取得
            kb_id = self._resolve_nar_kb_id(netkeiba_race_id, race_date)
            if not kb_id:
                logger.debug(f"KB変換失敗(NAR): {netkeiba_race_id}")
                return None
            url = f"{KB_CHIHOU_SEISEKI}/{kb_id}"

        soup = self.client.get(url, use_cache=True)
        if not soup:
            return None

        text = soup.get_text() if hasattr(soup, "get_text") else ""
        if "指定されたページは存在しません" in text:
            return None

        try:
            order = self._parse_result_table(soup)
            payouts = self._parse_payouts(soup)
            if not order:
                return None
            return {"order": order, "payouts": payouts}
        except Exception as e:
            logger.warning(f"競馬ブック結果パース失敗: {netkeiba_race_id} → {e}")
            return None

    def _resolve_nar_kb_id(
        self,
        netkeiba_race_id: str,
        race_date: Optional[Union[date, str]],
    ) -> Optional[str]:
        """NAR netkeiba race_id → KB NAR race_id を nittei ページ経由で解決"""
        venue_code = get_venue_code_from_race_id(netkeiba_race_id)
        kb_venue = NAR_VENUE_TO_KB.get(venue_code)
        if not kb_venue:
            return None

        if race_date is None:
            return None
        if isinstance(race_date, str):
            try:
                race_date = date.fromisoformat(race_date[:10])
            except ValueError:
                return None

        date_str = race_date.strftime("%Y%m%d")
        race_no = netkeiba_race_id[10:12]

        nittei_url = f"{KB_CHIHOU_NITTEI}/{date_str}{kb_venue}"
        soup = self.client.get(nittei_url)
        if not soup:
            return None

        # nittei ページのリンクからレース番号に対応する KB race_id を見つける
        # seiseki リンク: /chihou/seiseki/{16桁ID}
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "/seiseki/" not in href and "/syutuba/" not in href:
                continue
            # リンクテキストからレース番号を抽出
            link_text = a.get_text(strip=True)
            m = re.search(r"(\d+)\s*R", link_text)
            if m and m.group(1).zfill(2) == race_no:
                # href から KB ID を抽出
                parts = href.rstrip("/").split("/")
                if parts:
                    return parts[-1]

        return None

    def _parse_result_table(self, soup: "BeautifulSoup") -> list:
        """着順テーブル (table.default.seiseki) をパース"""
        table = soup.select_one("table.default.seiseki")
        if not table:
            # フォールバック: captionが「着順」のテーブルを探す
            for t in soup.select("table"):
                cap = t.select_one("caption")
                if cap and "着順" in cap.get_text():
                    table = t
                    break
        if not table:
            return []

        results = []
        for row in table.select("tbody tr"):
            cells = row.select("td")
            if not cells:
                continue

            try:
                # 着順
                finish_cell = row.select_one("td.cyakujun")
                if not finish_cell:
                    finish_cell = cells[0] if cells else None
                if not finish_cell:
                    continue
                finish_text = finish_cell.get_text(strip=True)
                if not finish_text or not finish_text.isdigit():
                    continue  # 取消・除外
                finish = int(finish_text)

                # 馬番 (td with waku* class)
                horse_no = 0
                for c in cells:
                    cls = " ".join(c.get("class", []))
                    if "waku" in cls:
                        hn_text = c.get_text(strip=True)
                        if hn_text.isdigit():
                            horse_no = int(hn_text)
                        break
                if horse_no == 0:
                    continue

                # 馬名含む複合セル (td.left)
                left_cell = row.select_one("td.left")
                last_3f = None
                corners = []

                # 通過順 (ul.tuka) — 有料限定のためパース試行
                if left_cell:
                    tuka = left_cell.select_one("ul.tuka")
                    if tuka:
                        tuka_text = tuka.get_text(strip=True)
                        if tuka_text and "**" not in tuka_text and "※" not in tuka_text:
                            corners = [int(x) for x in re.findall(r"\d+", tuka_text)]

                # タイム・上り3F (後半のセル)
                time_sec = 0.0
                for c in cells:
                    ct = c.get_text(strip=True)
                    # タイム: "M:SS.S" or "M.SS.S"
                    tm = re.match(r"^(\d):(\d{2})\.(\d)$", ct)
                    if tm:
                        time_sec = int(tm.group(1)) * 60 + int(tm.group(2)) + int(tm.group(3)) * 0.1
                    # 上り3F: "(34.0)" 形式
                    l3m = re.search(r"\((\d{2}\.\d)\)", ct)
                    if l3m:
                        val = float(l3m.group(1))
                        if 30.0 <= val <= 45.0:
                            last_3f = val

                entry = {
                    "horse_no": horse_no,
                    "finish": finish,
                    "corners": corners if corners else None,
                    "last_3f": last_3f,
                    "time_sec": time_sec if time_sec > 0 else None,
                }
                results.append(entry)

            except (ValueError, IndexError):
                continue

        return sorted(results, key=lambda x: x["finish"])

    def _parse_payouts(self, soup: "BeautifulSoup") -> dict:
        """払戻テーブル (table.default.kako-haraimoshi) をパース"""
        table = soup.select_one("table.default.kako-haraimoshi")
        if not table:
            for t in soup.select("table"):
                cap = t.select_one("caption")
                if cap and "払戻" in cap.get_text():
                    table = t
                    break
        if not table:
            return {}

        payouts = {}
        for row in table.select("tr"):
            cells = row.select("td")
            if len(cells) < 3:
                continue

            # 券種名
            midasi_cell = row.select_one("td.midasi")
            if not midasi_cell:
                midasi_cell = cells[0]
            ticket_name = midasi_cell.get_text(strip=True)
            key = _KB_TICKET_MAP.get(ticket_name)
            if not key:
                continue

            # 組番号と払戻金（複数の場合は <br> 区切り）
            combo_cell = cells[1] if len(cells) > 1 else None
            payout_cell = cells[2] if len(cells) > 2 else None
            if not combo_cell or not payout_cell:
                continue

            # <br> 区切りで複数値対応
            combo_parts = re.split(r"<br\s*/?>", str(combo_cell))
            payout_parts = re.split(r"<br\s*/?>", str(payout_cell))

            entries = []
            for i, (cp, pp) in enumerate(zip(combo_parts, payout_parts)):
                combo_text = BeautifulSoup(cp, "lxml").get_text(strip=True)
                payout_text = BeautifulSoup(pp, "lxml").get_text(strip=True)

                # 組番号のクリーニング
                combo = re.sub(r"[^\d\-]", "", combo_text).strip("-")
                if not combo:
                    continue

                # 払戻金: "1,230円" → 1230
                payout_val = re.sub(r"[^\d]", "", payout_text)
                if not payout_val:
                    continue

                entries.append({
                    "combo": combo,
                    "payout": int(payout_val),
                    "popularity": i + 1,  # 簡易的に順番を人気として設定
                })

            if entries:
                payouts[key] = entries

        return payouts

    # ----------------------------------------------------------
    # 過去走取得（Phase 3-B）
    # ----------------------------------------------------------

    def fetch_horse_history(
        self,
        kb_horse_id: str,
    ) -> list:
        """
        競馬ブック馬詳細ページから過去走データを取得する。

        Args:
            kb_horse_id: 競馬ブックのhorse_id（7桁数字）

        Returns:
            過去走リスト（dict形式）。各要素:
            {
                "race_date": "2026/3/15",
                "venue_race": "中山11R",
                "race_name": "スプリングＳ",
                "grade": "G2",
                "field_count": 16,
                "finish_pos": 1,
                "surface_distance_condition": "芝1800m良",
                "time_text": "1.46.0",
                "jockey": "津村明",
                "weight_kg": 57.0,
                "horse_weight_text": "500K",
                "gate": 15,
                "popularity": 8,
                "corners": [],  # 有料限定（取得できれば設定）
                "last_3f": None,  # 有料限定
            }
        """
        url = f"{KB_UMA_DB}/{kb_horse_id}/top"
        soup = self.client.get(url, use_cache=True)
        if not soup:
            return []

        text = soup.get_text() if hasattr(soup, "get_text") else ""
        if "指定されたページは存在しません" in text:
            return []

        try:
            return self._parse_horse_history(soup)
        except Exception as e:
            logger.warning(f"競馬ブック過去走パース失敗: {kb_horse_id} → {e}")
            return []

    def _parse_horse_history(self, soup: "BeautifulSoup") -> list:
        """馬詳細ページの過去走データをパース (div.uma_seiseki)"""
        results = []
        for block in soup.select("div.uma_seiseki"):
            try:
                entry = {}
                dls = block.select("dl")
                if len(dls) < 5:
                    continue

                # dl[0]: 日付・レース番号
                dt0 = dls[0].select_one("dt")
                if dt0:
                    negahi = dt0.select_one("span.negahi")
                    if negahi:
                        entry["venue_race"] = negahi.get_text(strip=True).replace("\xa0", " ")

                # dl[1]: レース名・頭数・着順
                dt1 = dls[1].select_one("dt")
                dd1 = dls[1].select_one("dd")
                if dt1:
                    rn = dt1.select_one("span.racename")
                    if rn:
                        entry["race_name"] = rn.get_text(strip=True)
                    grade_el = dt1.select_one("span.icon_grade")
                    if grade_el:
                        grade_cls = " ".join(grade_el.get("class", []))
                        if "g1" in grade_cls:
                            entry["grade"] = "G1"
                        elif "g2" in grade_cls:
                            entry["grade"] = "G2"
                        elif "g3" in grade_cls:
                            entry["grade"] = "G3"
                if dd1:
                    tosu = dd1.select_one("span.tosu")
                    if tosu:
                        m = re.search(r"(\d+)", tosu.get_text())
                        if m:
                            entry["field_count"] = int(m.group(1))
                    cyakujun = dd1.select_one("span.cyakujun")
                    if cyakujun:
                        ft = cyakujun.get_text(strip=True)
                        if ft.isdigit():
                            entry["finish_pos"] = int(ft)

                # dl[2]: 距離・タイム・騎手・斤量
                dt2 = dls[2].select_one("dt")
                dd2 = dls[2].select_one("dd")
                if dt2:
                    kyori = dt2.select_one("span.kyori")
                    if kyori:
                        entry["surface_distance_condition"] = kyori.get_text(strip=True)
                    time_el = dt2.select_one("span.time")
                    if time_el:
                        entry["time_text"] = time_el.get_text(strip=True)
                if dd2:
                    kisyu = dd2.select_one("span.kisyu")
                    if kisyu:
                        entry["jockey"] = kisyu.get_text(strip=True)
                    kinryo = dd2.select_one("span.kinryo")
                    if kinryo:
                        kt = kinryo.get_text(strip=True)
                        try:
                            entry["weight_kg"] = float(kt)
                        except ValueError:
                            pass

                # dl[3]: 上り3F・通過順（有料限定）
                dt3 = dls[3].select_one("dt")
                dd3 = dls[3].select_one("dd")
                if dt3:
                    agari = dt3.select_one("span.agari")
                    if agari:
                        at = agari.get_text(strip=True)
                        if at and "※" not in at and "**" not in at:
                            try:
                                entry["last_3f"] = float(at)
                            except ValueError:
                                pass
                if dd3:
                    tuka = dd3.select_one("ul.tuka")
                    if tuka:
                        tt = tuka.get_text(strip=True)
                        if tt and "※" not in tt and "**" not in tt:
                            entry["corners"] = [int(x) for x in re.findall(r"\d+", tt)]

                # dl[4]: 着差・勝馬・馬体重・ゲート・人気
                dt4 = dls[4].select_one("dt")
                dd4 = dls[4].select_one("dd")
                if dd4:
                    batai = dd4.select_one("span.batai")
                    if batai:
                        entry["horse_weight_text"] = batai.get_text(strip=True)
                    gate = dd4.select_one("span.gate")
                    if gate:
                        gm = re.search(r"(\d+)", gate.get_text())
                        if gm:
                            entry["gate"] = int(gm.group(1))
                    ninki = dd4.select_one("span.ninki")
                    if ninki:
                        nm = re.search(r"(\d+)", ninki.get_text())
                        if nm:
                            entry["popularity"] = int(nm.group(1))

                if entry.get("finish_pos") is not None:
                    results.append(entry)

            except Exception:
                continue

        return results

    def find_kb_horse_id(self, soup_result: "BeautifulSoup") -> Dict[int, str]:
        """
        結果ページから馬番→KB horse_id のマッピングを取得する。
        馬名リンク /db/uma/{KB_HORSE_ID} からIDを抽出。

        Returns:
            {1: "0954381", 3: "0812345", ...}  # 馬番 → KB horse_id
        """
        mapping = {}
        table = soup_result.select_one("table.default.seiseki")
        if not table:
            return mapping

        for row in table.select("tbody tr"):
            try:
                # 馬番
                horse_no = 0
                for c in row.select("td"):
                    cls = " ".join(c.get("class", []))
                    if "waku" in cls:
                        hn_text = c.get_text(strip=True)
                        if hn_text.isdigit():
                            horse_no = int(hn_text)
                        break
                if horse_no == 0:
                    continue

                # 馬名リンクから KB horse_id を抽出
                for a in row.select("a[href]"):
                    href = a.get("href", "")
                    m = re.search(r"/db/uma/(\d+)", href)
                    if m:
                        mapping[horse_no] = m.group(1)
                        break
            except Exception:
                continue

        return mapping


# ============================================================
# CLI: セットアップ & 動作確認
# ============================================================

if __name__ == "__main__":
    import argparse
    import getpass
    import sys

    _root = Path(__file__).resolve().parents[2]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

    p = argparse.ArgumentParser(description="競馬ブックスマートプレミアム 認証管理")
    p.add_argument("--setup", action="store_true", help="認証情報を対話式で設定")
    p.add_argument("--check", action="store_true", help="ログインテスト")
    p.add_argument("--fetch", type=str, default="", metavar="RACE_ID", help="調教データ取得テスト")
    p.add_argument("--clear-cache", action="store_true", help="調教・コメントキャッシュを削除")
    args = p.parse_args()

    if args.clear_cache:
        cache_dir = Path(_KB_DEFAULT_CACHE)
        n = 0
        if cache_dir.exists():
            for f in cache_dir.glob("*.html"):
                f.unlink()
                n += 1
        logger.info(f"キャッシュ削除: {n}件")

    elif args.setup:
        logger.info("=== 競馬ブックスマートプレミアム 認証情報セットアップ ===")
        logger.info("入力した情報は ~/.keiba_credentials.json に保存されます")
        uid = input("競馬ブック ID (メールアドレス): ").strip()
        pwd = getpass.getpass("パスワード: ")
        if uid and pwd:
            KeibabookCredentials.save(uid, pwd)
            client = KeibabookClient(uid, pwd)
            ok = client.login()
            if ok:
                logger.info(f"スマートプレミアム: {'有効' if client.is_premium else '無効（一般会員）'}")

    elif args.check:
        client = KeibabookClient()
        ok = client.login()
        logger.info(f"ログイン: {'OK' if ok else '失敗'}")
        if ok:
            logger.info(f"スマートプレミアム: {'有効' if client.is_premium else '無効'}")

    elif args.fetch:
        client = KeibabookClient()
        scraper = KeibabookTrainingScraper(client)
        result = scraper.fetch(args.fetch)
        logger.info(f"調教データ取得結果: {len(result)}頭")
        for name, records in result.items():
            for r in records:
                logger.info(f"{name}: {r.intensity_label} {r.course} {r.splits}")

    else:
        p.print_help()
