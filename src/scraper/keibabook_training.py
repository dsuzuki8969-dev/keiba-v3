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

# ポイント（門別等、danwaが無い会場のフォールバック）
KB_CHIHOU_POINT_BASE = "https://s.keibabook.co.jp"  # + /chihou/point/{sub}/{16桁ID}

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
    # netkeiba ne_venue(2桁) → KB venue(2桁)
    # ※ netkeiba race_id の ne_venue は JRA公式場コードとは01-04の割当が異なる
    #   netkeiba: 01=札幌,02=函館,03=福島,04=新潟,05=東京,...
    "05": "04",  # 東京  ✓ nittei 2026022104 で確認
    "06": "05",  # 中山  ✓ nittei 2026022805 + syutuba 202602050211 で確認
    "08": "00",  # 京都  ✓ nittei 2026020100 で確認
    "09": "01",  # 阪神  ✓ nittei 2026022101 + syutuba 202601010411 で確認
    "10": "03",  # 小倉  ✓ nittei 2026022203 で確認
    "07": "02",  # 中京  ✓ cyokyo 202401020101 で確認
    "03": "06",  # 福島  ✓ nittei 2026041106 + cyokyo 202401060101 で確認
    "04": "07",  # 新潟  ✓ cyokyo 202401070101 で確認
    "01": "08",  # 札幌  ✓ cyokyo 202401080101 で確認
    "02": "09",  # 函館  ✓ cyokyo 202401090101 で確認
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
    "48": "34",  # 名古屋 (nittei対応だが調教ページは出馬表を返す→調教非対応)
    "50": "37",  # 園田（正規コード）✓ netkeiba race_id 50 に統一（2026-04-28 修正）
    "49": "37",  # 園田（旧コード互換）✓ 旧 SPAT4 ベースの 49 を残す
    "51": "39",  # 姫路  ✓ (netkeibaコード51)
    "30": "42",  # 門別  ✓ (2026年シーズンからKBコード14→42に変更)
    "65": "58",  # 帯広  ✓
    "54": "26",  # 高知  ✓
    "55": "23",  # 佐賀  ✓
    "35": "15",  # 盛岡  ✓
    "36": "29",  # 水沢  ✓
    "46": "20",  # 金沢  ✓
}

# KB調教ページ対応: 門別(30), 浦和(42), 船橋(43), 大井(44), 川崎(45), 園田(50/49旧互換), 姫路(51) + JRA全場
# それ以外は調教データ非提供（交流重賞で例外的にある場合があるが通常はなし）
# 2026-04-28 修正: 園田の正規コードは 50。49 は旧 SPAT4 ベースコード（互換のみ）
_NAR_TRAINING_SUPPORTED: set = {"30", "42", "43", "44", "45", "49", "50", "51"}  # 門別,浦和,船橋,大井,川崎,園田,姫路

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
    "50": "園田",   # 正規 netkeiba コード（2026-04-28 統一）
    "49": "園田",   # 旧コード互換（SPAT4 ベース）
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
            logger.info("KB ログインページ取得: %s", KB_LOGIN)
            resp = self.session.get(KB_LOGIN, timeout=15)
            resp.encoding = "utf-8"
            logger.info("KB ログインページ: status=%d, len=%d", resp.status_code, len(resp.text))

            if resp.status_code != 200:
                logger.warning("KB ログインページ取得失敗: status=%d", resp.status_code)
                return False

            soup = BeautifulSoup(resp.text, "lxml")

            # メンテナンス検出: ログインフォームが存在しない場合のみ
            login_form = soup.select_one("input[name=login_id]")
            if not login_form:
                logger.warning("KB メンテナンス中（ログインフォームなし）")
                return False

            # CSRFトークン
            csrf = ""
            for inp in soup.select("input[name=_token]"):
                csrf = inp.get("value", "")
                break
            logger.info("KB CSRFトークン: %s", "取得済み" if csrf else "なし")

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
            logger.info("KB ログインPOST: status=%d, len=%d", post.status_code, len(post.text))

            if self._check_success(post):
                self._logged_in = True
                self._is_premium = self._check_premium(post)
                grade = "スマートプレミアム" if self._is_premium else "一般会員"
                logger.info("KB ログイン成功: %s (%s)", self._id, grade)
                return True
            else:
                # 失敗時のレスポンス一部をログ出力（診断用）
                body_snippet = post.text[:300].replace("\n", " ")
                logger.warning("KB ログイン失敗: status=%d, body=%s", post.status_code, body_snippet)
                return False

        except Exception as e:
            logger.warning("KB ログインエラー: %s", e, exc_info=True)
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

    def remove_cache(self, url: str, params: dict = None) -> bool:
        """指定URLのキャッシュファイルを削除する"""
        cache_key = self._cache_key(url, params)
        cache_path = os.path.join(self.cache_dir, cache_key + ".html")
        try:
            if os.path.exists(cache_path):
                os.remove(cache_path)
                return True
        except OSError:
            pass
        return False

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
                        recs[0].stable_comment = comments[name]

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

        # KB調教対応会場のみ取得（門別,浦和,船橋,大井,川崎,園田,姫路）
        if venue_code not in _NAR_TRAINING_SUPPORTED:
            return {}

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
            logger.warning(f"NAR nittei GET失敗: {nittei_url}")
            return {}

        text = soup.get_text() if hasattr(soup, "get_text") else ""
        if "指定されたページは存在しません" in text:
            logger.warning(f"NAR nittei ページなし: {nittei_url}")
            return {}

        kb_nar_id = self._find_nar_kb_race_id(soup, race_no)
        if not kb_nar_id:
            # デバッグ: syutubaリンクの状況をログ
            _syutuba_links = soup.select('a[href*="syutuba"]')
            logger.warning(
                f"NAR KB race_id が見つかりません: {netkeiba_race_id} R{race_no} on {date_str} "
                f"(syutubaリンク数={len(_syutuba_links)}, nittei_url={nittei_url})"
            )
            return {}

        logger.debug(f"NAR KB race_id 解決: {netkeiba_race_id} → {kb_nar_id}")
        cyokyo_url = f"{KB_CHIHOU_CYOKYO}/{kb_nar_id}"
        soup = self.client.get(cyokyo_url)
        if not soup:
            logger.warning(f"NAR cyokyo GET失敗: {cyokyo_url}")
            return {}

        text = soup.get_text() if hasattr(soup, "get_text") else ""
        if "指定されたページは存在しません" in text or "提供外" in text:
            logger.warning(f"NAR cyokyo ページなし/提供外: {cyokyo_url}")
            return {}
        if self._is_premium_wall(soup):
            logger.warning("プレミアムコンテンツ壁を検出")
            return {}

        # 調教ページではなく出馬表ページが返された場合を検出（名古屋等）
        _title = soup.title.string if soup.title else ""
        if _title and "出馬表" in _title and "調教" not in _title:
            venue_name = NAR_VENUE_NAMES.get(venue_code, venue_code)
            logger.info(f"{venue_name}(KB={kb_venue}) cyokyoページが出馬表を返却 → 調教非対応と判断。キャッシュ削除")
            # 不正キャッシュを削除（次回アクセス時に再取得させない）
            self.client.remove_cache(cyokyo_url)
            return {}

        training_map = self._parse_training_table(soup)

        # --- 厩舎コメント取得（danwa → point フォールバック） ---
        comments: Dict[str, str] = {}

        # cyokyoページからdanwa/pointリンクを動的に発見
        danwa_href = ""
        point_href = ""
        for a_tag in soup.select("a[href]"):
            href = a_tag.get("href", "")
            if "danwa" in href and not danwa_href:
                danwa_href = href
            if "point" in href and not point_href:
                point_href = href

        # 1) danwaページを試行（URLはcyokyoページ上のリンクを優先、なければ従来パス）
        danwa_url = (
            f"{KB_CHIHOU_POINT_BASE}{danwa_href}" if danwa_href
            else f"{KB_CHIHOU_DANWA}/{kb_nar_id}"
        )
        danwa_soup = self.client.get(danwa_url)
        if danwa_soup:
            dtext = danwa_soup.get_text() if hasattr(danwa_soup, "get_text") else ""
            if "指定されたページは存在しません" not in dtext and "提供外" not in dtext:
                comments = self._parse_danwa_table(danwa_soup)

        # 2) danwaが空ならポイントページにフォールバック（門別等）
        if not comments and point_href:
            point_url = f"{KB_CHIHOU_POINT_BASE}{point_href}"
            point_soup = self.client.get(point_url)
            if point_soup:
                ptext = point_soup.get_text() if hasattr(point_soup, "get_text") else ""
                if "指定されたページは存在しません" not in ptext and "提供外" not in ptext:
                    comments = self._parse_point_table(point_soup)
                    if comments:
                        logger.info(f"NAR danwa→pointフォールバック成功: {len(comments)}頭 ({point_url})")

        # コメントをtraining_mapに反映
        for name, recs in training_map.items():
            if name in comments and recs:
                recs[0].stable_comment = comments[name]

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

        HTML構造（2行1組）:
          row[0]: 枠番 | 馬番 | 馬名（3列）
          row[1]: colspan コメント本文（1列）
          row[2]: 空行（場合あり）
        """
        result: Dict[str, str] = {}
        # 最も行数が多いテーブルをdanwaテーブルと推定
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

        rows = table.select("tr")[1:]  # ヘッダー行をスキップ
        current_name = ""
        for row in rows:
            cells = row.select("td")
            if not cells:
                continue
            text = cells[-1].get_text(strip=True) if cells else ""

            if len(cells) >= 2:
                # 馬名行（枠番・馬番・馬名 の複数列）
                # 馬名はリンクから取得を優先
                name_el = row.select_one("a[href*='horse']")
                if name_el:
                    current_name = name_el.get_text(strip=True)
                else:
                    current_name = cells[-1].get_text(strip=True)
            elif len(cells) == 1 and current_name:
                # コメント行（colspan=全列、1セルのみ）
                comment = text
                if comment and len(comment) > 3:
                    result[current_name] = comment
                    current_name = ""  # 次の馬へ
        return result

    def _parse_point_table(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        ポイントページから馬名→コメントの辞書を取得。
        門別など「厩舎の話」がない会場のフォールバック用。

        HTML構造（3行1組）:
          row[0]: ヘッダー（枠番 | 馬番 | 馬名ポイント）
          row[1]: 枠番 | 馬番 | 馬名 （3列）
          row[2]: コメントテキスト（1列 colspan）- "馬名(評価) 本文..."
          row[3]: 空行
          以降 row[1]-[3] の繰り返し
        """
        result: Dict[str, str] = {}
        # 最も行数が多いテーブルをポイントテーブルと推定
        tables = soup.select("table")
        if not tables:
            return result
        table = max(tables, key=lambda t: len(t.select("tr")), default=None)
        if not table:
            return result

        rows = table.select("tr")[1:]  # ヘッダー行をスキップ
        current_name = ""
        for row in rows:
            cells = row.select("td")
            if not cells:
                continue
            text = cells[-1].get_text(strip=True) if cells else ""

            if len(cells) >= 3:
                # 馬名行（枠番・馬番・馬名）
                current_name = cells[-1].get_text(strip=True)
            elif len(cells) == 1 and current_name and text:
                # コメント行: "馬名(評価) 本文..." 形式
                # 馬名部分を除去してコメント本体を抽出
                comment = text
                # "フジノアルファ(軽視不可) 本文..." → "(軽視不可) 本文..." → 全体をコメントとして保持
                if comment.startswith(current_name):
                    comment = comment[len(current_name):].strip()
                if comment and len(comment) > 3:
                    result[current_name] = comment
                    current_name = ""
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

        HTMLテーブル列: 6F, 5F, 4F, 3F, 1F, 周回
        距離マッピング: 1200m, 1000m, 800m, 600m, 200m

        坂路補正:
          坂路データは6F列に周回数("1回"等)が入り、5F列以降にタイムが入るため
          実際のタイム位置が1列ずれる。坂路検出時はキーを1段シフトする。
          例: HTML上の5F列=実際は4F(800m), 4F列=3F(600m), 3F列=2F(400m)

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

        # 坂路判定: cells[0]が数値でない（"1回"等の周回数）場合は列シフト
        first_text = cells[0].get_text(strip=True) if cells else ""
        is_baro = False
        if first_text:
            try:
                float(first_text)
            except (ValueError, TypeError):
                # 先頭が数値でない → 坂路パターン（周回数が6F列に入っている）
                is_baro = True
                lap_count = first_text

        # 坂路シフトマップ: HTML上の列位置 → 実際の距離
        # 通常: [1200, 1000, 800, 600, 200]
        # 坂路: 先頭skip済みなので [1000→800, 800→600, 600→400, 200→200]
        shift = {1200: 1200, 1000: 1000, 800: 800, 600: 600, 200: 200}
        if is_baro:
            shift = {1000: 800, 800: 600, 600: 400, 200: 200}

        for i, cell in enumerate(cells):
            text = cell.get_text(strip=True)
            if i < len(dist_map):
                try:
                    val = float(text)
                    raw_dist = dist_map[i]
                    actual_dist = shift.get(raw_dist, raw_dist)
                    splits[actual_dist] = val
                except (ValueError, TypeError):
                    continue
            else:
                # 最後のセル = 周回数 "［7］" 等
                if text and not is_baro:
                    lap_count = text

        return splits, lap_count

    def _normalize_course(self, text: str) -> str:
        """コース表記を正規化。

        競馬ブックの元表記をそのまま保持する。
        例: 美坂, 栗坂, 美Ｗ, 栗ＣＷ, 美芝, 栗芝, 函館芝,
            函館ダ, 小倉ダ, 小林坂, 門別坂 等
        以前は "坂"→"坂路", "芝"→"芝コース" 等に統一していたが、
        コース別の基準タイムが異なるため区別を保持する。
        """
        if not text:
            return "不明"
        # 全角→半角の揺れだけ吸収（C.W → CW 等）
        text = text.replace("Ｃ．Ｗ", "ＣＷ").replace("C.W", "CW")
        return text

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
        競馬ブックの強度表記をそのまま返す。記載がなければ空文字。

        マスター指示 2026-04-23 v6.0.1:
          「ブックにない時に勝手にラップから推定して『極軽め』等を付けるのは禁止」。
          ブック原典に記載のある強度ラベルのみ採用し、無ければ空文字で UI 非表示扱いとする。
          旧実装の「推定ロジック」は廃止（calibration.py 側は別途検討）。
        """
        # 直接記載がある場合のみ採用
        for label in ["一杯", "強め", "馬なり", "軽め", "極軽め"]:
            if label in text:
                return label
        # 記載なし → 空文字（UI 側で非表示にする）
        return ""


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
# 後方互換: KeibabookResultScraper は keibabook_result.py に分離
# ============================================================
from src.scraper.keibabook_result import KeibabookResultScraper  # noqa: F401

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
