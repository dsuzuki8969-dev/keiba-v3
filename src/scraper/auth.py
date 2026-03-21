"""
競馬解析マスターシステム v3.0 - netkeiba ログイン認証
スーパープレミアムコース対応

取得できるようになる追加データ:
  - 調教タイム詳細（ラップ・追い切り強度）→ J-4 調教評価
  - 調教師コメント
  - 指数（netkeiba独自）
  - 馬柱 詳細版

認証フロー:
  1. ログインページでCSRFトークン取得
  2. ID/パスワードでPOST
  3. セッションクッキー維持
  4. 以降のリクエストはすべて同じsessionで実行

パスワードの保存:
  - 環境変数 NETKEIBA_ID / NETKEIBA_PASS を推奨
  - または ~/.keiba_credentials.json (権限600)
  - コード内への直書き厳禁
"""

import json
import os
import re
import shutil
import time
from pathlib import Path
from typing import Optional, Tuple

try:
    import requests
    from bs4 import BeautifulSoup

    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

from src.log import get_logger
from src.models import CourseMaster, Horse, RaceInfo, TrainingRecord
from src.scraper.netkeiba import BASE_URL, HEADERS, REQUEST_INTERVAL, RACE_URL, NetkeibaClient

logger = get_logger(__name__)

from config.settings import CACHE_DIR as _AUTH_DEFAULT_CACHE, DATA_DIR as _DATA_DIR
from data.masters.venue_master import JRA_CODES, get_venue_code_from_race_id, get_venue_name


def _build_race_from_result_cache(data: dict, all_courses: dict, race_id: str) -> Tuple[RaceInfo, list]:
    """結果ページの辞書から RaceInfo と Horse リストを構築（キャッシュフォールバック用）"""
    venue_code = data.get("venue_code", get_venue_code_from_race_id(race_id))
    surface = data.get("surface", "芝")
    distance = int(data.get("distance", 1600))
    course_id = f"{venue_code}_{surface}_{distance}"
    course = all_courses.get(course_id)
    if not course:
        venue = data.get("venue", get_venue_name(venue_code))
        course = CourseMaster(
            venue=venue,
            venue_code=venue_code,
            distance=distance,
            surface="芝" if "芝" in surface else "ダート",
            direction=data.get("direction", "右"),
            straight_m=350,
            corner_count=4,
            corner_type="大回り",
            first_corner="平均",
            slope_type="坂なし",
            inside_outside="なし",
            is_jra=venue_code in JRA_CODES,
        )
    race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
    cond = data.get("condition", "良")
    ri = RaceInfo(
        race_id=data.get("race_id", race_id),
        race_date=data.get("date", ""),
        venue=data.get("venue", get_venue_name(venue_code)),
        race_no=race_no,
        race_name=data.get("race_name", ""),
        grade=data.get("grade", ""),
        condition=cond,
        course=course,
        field_count=int(data.get("field_count", 0)),
        is_jra=data.get("is_jra", venue_code in JRA_CODES),
    )
    ri.track_condition_turf = cond if course.surface == "芝" else ""
    ri.track_condition_dirt = cond if course.surface == "ダート" else ""

    horses = []
    for h in data.get("horses", []):
        sex = (h.get("sex") or "牡")[:1]
        age = int(h.get("age") or 0)
        horses.append(
            Horse(
                horse_id=h.get("horse_id", ""),
                horse_name=h.get("horse_name", ""),
                sex=sex,
                age=age,
                color="",
                trainer=h.get("trainer", ""),
                trainer_id=h.get("trainer_id", ""),
                owner="",
                breeder="",
                sire="",
                dam="",
                race_date=ri.race_date,
                venue=venue_code,
                race_no=race_no,
                gate_no=int(h.get("gate_no") or 0),
                horse_no=int(h.get("horse_no") or 0),
                jockey=h.get("jockey", ""),
                jockey_id=h.get("jockey_id", ""),
                weight_kg=float(h.get("weight_kg") or 55),
                base_weight_kg=float(h.get("weight_kg") or 55),
                horse_weight=h.get("horse_weight"),
                weight_change=h.get("weight_change"),
                odds=h.get("odds"),
                popularity=h.get("popularity"),
                past_runs=[],
            )
        )
    return ri, horses


# ============================================================
# 定数
# ============================================================

LOGIN_URL = "https://regist.netkeiba.com/account/?pid=login"
LOGIN_POST = "https://regist.netkeiba.com/account/"
TRAINING_URL = "https://race.netkeiba.com/race/oikiri.html"
MEMBER_CHECK = "https://member.netkeiba.com/?pid=member_top"

CREDENTIALS_FILE = Path.home() / ".keiba_credentials.json"


# ============================================================
# 認証情報マネージャー
# ============================================================


class CredentialsManager:
    """
    認証情報の読み込み優先順位:
      1. 環境変数 NETKEIBA_ID / NETKEIBA_PASS
      2. ~/.keiba_credentials.json
      3. 引数で直接渡す
    """

    @staticmethod
    def load() -> Tuple[str, str]:
        """(netkeiba_id, password) を返す。見つからなければ ("", "")"""

        # 1. 環境変数
        env_id = os.environ.get("NETKEIBA_ID", "")
        env_pass = os.environ.get("NETKEIBA_PASS", "")
        if env_id and env_pass:
            return env_id, env_pass

        # 2. ~/.keiba_credentials.json
        if CREDENTIALS_FILE.exists():
            try:
                with open(CREDENTIALS_FILE, "r") as f:
                    creds = json.load(f)
                uid = creds.get("netkeiba_id", "")
                pwd = creds.get("netkeiba_pass", "")
                if uid and pwd:
                    return uid, pwd
            except Exception:
                logger.debug("credentials load failed", exc_info=True)

        return "", ""

    @staticmethod
    def save(netkeiba_id: str, password: str):
        """
        ~/.keiba_credentials.json に保存する。
        権限を 600 (本人のみ読み書き) に設定する。
        """
        data = {
            "netkeiba_id": netkeiba_id,
            "netkeiba_pass": password,
        }
        with open(CREDENTIALS_FILE, "w") as f:
            json.dump(data, f, indent=2)
        # Unix系: 権限を600に設定
        try:
            os.chmod(CREDENTIALS_FILE, 0o600)
        except Exception:
            logger.debug("chmod credentials failed", exc_info=True)
        logger.info(f"認証情報を保存しました: {CREDENTIALS_FILE}")
        logger.info("このファイルは本人以外が読めない権限(600)に設定済みです")

    @staticmethod
    def setup_interactive():
        """
        初回セットアップ用の対話式入力。
        パスワードは画面に表示しない。
        """
        import getpass

        logger.info("\n=== netkeiba 認証情報セットアップ ===")
        logger.info("入力した情報は ~/.keiba_credentials.json に保存されます")
        logger.info("環境変数 NETKEIBA_ID / NETKEIBA_PASS でも設定可能です\n")
        uid = input("netkeiba ID (メールアドレス): ").strip()
        pwd = getpass.getpass("パスワード: ")
        if uid and pwd:
            CredentialsManager.save(uid, pwd)
            return uid, pwd
        return "", ""


# ============================================================
# ログイン対応クライアント
# ============================================================


class AuthenticatedClient(NetkeibaClient):
    """
    NetkeibaClient を継承し、ログイン認証を追加。
    ログイン後はすべてのリクエストが会員セッションで実行される。
    """

    def __init__(
        self,
        netkeiba_id: str = "",
        password: str = "",
        cache_dir: str = None,
        no_cache: bool = False,
        ignore_ttl: bool = False,
    ):
        if cache_dir is None:
            cache_dir = _AUTH_DEFAULT_CACHE
        super().__init__(cache_dir, no_cache=no_cache, ignore_ttl=ignore_ttl)
        self._logged_in = False
        self._is_premium = False
        self._netkeiba_id = netkeiba_id
        self._password = password

        # 認証情報が渡されていなければ自動ロード
        if not (self._netkeiba_id and self._password):
            self._netkeiba_id, self._password = CredentialsManager.load()

    def login(self) -> bool:
        """
        netkeibaにログインする。
        Returns: 成功=True, 失敗=False
        """
        if not (self._netkeiba_id and self._password):
            logger.warning("認証情報が見つかりません")
            logger.warning("セットアップ: python -m src.setup_credentials")
            return False

        if self._logged_in:
            return True

        try:
            # Step 1: ログインページを取得してCSRFトークンを取る
            resp = self.session.get(LOGIN_URL, timeout=15)
            if resp.status_code == 403:
                logger.warning("ログインページ 403 (Akamai WAF ブロック)")
                return False
            resp.encoding = "euc-jp"
            soup = BeautifulSoup(resp.text, "lxml")

            # CSRFトークン (hidden input)
            csrf_token = ""
            for inp in soup.select("input[type=hidden]"):
                name = inp.get("name", "")
                if "token" in name.lower() or "csrf" in name.lower():
                    csrf_token = inp.get("value", "")
                    break

            # Step 2: ログインPOST（間隔を空ける）
            time.sleep(max(REQUEST_INTERVAL, 2.0))
            payload = {
                "pid": "login",
                "action": "auth",
                "login_id": self._netkeiba_id,
                "pswd": self._password,
                "return_url2": "",
                "mem_tp": "",
            }

            post_resp = self.session.post(
                LOGIN_POST,
                data=payload,
                headers={**HEADERS, "Referer": LOGIN_URL},
                timeout=15,
            )
            if post_resp.status_code == 403:
                logger.warning("ログインPOST 403 (Akamai WAF ブロック)")
                return False
            post_resp.encoding = "euc-jp"

            # Step 3: ログイン成功チェック（クッキー必須）
            has_auth_cookie = bool(
                self.session.cookies.get("nkauth")
                or self.session.cookies.get("netkeiba")
            )
            if not has_auth_cookie:
                logger.warning("ログイン後にセッションクッキーなし（ブロックの可能性）")
                return False

            if self._check_login_success(post_resp):
                self._logged_in = True
                self._is_premium = self._check_premium()
                grade = "スーパープレミアム" if self._is_premium else "一般会員"
                logger.info("ログイン成功: %s (%s)", self._netkeiba_id, grade)
                return True
            else:
                logger.warning("ログイン失敗: ID/パスワードを確認してください (URL: %s)", post_resp.url)
                return False

        except Exception as e:
            logger.warning("netkeiba login error: %s", e, exc_info=True)
            return False

    def _check_login_success(self, resp) -> bool:
        """ログイン成功を判定する"""
        # ログイン成功するとリダイレクトされるか、会員用コンテンツが表示される
        if "login" in resp.url.lower() and "action=auth" not in resp.url:
            return False
        text = resp.text
        # 失敗パターン
        fail_patterns = [
            "IDまたはパスワードが違います",
            "ログインに失敗",
            "id_error",
            "pass_error",
        ]
        for pattern in fail_patterns:
            if pattern in text:
                return False
        # 成功パターン
        success_patterns = [
            "ログアウト",
            "マイページ",
            "member_top",
        ]
        for pattern in success_patterns:
            if pattern in text:
                return True
        # クッキーにログインセッションがあれば成功とみなす
        for cookie in self.session.cookies:
            if "member" in cookie.name.lower() or "login" in cookie.name.lower():
                return True
        # リダイレクト先がログイン以外ならOK
        return "login" not in resp.url

    def _check_premium(self) -> bool:
        """スーパープレミアム会員かどうかを確認する"""
        # 方法1: db.netkeiba.com のプロフィールページで is_super_premium を確認
        try:
            resp = self.session.get(
                f"{BASE_URL}/jockey/05339/", timeout=15, headers=HEADERS
            )
            resp.encoding = "euc-jp"
            if "is_super_premium = '1'" in resp.text:
                return True
        except Exception:
            pass
        # 方法2: member.netkeiba.com で確認（フォールバック）
        try:
            resp = self.session.get(MEMBER_CHECK, timeout=15)
            resp.encoding = "euc-jp"
            t = resp.text
            return any(
                k in t
                for k in [
                    "スーパープレミアム",
                    "super_premium",
                    "プレミアムコース",
                    "プレミアム会員",
                    "super premium",
                ]
            )
        except Exception:
            return False

    @property
    def is_logged_in(self) -> bool:
        return self._logged_in

    @property
    def is_premium(self) -> bool:
        return self._is_premium

    def ensure_login(self) -> bool:
        """ログインが必要な操作の前に呼ぶ"""
        if not self._logged_in:
            return self.login()
        return True

    def clone(self) -> "AuthenticatedClient":
        """並列ワーカー用にクライアントを複製する（再ログイン不要）。"""
        c = NetkeibaClient.clone(self)
        c.__class__ = AuthenticatedClient
        c._logged_in = self._logged_in
        c._is_premium = self._is_premium
        c._netkeiba_id = self._netkeiba_id
        c._password = self._password
        return c


# ============================================================
# 調教データスクレイパー（プレミアム限定）
# ============================================================


class TrainingScraper:
    """
    追い切りタイム・調教内容を取得する
    URL: https://race.netkeiba.com/race/oikiri.html?race_id=XXXX

    プレミアム会員でないとラップ詳細が取れない場合がある。
    ログインなしでも概要は取れる。
    """

    def __init__(self, client: AuthenticatedClient):
        self.client = client

    def fetch(self, race_id: str) -> dict:
        """
        レースの全出走馬の調教データを取得する。

        Returns:
            {horse_id: [TrainingRecord]}
        """
        url = f"{TRAINING_URL}"
        soup = self.client.get(url, params={"race_id": race_id}, use_cache=False)
        if not soup:
            return {}

        result = {}
        for row in soup.select("table.OikiriTable tr, table.Training_Table tr"):
            try:
                data = self._parse_row(row)
                if not data:
                    continue
                hid = data.pop("horse_id", "")
                if hid:
                    result.setdefault(hid, []).append(TrainingRecord(**data))
            except Exception:
                logger.debug("training row parse failed", exc_info=True)
                continue

        return result

    def _parse_row(self, row) -> Optional[dict]:
        cells = row.select("td")
        if len(cells) < 5:
            return None

        # 馬IDの取得
        horse_link = row.select_one("a[href*='/horse/']")
        if not horse_link:
            return None
        m = re.search(r"/horse/([A-Za-z]?\d+)", horse_link.get("href", ""))
        horse_id = m.group(1) if m else ""

        # 調教日
        date_cell = cells[0].get_text(strip=True)
        dm = re.search(r"(\d{1,2})/(\d{1,2})", date_cell)
        if not dm:
            return None

        # 調教コース
        course_map = {
            "坂路": "坂路",
            "W": "ウッド",
            "CW": "CW",
            "P": "ポリトラック",
            "芝": "芝",
            "ダ": "ダート",
            "南W": "南W",
            "北W": "北W",
        }
        course_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        course = "坂路"
        for key, val in course_map.items():
            if key in course_text:
                course = val
                break

        # ラップタイム (例: 12.5-11.8-11.5-11.2)
        lap_text = ""
        splits = {}
        for ci in range(2, min(8, len(cells))):
            t = cells[ci].get_text(strip=True)
            if re.match(r"\d+\.\d+", t):
                lap_text = t
                break

        if lap_text:
            parts = re.findall(r"[\d.]+", lap_text)
            for i, p in enumerate(parts):
                try:
                    splits[f"{(i + 1) * 200}m"] = float(p)
                except ValueError:
                    pass

        # 強度ラベル推定
        intensity = self._infer_intensity(splits, course)

        return {
            "horse_id": horse_id,
            "date": date_cell,
            "venue": "",
            "course": course,
            "splits": splits,
            "intensity_label": intensity,
            "comment": "",
        }

    def _infer_intensity(self, splits: dict, course: str) -> str:
        """
        ラップタイムから調教強度を推定する。
        坂路の場合は4F〜1Fタイムで判断。
        ウッドの場合は5F〜1Fタイムで判断。

        設計書 J-4: 5段階（一杯/強め/馬なり/軽め/極軽め）
        """
        if not splits:
            return "馬なり"

        # 最後の200mタイム（上がり）
        last_split = list(splits.values())[-1] if splits else 12.0

        # 坂路基準
        if "坂路" in course:
            if last_split <= 11.5:
                return "一杯"
            if last_split <= 12.0:
                return "強め"
            if last_split <= 12.5:
                return "馬なり"
            if last_split <= 13.0:
                return "軽め"
            return "極軽め"

        # ウッド・CW基準
        if last_split <= 11.2:
            return "一杯"
        if last_split <= 11.8:
            return "強め"
        if last_split <= 12.3:
            return "馬なり"
        if last_split <= 13.0:
            return "軽め"
        return "極軽め"


# ============================================================
# プレミアム対応スクレイパーファサード
# ============================================================


class PremiumNetkeibaScraper:
    """
    認証済みクライアントを使う上位ファサード。
    NetkeibaScraper の認証対応版。
    """

    def __init__(
        self,
        all_courses: dict,
        netkeiba_id: str = "",
        password: str = "",
        cache_dir: str = None,
        no_cache: bool = False,
        quiet: bool = False,
        ignore_ttl: bool = False,
    ):
        if cache_dir is None:
            cache_dir = _AUTH_DEFAULT_CACHE
        from src.scraper.netkeiba import (
            HorseHistoryParser,
            OddsScraper,
            RaceEntryParser,
            RaceListScraper,
        )

        self.client = AuthenticatedClient(netkeiba_id, password, cache_dir, no_cache=no_cache,
                                          ignore_ttl=ignore_ttl)
        self._quiet = quiet
        self.races = RaceListScraper(self.client)
        self.entry = RaceEntryParser(self.client, all_courses)
        self.history = HorseHistoryParser(self.client)
        self.odds = OddsScraper(self.client)
        # 調教データは競馬ブックスマートプレミアムから取得（中央+地方対応）
        from src.scraper.keibabook_training import KbTrainingAdapter

        self.training = KbTrainingAdapter()
        # JRA/NAR公式スクレイパー（シングルトン：セッション・CNAMEキャッシュを維持）
        self._official_odds = None
        self._official_only = False  # --official フラグ

    def login(self) -> bool:
        return self.client.login()

    def clone_worker(self) -> "PremiumNetkeibaScraper":
        """並列フェッチ用のワーカークローンを作成する。
        HTTPセッション（Cookie）を共有し、レートリミットは独立。
        トレーニングクライアントは共有（別ドメイン・別レートリミット）。
        """
        from src.scraper.netkeiba import (
            HorseHistoryParser,
            OddsScraper,
            RaceEntryParser,
            RaceListScraper,
        )

        w = object.__new__(PremiumNetkeibaScraper)
        w.client = self.client.clone()
        w._quiet = True  # ワーカーモード: ログ抑制
        w.races = RaceListScraper(w.client)
        w.entry = RaceEntryParser(w.client, self.entry.courses)
        w.history = HorseHistoryParser(w.client)
        w.odds = OddsScraper(w.client)
        w.training = self.training  # keibabook クライアントは共有（別ドメイン）
        w._official_odds = self._official_odds  # 公式スクレイパーも共有（別ドメイン）
        w._official_only = self._official_only  # --official フラグも伝播
        return w

    def fetch_race(
        self,
        race_id: str,
        fetch_history: bool = True,
        fetch_odds: bool = True,
        fetch_training: bool = True,
        use_cache: bool = True,
        target_date: str = "",
        prefer_cache: bool = False,
    ):
        """
        1レース分のデータを完全取得する（プレミアム対応版）

        use_cache=True の場合、前回 fetch 結果を JSON キャッシュから復元し、
        ネット競馬への再スクレイピングを回避する（予想再生成時の高速化）。
        target_date: 分析対象日 (YYYY-MM-DD)。指定時はキャッシュの race_date と
                     整合性チェックし、不一致なら再取得する。
        prefer_cache: True の場合、netkeiba出走表をスキップしてキャッシュ済み
                      result.html → JRA/NAR公式 の順で取得する（過去レース用）。
        """
        # ── レースデータキャッシュ復元 ──
        if use_cache and fetch_history:
            try:
                from src.scraper.race_cache import load_race_cache, invalidate_race_cache
                cached = load_race_cache(race_id)
                if cached:
                    race_info, horses = cached
                    # 日付整合性チェック: キャッシュの race_date が
                    # target_date と一致しない場合は破棄して再取得
                    cached_date = getattr(race_info, "race_date", "")
                    td = target_date.replace("-", "").strip()
                    cd = cached_date.replace("-", "").strip() if cached_date else ""
                    if td and cd and td != cd:
                        logger.info(
                            "キャッシュ日付不整合 → 再取得: %s "
                            "(cached=%s, expected=%s)",
                            race_id, cached_date, target_date,
                        )
                        invalidate_race_cache(race_id)
                        cached = None  # fall through to re-fetch
                    # past_runsが1頭もない不完全キャッシュは破棄して再取得
                    elif fetch_history and not any(h.past_runs for h in horses):
                        logger.info(
                            "キャッシュ不完全（past_runsなし）→ 再取得: %s", race_id
                        )
                        invalidate_race_cache(race_id)
                        cached = None
                    # 性齢が「不明」の馬が半数以上 → 不良キャッシュ破棄
                    elif sum(1 for h in horses if getattr(h, "sex", "") == "不明") > len(horses) // 2:
                        logger.info(
                            "キャッシュ不完全（性齢不明）→ 再取得: %s", race_id
                        )
                        invalidate_race_cache(race_id)
                        cached = None
                    else:
                        if not self._quiet:
                            logger.info("キャッシュ復元: %s %d頭", race_info.race_name, len(horses))
                        # オッズは変動するので公式から再取得
                        if fetch_odds:
                            self._enrich_with_official(race_id, race_info, horses)
                        # 調教データは再取得
                        if fetch_training:
                            self._fetch_training_data(race_id, race_info, horses, {})
                        return race_info, horses
            except Exception:
                logger.debug("レースキャッシュ復元スキップ", exc_info=True)

        # E2E用: シードがあればキャッシュにコピー（netkeiba 400 時も分析を通す）
        cache_key = "race.netkeiba.com_race_result.html_race_id=" + race_id
        cache_path = os.path.join(self.client.cache_dir, cache_key + ".html")
        seed_path = os.path.join(_DATA_DIR, "e2e_seed", f"result_{race_id}.html")
        if os.path.isfile(seed_path) and not os.path.exists(cache_path):
            try:
                shutil.copy2(seed_path, cache_path)
                if not self._quiet:
                    logger.info("E2Eシードをキャッシュにコピー: %s", race_id)
            except OSError as e:
                logger.debug("e2e_seed copy failed: %s", e)

        # --official モード: ネット競馬をスキップして直接公式を使用
        if self._official_only:
            race_info, horses = self._fetch_from_official(race_id, fetch_history)
            if not race_info:
                return None, []
            # オッズは公式から
            if fetch_odds:
                self._enrich_with_official(race_id, race_info, horses)
            # 調教データ
            if fetch_training:
                self._fetch_training_data(race_id, race_info, horses, {})
            race_info.field_count = len(horses)
            # キャッシュ保存
            if use_cache and fetch_history:
                try:
                    from src.scraper.race_cache import save_race_cache
                    save_race_cache(race_id, race_info, horses)
                except Exception:
                    logger.debug("レースキャッシュ保存スキップ", exc_info=True)
            if not self._quiet:
                logger.info("公式完了: %s %d頭", race_info.race_name, len(horses))
            return race_info, horses

        # ── prefer_cache モード: キャッシュ済みresult.html → 公式 → netkeiba の順 ──
        race_info = None
        horses = []
        training_from_newspaper = {}

        if prefer_cache:
            # 1. ローカルキャッシュの result.html から構築（netkeiba不要）
            race_info, horses = self._build_from_cached_result(race_id)
            if race_info and not self._quiet:
                logger.info("キャッシュresult.htmlから構築: %s %d頭", race_info.race_name, len(horses))
            # 2. キャッシュなければ JRA/NAR 公式
            if not race_info:
                race_info, horses = self._fetch_from_official(race_id, fetch_history)
                if race_info and not self._quiet:
                    logger.info("公式フォールバック: %s %d頭", race_info.race_name, len(horses))
            # 3. それでもなければ netkeiba にフォールバック（従来フロー）
            if not race_info:
                if not self._quiet:
                    logger.info("キャッシュ/公式なし → netkeiba取得: %s", race_id)
                race_info, horses, training_from_newspaper = self._fetch_from_netkeiba(race_id)
        else:
            # 従来フロー: netkeiba → キャッシュ → 公式
            race_info, horses, training_from_newspaper = self._fetch_from_netkeiba(race_id)

        if not race_info:
            return None, []

        # ── 過去走取得 ──
        # _fetch_from_official 経由の場合は内部で過去走取得済み → スキップ
        _history_done = any(h.past_runs for h in horses)

        if fetch_history and not _history_done:
            # prefer_cache 時: 公式出走表は過去レースでは利用不可のため、
            # netkeiba馬ページキャッシュを利用（--ignore-ttl でディスクから読み込み）。
            # 当日/未来レースは _fetch_from_official が公式から取得済み。
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def _fetch_one(horse):
                runs = self.history.parse(horse.horse_id, horse=horse)
                return horse, runs

            with ThreadPoolExecutor(max_workers=3) as pool:
                futs = {pool.submit(_fetch_one, h): h for h in horses}
                done_count = 0
                for fut in as_completed(futs):
                    done_count += 1
                    horse, runs = fut.result()
                    horse.past_runs = runs
                    if runs:
                        horse.prev_jockey = runs[0].jockey
                    if not self._quiet:
                        logger.debug("馬過去走 %d/%d: %s", done_count, len(horses), horse.horse_name)

        # ── 性齢フォールバック: 過去走取得後も性齢が不明な馬がいたら
        # shutuba.html から再取得を試みる ──
        _unknown_sex = [h for h in horses if getattr(h, "sex", "") in ("不明", "")]
        if _unknown_sex:
            try:
                shutuba_soup = self.client.get(
                    f"{RACE_URL}/race/shutuba.html", params={"race_id": race_id}
                )
                if shutuba_soup:
                    for row in shutuba_soup.select("table.ShutubaTable tr.HorseList"):
                        horse_link = row.select_one("a[href*='/horse/']")
                        if not horse_link:
                            continue
                        _m = re.search(r"/horse/([A-Za-z]?\d+)", horse_link.get("href", ""))
                        if not _m:
                            continue
                        _hid = _m.group(1)
                        for h in _unknown_sex:
                            if h.horse_id == _hid:
                                cells = row.select("td")
                                for _i, _c in enumerate(cells):
                                    _cls = _c.get("class") or []
                                    if "Barei" in _cls:
                                        _t = _c.get_text(strip=True)
                                        _sm = re.match(r"([牡牝セ])(\d+)", _t)
                                        if _sm:
                                            h.sex = _sm.group(1)
                                            h.age = int(_sm.group(2))
                                        break
                                    # shutuba の性齢セル（印列なしの場合）
                                    _t = _c.get_text(strip=True)
                                    if re.match(r"^[牡牝セ]\d+$", _t):
                                        h.sex = _t[0]
                                        h.age = int(_t[1:])
                                        break
                                break
                    _still_unknown = sum(1 for h in horses if getattr(h, "sex", "") in ("不明", ""))
                    if _still_unknown < len(_unknown_sex):
                        logger.info("性齢補完: %d/%d頭", len(_unknown_sex) - _still_unknown, len(_unknown_sex))
            except Exception:
                logger.debug("性齢フォールバック失敗", exc_info=True)

        # ── オッズ取得 ──
        if fetch_odds:
            if prefer_cache:
                # キャッシュresult.htmlに確定オッズがあればそれを使う（既にHorseにセット済み）
                # なければ公式から取得
                if not any(h.odds for h in horses):
                    self._enrich_with_official(race_id, race_info, horses)
            else:
                odds_data = self.odds.get_tansho(race_id)
                for horse in horses:
                    if horse.horse_no in odds_data:
                        horse.odds, horse.popularity = odds_data[horse.horse_no]

        # 調教データ
        if fetch_training:
            self._fetch_training_data(race_id, race_info, horses, training_from_newspaper)

        race_info.field_count = len(horses)

        # ── マルチソース補完（JRA/NAR公式から馬体重・馬主・ID） ──
        if not prefer_cache:
            self._enrich_with_official(race_id, race_info, horses)

        # ── レースデータキャッシュ保存 ──
        if use_cache and fetch_history:
            try:
                from src.scraper.race_cache import save_race_cache
                save_race_cache(race_id, race_info, horses)
            except Exception:
                logger.debug("レースキャッシュ保存スキップ", exc_info=True)

        if not self._quiet:
            logger.info("完了: %s %d頭", race_info.race_name, len(horses))
        return race_info, horses

    def _build_from_cached_result(self, race_id: str):
        """ローカルキャッシュの result.html から RaceInfo + Horse[] を構築（netkeiba不要）"""
        from data.masters.venue_master import JRA_CODES, get_venue_code_from_race_id
        venue_code = get_venue_code_from_race_id(race_id)
        is_jra = venue_code in JRA_CODES

        # NARレースはNARキャッシュを優先（JRAキャッシュだと解析不完全）
        if is_jra:
            keys = [
                "race.netkeiba.com_race_result.html_race_id=" + race_id,
                "nar.netkeiba.com_race_result.html_race_id=" + race_id,
            ]
        else:
            keys = [
                "nar.netkeiba.com_race_result.html_race_id=" + race_id,
                "race.netkeiba.com_race_result.html_race_id=" + race_id,
            ]

        cache_path = None
        lz4_path = None
        for key in keys:
            cp = os.path.join(self.client.cache_dir, key + ".html")
            lp = cp + ".lz4"
            if os.path.exists(cp) or os.path.exists(lp):
                cache_path = cp
                lz4_path = lp
                break

        if not cache_path and not lz4_path:
            return None, []
        try:
            from src.scraper.ml_data_collector import parse_result_page

            # lz4 → プレーンHTML の優先順
            html = None
            if os.path.exists(lz4_path):
                try:
                    import lz4.frame
                    with open(lz4_path, "rb") as f:
                        html = lz4.frame.decompress(f.read()).decode("utf-8", errors="ignore")
                except Exception:
                    pass
            if html is None and os.path.exists(cache_path):
                with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
                    html = f.read()
            if not html:
                return None, []
            soup = BeautifulSoup(html, "lxml")
            data = parse_result_page(soup, race_id)
            if data and data.get("horses"):
                race_info, horses = _build_race_from_result_cache(
                    data, self.entry.courses, race_id
                )
                # ソースタグ付与（キャッシュ）
                if horses:
                    for h in horses:
                        h.source = "cache"
                return race_info, horses
        except Exception as e:
            logger.debug("cached result build failed: %s", e)
        return None, []

    def _fetch_from_netkeiba(self, race_id: str):
        """netkeiba から出走表を取得する従来フロー"""
        race_info = None
        horses = []
        training_from_newspaper = {}

        # ログイン
        if not self.client.ensure_login() and not self._quiet:
            logger.info("ログインなしで続行（一般会員範囲のみ）")

        if not self._quiet:
            logger.info("出走表取得: %s", race_id)
        race_info, horses, training_from_newspaper = self.entry.parse(race_id)
        # ソースタグ付与（netkeiba）
        if race_info and horses:
            for h in horses:
                if not h.source:
                    h.source = "netkeiba"

        # newspaper / shutuba 失敗時は result.html を取得して構築
        if not race_info:
            result_soup = self.client.get(
                f"{RACE_URL}/race/result.html", params={"race_id": race_id}
            )
            if result_soup:
                try:
                    from src.scraper.ml_data_collector import parse_result_page

                    data = parse_result_page(result_soup, race_id)
                    if data and data.get("horses"):
                        race_info, horses = _build_race_from_result_cache(
                            data, self.entry.courses, race_id
                        )
                        training_from_newspaper = {}
                        if not self._quiet:
                            logger.info(
                                "結果ページから構築: %s %d頭", race_info.race_name, len(horses)
                            )
                except Exception as e:
                    logger.debug("result.html fallback failed: %s", e)
        # まだ無ければキャッシュファイルの result.html から構築
        if not race_info:
            race_info, horses = self._build_from_cached_result(race_id)
            if race_info:
                training_from_newspaper = {}
        # JRA/NAR 公式フォールバック
        if not race_info:
            race_info, horses = self._fetch_from_official(race_id, fetch_history=True)
            training_from_newspaper = {}

        return race_info, horses, training_from_newspaper

    def _enrich_with_official(self, race_id: str, race_info, horses):
        """JRA/NAR公式サイトからの馬体重・馬主・ID・オッズ補完"""
        try:
            from src.scraper.multi_source import MultiSourceEnricher
            # シングルトン: セッション・CNAMEキャッシュ・レートリミットを維持
            if self._official_odds is None:
                from src.scraper.official_odds import OfficialOddsScraper
                self._official_odds = OfficialOddsScraper()
            enricher = MultiSourceEnricher(official_odds=self._official_odds)
            enricher.enrich(
                race_id, race_info, horses,
                fetch_odds=True,
                fetch_weights=True,
                fetch_ids=True,
            )
        except Exception:
            logger.debug("マルチソース補完スキップ", exc_info=True)

    def _fetch_training_data(self, race_id, race_info, horses, training_from_newspaper):
        """調教データ取得（競馬ブック → newspaper OikiriTable → oikiri.html）"""
        training_map = {}  # horse_name → [TrainingRecord]
        if self.training.is_logged_in:
            if not self._quiet:
                logger.info("調教データ取得: %s", race_id)
            # race_date を渡す（NAR の場合 nittei ルックアップに必要）
            race_date = getattr(race_info, "race_date", None)
            training_map = self.training.fetch(race_id, race_date=race_date)
        # newspaper OikiriTable で補完。netkeiba oikiri.html は中央競馬のみ（地方は非対応）
        oikiri_by_id = {}
        try:
            from data.masters.venue_master import JRA_CODES, get_venue_code_from_race_id

            is_jra = get_venue_code_from_race_id(race_id) in JRA_CODES
        except Exception:
            is_jra = True
        missing = [h for h in horses if not training_map.get(h.horse_name)]
        # KB非対応かつ newspaper データなし → netkeiba newspaper を直接取得して補完
        if missing and not training_from_newspaper:
            try:
                training_from_newspaper = self._fetch_newspaper_training(race_id, is_jra)
            except Exception:
                logger.debug("newspaper調教取得失敗", exc_info=True)
        if missing and self.client.is_logged_in and is_jra:
            try:
                ts = TrainingScraper(self.client)
                oikiri_by_id = ts.fetch(race_id)  # {horse_id: [TrainingRecord]}
            except Exception:
                logger.debug("oikiri fetch failed", exc_info=True)
        for horse in horses:
            records = training_map.get(horse.horse_name, [])
            if not records and training_from_newspaper:
                records = training_from_newspaper.get(horse.horse_name, [])
            if not records and oikiri_by_id and horse.horse_id:
                records = oikiri_by_id.get(horse.horse_id, [])
            if records:
                horse.training_records = records
                if not self._quiet:
                    best = records[0]
                    cm = f" 「{best.comment}」" if getattr(best, "comment", None) else ""
                    logger.debug(
                        "調教 %s: %s %s%s", horse.horse_name, best.intensity_label, best.course, cm
                    )

    def _fetch_newspaper_training(self, race_id: str, is_jra: bool) -> dict:
        """netkeiba newspaper.html から調教データのみ取得（KB非対応会場用フォールバック）"""
        from src.scraper.netkeiba import RACE_URL, NAR_URL

        base = RACE_URL if is_jra else NAR_URL
        soup = self.client.get(f"{base}/race/newspaper.html", params={"race_id": race_id})
        if not soup:
            # NAR は race.netkeiba.com にフォールバック
            if not is_jra:
                soup = self.client.get(f"{RACE_URL}/race/newspaper.html", params={"race_id": race_id})
            if not soup:
                return {}
        training = self.entry._parse_training_from_oikiri_table(soup)
        if training and not self._quiet:
            logger.info("newspaper調教データ取得: %s (%d頭)", race_id, len(training))
        return training

    def _fetch_from_official(self, race_id: str, fetch_history: bool = True):
        """JRA/NAR公式のみでレースデータを構築（ネット競馬フォールバック）"""
        from src.scraper.official_odds import OfficialOddsScraper, _JRA_VENUE_CODES

        venue_code = race_id[4:6]
        is_jra = venue_code in _JRA_VENUE_CODES

        if is_jra:
            try:
                if self._official_odds is None:
                    self._official_odds = OfficialOddsScraper()
                race_info, horses = self._official_odds.get_full_entry(race_id)
                if race_info and horses:
                    if not self._quiet:
                        logger.info(
                            "JRA公式フォールバック: %s %dR %d頭",
                            race_info.venue, race_info.race_no, len(horses),
                        )
                    # ソースタグ付与
                    for h in horses:
                        h.source = "official_jra"
                    # 過去走 + 血統を取得
                    if fetch_history:
                        self._fetch_jra_history_for_horses(
                            horses, self._official_odds
                        )
                    return race_info, horses
            except Exception as e:
                logger.warning("JRA公式フォールバック失敗: %s", e)
        else:
            # NAR公式フォールバック
            try:
                from src.scraper.official_nar import OfficialNARScraper
                nar = OfficialNARScraper()
                race_info, horses = nar.get_full_entry_from_race_id(race_id)
                if race_info and horses:
                    if not self._quiet:
                        logger.info(
                            "NAR公式フォールバック: %s %dR %d頭",
                            race_info.venue, race_info.race_no, len(horses),
                        )
                    # ソースタグ付与
                    for h in horses:
                        h.source = "official_nar"
                    # NAR馬詳細から過去走を取得
                    if fetch_history:
                        self._fetch_nar_history_for_horses(horses, nar)
                    return race_info, horses
            except ImportError:
                logger.debug("NAR公式スクレイパー未実装")
            except Exception as e:
                logger.warning("NAR公式フォールバック失敗: %s", e)

        return None, []

    def _fetch_jra_history_for_horses(self, horses, official):
        """JRA公式から各馬の過去走・血統を取得"""
        for i, horse in enumerate(horses):
            profile_cname = getattr(horse, "_profile_cname", "")
            if not profile_cname:
                continue
            try:
                past_runs, pedigree = official.fetch_horse_history(
                    profile_cname,
                    horse_name=horse.horse_name,
                    horse_id=horse.horse_id,
                    max_enrichment=3,
                )
                if past_runs:
                    horse.past_runs = past_runs
                    if past_runs:
                        horse.prev_jockey = past_runs[0].jockey
                # 血統情報を反映
                if pedigree:
                    if pedigree.get("sire"):
                        horse.sire = pedigree["sire"]
                    if pedigree.get("dam"):
                        horse.dam = pedigree["dam"]
                    if pedigree.get("maternal_grandsire"):
                        horse.maternal_grandsire = pedigree[
                            "maternal_grandsire"
                        ]
                    if pedigree.get("sire_id"):
                        horse.sire_id = pedigree["sire_id"]
                    if pedigree.get("dam_id"):
                        horse.dam_id = pedigree["dam_id"]
                    if pedigree.get("mgs_id"):
                        horse.maternal_grandsire_id = pedigree["mgs_id"]
                if not self._quiet:
                    logger.debug(
                        "JRA history %d/%d: %s %d走",
                        i + 1, len(horses),
                        horse.horse_name,
                        len(horse.past_runs),
                    )
            except Exception as e:
                logger.debug(
                    "JRA history failed for %s: %s",
                    horse.horse_name, e,
                )

    def _fetch_nar_history_for_horses(self, horses, nar):
        """NAR公式から各馬の過去走を取得"""
        for i, horse in enumerate(horses):
            lineage_code = getattr(horse, "_lineage_code", "")
            if not lineage_code:
                continue
            try:
                past_runs, pedigree = nar.fetch_horse_history(
                    lineage_code, horse_name=horse.horse_name,
                )
                if past_runs:
                    horse.past_runs = past_runs
                    if past_runs:
                        horse.prev_jockey = past_runs[0].jockey
                if pedigree:
                    if pedigree.get("sire") and not horse.sire:
                        horse.sire = pedigree["sire"]
                    if pedigree.get("dam") and not horse.dam:
                        horse.dam = pedigree["dam"]
                    if (pedigree.get("maternal_grandsire")
                            and not horse.maternal_grandsire):
                        horse.maternal_grandsire = pedigree[
                            "maternal_grandsire"
                        ]
                if not self._quiet:
                    logger.debug(
                        "NAR history %d/%d: %s %d走",
                        i + 1, len(horses),
                        horse.horse_name,
                        len(horse.past_runs),
                    )
            except Exception as e:
                logger.debug(
                    "NAR history failed for %s: %s",
                    horse.horse_name, e,
                )

    def fetch_date(self, date: str):
        netkeiba_ids = []
        if not self._official_only:
            netkeiba_ids = self.races.get_race_ids(date) or []

        # ── 常にJRA/NAR公式も取得して補完（netkeibaの制限時にNARが欠落するのを防ぐ） ──
        all_ids = list(netkeiba_ids)
        existing = set(all_ids)

        # JRA公式（netkeibaが空の場合のみ）
        if not netkeiba_ids:
            try:
                from src.scraper.official_odds import OfficialOddsScraper
                if self._official_odds is None:
                    self._official_odds = OfficialOddsScraper()
                jra_ids = self._official_odds.get_jra_race_list(target_date=date)
                if jra_ids:
                    new = [r for r in jra_ids if r not in existing]
                    logger.info("JRA公式: %d レース（新規%d）", len(jra_ids), len(new))
                    all_ids.extend(new)
                    existing.update(new)
            except Exception as e:
                logger.warning("JRA公式レース一覧取得失敗: %s", e)

        # NAR公式（常に補完 — netkeibaが制限中でもNARレースを確保）
        # ※ NAR公式(keiba.go.jp)はばんえいを含まない（帯広市独自開催のため）
        try:
            from src.scraper.official_nar import OfficialNARScraper
            nar = OfficialNARScraper()
            nar_ids = nar.get_race_ids(date)
            if nar_ids:
                new = [r for r in nar_ids if r not in existing]
                if new:
                    logger.info("NAR公式補完: %d レース追加", len(new))
                    all_ids.extend(new)
                    existing.update(new)
        except ImportError:
            logger.debug("NAR公式スクレイパー未実装")
        except Exception as e:
            logger.warning("NAR公式レース一覧取得失敗: %s", e)

        # ── ばんえい(帯広)補完 ──
        # NAR公式(keiba.go.jp)はばんえいを含まない。
        # netkeibaが制限中だとばんえいレースが欠落するため、
        # キャッシュ確認 → nar.netkeiba.com個別取得で補完する。
        if not any(rid[4:6] == "65" for rid in all_ids):
            banei_ids = self._supplement_banei(date)
            if banei_ids:
                new = [r for r in banei_ids if r not in existing]
                if new:
                    logger.info("ばんえい補完: %d レース追加", len(new))
                    all_ids.extend(new)
                    existing.update(new)

        return all_ids

    def _supplement_banei(self, date: str) -> list:
        """ばんえい(帯広)のレースIDを補完取得

        NAR公式(keiba.go.jp)はばんえいを含まないため、
        1. キャッシュにばんえいHTMLが存在すればID生成
        2. なければnar.netkeiba.comで1Rを試行し、成功すれば12R分生成
        """
        import glob as _glob_banei
        year = date[:4]
        mmdd = date[5:7] + date[8:10]

        # 方法1: キャッシュにばんえいレースが存在するか確認
        cache_dir = self.client.cache_dir
        banei_pattern = os.path.join(
            cache_dir, f"*race_id={year}65{mmdd}*"
        )
        cached = _glob_banei.glob(banei_pattern)
        if cached:
            # キャッシュからレース番号を検出
            race_nos = set()
            for path in cached:
                m = re.search(rf"{year}65{mmdd}(\d{{2}})", os.path.basename(path))
                if m:
                    race_nos.add(int(m.group(1)))
            max_race = max(race_nos) if race_nos else 12
            ids = [f"{year}65{mmdd}{rno:02d}" for rno in range(1, max_race + 1)]
            logger.info("ばんえいキャッシュ検出: %dR分のIDを生成", max_race)
            return ids

        # 方法2: nar.netkeiba.comで1R目を試行
        try:
            probe_id = f"{year}65{mmdd}01"
            from src.scraper.netkeiba import NAR_URL
            probe_url = f"{NAR_URL}/race/shutuba.html"
            probe_soup = self.client.get(probe_url, params={"race_id": probe_id})
            if probe_soup:
                # 出馬表テーブルが存在すれば開催日と判断
                if probe_soup.select("table.Shutuba_Table, table.RaceTable01, table"):
                    ids = [f"{year}65{mmdd}{rno:02d}" for rno in range(1, 13)]
                    logger.info("ばんえいプローブ成功: 12R分のIDを生成")
                    return ids
        except Exception as e:
            logger.debug("ばんえいプローブ失敗: %s", e)

        return []


# ============================================================
# CLI: 初回セットアップ & 動作確認
# ============================================================

if __name__ == "__main__":
    import argparse
    import sys

    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if _root not in sys.path:
        sys.path.insert(0, _root)

    p = argparse.ArgumentParser(description="netkeiba 認証管理")
    p.add_argument("--setup", action="store_true", help="認証情報を対話式で設定")
    p.add_argument("--check", action="store_true", help="ログインテスト")
    p.add_argument("--id", type=str, default="", help="netkeiba ID (引数で渡す場合)")
    p.add_argument("--password", type=str, default="", help="パスワード (引数で渡す場合)")
    args = p.parse_args()

    if args.setup:
        logger.info("=== 認証情報セットアップ ===")
        logger.info("")
        logger.info("--- 1. netkeiba (出走表・過去走・オッズ用) ---")
        uid, pwd = CredentialsManager.setup_interactive()
        if uid:
            client = AuthenticatedClient(uid, pwd)
            client.login()
        logger.info("")
        logger.info("--- 2. 競馬ブックスマートプレミアム（調教データ用）---")
        import getpass

        from src.scraper.keibabook_training import KeibabookClient, KeibabookCredentials

        kb_id = input("競馬ブック ID (メールアドレス): ").strip()
        kb_pwd = getpass.getpass("パスワード: ")
        if kb_id and kb_pwd:
            KeibabookCredentials.save(kb_id, kb_pwd)
            kb_client = KeibabookClient(kb_id, kb_pwd)
            kb_client.login()
    elif args.check:
        client = AuthenticatedClient(args.id, args.password)
        ok = client.login()
        if ok:
            logger.info("  ログイン: OK")
            logger.info(f"  スーパープレミアム: {'有効' if client.is_premium else '無効'}")
        else:
            logger.error("  ログイン: 失敗")
    else:
        p.print_help()
