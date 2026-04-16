"""
公式サイトからオッズを取得するスクレーパー

JRA: https://www.jra.go.jp/JRADB/accessO.html (POST + CNAME)
NAR: https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/OddsTanFuku (GET)
"""

import logging
import re
import threading
import time
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("keiba.scraper.official_odds")

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
}
_REQ_INTERVAL = 2.0  # リクエスト間隔（秒）

# ── JRA venue code → venue name mapping ──
_JRA_VENUE_NAMES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}

# ── JRA venue code mapping ──
# netkeiba race_id format: YYYYVVRRTTRR where VV = venue code
_JRA_VENUE_CODES = {
    "01": "01", "02": "02", "03": "03", "04": "04", "05": "05",
    "06": "06", "07": "07", "08": "08", "09": "09", "10": "10",
}

# ── NAR venue code mapping: netkeiba code → keiba.go.jp k_babaCode ──
_NAR_BABA_CODES = {
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
    "65": "3",    # 帯広（ばんえい）
}

# ── JRA venue name → venue code 逆引き ──
_JRA_NAME_TO_CODE = {v: k for k, v in _JRA_VENUE_NAMES.items()}


def _parse_payout_row_entries(cells) -> List[dict]:
    """払戻行のセルリストから組番号・払戻金・人気のエントリを抽出

    Args:
        cells: 券種名セル以降のtdセルリスト

    Returns:
        [{"combo": "3-8", "payout": 2340, "popularity": 8}, ...]
    """
    entries = []
    if not cells:
        return entries

    # セルのテキストを取得
    texts = [c.get_text(strip=True) for c in cells]

    # パターン1: [組番号, 払戻金, 人気] の3セット
    # パターン2: [組番号, 払戻金] の2セット（人気なし）
    # 組番号: 数字 or 数字-数字 or 数字-数字-数字
    combo_pat = re.compile(r"^\d{1,2}(?:\s*[-−ー]\s*\d{1,2}){0,2}$")
    # 払戻金: カンマ区切り数字、"円" 付きの場合もある
    payout_pat = re.compile(r"[\d,]+")
    # 人気: 数字のみ
    pop_pat = re.compile(r"^\d{1,3}$")

    i = 0
    while i < len(texts):
        txt = texts[i]
        if not txt:
            i += 1
            continue

        # 組番号を検出
        # ハイフン正規化（全角ダッシュ等）
        normalized = re.sub(r"[−ー―–]", "-", txt).strip()
        if not combo_pat.match(normalized):
            i += 1
            continue

        combo = normalized.replace(" ", "")
        payout = 0
        popularity = 0

        # 次のセルから払戻金を取得
        if i + 1 < len(texts):
            payout_text = texts[i + 1].replace(",", "").replace("円", "").replace("¥", "").strip()
            m = payout_pat.match(payout_text)
            if m:
                try:
                    payout = int(m.group().replace(",", ""))
                except ValueError:
                    pass

        # その次のセルから人気を取得
        if i + 2 < len(texts):
            pop_text = texts[i + 2].strip()
            if pop_pat.match(pop_text):
                try:
                    popularity = int(pop_text)
                except ValueError:
                    pass
            i += 3
        else:
            i += 2

        if payout > 0:
            entries.append({
                "combo": combo,
                "payout": payout,
                "popularity": popularity,
            })

    return entries


class OfficialOddsScraper:
    """JRA/NAR 公式サイトからオッズを取得"""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)
        self._last_req = 0.0
        # JRA の CNAME キャッシュ {race_key: cname}
        # race_key = "VV_KKNN_RR" (venue_code + 回日 + race_no) ← 土日衝突防止
        self._jra_cname_cache: Dict[str, str] = {}
        self._jra_cname_date: str = ""
        self._jra_cname_fetched_at: float = 0.0  # 最終取得時刻
        _CNAME_REFRESH_INTERVAL = 300  # 5分間は再取得しない
        # JRA 出馬表 CNAME キャッシュ（オッズとは別構造）
        self._jra_shutuba_cname_cache: Dict[str, str] = {}
        self._jra_shutuba_cname_fetched_at: float = 0.0
        # JRA 結果ページ CNAME キャッシュ
        self._jra_result_cname_cache: Dict[str, str] = {}
        self._jra_result_cname_fetched_at: float = 0.0
        # スレッドセーフなレートリミット用ロック
        self._lock = threading.Lock()
        # JRA レース結果ページキャッシュ（セッション内メモリ）
        self._result_page_cache: Dict[str, dict] = {}
        # ばんえい: 直近の DebaTable から取得した馬場水分量
        self._last_banei_moisture: Optional[float] = None

    def _wait(self):
        """リクエスト間隔を確保（スレッドセーフ）"""
        with self._lock:
            elapsed = time.time() - self._last_req
            if elapsed < _REQ_INTERVAL:
                time.sleep(_REQ_INTERVAL - elapsed)
            self._last_req = time.time()

    @staticmethod
    def _extract_kk_nn(cname: str) -> str:
        """CNAME から 回+日 (KKNN) を抽出する

        CNAME形式: pw0Xyyy XXYY YYYYKKNN RR YYYYMMDD /XX
                    [0:7]  [7:9][9:11][11:19][19:21][21:29]
        cname[11:19] = YYYYKKNN (年+回+日)
        cname[21:25] = YYYY (年)
        → 回+日 = cname[15:19] (YYYYの後の4桁)
        """
        if len(cname) < 19:
            return "0000"
        kai_nichi = cname[11:19]  # YYYYKKNN
        if len(cname) >= 29:
            year = cname[21:25]
            if kai_nichi[:4] == year:
                return kai_nichi[4:8]  # KKNN
        # フォールバック: 先頭4桁が年なら残り4桁
        return kai_nichi[4:8] if kai_nichi[:2] == "20" else kai_nichi[:4]

    # ================================================================
    # 公開 API
    # ================================================================

    def get_tansho(self, race_id: str) -> Dict[int, Tuple[float, int]]:
        """
        race_id (netkeiba形式) から単勝オッズ・人気を取得

        Returns: {馬番: (オッズ, 人気)}
        """
        venue_code = race_id[4:6]
        is_jra = venue_code in _JRA_VENUE_CODES
        if is_jra:
            return self._get_jra_odds(race_id)
        else:
            return self._get_nar_odds(race_id)

    def get_sanrenpuku_odds(self, race_id: str) -> Dict[Tuple[int, ...], float]:
        """三連複オッズを公式サイトから取得

        Returns: {(馬番1, 馬番2, 馬番3): オッズ}  ※タプルはソート済み
        """
        venue_code = race_id[4:6]
        if venue_code in _JRA_VENUE_CODES:
            return self._get_jra_sanrenpuku(race_id)
        else:
            return self._get_nar_sanrenpuku(race_id)

    def get_result_cname(self, race_id: str) -> str:
        """JRA結果ページのCNAMEを取得（キャッシュ付き）

        CNAME形式: pw01sde 01 JJ YYYYKKNN RR YYYYMMDD /XX
        [0:7]  [7:9][9:11][11:19][19:21][21:29][29:32]
        """
        if not race_id or len(race_id) < 12:
            return ""
        venue_code = race_id[4:6]
        if venue_code not in _JRA_VENUE_CODES:
            return ""
        race_no = int(race_id[10:12])
        if not race_no:
            return ""
        kk_nn = race_id[6:10] if len(race_id) >= 10 else "0000"
        cache_key = f"{venue_code}_{kk_nn}_{race_no:02d}"
        if cache_key in self._jra_result_cname_cache:
            return self._jra_result_cname_cache[cache_key]
        # 5分以内に取得済みなら再取得しない
        elapsed = time.time() - self._jra_result_cname_fetched_at
        if elapsed < 300:
            return ""
        self._fetch_jra_result_list()
        self._jra_result_cname_fetched_at = time.time()
        return self._jra_result_cname_cache.get(cache_key, "")

    def get_weights(self, race_id: str) -> Dict[int, Dict]:
        """
        race_id から馬体重・馬主を取得

        Returns: {馬番: {"weight": int, "weight_change": int, "owner": str}}
        """
        venue_code = race_id[4:6]
        is_jra = venue_code in _JRA_VENUE_CODES
        if is_jra:
            return self._get_jra_weights(race_id)
        else:
            return self._get_nar_weights(race_id)

    def get_banei_moisture(self, race_id: str) -> Optional[float]:
        """ばんえいの馬場水分量を取得。
        get_weights() と連携して使用: get_weights() 呼出後に
        _last_banei_moisture がセットされるため、追加リクエスト不要。
        単体で呼ばれた場合は DebaTable を取得する。
        """
        venue_code = race_id[4:6]
        if venue_code not in ("52", "65"):
            return None
        if self._last_banei_moisture is not None:
            return self._last_banei_moisture
        # get_weights が先に呼ばれていない場合のフォールバック
        self._get_nar_weights(race_id)
        return self._last_banei_moisture

    def get_jra_result(self, race_id: str) -> Optional[dict]:
        """JRA公式サイトからレース結果（着順・払戻・ラップ）を取得

        Args:
            race_id: netkeiba形式のレースID (YYYYVVRRTTRR)

        Returns:
            {
                "order": [{"horse_no": int, "finish": int, "last_3f": float, "corners": [int]}, ...],
                "payouts": {"tansho": [...], "fukusho": [...], ...},
                "lap_times": {"first_3f": float, "last_3f": float, "laps": [float]}
            }
            取得できない場合は None
        """
        if not race_id or len(race_id) < 12:
            return None
        venue_code = race_id[4:6]
        if venue_code not in _JRA_VENUE_CODES:
            logger.debug("get_jra_result: NARレースは非対応: %s", race_id)
            return None

        # Step 1: CNAME を取得
        cname = self.get_result_cname(race_id)
        if not cname:
            logger.debug("get_jra_result: CNAMEが見つからない: %s", race_id)
            return None

        # Step 2: 結果ページのHTMLを取得
        try:
            self._wait()
            resp = self._session.post(
                "https://www.jra.go.jp/JRADB/accessS.html",
                data={"cname": cname},
                headers={
                    **_HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": "https://www.jra.go.jp/JRADB/accessS.html",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("get_jra_result: HTTP %d: %s", resp.status_code, race_id)
                return None
            resp.encoding = "shift_jis"
            html = resp.text
        except Exception as e:
            logger.warning("get_jra_result: 取得失敗: %s (%s)", race_id, e)
            return None

        # Step 3: 着順テーブルをパース（orderリスト構築）
        order = self._parse_jra_result_order(html)

        # Step 4: 払戻テーブルをパース
        payouts = self._parse_jra_payouts(html)

        # Step 5: ラップタイムをパース
        lap_times = self._parse_jra_lap_times(html)

        result = {
            "order": order,
            "payouts": payouts,
        }
        if lap_times is not None:
            result["lap_times"] = lap_times

        logger.info(
            "get_jra_result: %s -> 着順%d頭, 払戻%d券種, ラップ%s",
            race_id, len(order), len(payouts),
            "あり" if lap_times else "なし",
        )
        return result

    # ================================================================
    # JRA 公式 (jra.go.jp)
    # ================================================================

    def _get_jra_odds(self, race_id: str) -> Dict[int, Tuple[float, int]]:
        """JRA 公式サイトから単勝オッズを取得"""
        venue_code = race_id[4:6]
        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        if not race_no:
            return {}

        # Step 1: CNAME を取得（キャッシュ or オッズ一覧ページ）
        cname = self._get_jra_cname(race_id)
        if not cname:
            return {}

        # Step 2: 単複オッズページ取得
        try:
            self._wait()
            resp = self._session.post(
                "https://www.jra.go.jp/JRADB/accessO.html",
                data={"cname": cname},
                headers={
                    **_HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": "https://www.jra.go.jp/JRADB/accessO.html",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("JRA odds page %d: %s", resp.status_code, race_id)
                return {}
            resp.encoding = "shift_jis"
        except Exception as e:
            logger.warning("JRA odds fetch failed: %s", e)
            return {}

        return self._parse_jra_odds_table(resp.text)

    def _get_jra_cname(self, race_id: str) -> str:
        """レース用のオッズ CNAME を取得"""
        venue_code = race_id[4:6]
        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        kk_nn = race_id[6:10] if len(race_id) >= 10 else "0000"

        # キャッシュ確認（回+日を含むキー）
        cache_key = f"{venue_code}_{kk_nn}_{race_no:02d}"
        if cache_key in self._jra_cname_cache:
            return self._jra_cname_cache[cache_key]

        # 5分以内に取得済みなら再取得しない（不要な再リクエスト防止）
        elapsed = time.time() - self._jra_cname_fetched_at
        if elapsed < 300:
            return ""

        # オッズ一覧ページから CNAME を取得
        self._fetch_jra_odds_list()
        self._jra_cname_fetched_at = time.time()
        return self._jra_cname_cache.get(cache_key, "")

    def _fetch_jra_odds_list(self):
        """JRA オッズ一覧ページを取得して CNAME を抽出（2段階方式）"""
        _POST_URL = "https://www.jra.go.jp/JRADB/accessO.html"
        _POST_HEADERS = {
            **_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://www.jra.go.jp/JRADB/accessO.html",
        }

        # ── Step 1: オッズ一覧ページ → 開催日別リンク（pw15orl00...）を取得 ──
        try:
            self._wait()
            resp = self._session.post(
                _POST_URL,
                data={"cname": "pw15oli00/6D"},
                headers={**_POST_HEADERS, "Referer": "https://www.jra.go.jp/keiba/"},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("JRA odds list %d", resp.status_code)
                return
            resp.encoding = "shift_jis"
        except Exception as e:
            logger.warning("JRA odds list fetch failed: %s", e)
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        do_action_pat = re.compile(
            r"doAction\('/JRADB/accessO\.html'\s*,\s*'([^']+)'\)"
        )

        # 開催日別リンク（pw15orl00...）と個別レースCNAME（pw151ou...）を収集
        venue_day_cnames = []
        for el in soup.select("[onclick]"):
            onclick = el.get("onclick", "")
            m = do_action_pat.search(onclick)
            if not m:
                continue
            cname = m.group(1)
            if cname.startswith("pw15orl00"):
                venue_day_cnames.append(cname)
            elif cname.startswith("pw151ou"):
                self._cache_jra_cname(cname)

        # ── Step 2: 各開催日ページ → 全レースの CNAME を取得 ──
        for vd_cname in venue_day_cnames:
            try:
                self._wait()
                resp2 = self._session.post(
                    _POST_URL,
                    data={"cname": vd_cname},
                    headers=_POST_HEADERS,
                    timeout=15,
                )
                if resp2.status_code != 200:
                    continue
                resp2.encoding = "shift_jis"
            except Exception:
                continue

            soup2 = BeautifulSoup(resp2.text, "html.parser")
            for el2 in soup2.select("[onclick]"):
                onclick2 = el2.get("onclick", "")
                m2 = do_action_pat.search(onclick2)
                if m2 and m2.group(1).startswith("pw151ou"):
                    self._cache_jra_cname(m2.group(1))

        logger.info("JRA CNAME %d レース分取得", len(self._jra_cname_cache))

    def _cache_jra_cname(self, cname: str):
        """CNAME をキャッシュに保存（回+日をキーに含め日付衝突を防止）"""
        # pw151ou S 3 06 20260203 11 20260307 Z /53
        # [0:7]  [7][8][9:11][11:19][19:21][21:29][29][30:33]
        if len(cname) < 21:
            return
        vc = cname[9:11]
        race_no_str = cname[19:21]
        try:
            rno = int(race_no_str)
        except ValueError:
            return
        # 回+日 を抽出してキーに含める（土日衝突防止）
        kk_nn = self._extract_kk_nn(cname)
        cache_key = f"{vc}_{kk_nn}_{rno:02d}"
        if cache_key not in self._jra_cname_cache:
            self._jra_cname_cache[cache_key] = cname
            logger.debug("JRA CNAME cached: %s -> %s", cache_key, cname[:45])

    def _parse_jra_odds_table(self, html: str) -> Dict[int, Tuple[float, int]]:
        """JRA オッズ HTML テーブルをパース

        テーブル構造: 枠, 馬番, 馬名, 単勝, 複勝(3着払い), 性齢, 馬体重, ...
        枠セルが rowspan の場合、後続行では枠セルが省略されるため
        カラムインデックスが1ずれる → rowspan追跡で補正
        """
        soup = BeautifulSoup(html, "html.parser")
        result: Dict[int, Tuple[float, int]] = {}

        table = soup.select_one("table")
        if not table:
            return result

        rows = table.select("tr")
        if not rows:
            return result

        # ── Step 1: ヘッダ行から馬番・単勝のカラム位置を特定 ──
        horse_no_idx = -1
        odds_idx = -1
        for header_row in rows:
            header_cells = header_row.select("th, td")
            headers = [c.get_text(strip=True) for c in header_cells]
            for i, h in enumerate(headers):
                if "馬番" in h:
                    horse_no_idx = i
                elif "単勝" in h and odds_idx < 0:
                    odds_idx = i
            if horse_no_idx >= 0 and odds_idx >= 0:
                break

        if horse_no_idx < 0 or odds_idx < 0:
            # フォールバック: 位置ベース (枠=0, 馬番=1, 馬名=2, 単勝=3)
            horse_no_idx = 1
            odds_idx = 3

        # ── Step 2: データ行をパース（rowspan追跡で枠セル省略に対応）──
        gate_rowspan_remaining = 0  # 枠セルのrowspan残り行数

        for row in rows:
            cells = row.select("td")
            if not cells:
                continue

            # rowspan による枠セル省略の判定:
            # 枠セルが省略されている行は、通常行より td が1少ない
            if gate_rowspan_remaining > 0:
                # この行は枠セルが省略されている → インデックスを -1 補正
                offset = -1
                gate_rowspan_remaining -= 1
            else:
                offset = 0
                # 最初のセルの rowspan を確認（枠セル）
                rs = cells[0].get("rowspan")
                if rs and rs.isdigit() and int(rs) > 1:
                    gate_rowspan_remaining = int(rs) - 1

            adj_horse_idx = horse_no_idx + offset
            adj_odds_idx = odds_idx + offset

            if adj_horse_idx < 0 or adj_odds_idx < 0:
                continue
            if len(cells) <= max(adj_horse_idx, adj_odds_idx):
                continue

            try:
                no_text = cells[adj_horse_idx].get_text(strip=True)
                odds_text = cells[adj_odds_idx].get_text(strip=True)

                if not no_text.isdigit():
                    continue
                horse_no = int(no_text)
                if not (1 <= horse_no <= 18):
                    continue

                # オッズテキストのクリーニング
                odds_text = odds_text.replace(",", "").strip()
                if not odds_text or odds_text in ("---", "---.-", "-", ""):
                    continue
                # 複勝の範囲表記（"2.3 - 3.9"）を除外
                if " - " in odds_text or "-" in odds_text.replace(".", ""):
                    # ただし "---.-" 等は上で除外済み、"-" 付きは非数値
                    try:
                        odds_val = float(odds_text)
                    except ValueError:
                        continue
                else:
                    odds_val = float(odds_text)

                if 0.1 < odds_val < 9999:
                    if horse_no in result:
                        logger.warning("JRA odds: 馬番%d 重複検出 (既存=%.1f, 新=%.1f)",
                                       horse_no, result[horse_no][0], odds_val)
                    result[horse_no] = (odds_val, 0)
            except (ValueError, TypeError):
                continue

        # 人気順を計算（オッズ昇順）
        if result:
            sorted_by_odds = sorted(result.items(), key=lambda x: x[1][0])
            for rank, (no, (odds, _)) in enumerate(sorted_by_odds, 1):
                result[no] = (odds, rank)

        return result

    # ================================================================
    # JRA 馬体重 (jra.go.jp 出馬表)
    # ================================================================

    def _get_jra_weights(self, race_id: str) -> Dict[int, Dict]:
        """JRA 公式 出馬表ページから馬体重・馬主を取得（2段階CNAME方式）"""
        venue_code = race_id[4:6]
        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        if not race_no:
            return {}

        # 出馬表用 CNAME を取得
        cname = self._get_jra_shutuba_cname(race_id)
        if not cname:
            return {}

        try:
            self._wait()
            resp = self._session.post(
                "https://www.jra.go.jp/JRADB/accessD.html",
                data={"cname": cname},
                headers={
                    **_HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": "https://www.jra.go.jp/JRADB/accessD.html",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                logger.debug("JRA shutuba page %d: %s", resp.status_code, race_id)
                return {}
            resp.encoding = "shift_jis"
        except Exception as e:
            logger.debug("JRA shutuba fetch failed: %s", e)
            return {}

        return self._parse_jra_shutuba(resp.text)

    def _get_jra_shutuba_cname(self, race_id: str) -> str:
        """出馬表用の CNAME を取得（キャッシュ or 一覧ページから）"""
        venue_code = race_id[4:6]
        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        kk_nn = race_id[6:10] if len(race_id) >= 10 else "0000"
        cache_key = f"{venue_code}_{kk_nn}_{race_no:02d}"

        if cache_key in self._jra_shutuba_cname_cache:
            return self._jra_shutuba_cname_cache[cache_key]

        # 5分以内に取得済みなら再取得しない
        elapsed = time.time() - self._jra_shutuba_cname_fetched_at
        if elapsed < 300:
            return ""

        self._fetch_jra_shutuba_list()
        self._jra_shutuba_cname_fetched_at = time.time()
        return self._jra_shutuba_cname_cache.get(cache_key, "")

    def _fetch_jra_shutuba_list(self):
        """JRA 出馬表一覧を取得して個別レース CNAME を抽出（2段階方式）

        Step 1: pw01dli00/F3 → 開催日別リンク（pw01drl00...）
        Step 2: 各開催日ページ → 個別レースリンク（pw01dde...）
        """
        _POST_URL = "https://www.jra.go.jp/JRADB/accessD.html"
        _POST_HEADERS = {
            **_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://www.jra.go.jp/JRADB/accessD.html",
        }

        # ── Step 1: 出馬表一覧 → 開催日別リンク ──
        try:
            self._wait()
            resp = self._session.post(
                _POST_URL,
                data={"cname": "pw01dli00/F3"},
                headers={**_POST_HEADERS, "Referer": "https://www.jra.go.jp/keiba/"},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("JRA shutuba list %d", resp.status_code)
                return
            resp.encoding = "shift_jis"
        except Exception as e:
            logger.warning("JRA shutuba list fetch failed: %s", e)
            return

        soup = BeautifulSoup(resp.text, "html.parser")

        # doAction リンクから開催日別 CNAME を収集
        do_action_pat = re.compile(
            r"doAction\('/JRADB/accessD\.html'\s*,\s*'([^']+)'\)"
        )
        venue_day_cnames = []
        for el in soup.select("[onclick]"):
            onclick = el.get("onclick", "")
            m = do_action_pat.search(onclick)
            if m and m.group(1).startswith("pw01drl"):
                venue_day_cnames.append(m.group(1))

        # <a> リンクからも個別レース CNAME を収集
        for a_tag in soup.select("a[href*='accessD.html?CNAME=pw01dde']"):
            href = a_tag.get("href", "")
            cname_m = re.search(r"CNAME=([^&\"']+)", href)
            if cname_m:
                self._cache_jra_shutuba_cname(cname_m.group(1))

        # ── Step 2: 各開催日ページ → 個別レース CNAME ──
        for vd_cname in venue_day_cnames:
            try:
                self._wait()
                resp2 = self._session.post(
                    _POST_URL,
                    data={"cname": vd_cname},
                    headers=_POST_HEADERS,
                    timeout=15,
                )
                if resp2.status_code != 200:
                    continue
                resp2.encoding = "shift_jis"
            except Exception:
                continue

            soup2 = BeautifulSoup(resp2.text, "html.parser")
            # <a> リンクから個別レース CNAME (pw01dde...) を収集
            for a_tag in soup2.select("a[href*='accessD.html?CNAME=pw01dde']"):
                href = a_tag.get("href", "")
                cname_m = re.search(r"CNAME=([^&\"']+)", href)
                if cname_m:
                    self._cache_jra_shutuba_cname(cname_m.group(1))

        logger.info("JRA shutuba CNAME %d レース分取得", len(self._jra_shutuba_cname_cache))

    def _cache_jra_shutuba_cname(self, cname: str):
        """出馬表 CNAME をキャッシュに保存（回+日をキーに含め日付衝突を防止）

        pw01dde XXYY YYYYYYYY RR YYYYMMDD /XX
        [0:7]  [7:9][9:11][11:19][19:21][21:29][30:33]
        """
        if len(cname) < 21:
            return
        vc = cname[9:11]
        race_no_str = cname[19:21]
        try:
            rno = int(race_no_str)
        except ValueError:
            return
        kk_nn = self._extract_kk_nn(cname)
        cache_key = f"{vc}_{kk_nn}_{rno:02d}"
        if cache_key not in self._jra_shutuba_cname_cache:
            self._jra_shutuba_cname_cache[cache_key] = cname
            logger.debug("JRA shutuba CNAME cached: %s -> %s", cache_key, cname[:45])

    # ================================================================
    # JRA 結果ページ CNAME
    # ================================================================

    def _fetch_jra_result_list(self):
        """JRA 成績一覧を取得して個別レース CNAME を抽出（3段階方式）

        Step 1: pw01sli00/F3 → 中間ページ（pw01skl00999999/B3）
        Step 2: pw01skl... → 開催日別リンク（pw01srl00...）
        Step 3: 各開催日ページ → 個別レースリンク（pw01sde... in <a> href）
        """
        _POST_URL = "https://www.jra.go.jp/JRADB/accessS.html"
        _POST_HEADERS = {
            **_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://www.jra.go.jp/JRADB/accessS.html",
        }
        do_action_pat = re.compile(
            r"doAction\('/JRADB/accessS\.html'\s*,\s*'([^']+)'\)"
        )

        def _extract_cnames(soup_obj):
            """onclick + <a> href 両方から CNAME を収集"""
            found = {}
            for el in soup_obj.select("[onclick]"):
                onclick = el.get("onclick", "")
                m = do_action_pat.search(onclick)
                if m:
                    found[m.group(1)] = "onclick"
            for a_tag in soup_obj.select("a"):
                href = a_tag.get("href", "")
                m = re.search(r"CNAME=([^&\"']+)", href)
                if m:
                    found[m.group(1)] = "href"
            return found

        # ── Step 1: 成績一覧トップ ──
        try:
            self._wait()
            resp = self._session.post(
                _POST_URL,
                data={"cname": "pw01sli00/F3"},
                headers={**_POST_HEADERS, "Referer": "https://www.jra.go.jp/keiba/"},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("JRA result list %d", resp.status_code)
                return
            resp.encoding = "shift_jis"
        except Exception as e:
            logger.warning("JRA result list fetch failed: %s", e)
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        cnames1 = _extract_cnames(soup)

        # pw01skl... (過去のレース結果) を見つけて遷移
        intermediate = None
        venue_day_cnames = []
        for cn in cnames1:
            if cn.startswith("pw01skl"):
                intermediate = cn
            elif cn.startswith("pw01srl00"):
                venue_day_cnames.append(cn)
            elif cn.startswith("pw01sde"):
                self._cache_jra_result_cname(cn)

        # ── Step 2: 中間ページ → 開催日別リンク ──
        # ※ pw01sde は過去日と当日が混在するため、ここではキャッシュしない
        if intermediate and not venue_day_cnames:
            try:
                self._wait()
                resp2 = self._session.post(
                    _POST_URL,
                    data={"cname": intermediate},
                    headers=_POST_HEADERS,
                    timeout=15,
                )
                if resp2.status_code == 200:
                    resp2.encoding = "shift_jis"
                    soup2 = BeautifulSoup(resp2.text, "html.parser")
                    cnames2 = _extract_cnames(soup2)
                    for cn in cnames2:
                        if cn.startswith("pw01srl00"):
                            venue_day_cnames.append(cn)
            except Exception as e:
                logger.warning("JRA result intermediate fetch failed: %s", e)

        # ── Step 3: 各開催日ページ → 個別レース CNAME ──
        for vd_cname in venue_day_cnames:
            try:
                self._wait()
                resp3 = self._session.post(
                    _POST_URL,
                    data={"cname": vd_cname},
                    headers=_POST_HEADERS,
                    timeout=15,
                )
                if resp3.status_code != 200:
                    continue
                resp3.encoding = "shift_jis"
            except Exception:
                continue

            soup3 = BeautifulSoup(resp3.text, "html.parser")
            cnames3 = _extract_cnames(soup3)
            for cn in cnames3:
                if cn.startswith("pw01sde"):
                    self._cache_jra_result_cname(cn)

        logger.info("JRA result CNAME %d レース分取得", len(self._jra_result_cname_cache))

    def _cache_jra_result_cname(self, cname: str):
        """結果 CNAME をキャッシュに保存（回+日をキーに含め日付衝突を防止）

        pw01sde XXYY YYYYYYYY RR YYYYMMDD /XX
        [0:7]  [7:9][9:11][11:19][19:21][21:29][29:32]
        """
        if len(cname) < 21:
            return
        vc = cname[9:11]
        race_no_str = cname[19:21]
        try:
            rno = int(race_no_str)
        except ValueError:
            return
        kk_nn = self._extract_kk_nn(cname)
        cache_key = f"{vc}_{kk_nn}_{rno:02d}"
        if cache_key not in self._jra_result_cname_cache:
            self._jra_result_cname_cache[cache_key] = cname
            logger.debug("JRA result CNAME cached: %s -> %s", cache_key, cname[:45])

    def _parse_jra_shutuba(self, html: str) -> Dict[int, Dict]:
        """JRA 出馬表HTMLから馬体重・馬主・公式IDをパース

        構造: 各行 tr に
          - td.num: 馬番
          - td.horse 内:
            - div.cell.weight: "448kg<span class='transition'>(0)</span>"
            - p.owner: 馬主名
          - リンク内 CNAME から公式ID抽出:
            - pw01dud00 + 10桁 → horse_id (netkeibaと同一)
            - pw04kmk00 + 4桁  → jockey_id (5桁にゼロパッド)
            - pw05cmk00 + 4桁  → trainer_id (5桁にゼロパッド)
        """
        soup = BeautifulSoup(html, "html.parser")
        result: Dict[int, Dict] = {}

        # ID抽出用正規表現
        _horse_id_pat = re.compile(r"pw01dud00(\d{10})")
        _jockey_id_pat = re.compile(r"pw04kmk00(\d{4,5})")
        _trainer_id_pat = re.compile(r"pw05cmk00(\d{4,5})")

        for row in soup.select("tr"):
            # 馬番セル (td.num)
            num_cell = row.select_one("td.num")
            if not num_cell:
                continue
            num_text = num_cell.get_text(strip=True)
            if not num_text.isdigit():
                continue
            horse_no = int(num_text)

            horse_cell = row.select_one("td.horse")
            if not horse_cell:
                continue

            weight = None
            change = 0
            owner = ""

            # 馬体重: div.cell.weight
            wt_div = horse_cell.select_one("div.weight")
            if wt_div:
                wt_text = wt_div.get_text(strip=True)
            else:
                # フォールバック: セル全体から探す
                wt_text = horse_cell.get_text(strip=True)

            m = re.search(r"(\d{3,4})kg\(([+-]?\d+)\)", wt_text)
            if m:
                weight = int(m.group(1))
                change = int(m.group(2))
            else:
                m2 = re.search(r"(\d{3,4})kg", wt_text)
                if m2:
                    weight = int(m2.group(1))

            # 馬主: p.owner
            owner_el = horse_cell.select_one("p.owner")
            if owner_el:
                owner = owner_el.get_text(strip=True)

            # ── 公式ID・名前抽出（行内の全リンクから） ──
            horse_id_official = ""
            jockey_id_official = ""
            jockey_name_official = ""
            trainer_id_official = ""
            trainer_name_official = ""

            for a_tag in row.select("a[href], a[onclick], [onclick]"):
                href = a_tag.get("href", "") + " " + a_tag.get("onclick", "")
                # 馬ID: pw01dud00 + 10桁
                hm = _horse_id_pat.search(href)
                if hm:
                    horse_id_official = hm.group(1)
                # 騎手ID: pw04kmk00 + 4桁 → 5桁ゼロパッド + リンクテキスト=騎手名
                jm = _jockey_id_pat.search(href)
                if jm:
                    jockey_id_official = jm.group(1).zfill(5)
                    _jname = a_tag.get_text(strip=True)
                    if _jname and not _jname.isdigit():
                        jockey_name_official = _jname
                # 調教師ID: pw05cmk00 + 4桁 → 5桁ゼロパッド + リンクテキスト=調教師名
                tm = _trainer_id_pat.search(href)
                if tm:
                    trainer_id_official = tm.group(1).zfill(5)
                    _tname = a_tag.get_text(strip=True)
                    if _tname and not _tname.isdigit():
                        trainer_name_official = _tname

            # 馬体重未発表でもID・名前情報は返す
            entry = {
                "weight": weight,
                "weight_change": change,
                "owner": owner,
                "horse_id": horse_id_official,
                "jockey_id": jockey_id_official,
                "jockey_name": jockey_name_official,
                "trainer_id": trainer_id_official,
                "trainer_name": trainer_name_official,
            }
            result[horse_no] = entry

        if result:
            id_count = sum(1 for v in result.values() if v.get("horse_id"))
            logger.info("JRA shutuba: %d頭取得 (owner: %d, IDs: %d)",
                        len(result),
                        sum(1 for v in result.values() if v.get("owner")),
                        id_count)
        return result

    # ================================================================
    # JRA 出馬表フル解析 (Phase 1)
    # ================================================================

    def get_full_entry(self, race_id: str):
        """JRA公式出馬表から完全なレースデータ（RaceInfo + List[Horse]）を取得

        Returns: (RaceInfo, List[Horse]) or (None, [])
        """
        from src.models import RaceInfo, Horse, CourseMaster
        from data.masters.course_master import ALL_COURSES

        venue_code = race_id[4:6]
        if venue_code not in _JRA_VENUE_CODES:
            logger.debug("get_full_entry: Not JRA venue code %s", venue_code)
            return None, []

        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        if not race_no:
            return None, []

        # 出馬表用 CNAME を取得
        cname = self._get_jra_shutuba_cname(race_id)
        if not cname:
            logger.warning("get_full_entry: No CNAME for %s", race_id)
            return None, []

        try:
            self._wait()
            resp = self._session.post(
                "https://www.jra.go.jp/JRADB/accessD.html",
                data={"cname": cname},
                headers={
                    **_HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": "https://www.jra.go.jp/JRADB/accessD.html",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("get_full_entry: HTTP %d for %s", resp.status_code, race_id)
                return None, []
            resp.encoding = "shift_jis"
        except Exception as e:
            logger.warning("get_full_entry: fetch failed: %s", e)
            return None, []

        return self._parse_jra_shutuba_full(resp.text, race_id, cname)

    def get_jra_race_list(self, target_date: str = "") -> List[str]:
        """JRAレースID一覧を取得（shutuba CNAMEキャッシュから）

        target_date: "YYYY-MM-DD" or "YYYYMMDD" 形式。指定時はその日のレースのみ返す。
                     未指定時は全キャッシュ分（土日両日）を返す。
        """
        self._fetch_jra_shutuba_list()
        self._jra_shutuba_cname_fetched_at = time.time()

        # 日付フィルタ正規化
        date_filter = target_date.replace("-", "") if target_date else ""

        race_ids = []
        for cache_key, cname in self._jra_shutuba_cname_cache.items():
            # 日付フィルタ: CNAME[21:29] = YYYYMMDD
            if date_filter and len(cname) >= 29:
                cname_date = cname[21:29]
                if cname_date != date_filter:
                    continue
            rid = self._cname_to_race_id(cname)
            if rid:
                race_ids.append(rid)

        race_ids.sort()
        logger.info("JRA race list: %d レース%s", len(race_ids),
                     f" (date={target_date})" if target_date else "")
        return race_ids

    def _cname_to_race_id(self, cname: str) -> str:
        """出馬表 CNAME から netkeiba 形式の race_id を構築

        CNAME format: pw01dde XXYY YYYYYYYY RR YYYYMMDD /XX
        [0:7]  [7:9][9:11][11:19][19:21][21:29][29:32]

        race_id format: YYYY VV KK NN RR (12桁)
        year=cname[21:25], venue=cname[9:11], kai/nichi=cname[11:19]の一部, race_no=cname[19:21]
        """
        if len(cname) < 29:
            return ""
        try:
            year = cname[21:25]
            venue_code = cname[9:11]
            # KKKK NNNN 部分は cname[11:19] だが、そこには日付が詰まっている
            # 実際の race_id 構造: YYYY + VV + KKKK + RR
            # cname[11:19] は YYYYKKNN 形式: 年+回+日
            kai_nichi = cname[11:19]  # e.g., "20260203" → 年を除いた回+日
            # race_id の KK NN 部分 (4桁)
            if kai_nichi[:4] == year:
                kk_nn = kai_nichi[4:8]  # 回+日: e.g., "0203"
            else:
                kk_nn = kai_nichi[:4]   # フォールバック
            race_no = cname[19:21]
            return f"{year}{venue_code}{kk_nn}{race_no}"
        except (ValueError, IndexError):
            return ""

    def _parse_jra_shutuba_full(self, html: str, race_id: str, cname: str = ""):
        """JRA 出馬表HTMLから RaceInfo + Horse[] をフル構築

        Returns: (RaceInfo, List[Horse]) or (None, [])
        """
        from src.models import RaceInfo, Horse, CourseMaster
        from data.masters.course_master import ALL_COURSES

        soup = BeautifulSoup(html, "html.parser")

        venue_code = race_id[4:6]
        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        venue_name = _JRA_VENUE_NAMES.get(venue_code, "")

        # ── レースヘッダ解析 ──
        race_name = ""
        grade = ""
        distance = 0
        surface = ""
        direction = ""
        race_date = ""
        post_time = ""
        class_name = ""

        # レース名
        name_el = soup.select_one("span.race_name, h1.race_name, .race_name")
        if name_el:
            race_name = name_el.get_text(strip=True)

        # グレード (G1/G2/G3)
        grade_img = soup.select_one("span.grade_icon img, img[alt*='G']")
        if grade_img:
            alt = grade_img.get("alt", "")
            if "GI" in alt and "II" not in alt and "III" not in alt:
                grade = "G1"
            elif "GII" in alt and "III" not in alt:
                grade = "G2"
            elif "GIII" in alt:
                grade = "G3"

        # 距離・馬場
        course_el = soup.select_one("div.cell.course, span.course_detail, .race_course_detail")
        if course_el:
            course_text = course_el.get_text(strip=True)
            m_dist = re.search(r"(\d{1,2},?\d{3})メートル", course_text)
            if m_dist:
                distance = int(m_dist.group(1).replace(",", ""))
            if "ダート" in course_text:
                surface = "ダート"
            elif "芝" in course_text:
                surface = "芝"
            m_dir = re.search(r"(右|左|直線)", course_text)
            if m_dir:
                direction = m_dir.group(1)

        # フォールバック: ページ内のテキストから距離を探す
        if not distance:
            for el in soup.select("div, span, td"):
                text = el.get_text(strip=True)
                m = re.search(r"(\d{1,2},?\d{3})m", text)
                if m:
                    distance = int(m.group(1).replace(",", ""))
                    break

        # 日付
        date_el = soup.select_one("div.cell.date, .race_date")
        if date_el:
            date_text = date_el.get_text(strip=True)
            m_date = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_text)
            if m_date:
                race_date = f"{m_date.group(1)}-{int(m_date.group(2)):02d}-{int(m_date.group(3)):02d}"

        # 日付フォールバック: CNAMEから
        if not race_date and len(cname) >= 29:
            y = cname[21:25]
            m = cname[25:27]
            d = cname[27:29]
            try:
                race_date = f"{y}-{int(m):02d}-{int(d):02d}"
            except ValueError:
                pass

        # 日付フォールバック: race_id から
        if not race_date:
            from datetime import datetime
            race_date = datetime.now().strftime("%Y-%m-%d")

        # 発走時刻
        time_el = soup.select_one("div.cell.time, .post_time")
        if time_el:
            time_text = time_el.get_text(strip=True)
            m_time = re.search(r"(\d{1,2})時(\d{2})分", time_text)
            if m_time:
                post_time = f"{int(m_time.group(1)):02d}:{m_time.group(2)}"

        # クラス
        class_el = soup.select_one("div.cell.class, .race_class")
        if class_el:
            class_name = class_el.get_text(strip=True)

        # ── CourseMaster 検索 ──
        course = None
        if venue_code and surface and distance:
            course_id = f"{venue_code}_{surface}_{distance}"
            for c in ALL_COURSES:
                if c.course_id == course_id:
                    course = c
                    break
        # フォールバック: 最も近いコースを探す
        if not course and venue_code and distance:
            best = None
            best_diff = 9999
            for c in ALL_COURSES:
                if c.venue_code == venue_code:
                    diff = abs(c.distance - distance)
                    if diff < best_diff:
                        best = c
                        best_diff = diff
            if best:
                course = best
        # 最終フォールバック: ダミーコース
        if not course:
            course = CourseMaster(
                venue=venue_name, venue_code=venue_code,
                distance=distance or 2000, surface=surface or "芝",
                direction=direction or "右", straight_m=300,
                corner_count=4, corner_type="小回り",
                _first_corner="平均", slope_type="坂なし",
                inside_outside="なし", is_jra=True,
            )

        # ── 馬リスト解析 ──
        horses = []
        # ID抽出用正規表現
        _horse_id_pat = re.compile(r"pw01dud00(\d{10})")
        _horse_profile_cname_pat = re.compile(r"(pw01dud\d+/\d+)")
        _jockey_id_pat = re.compile(r"pw04kmk00(\d{4,5})")
        _trainer_id_pat = re.compile(r"pw05cmk00(\d{4,5})")

        for row in soup.select("tr"):
            num_cell = row.select_one("td.num")
            if not num_cell:
                continue
            num_text = num_cell.get_text(strip=True)
            if not num_text.isdigit():
                continue
            horse_no = int(num_text)

            # 枠番
            gate_no = 0
            waku_cell = row.select_one("td.waku")
            if waku_cell:
                waku_img = waku_cell.select_one("img")
                if waku_img:
                    alt = waku_img.get("alt", "")
                    m_waku = re.search(r"(\d)", alt)
                    if m_waku:
                        gate_no = int(m_waku.group(1))
                if not gate_no:
                    waku_text = waku_cell.get_text(strip=True)
                    if waku_text.isdigit():
                        gate_no = int(waku_text)

            # 馬名
            horse_name = ""
            horse_cell = row.select_one("td.horse")
            if horse_cell:
                name_a = horse_cell.select_one("a")
                if name_a:
                    horse_name = name_a.get_text(strip=True)
                else:
                    # <a>がなければテキストから馬名を推定
                    texts = [t.strip() for t in horse_cell.stripped_strings]
                    if texts:
                        horse_name = texts[0]

            # 性齢
            sex = ""
            age = 0
            age_cell = row.select_one("td.age")
            if age_cell:
                age_text = age_cell.get_text(strip=True)
                m_sex = re.match(r"(牡|牝|セン|セ)(\d+)", age_text)
                if m_sex:
                    sex = m_sex.group(1)
                    if sex == "セ":
                        sex = "セン"
                    age = int(m_sex.group(2))

            # 斤量
            weight_kg = 55.0
            weight_cell = row.select_one("td.weight")
            if weight_cell:
                wt_text = weight_cell.get_text(strip=True)
                m_wt = re.search(r"([\d.]+)", wt_text)
                if m_wt:
                    try:
                        weight_kg = float(m_wt.group(1))
                    except ValueError:
                        pass

            # 騎手名
            jockey_name = ""
            jockey_cell = row.select_one("td.jockey")
            if jockey_cell:
                j_a = jockey_cell.select_one("a")
                if j_a:
                    jockey_name = j_a.get_text(strip=True)
                else:
                    jockey_name = jockey_cell.get_text(strip=True)

            # 調教師名
            trainer_name = ""
            trainer_cell = row.select_one("td.trainer")
            if trainer_cell:
                t_a = trainer_cell.select_one("a")
                if t_a:
                    trainer_name = t_a.get_text(strip=True)
                else:
                    trainer_name = trainer_cell.get_text(strip=True)

            # オッズ
            odds_val = None
            odds_cell = row.select_one("td.odds")
            if odds_cell:
                odds_text = odds_cell.get_text(strip=True)
                try:
                    odds_val = float(odds_text.replace(",", ""))
                except (ValueError, TypeError):
                    pass

            # 馬体重
            horse_weight = None
            weight_change = None
            h_weight_cell = row.select_one("td.h_weight")
            if h_weight_cell:
                hw_text = h_weight_cell.get_text(strip=True)
                m_hw = re.search(r"(\d{3,4})kg\(([+-]?\d+)\)", hw_text)
                if m_hw:
                    horse_weight = int(m_hw.group(1))
                    weight_change = int(m_hw.group(2))
                else:
                    m_hw2 = re.search(r"(\d{3,4})", hw_text)
                    if m_hw2:
                        horse_weight = int(m_hw2.group(1))

            # 馬主
            owner = ""
            if horse_cell:
                owner_el = horse_cell.select_one("p.owner")
                if owner_el:
                    owner = owner_el.get_text(strip=True)

            # ── ID抽出 ──
            horse_id_official = ""
            horse_profile_cname = ""
            jockey_id_official = ""
            trainer_id_official = ""

            for a_tag in row.select("a[href], a[onclick], [onclick]"):
                href = a_tag.get("href", "") + " " + a_tag.get("onclick", "")
                hm = _horse_id_pat.search(href)
                if hm:
                    horse_id_official = hm.group(1)
                # 馬プロフ CNAME
                pm = _horse_profile_cname_pat.search(href)
                if pm:
                    horse_profile_cname = pm.group(1)
                jm = _jockey_id_pat.search(href)
                if jm:
                    jockey_id_official = jm.group(1).zfill(5)
                tm = _trainer_id_pat.search(href)
                if tm:
                    trainer_id_official = tm.group(1).zfill(5)

            if not horse_name:
                continue

            horse = Horse(
                horse_id=horse_id_official or f"jra_{race_id}_{horse_no}",
                horse_name=horse_name,
                sex=sex or "不明",
                age=age,
                color="",
                trainer=trainer_name,
                trainer_id=trainer_id_official,
                owner=owner,
                breeder="",
                sire="",
                dam="",
                race_date=race_date,
                venue=venue_name,
                race_no=race_no,
                gate_no=gate_no,
                horse_no=horse_no,
                jockey=jockey_name,
                jockey_id=jockey_id_official,
                weight_kg=weight_kg,
                odds=odds_val,
                horse_weight=horse_weight,
                weight_change=weight_change,
            )
            # 馬プロフ CNAME を後で過去走取得に使う
            if horse_profile_cname:
                horse._profile_cname = horse_profile_cname

            horses.append(horse)

        if not horses:
            logger.warning("get_full_entry: No horses parsed for %s", race_id)
            return None, []

        # ── RaceInfo 構築 ──
        race_info = RaceInfo(
            race_id=race_id,
            race_date=race_date,
            venue=venue_name,
            race_no=race_no,
            race_name=race_name or f"{venue_name}{race_no}R",
            grade=grade,
            condition=class_name,
            course=course,
            field_count=len(horses),
            post_time=post_time,
            is_jra=True,
        )

        logger.info(
            "JRA full entry: %s %dR %s %s%dm %d頭",
            venue_name, race_no, race_name, surface, distance, len(horses),
        )
        return race_info, horses

    # ================================================================
    # JRA 馬プロフ → 過去走 + 血統 (Phase 2)
    # ================================================================

    def fetch_horse_history(self, horse_profile_cname: str, horse_name: str = "",
                            horse_id: str = "", max_enrichment: int = 3):
        """JRA馬プロフページから過去走と血統情報を取得

        Step 1: 馬プロフページ取得・解析 → 基本過去走 + 血統
        Step 2: 直近 max_enrichment 件の結果ページから上がり3F等を補完

        Args:
            horse_profile_cname: 馬プロフCNAME (pw01dud...)
            horse_name: 馬名 (結果ページでの行特定用)
            horse_id: 馬ID (結果ページでの行特定用)
            max_enrichment: 結果ページから補完する最大走数

        Returns:
            (List[PastRun], pedigree_dict)
        """
        from src.models import PastRun

        if not horse_profile_cname:
            return [], {}

        # Step 1: プロフページ取得
        try:
            self._wait()
            resp = self._session.post(
                "https://www.jra.go.jp/JRADB/accessD.html",
                data={"cname": horse_profile_cname},
                headers={
                    **_HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": "https://www.jra.go.jp/JRADB/accessD.html",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                logger.debug("Horse profile %d: %s", resp.status_code,
                             horse_profile_cname[:30])
                return [], {}
            resp.encoding = "shift_jis"
        except Exception as e:
            logger.debug("Horse profile fetch failed: %s", e)
            return [], {}

        basic_runs, pedigree = self._parse_jra_horse_profile(resp.text)

        if not basic_runs:
            logger.debug("No past runs found for %s", horse_name)
            return [], pedigree

        # Step 2: 直近 max_enrichment 件の結果ページから補完
        enriched_count = 0
        for i, run_data in enumerate(basic_runs[:max_enrichment]):
            result_cname = run_data.get("_result_cname", "")
            if not result_cname:
                continue

            # セッション内キャッシュ
            if result_cname not in self._result_page_cache:
                try:
                    self._wait()
                    resp2 = self._session.post(
                        "https://www.jra.go.jp/JRADB/accessS.html",
                        data={"cname": result_cname},
                        headers={
                            **_HEADERS,
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Referer": "https://www.jra.go.jp/JRADB/accessS.html",
                        },
                        timeout=15,
                    )
                    if resp2.status_code == 200:
                        resp2.encoding = "shift_jis"
                        self._result_page_cache[result_cname] = (
                            self._parse_jra_race_result(resp2.text)
                        )
                except Exception as e:
                    logger.debug("Result page fetch failed: %s", e)

            if result_cname in self._result_page_cache:
                all_horse_data = self._result_page_cache[result_cname]
                enrichment = all_horse_data.get(horse_id, {})
                if not enrichment and horse_name:
                    enrichment = all_horse_data.get(horse_name, {})
                if enrichment:
                    run_data.update(enrichment)
                    enriched_count += 1

        # Step 3: PastRun オブジェクト構築
        past_runs = []
        for run_data in basic_runs:
            # _result_cname → result_cname としてPastRunに渡す
            _rcname = run_data.pop("_result_cname", "")
            if _rcname:
                run_data["result_cname"] = _rcname
            pr = self._build_past_run_from_profile(run_data)
            if pr:
                past_runs.append(pr)

        logger.info(
            "JRA history: %s %d走 (enriched: %d/%d)",
            horse_name, len(past_runs),
            enriched_count, min(max_enrichment, len(basic_runs)),
        )
        return past_runs, pedigree

    def _parse_jra_horse_profile(self, html: str):
        """JRA馬プロフページ解析

        Returns:
            (basic_runs, pedigree)
            basic_runs = [{"race_date": ..., "_result_cname": ..., ...}, ...]
            pedigree = {"sire": str, "dam": str, "maternal_grandsire": str,
                        "sire_id": str, "dam_id": str, "mgs_id": str}
        """
        soup = BeautifulSoup(html, "html.parser")
        pedigree = {
            "sire": "", "dam": "", "maternal_grandsire": "",
            "sire_id": "", "dam_id": "", "mgs_id": "",
        }
        basic_runs = []

        # ── 血統情報 ──
        # JRA馬プロフの血統テーブルは多段形式
        # パターン1: 4世代血統表（table内のtd）
        # パターン2: プロフィール欄のテキスト
        full_text = soup.get_text()

        # テーブル構造から血統を取得
        # JRAプロフの「プロフィール」セクション内のテーブルを探す
        profile_tables = soup.select("table")
        for tbl in profile_tables:
            rows = tbl.select("tr")
            for row in rows:
                cells = row.select("td, th")
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                if label in ("父", "父馬") and value:
                    pedigree["sire"] = value
                    # 父リンクからID
                    a = cells[1].select_one("a")
                    if a:
                        href = a.get("href", "") + " " + a.get("onclick", "")
                        m = re.search(r"pw01dud00(\d{10})", href)
                        if m:
                            pedigree["sire_id"] = m.group(1)
                elif label in ("母", "母馬") and value:
                    pedigree["dam"] = value
                    a = cells[1].select_one("a")
                    if a:
                        href = a.get("href", "") + " " + a.get("onclick", "")
                        m = re.search(r"pw01dud00(\d{10})", href)
                        if m:
                            pedigree["dam_id"] = m.group(1)
                elif label in ("母の父", "母父", "BMS") and value:
                    pedigree["maternal_grandsire"] = value
                    a = cells[1].select_one("a")
                    if a:
                        href = a.get("href", "") + " " + a.get("onclick", "")
                        m = re.search(r"pw01dud00(\d{10})", href)
                        if m:
                            pedigree["mgs_id"] = m.group(1)

        # テキストベースのフォールバック（テーブルで取れなかった場合）
        if not pedigree["sire"]:
            m = re.search(r'(?:父|サイアー)[：:\s]+([^\s/（(、,]+)', full_text)
            if m:
                pedigree["sire"] = m.group(1).strip()
        if not pedigree["dam"]:
            m = re.search(r'(?:母)[：:\s]+([^\s/（(、,]+)', full_text)
            if m:
                pedigree["dam"] = m.group(1).strip()
        if not pedigree["maternal_grandsire"]:
            m = re.search(r'(?:母の父|母父|BMS)[：:\s]+([^\s/（(、,]+)', full_text)
            if m:
                pedigree["maternal_grandsire"] = m.group(1).strip()

        # 血統テーブル（4世代表示）からのフォールバック
        # JRA公式は典型的に2列×8行の血統テーブルを持つ
        if not pedigree["sire"]:
            for tbl in profile_tables:
                tds = tbl.select("td")
                if len(tds) >= 8:
                    # 典型的な4世代血統表: 最初のtdが父系
                    texts = [td.get_text(strip=True) for td in tds]
                    if texts[0] and len(texts[0]) >= 2:
                        pedigree["sire"] = texts[0]
                        if len(tds) >= 4:
                            pedigree["dam"] = tds[2].get_text(strip=True) or pedigree["dam"]
                        break

        # ── 過去走テーブル ──
        result_cname_pat = re.compile(r"(pw01sde[^\"'&\s]+)")
        jockey_id_pat = re.compile(r"pw04kmk00(\d{4,5})")
        date_pat = re.compile(r"(\d{4})[年/\-.](\d{1,2})[月/\-.](\d{1,2})")

        # 過去走テーブルを探す: 日付パターンを含む行が多いテーブル
        race_table = None
        max_date_rows = 0
        for tbl in profile_tables:
            date_rows = 0
            for row in tbl.select("tr"):
                cells = row.select("td")
                if cells:
                    first_text = cells[0].get_text(strip=True)
                    if date_pat.search(first_text):
                        date_rows += 1
            if date_rows > max_date_rows:
                max_date_rows = date_rows
                race_table = tbl

        if not race_table or max_date_rows == 0:
            return basic_runs, pedigree

        # 過去走テーブルの各行を解析
        rows = race_table.select("tr")
        for row in rows:
            cells = row.select("td")
            if len(cells) < 8:
                continue

            # 最初のセルが日付かチェック
            first_text = cells[0].get_text(strip=True)
            m_date = date_pat.search(first_text)
            if not m_date:
                continue

            try:
                run_data = {}

                # 日付
                run_data["race_date"] = (
                    f"{m_date.group(1)}-"
                    f"{int(m_date.group(2)):02d}-"
                    f"{int(m_date.group(3)):02d}"
                )

                # 会場 (2列目)
                venue_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                run_data["venue"] = venue_text

                # レース名 + 結果ページCNAME (3列目)
                if len(cells) > 2:
                    race_cell = cells[2]
                    run_data["class_name"] = race_cell.get_text(strip=True)
                    # 結果ページCNAMEをリンクから取得
                    for a_tag in race_cell.select("a"):
                        href = (a_tag.get("href", "") + " " +
                                a_tag.get("onclick", ""))
                        m_cn = result_cname_pat.search(href)
                        if m_cn:
                            run_data["_result_cname"] = m_cn.group(1)
                            break

                # 距離・馬場 (4列目)
                if len(cells) > 3:
                    dist_text = cells[3].get_text(strip=True)
                    m_dist = re.search(
                        r"(芝|ダ|ダート|障)[\s]*(\d{3,4})", dist_text
                    )
                    if m_dist:
                        s = m_dist.group(1)
                        run_data["surface"] = (
                            "芝" if s == "芝"
                            else ("ダート" if s in ("ダ", "ダート") else "障害")
                        )
                        run_data["distance"] = int(m_dist.group(2))
                    else:
                        m_d = re.search(r"(\d{3,4})", dist_text)
                        run_data["distance"] = int(m_d.group(1)) if m_d else 0
                        run_data["surface"] = "芝"

                # 馬場状態 (5列目)
                if len(cells) > 4:
                    cond = cells[4].get_text(strip=True)
                    run_data["condition"] = cond if cond else "良"

                # 頭数 (6列目)
                if len(cells) > 5:
                    fc = cells[5].get_text(strip=True)
                    try:
                        run_data["field_count"] = int(fc)
                    except ValueError:
                        run_data["field_count"] = 0

                # 人気 (7列目)
                if len(cells) > 6:
                    pop = cells[6].get_text(strip=True)
                    try:
                        run_data["popularity"] = int(pop)
                    except ValueError:
                        pass

                # 着順 (8列目)
                if len(cells) > 7:
                    pos = cells[7].get_text(strip=True)
                    try:
                        run_data["finish_pos"] = int(pos)
                    except ValueError:
                        run_data["finish_pos"] = 0  # 除外/中止等

                # 騎手 (9列目)
                if len(cells) > 8:
                    jockey_cell = cells[8]
                    j_a = jockey_cell.select_one("a")
                    if j_a:
                        run_data["jockey"] = j_a.get_text(strip=True)
                        j_href = (j_a.get("href", "") + " " +
                                  j_a.get("onclick", ""))
                        jm = jockey_id_pat.search(j_href)
                        if jm:
                            run_data["jockey_id"] = jm.group(1).zfill(5)
                    else:
                        run_data["jockey"] = jockey_cell.get_text(strip=True)

                # 斤量 (10列目)
                if len(cells) > 9:
                    try:
                        run_data["weight_kg"] = float(
                            cells[9].get_text(strip=True)
                        )
                    except ValueError:
                        run_data["weight_kg"] = 55.0

                # 馬体重 (11列目)
                if len(cells) > 10:
                    hw = cells[10].get_text(strip=True)
                    m_hw = re.search(r"(\d{3,4})", hw)
                    if m_hw:
                        run_data["horse_weight"] = int(m_hw.group(1))

                # タイム (12列目)
                if len(cells) > 11:
                    t = cells[11].get_text(strip=True)
                    run_data["finish_time_sec"] = self._parse_time(t)

                basic_runs.append(run_data)

            except Exception as e:
                logger.debug("Profile past run parse error: %s", e)
                continue

        return basic_runs, pedigree

    def _parse_jra_race_result(self, html: str) -> dict:
        """JRAレース結果ページの全馬データを解析

        Returns:
            {horse_id: enrichment, horse_name: enrichment, ...}
            enrichment = {"gate_no": int, "horse_no": int,
                          "last_3f_sec": float, "positions_corners": List[int],
                          "margin_text": str}
        """
        soup = BeautifulSoup(html, "html.parser")
        result = {}
        _horse_id_pat = re.compile(r"pw01dud00(\d{10})")

        # 着順テーブルを探す
        tables = soup.select("table")
        race_table = None
        for tbl in tables:
            rows = tbl.select("tr")
            # 着順テーブルは数字で始まる行が複数ある
            num_rows = sum(
                1 for r in rows
                if r.select("td") and
                r.select("td")[0].get_text(strip=True).isdigit()
            )
            if num_rows >= 3:
                race_table = tbl
                break

        if not race_table:
            return result

        for row in race_table.select("tr"):
            cells = row.select("td")
            if len(cells) < 8:
                continue

            try:
                # 着順 (1列目)
                pos_text = cells[0].get_text(strip=True)
                if not pos_text.isdigit():
                    continue

                # 枠番 (2列目)
                gate_no = 0
                gate_text = cells[1].get_text(strip=True)
                if gate_text.isdigit():
                    gate_no = int(gate_text)

                # 馬番 (3列目)
                horse_no = 0
                umaban_text = cells[2].get_text(strip=True)
                if umaban_text.isdigit():
                    horse_no = int(umaban_text)

                # 馬名 + 馬ID (4列目)
                h_name = ""
                h_id = ""
                name_cell = cells[3]
                name_a = name_cell.select_one("a")
                if name_a:
                    h_name = name_a.get_text(strip=True)
                    href = (name_a.get("href", "") + " " +
                            name_a.get("onclick", ""))
                    hm = _horse_id_pat.search(href)
                    if hm:
                        h_id = hm.group(1)
                else:
                    h_name = name_cell.get_text(strip=True)

                enrichment = {
                    "gate_no": gate_no,
                    "horse_no": horse_no,
                }

                # 残りのセルから上がり3F、通過順位、着差を探す
                for ci in range(4, len(cells)):
                    text = cells[ci].get_text(strip=True)
                    if not text:
                        continue

                    # 上がり3F: XX.X 形式 (30-45秒)
                    if "last_3f_sec" not in enrichment:
                        m_3f = re.match(r"^(\d{2}\.\d)$", text)
                        if m_3f:
                            f3 = float(m_3f.group(1))
                            if 30.0 <= f3 <= 45.0:
                                enrichment["last_3f_sec"] = f3
                                continue

                    # 通過順位: "2-2-2-2" 形式
                    if "positions_corners" not in enrichment:
                        if re.match(r"^\d+-\d+(-\d+)*$", text):
                            corners = [int(x) for x in text.split("-")]
                            enrichment["positions_corners"] = corners
                            continue

                    # 着差: ハナ, クビ, アタマ, X.X/X, 数字
                    if "margin_text" not in enrichment:
                        if text in (
                            "ハナ", "クビ", "アタマ", "大差",
                        ) or re.match(r"^\d+(\.\d+)?(/\d)?$", text):
                            enrichment["margin_text"] = text
                            continue
                        # "1/2", "3/4" 等の分数
                        if re.match(r"^\d+/\d$", text):
                            enrichment["margin_text"] = text
                            continue

                # 両キーで登録
                if h_id:
                    result[h_id] = enrichment
                if h_name:
                    result[h_name] = enrichment

            except Exception as e:
                logger.debug("Result row parse error: %s", e)
                continue

        return result

    def _parse_jra_result_order(self, html: str) -> List[dict]:
        """JRA結果ページHTMLから着順リストを構築

        _parse_jra_race_result() は {horse_id: enrichment} 形式を返すが、
        このメソッドは着順付きリストを返す。

        Returns:
            [{"horse_no": int, "finish": int, "last_3f": float, "corners": [int]}, ...]
        """
        soup = BeautifulSoup(html, "html.parser")
        order = []

        # 着順テーブルを探す
        tables = soup.select("table")
        race_table = None
        for tbl in tables:
            rows = tbl.select("tr")
            num_rows = sum(
                1 for r in rows
                if r.select("td") and
                r.select("td")[0].get_text(strip=True).isdigit()
            )
            if num_rows >= 3:
                race_table = tbl
                break

        if not race_table:
            return order

        for row in race_table.select("tr"):
            cells = row.select("td")
            if len(cells) < 8:
                continue

            try:
                # 着順 (1列目)
                pos_text = cells[0].get_text(strip=True)
                if not pos_text.isdigit():
                    continue
                finish = int(pos_text)

                # 馬番 (3列目)
                umaban_text = cells[2].get_text(strip=True)
                if not umaban_text.isdigit():
                    continue
                horse_no = int(umaban_text)

                entry = {
                    "horse_no": horse_no,
                    "finish": finish,
                }

                # 残りのセルから上がり3F、通過順位を探す
                for ci in range(4, len(cells)):
                    text = cells[ci].get_text(strip=True)
                    if not text:
                        continue

                    # 上がり3F: XX.X 形式 (30-45秒)
                    if "last_3f" not in entry:
                        m_3f = re.match(r"^(\d{2}\.\d)$", text)
                        if m_3f:
                            f3 = float(m_3f.group(1))
                            if 30.0 <= f3 <= 45.0:
                                entry["last_3f"] = f3
                                continue

                    # 通過順位: "2-2-2-2" 形式
                    if "corners" not in entry:
                        if re.match(r"^\d+-\d+(-\d+)*$", text):
                            corners = [int(x) for x in text.split("-")]
                            entry["corners"] = corners
                            continue

                order.append(entry)

            except Exception as e:
                logger.debug("Result order parse error: %s", e)
                continue

        return order

    @staticmethod
    def _parse_jra_payouts(html: str) -> dict:
        """JRA結果ページのHTMLから払戻金テーブルをパース

        返却形式 (netkeiba互換):
            {
                "tansho": [{"combo": "3", "payout": 450, "popularity": 2}],
                "fukusho": [{"combo": "3", "payout": 180, "popularity": 2}, ...],
                "wakuren": [{"combo": "2-5", "payout": 1230, "popularity": 5}],
                "umaren": [{"combo": "3-8", "payout": 2340, "popularity": 8}],
                "wide": [{"combo": "3-5", "payout": 560, "popularity": 3}, ...],
                "umatan": [{"combo": "3-8", "payout": 4560, "popularity": 12}],
                "sanrenpuku": [{"combo": "3-5-8", "payout": 12340, "popularity": 35}],
                "sanrentan": [{"combo": "3-8-5", "payout": 78900, "popularity": 123}],
            }
            パースできない場合は空dict {} を返す
        """
        # 券種名 → 内部キー名のマッピング
        _BET_TYPE_MAP = {
            "単勝": "tansho",
            "複勝": "fukusho",
            "枠連": "wakuren",
            "馬連": "umaren",
            "ワイド": "wide",
            "馬単": "umatan",
            "三連複": "sanrenpuku",
            "三連単": "sanrentan",
        }

        try:
            soup = BeautifulSoup(html, "html.parser")
            result = {}

            # 払戻テーブルを特定: 券種名テキストを含むテーブルを探す
            payout_tables = []
            for tbl in soup.select("table"):
                tbl_text = tbl.get_text()
                # 「単勝」と「払戻」または「人気」を含むテーブルを払戻テーブルと判定
                if "単勝" in tbl_text and ("払戻" in tbl_text or "人気" in tbl_text
                                           or "円" in tbl_text):
                    payout_tables.append(tbl)

            if not payout_tables:
                # フォールバック: 着順テーブル以外のテーブルで券種名を含むものを探す
                for tbl in soup.select("table"):
                    tbl_text = tbl.get_text()
                    bet_type_count = sum(
                        1 for bt in _BET_TYPE_MAP if bt in tbl_text
                    )
                    if bet_type_count >= 3:
                        payout_tables.append(tbl)

            if not payout_tables:
                return {}

            for tbl in payout_tables:
                for row in tbl.select("tr"):
                    cells = row.select("td")
                    if len(cells) < 3:
                        continue

                    # 券種名を含むセルを探す
                    bet_key = None
                    bet_cell_idx = -1
                    for ci, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True)
                        for jp_name, key in _BET_TYPE_MAP.items():
                            if jp_name in cell_text:
                                bet_key = key
                                bet_cell_idx = ci
                                break
                        if bet_key:
                            break

                    if not bet_key:
                        # 複勝・ワイドの継続行（券種名セルがない行）
                        # 直前の券種が複数行タイプの場合に該当
                        continue

                    # 券種名セル以降のセルから組番号・払戻金・人気を抽出
                    remaining = cells[bet_cell_idx + 1:]
                    if len(remaining) < 2:
                        continue

                    # rowspan で複数行にまたがる場合があるため、
                    # 同じテーブル内の後続行も同じ券種として処理する
                    # まず現在行のデータを取得
                    entries = _parse_payout_row_entries(remaining)
                    if entries:
                        if bet_key not in result:
                            result[bet_key] = []
                        result[bet_key].extend(entries)

                    # rowspan がある場合、後続行を処理
                    # 券種名セルの rowspan を確認
                    bet_cell = cells[bet_cell_idx]
                    rowspan = 1
                    try:
                        rowspan = int(bet_cell.get("rowspan", 1))
                    except (ValueError, TypeError):
                        pass

                    if rowspan > 1:
                        # 後続行を探す（同じテーブル内）
                        current_row = row
                        for _ in range(rowspan - 1):
                            current_row = current_row.find_next_sibling("tr")
                            if not current_row:
                                break
                            sub_cells = current_row.select("td")
                            if not sub_cells:
                                continue
                            sub_entries = _parse_payout_row_entries(sub_cells)
                            if sub_entries:
                                if bet_key not in result:
                                    result[bet_key] = []
                                result[bet_key].extend(sub_entries)

            return result

        except Exception as e:
            logger.debug("払戻テーブルのパースに失敗: %s", e)
            return {}

    @staticmethod
    def _parse_jra_lap_times(html: str) -> Optional[dict]:
        """JRA結果ページのHTMLからハロンタイム（ラップタイム）をパース

        Returns:
            {
                "first_3f": 35.7,   # 最初の3ハロン合計
                "last_3f": 36.0,    # 最後の3ハロン合計
                "laps": [12.5, 11.2, 12.0, ...]  # 各ハロン
            }
            パースできない場合は None を返す
        """
        try:
            soup = BeautifulSoup(html, "html.parser")

            # ラップテーブルを探す: "ラップ" ヘッダーの近くのテーブル
            lap_table = None
            for header in soup.find_all(["h2", "h3", "h4", "th", "dt"]):
                header_text = header.get_text()
                if "ラップ" not in header_text and "lap" not in header_text.lower():
                    continue
                table = header.find_next("table")
                if table:
                    lap_table = table
                    break

            if not lap_table:
                # フォールバック: 200, 400, 600... のヘッダーパターンを探す
                for tbl in soup.select("table"):
                    first_row = tbl.select_one("tr")
                    if not first_row:
                        continue
                    header_cells = first_row.find_all(["th", "td"])
                    dist_count = sum(
                        1 for c in header_cells
                        if re.match(r"^\d{3,4}(m)?$", c.get_text(strip=True))
                    )
                    if dist_count >= 3:
                        lap_table = tbl
                        break

            if not lap_table:
                return None

            rows = lap_table.select("tr")
            if len(rows) < 2:
                return None

            # ヘッダー行から距離列を解析
            header_row = rows[0]
            header_cells = header_row.find_all(["th", "td"])

            # ラップ値の行を探す: XX.X 形式の数値が複数あるデータ行
            laps = []
            lap_pat = re.compile(r"^\d{1,2}\.\d$")

            for row in rows[1:]:
                rcells = row.select("td") or row.find_all("td")
                if not rcells:
                    continue
                # ラップ候補を集める
                candidate_laps = []
                for c in rcells:
                    txt = c.get_text(strip=True).replace(",", ".")
                    if lap_pat.match(txt):
                        candidate_laps.append(float(txt))
                # ラップ行と判定: 3つ以上のラップ値がある行
                if len(candidate_laps) >= 3:
                    laps = candidate_laps
                    break

            # 累積タイムの行からラップを逆算する方式（フォールバック）
            if not laps:
                for row in rows[1:]:
                    rcells = row.select("td") or row.find_all("td")
                    if not rcells:
                        continue
                    # 累積タイム: "M:SS.S" または "SS.S" 形式
                    cumulative = []
                    for c in rcells:
                        txt = c.get_text(strip=True).replace(",", ".")
                        try:
                            if ":" in txt:
                                parts = txt.split(":")
                                val = int(parts[0]) * 60 + float(parts[1])
                            else:
                                val = float(txt)
                            cumulative.append(val)
                        except (ValueError, IndexError):
                            continue
                    # 累積値は単調増加するはず
                    if (len(cumulative) >= 3 and
                            all(cumulative[i] < cumulative[i + 1]
                                for i in range(len(cumulative) - 1))):
                        # 累積値からラップを計算
                        laps = [cumulative[0]]
                        for i in range(1, len(cumulative)):
                            laps.append(round(cumulative[i] - cumulative[i - 1], 1))
                        break

            if not laps or len(laps) < 3:
                return None

            # first_3f: 最初の3ハロン合計
            first_3f = round(sum(laps[:3]), 1)
            # last_3f: 最後の3ハロン合計
            last_3f = round(sum(laps[-3:]), 1)

            return {
                "first_3f": first_3f,
                "last_3f": last_3f,
                "laps": laps,
            }

        except Exception as e:
            logger.debug("ラップタイムのパースに失敗: %s", e)
            return None

    def _build_past_run_from_profile(self, data: dict):
        """プロフィール解析データから PastRun を構築"""
        from src.models import PastRun

        venue = data.get("venue", "")
        surface = data.get("surface", "芝")
        distance = data.get("distance", 0)

        # venue名 → venue_code
        venue_code = _JRA_NAME_TO_CODE.get(venue, "")
        course_id = (
            f"{venue_code}_{surface}_{distance}"
            if venue_code and surface and distance
            else ""
        )

        finish_pos = data.get("finish_pos", 0)
        field_count = data.get("field_count", 0)

        # position_4c: positions_corners末尾、なければ着順推定
        positions_corners = data.get("positions_corners", [])
        position_4c = (
            positions_corners[-1] if positions_corners
            else (finish_pos if finish_pos else 0)
        )

        # 着差テキスト → 秒換算
        margin_behind = 0.0
        margin_text = data.get("margin_text", "")
        if margin_text:
            margin_behind = self._margin_to_seconds(margin_text)

        try:
            return PastRun(
                race_date=data.get("race_date", ""),
                venue=venue,
                course_id=course_id,
                distance=distance,
                surface=surface,
                condition=data.get("condition", "良"),
                class_name=data.get("class_name", ""),
                grade=data.get("grade", ""),
                field_count=field_count,
                gate_no=data.get("gate_no", 0),
                horse_no=data.get("horse_no", 0),
                jockey=data.get("jockey", ""),
                weight_kg=data.get("weight_kg", 55.0),
                position_4c=position_4c,
                finish_pos=finish_pos,
                finish_time_sec=data.get("finish_time_sec", 0.0),
                last_3f_sec=data.get("last_3f_sec", 0.0),
                margin_behind=margin_behind,
                margin_ahead=0.0,
                horse_weight=data.get("horse_weight"),
                positions_corners=positions_corners,
                jockey_id=data.get("jockey_id", ""),
                popularity_at_race=data.get("popularity"),
                result_cname=data.get("result_cname", ""),
            )
        except Exception as e:
            logger.debug("PastRun build error: %s", e)
            return None

    @staticmethod
    def _parse_time(time_text: str) -> float:
        """タイムテキスト → 秒変換 ("1:22.6" → 82.6, "58.3" → 58.3)"""
        if not time_text:
            return 0.0
        m = re.match(r"(\d+):(\d+)\.(\d+)", time_text)
        if m:
            return (int(m.group(1)) * 60 + int(m.group(2))
                    + int(m.group(3)) / 10.0)
        m2 = re.match(r"(\d+)\.(\d+)", time_text)
        if m2:
            return int(m2.group(1)) + int(m2.group(2)) / 10.0
        return 0.0

    @staticmethod
    def _margin_to_seconds(margin_text: str) -> float:
        """着差テキスト → 秒変換"""
        _MAP = {
            "ハナ": 0.02, "アタマ": 0.05, "クビ": 0.08,
            "1/2": 0.15, "3/4": 0.23,
            "1": 0.30, "1.1/4": 0.38, "1.1/2": 0.45, "1.3/4": 0.53,
            "2": 0.60, "2.1/2": 0.75,
            "3": 0.90, "3.1/2": 1.05,
            "4": 1.20, "5": 1.50,
            "大差": 2.00,
        }
        return _MAP.get(margin_text, 0.0)

    # ================================================================
    # NAR 公式 (keiba.go.jp)
    # ================================================================

    def _get_nar_weights(self, race_id: str) -> Dict[int, Dict]:
        """NAR 公式 DebaTable から馬体重・馬主を取得"""
        venue_code = race_id[4:6]
        baba_code = _NAR_BABA_CODES.get(venue_code)
        if not baba_code:
            return {}

        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        if not race_no:
            return {}

        from datetime import datetime
        today = datetime.now().strftime("%Y/%m/%d")

        try:
            self._wait()
            resp = self._session.get(
                "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/DebaTable",
                params={
                    "k_raceDate": today,
                    "k_raceNo": str(race_no),
                    "k_babaCode": baba_code,
                },
                headers=_HEADERS,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.debug("NAR DebaTable %d: %s", resp.status_code, race_id)
                return {}
        except Exception as e:
            logger.debug("NAR DebaTable fetch failed: %s", e)
            return {}

        return self._parse_nar_debatable(resp.text)

    def _parse_nar_debatable(self, html: str) -> Dict[int, Dict]:
        """NAR DebaTable HTMLから馬体重・馬主をパース

        DebaTable は5行/馬の構造:
        - Row 1: 馬名, 騎手, オッズ
        - Row 2: 性齢, 斤量
        - Row 3: 父名, 調教師, 馬体重(td.odds_weight)
        - Row 4: 母名, 馬主
        - Row 5: 母父名, 生産牧場
        """
        soup = BeautifulSoup(html, "html.parser")
        result: Dict[int, Dict] = {}

        # ばんえい: dataArea から馬場水分量を取得
        self._last_banei_moisture = None
        da = soup.select_one("ul.dataArea")
        if da:
            da_text = da.get_text()
            wm = re.search(r"馬場[：:]\s*([\d.]+)", da_text)
            if wm:
                self._last_banei_moisture = float(wm.group(1))

        # 馬番セルを持つ行を見つけて、そこから5行ブロックを構成
        table = soup.select_one("section.cardTable table")
        if not table:
            # フォールバック: 最初のtable
            table = soup.select_one("table")
        if not table:
            return result

        rows = table.select("tr")
        current_horse_no = 0
        row_in_block = 0

        for row in rows:
            cells = row.select("td")
            if not cells:
                continue

            # 馬番セルの検出: 最初のtdが数字1-18
            first_text = cells[0].get_text(strip=True)
            if first_text.isdigit() and 1 <= int(first_text) <= 18:
                # 新しい馬ブロック開始 (枠番=cells[0], 馬番=cells[1])
                if len(cells) >= 2:
                    umaban_text = cells[1].get_text(strip=True)
                    if umaban_text.isdigit():
                        current_horse_no = int(umaban_text)
                        row_in_block = 1
                        result.setdefault(current_horse_no, {
                            "weight": None,
                            "weight_change": None,
                            "owner": "",
                            "jockey_name": "",
                            "trainer_name": "",
                        })
                        # Row 1 から騎手名を取得（馬名の次のリンクテキスト）
                        jockey_links = [
                            a for a in row.select("a")
                            if "JockeyMark" in a.get("href", "")
                        ]
                        if jockey_links:
                            result[current_horse_no]["jockey_name"] = jockey_links[0].get_text(strip=True)
                        continue
                # 枠番と馬番が同じセルの場合
                current_horse_no = int(first_text)
                row_in_block = 1
                result.setdefault(current_horse_no, {
                    "weight": None,
                    "weight_change": None,
                    "owner": "",
                    "jockey_name": "",
                    "trainer_name": "",
                })
                continue

            if current_horse_no == 0:
                continue

            row_in_block += 1

            # 調教師名: TrainerMark リンクがある行から取得
            trainer_links = [
                a for a in row.select("a")
                if "TrainerMark" in a.get("href", "")
            ]
            if trainer_links:
                result[current_horse_no]["trainer_name"] = trainer_links[0].get_text(strip=True)

            # 馬体重: td.odds_weight がある行から取得（ばんえいは行がずれるため行番号不問）
            wt_cell = row.select_one("td.odds_weight")
            if wt_cell and result[current_horse_no]["weight"] is None:
                wt_text = wt_cell.get_text(strip=True)
                # "444(0)" or "444(-2)" or "444(+4)"
                m = re.match(r"(\d+)\(([+-]?\d+)\)", wt_text)
                if m:
                    result[current_horse_no]["weight"] = int(m.group(1))
                    result[current_horse_no]["weight_change"] = int(m.group(2))

            # Row 4: 馬主 (class なしの td)
            if row_in_block == 4:
                # 馬主は列位置で特定 (騎手/調教師/馬主列)
                for cell in cells:
                    text = cell.get_text(strip=True)
                    # 馬主名は通常2文字以上で括弧つきの法人名も
                    if text and len(text) >= 2 and not text.isdigit():
                        # 母名（馬名っぽい）を除外: 普通の日本語の名前
                        # 母名は通常カタカナだが馬主名は漢字が多い
                        colspan = cell.get("colspan", "1")
                        if colspan == "1":
                            result[current_horse_no]["owner"] = text
                            break

        # weight が None でも騎手・調教師情報があれば残す
        # (馬体重未発表でも名前更新は行う)

        if result:
            wt_count = sum(1 for v in result.values() if v.get("weight") is not None)
            logger.info("NAR DebaTable: %d頭取得 (weight: %d, owner: %d, trainer: %d)",
                        len(result), wt_count,
                        sum(1 for v in result.values() if v.get("owner")),
                        sum(1 for v in result.values() if v.get("trainer_name")))
        return result

    def _get_nar_odds(self, race_id: str) -> Dict[int, Tuple[float, int]]:
        """NAR 公式サイトから単勝オッズを取得"""
        venue_code = race_id[4:6]
        baba_code = _NAR_BABA_CODES.get(venue_code)
        if not baba_code:
            logger.debug("NAR venue code %s not mapped", venue_code)
            return {}

        # race_id から日付とレース番号を抽出
        year = race_id[:4]
        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        if not race_no:
            return {}

        # 日付の特定: race_id の KKKK NNNN 部分から日付を算出する必要がある
        # netkeiba NAR race_id: YYYY VV KK NN RR
        # 日付は predictions ファイルから取得するのが確実だが、
        # ここでは今日の日付を使う（ライブオッズ用途のため）
        from datetime import datetime
        today = datetime.now().strftime("%Y/%m/%d")

        try:
            self._wait()
            resp = self._session.get(
                "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/OddsTanFuku",
                params={
                    "k_raceDate": today,
                    "k_raceNo": str(race_no),
                    "k_babaCode": baba_code,
                },
                headers=_HEADERS,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("NAR odds page %d: %s", resp.status_code, race_id)
                return {}
        except Exception as e:
            logger.warning("NAR odds fetch failed: %s", e)
            return {}

        return self._parse_nar_odds_table(resp.text)

    def _parse_nar_odds_table(self, html: str) -> Dict[int, Tuple[float, int]]:
        """NAR オッズ HTML テーブルをパース"""
        soup = BeautifulSoup(html, "html.parser")
        result: Dict[int, Tuple[float, int]] = {}

        table = soup.select_one("table")
        if not table:
            return result

        # ヘッダ行を確認してカラムインデックスを特定
        header_row = table.select_one("tr")
        if not header_row:
            return result

        headers = [th.get_text(strip=True) for th in header_row.select("th, td")]
        # 馬番と単勝オッズのカラム位置を特定
        horse_no_idx = -1
        odds_idx = -1
        for i, h in enumerate(headers):
            if h == "馬番":
                horse_no_idx = i
            elif "単勝" in h:
                odds_idx = i

        if horse_no_idx < 0 or odds_idx < 0:
            # フォールバック: 位置ベース (枠=0, 馬番=1, 馬名=2, 単勝=3)
            horse_no_idx = 1
            odds_idx = 3

        for row in table.select("tr")[1:]:  # ヘッダ行をスキップ
            cells = row.select("td")
            if len(cells) <= max(horse_no_idx, odds_idx):
                continue
            try:
                no_text = cells[horse_no_idx].get_text(strip=True)
                odds_text = cells[odds_idx].get_text(strip=True)
                if not no_text.isdigit():
                    continue
                horse_no = int(no_text)
                # オッズテキストのクリーニング
                odds_text = odds_text.replace(",", "").strip()
                if not odds_text or odds_text in ("---", "---.-", "-"):
                    continue
                odds_val = float(odds_text)
                if 0.1 < odds_val < 9999:
                    result[horse_no] = (odds_val, 0)
            except (ValueError, TypeError):
                continue

        # 人気順を計算
        if result:
            sorted_by_odds = sorted(result.items(), key=lambda x: x[1][0])
            for rank, (no, (odds, _)) in enumerate(sorted_by_odds, 1):
                result[no] = (odds, rank)

        return result

    # ================================================================
    # JRA 三連複オッズ (jra.go.jp)
    # ================================================================

    def _get_jra_sanrenpuku(self, race_id: str) -> Dict[Tuple[int, ...], float]:
        """JRA公式から三連複オッズを取得（CNAME導出方式）

        単勝CNAMEの pw151ou を pw156ou に変換して三連複ページを取得。
        JRA公式の券種別CNAMEプレフィックス:
          1=単複, 2=枠連, 3=馬連, 4=ワイド, 5=馬単, 6=三連複, 7=三連単
        """
        tansho_cname = self._get_jra_cname(race_id)
        if not tansho_cname or not tansho_cname.startswith("pw151ou"):
            logger.debug("JRA三連複: 単勝CNAMEが取得できない: %s", race_id)
            return {}

        # pw151ou → pw156ou (三連複)
        san_cname = "pw156ou" + tansho_cname[7:]
        try:
            self._wait()
            resp = self._session.post(
                "https://www.jra.go.jp/JRADB/accessO.html",
                data={"cname": san_cname},
                headers={
                    **_HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": "https://www.jra.go.jp/JRADB/accessO.html",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("JRA三連複: HTTP %d: %s", resp.status_code, race_id)
                return {}
            resp.encoding = "shift_jis"
        except Exception as e:
            logger.warning("JRA三連複: 取得失敗: %s (%s)", race_id, e)
            return {}

        result = self._parse_sanrenpuku_table(resp.text)
        if result:
            logger.info("JRA三連複オッズ: %s → %d組取得", race_id, len(result))
        else:
            logger.debug("JRA三連複: パース結果0件（ページ構造が異なる可能性）: %s", race_id)
        return result

    # ================================================================
    # NAR 三連複オッズ (keiba.go.jp)
    # ================================================================

    def _get_nar_sanrenpuku(self, race_id: str) -> Dict[Tuple[int, ...], float]:
        """NAR公式サイトから三連複オッズを取得

        注意: keiba.go.jp は単勝/複勝のみ提供。三連複は OddsPark に
        リダイレクトされるため取得不可の場合が多い。
        """
        venue_code = race_id[4:6]
        baba_code = _NAR_BABA_CODES.get(venue_code)
        if not baba_code:
            logger.debug("NAR三連複: venue code %s not mapped", venue_code)
            return {}

        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        if not race_no:
            return {}

        from datetime import datetime
        today = datetime.now().strftime("%Y/%m/%d")

        # keiba.go.jp の三連複は存在しない場合が多い（404）
        # それでも将来提供される可能性があるためリクエストは残す
        try:
            self._wait()
            resp = self._session.get(
                "https://www.keiba.go.jp/KeibaWeb/TodayRaceInfo/OddsSanrenFuku",
                params={
                    "k_raceDate": today,
                    "k_raceNo": str(race_no),
                    "k_babaCode": baba_code,
                },
                headers=_HEADERS,
                timeout=15,
            )
            if resp.status_code != 200:
                # NAR公式は三連複非対応が通常（404）→ debug レベル
                logger.debug("NAR三連複: HTTP %d（NAR公式は三連複非対応）: %s", resp.status_code, race_id)
                return {}
        except Exception as e:
            logger.debug("NAR三連複: 取得失敗: %s (%s)", race_id, e)
            return {}

        result = self._parse_sanrenpuku_table(resp.text)
        if result:
            logger.info("NAR三連複オッズ: %s → %d組取得", race_id, len(result))
        return result

    # ================================================================
    # 三連複テーブルパーサー（JRA/NAR共通）
    # ================================================================

    def _parse_sanrenpuku_table(self, html: str) -> Dict[Tuple[int, ...], float]:
        """三連複オッズHTMLテーブルをパース（JRA公式/NAR公式共通）

        組番号の形式: "1-2-3", "1−2−3", "1 - 2 - 3" 等
        テーブル行ごとに組番号セルとオッズセルを探索。
        """
        soup = BeautifulSoup(html, "html.parser")
        result: Dict[Tuple[int, ...], float] = {}

        # 3頭の組番号パターン（各種ハイフン・スペース対応）
        combo_pat = re.compile(
            r"(\d{1,2})\s*[-\u2010-\u2015\u2212\u30FC\uFF0D]\s*"
            r"(\d{1,2})\s*[-\u2010-\u2015\u2212\u30FC\uFF0D]\s*"
            r"(\d{1,2})"
        )

        for table in soup.select("table"):
            for row in table.select("tr"):
                cells = row.select("td")
                if len(cells) < 2:
                    continue
                # 各セルを走査して組番号を探す
                for i, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    m = combo_pat.search(text)
                    if not m:
                        continue
                    a, b, c = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    if not (1 <= a <= 18 and 1 <= b <= 18 and 1 <= c <= 18):
                        continue
                    # 組番号の次のセルからオッズを取得
                    for j in range(i + 1, len(cells)):
                        odds_text = cells[j].get_text(strip=True).replace(",", "").replace("円", "")
                        if not odds_text or odds_text in ("---", "---.-", "-", ""):
                            break
                        try:
                            odds = float(odds_text)
                            if 1.0 < odds < 999999:
                                key = tuple(sorted([a, b, c]))
                                result[key] = odds
                                break
                        except ValueError:
                            continue
                    break  # 組番号が見つかったらこの行は完了

        return result
