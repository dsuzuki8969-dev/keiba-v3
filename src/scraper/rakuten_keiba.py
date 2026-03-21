"""
楽天競馬スクレイパー（NAR結果取得専用）

NAR地方競馬の結果データを楽天競馬から取得する。
フォールバックチェーンの4番目として使用。
特にコーナー通過順位が各コーナー別で最も詳細。

取得対象:
  1. 着順・タイム
  2. コーナー通過順位（各コーナー別）
  3. 払戻金
"""

import re
import time
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from src.log import get_logger

logger = get_logger(__name__)

# ============================================================
# 定数・設定
# ============================================================

_BASE_URL = "https://keiba.rakuten.co.jp"
_REQ_INTERVAL = 2.0  # リクエスト間隔（秒）

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
}

# 券種名マッピング（楽天競馬 → netkeiba互換キー）
_PAYOUT_KEY_MAP = {
    "単勝": "tansho",
    "複勝": "fukusho",
    "馬複": "umaren",
    "馬単": "umatan",
    "ワイド": "wide",
    "枠複": "wakuren",
    "三連複": "sanrenpuku",
    "三連単": "sanrentan",
    # 枠単は netkeiba 側に対応キーがないためスキップ
}


class RakutenKeibaScraper:
    """楽天競馬スクレイパー（NAR結果取得専用）"""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)
        self._last_req = 0.0

    # ============================================================
    # レート制限
    # ============================================================

    def _wait(self):
        """リクエスト間隔を _REQ_INTERVAL 秒以上に保つ"""
        elapsed = time.time() - self._last_req
        if elapsed < _REQ_INTERVAL:
            time.sleep(_REQ_INTERVAL - elapsed)
        self._last_req = time.time()

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        """GETリクエストを実行し、BeautifulSoupオブジェクトを返す"""
        self._wait()
        try:
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            logger.warning(f"楽天競馬リクエスト失敗: {url} → {e}")
            return None

    # ============================================================
    # 着順テーブルのパース
    # ============================================================

    def _parse_result_table(self, soup: BeautifulSoup) -> Optional[List[Dict]]:
        """
        着順テーブル (table.dataTable) をパースする。

        返却:
            [{"horse_no": 6, "finish": 1, "time_sec": 84.5}, ...]
            パース失敗時は None
        """
        try:
            table = soup.select_one("table.dataTable")
            if not table:
                logger.debug("楽天競馬: 着順テーブルが見つからない")
                return None

            results = []
            rows = table.select("tbody tr")
            for row in rows:
                # 着順
                order_td = row.select_one("td.order")
                if not order_td:
                    continue
                order_text = order_td.get_text(strip=True)

                # 除外・取消・中止などは着順が数値でない
                try:
                    finish = int(order_text)
                except ValueError:
                    # 「除」「取」「中」等 → finish = 0 として記録
                    finish = 0

                # 馬番
                number_td = row.select_one("td.number")
                if not number_td:
                    continue
                try:
                    horse_no = int(number_td.get_text(strip=True))
                except ValueError:
                    continue

                # タイム
                time_td = row.select_one("td.time")
                time_sec = None
                if time_td:
                    time_sec = self._parse_time(time_td.get_text(strip=True))

                results.append({
                    "horse_no": horse_no,
                    "finish": finish,
                    "time_sec": time_sec,
                })

            if not results:
                logger.debug("楽天競馬: 着順データが空")
                return None

            return results

        except Exception as e:
            logger.warning(f"楽天競馬: 着順テーブルのパース失敗 → {e}")
            return None

    @staticmethod
    def _parse_time(time_str: str) -> Optional[float]:
        """
        タイム文字列を秒に変換する。
        例: "1:24.5" → 84.5, "55.3" → 55.3
        """
        if not time_str:
            return None
        # "分:秒.コンマ" 形式
        m = re.match(r"(\d+):(\d+\.\d+)", time_str)
        if m:
            return int(m.group(1)) * 60 + float(m.group(2))
        # "秒.コンマ" 形式（1分未満）
        m = re.match(r"(\d+\.\d+)", time_str)
        if m:
            return float(m.group(1))
        return None

    # ============================================================
    # コーナー通過順位のパース
    # ============================================================

    def _parse_corner_positions(self, soup: BeautifulSoup) -> Optional[Dict[int, List[int]]]:
        """
        コーナー通過順位テーブルをパースする。

        セレクタ: table.contentsTable[summary="コーナー通過順位"]
        各行 = 1コーナー、td.time に馬番がカンマ区切り。
        並走は (2,1) のように括弧で囲まれる。

        返却:
            {馬番: [1コーナー順位, 2コーナー順位, ...], ...}
            例: {6: [1, 1, 1, 1], 3: [3, 3, 2, 2]}
            パース失敗時は None
        """
        try:
            table = soup.select_one('table.contentsTable[summary="コーナー通過順位"]')
            if not table:
                logger.debug("楽天競馬: コーナー通過順位テーブルが見つからない")
                return None

            # 各コーナーの馬番順序リストを抽出
            corner_orders: List[List[List[int]]] = []
            rows = table.select("tbody tr")
            if not rows:
                rows = table.select("tr")

            for row in rows:
                th = row.select_one("th")
                td = row.select_one("td.time")
                if not th or not td:
                    continue

                th_text = th.get_text(strip=True)
                # 「１コーナー」「２コーナー」等であることを確認
                if "コーナー" not in th_text:
                    continue

                raw = td.get_text(strip=True)
                order_list = self._parse_corner_order(raw)
                if order_list:
                    corner_orders.append(order_list)

            if not corner_orders:
                logger.debug("楽天競馬: コーナー通過順位データが空")
                return None

            # 各馬のコーナー通過順位を構築
            result: Dict[int, List[int]] = {}
            for corner_idx, groups in enumerate(corner_orders):
                pos = 1
                for group in groups:
                    for horse_no in group:
                        if horse_no not in result:
                            result[horse_no] = []
                        # 前のコーナーが欠落している場合は 0 で埋める
                        while len(result[horse_no]) < corner_idx:
                            result[horse_no].append(0)
                        result[horse_no].append(pos)
                    pos += len(group)

            return result if result else None

        except Exception as e:
            logger.warning(f"楽天競馬: コーナー通過順位のパース失敗 → {e}")
            return None

    @staticmethod
    def _parse_corner_order(raw: str) -> Optional[List[List[int]]]:
        """
        コーナー通過順位の生テキストをパースする。

        例: "7,8,9,6,10,11,4,3,12,2,5,1"
             → [[7],[8],[9],[6],[10],[11],[4],[3],[12],[2],[5],[1]]
        例: "2,(3,1),5,4"
             → [[2],[3,1],[5],[4]]
        並走グループ (a,b) は同一順位として扱う。
        """
        if not raw:
            return None

        groups: List[List[int]] = []
        i = 0
        while i < len(raw):
            c = raw[i]
            if c in (" ", ",", "、", "\u3000"):
                i += 1
                continue
            if c == "(":
                # 並走グループ
                end = raw.find(")", i)
                if end == -1:
                    end = len(raw)
                inner = raw[i + 1:end]
                nums = re.findall(r"\d+", inner)
                if nums:
                    groups.append([int(n) for n in nums])
                i = end + 1
            elif c.isdigit():
                # 単独馬番
                m = re.match(r"\d+", raw[i:])
                if m:
                    groups.append([int(m.group())])
                    i += m.end()
                else:
                    i += 1
            else:
                i += 1

        return groups if groups else None

    # ============================================================
    # 払戻テーブルのパース
    # ============================================================

    def _parse_payouts(self, soup: BeautifulSoup) -> Optional[Dict[str, List[Dict]]]:
        """
        払戻金テーブルをパースする。

        セレクタ: table.contentsTable[summary="払戻金"]
        1行に左右2券種が並ぶ:
          <th>単勝</th><td class="number">4</td><td class="money">370 円</td><td class="rank">2番人気</td>
          <th>馬単</th><td class="number">4-10</td><td class="money">1,860 円</td><td class="rank">5番人気</td>

        返却:
            {
                "tansho": [{"combination": "4", "payout": 370, "popularity": 2}],
                "umatan": [{"combination": "4-10", "payout": 1860, "popularity": 5}],
                ...
            }
            パース失敗時は None
        """
        try:
            table = soup.select_one('table.contentsTable[summary="払戻金"]')
            if not table:
                logger.debug("楽天競馬: 払戻テーブルが見つからない")
                return None

            payouts: Dict[str, List[Dict]] = {}
            rows = table.select("tr")

            for row in rows:
                # 1行に複数の th が含まれる（左右2券種）
                ths = row.select("th")
                # 各 th 以降の td を取得して券種ごとに処理
                cells = list(row.children)
                # タグのみ抽出
                tags = [c for c in cells if hasattr(c, "name") and c.name in ("th", "td")]

                # th の位置を見つけて、その後ろの td 群を取得
                th_positions = [i for i, t in enumerate(tags) if t.name == "th"]

                for th_idx in th_positions:
                    th_tag = tags[th_idx]
                    bet_type_text = th_tag.get_text(strip=True)
                    key = _PAYOUT_KEY_MAP.get(bet_type_text)
                    if not key:
                        # 枠単など未対応の券種はスキップ
                        continue

                    # th の後ろにある td を最大3つ取得 (number, money, rank)
                    following_tds = []
                    for j in range(th_idx + 1, len(tags)):
                        if tags[j].name == "th":
                            break
                        if tags[j].name == "td":
                            following_tds.append(tags[j])

                    if len(following_tds) < 2:
                        continue

                    # 組み合わせ
                    number_td = following_tds[0]
                    combination = number_td.get_text(strip=True)

                    # 金額
                    money_td = following_tds[1]
                    payout = self._parse_money(money_td.get_text(strip=True))

                    # 人気
                    popularity = None
                    if len(following_tds) >= 3:
                        rank_td = following_tds[2]
                        popularity = self._parse_popularity(rank_td.get_text(strip=True))

                    if payout is None:
                        continue

                    entry = {
                        "combination": combination,
                        "payout": payout,
                    }
                    if popularity is not None:
                        entry["popularity"] = popularity

                    # 複勝・ワイドは複数行にまたがるため追記
                    if key not in payouts:
                        payouts[key] = []
                    payouts[key].append(entry)

            return payouts if payouts else None

        except Exception as e:
            logger.warning(f"楽天競馬: 払戻テーブルのパース失敗 → {e}")
            return None

    @staticmethod
    def _parse_money(text: str) -> Optional[int]:
        """
        金額文字列を整数に変換する。
        例: "370 円" → 370, "1,860 円" → 1860
        """
        if not text:
            return None
        # カンマ・スペース・「円」を除去して数値抽出
        cleaned = re.sub(r"[,\s円]", "", text)
        m = re.search(r"\d+", cleaned)
        return int(m.group()) if m else None

    @staticmethod
    def _parse_popularity(text: str) -> Optional[int]:
        """
        人気文字列を整数に変換する。
        例: "2番人気" → 2
        """
        if not text:
            return None
        m = re.search(r"(\d+)", text)
        return int(m.group(1)) if m else None

    # ============================================================
    # 公開メソッド: 結果取得
    # ============================================================

    def get_result(self, race_id: str, date: str = "") -> Optional[Dict]:
        """
        楽天競馬のrace_idで結果を取得する。

        Args:
            race_id: 楽天競馬のレースID（18桁）
            date: 日付文字列（ログ用、省略可）

        Returns:
            {
                "order": [
                    {
                        "horse_no": 6,
                        "finish": 1,
                        "time_sec": 84.5,
                        "corners": [1, 1, 1, 1],
                        "last_3f": None,
                    },
                    ...
                ],
                "payouts": {
                    "tansho": [{"combination": "4", "payout": 370, "popularity": 2}],
                    ...
                }
            }
            取得失敗時は None
        """
        url = f"{_BASE_URL}/race_performance/list/RACEID/{race_id}"
        log_label = f"楽天競馬 {date} {race_id}" if date else f"楽天競馬 {race_id}"

        logger.debug(f"{log_label}: 結果取得開始")

        soup = self._get(url)
        if not soup:
            return None

        # 着順テーブル
        result_rows = self._parse_result_table(soup)
        if not result_rows:
            logger.info(f"{log_label}: 着順データなし（レース未実施 or ページ構造変更）")
            return None

        # コーナー通過順位
        corners_map = self._parse_corner_positions(soup) or {}

        # 払戻
        payouts = self._parse_payouts(soup)

        # 結果を統合
        order = []
        for row in result_rows:
            horse_no = row["horse_no"]
            entry = {
                "horse_no": horse_no,
                "finish": row["finish"],
                "time_sec": row.get("time_sec"),
                "corners": corners_map.get(horse_no, []),
                "last_3f": None,  # 楽天競馬には上がり3Fデータなし
            }
            order.append(entry)

        result = {"order": order}
        if payouts:
            result["payouts"] = payouts

        logger.info(
            f"{log_label}: 結果取得完了 "
            f"(着順={len(order)}頭, コーナー={len(corners_map)}頭, "
            f"払戻={len(payouts) if payouts else 0}券種)"
        )
        return result

    # ============================================================
    # 公開メソッド: race_id 検索
    # ============================================================

    def find_race_id(
        self, date: str, venue_name: str, race_no: int
    ) -> Optional[str]:
        """
        日付・場名・レース番号から楽天競馬のrace_idを検索する。

        日程ページにアクセスし、各開催場のレースリンクから
        該当するrace_idを見つける。

        Args:
            date: 日付文字列 "YYYY-MM-DD" or "YYYYMMDD"
            venue_name: 場名（例: "水沢", "大井", "船橋"）
            race_no: レース番号（1-12）

        Returns:
            楽天競馬のrace_id（18桁文字列）。見つからない場合は None。
        """
        # 日付を YYYYMMDD に正規化
        date_clean = date.replace("-", "")
        if len(date_clean) != 8:
            logger.warning(f"楽天競馬: 不正な日付形式 → {date}")
            return None

        # 日程ページURL: 末尾10桁は0埋め
        schedule_url = f"{_BASE_URL}/race_card/list/RACEID/{date_clean}0000000000"
        log_label = f"楽天競馬 find_race_id({date}, {venue_name}, {race_no}R)"

        logger.debug(f"{log_label}: 日程ページ取得 → {schedule_url}")

        soup = self._get(schedule_url)
        if not soup:
            logger.info(f"{log_label}: 日程ページ取得失敗")
            return None

        try:
            # レースカードのリンクを全て取得
            # リンク形式: /race_card/list/RACEID/{18桁race_id}
            # または: /race_performance/list/RACEID/{18桁race_id}
            links = soup.select("a[href*='/RACEID/']")

            # 場名でフィルタリング
            # 日程ページには開催場ごとにセクションがある
            # まず場名を含むセクションを探す
            candidates = []
            for link in links:
                href = link.get("href", "")
                # race_id を抽出
                m = re.search(r"/RACEID/(\d{18})", href)
                if not m:
                    continue
                rid = m.group(1)

                # リンクテキストまたは周辺テキストから場名を判定
                # 日程ページではレースリストが場名セクションの下に並ぶ
                # リンクの親要素をたどって場名を確認
                parent_text = self._find_venue_context(link, venue_name)
                if parent_text:
                    candidates.append(rid)

            if not candidates:
                # フォールバック: 場名でフィルタできない場合、
                # 全 race_id の末尾2桁がレース番号に一致するものを返す
                logger.debug(f"{log_label}: 場名一致なし → 末尾R番号で全候補検索")
                race_no_str = f"{race_no:02d}"
                for link in links:
                    href = link.get("href", "")
                    m = re.search(r"/RACEID/(\d{18})", href)
                    if m and m.group(1).endswith(race_no_str):
                        # 場名の手がかりが全くない場合は最初の一致を返す
                        candidates.append(m.group(1))

            # レース番号でフィルタ
            race_no_str = f"{race_no:02d}"
            for rid in candidates:
                if rid.endswith(race_no_str):
                    logger.info(f"{log_label}: race_id 発見 → {rid}")
                    return rid

            logger.info(f"{log_label}: 一致するrace_idなし (候補={len(candidates)}件)")
            return None

        except Exception as e:
            logger.warning(f"{log_label}: 検索失敗 → {e}")
            return None

    @staticmethod
    def _find_venue_context(link_tag, venue_name: str) -> bool:
        """
        リンクタグの周辺コンテキストから場名が含まれるか判定する。
        親要素を最大5階層までたどって場名テキストを探す。
        """
        current = link_tag
        for _ in range(5):
            parent = current.parent
            if parent is None:
                break
            # 親要素のテキスト（子要素含む）に場名が含まれるか
            # ただし巨大な要素のテキストを全部取ると遅いので、
            # 直近の見出し (h2, h3, th, caption) を優先探索
            for heading_tag in ("h1", "h2", "h3", "h4", "th", "caption", "dt"):
                headings = parent.select(heading_tag)
                for h in headings:
                    if venue_name in h.get_text():
                        return True
            current = parent
        return False
