"""
競馬ブック レース結果 + 過去走スクレイパー

keibabook_training.py から分離。結果取得・着順パース・払戻パース・過去走取得を担当。
"""

import re
from datetime import date
from typing import Dict, Optional, Union

try:
    from bs4 import BeautifulSoup
except ImportError:
    pass

from src.log import get_logger

logger = get_logger(__name__)

try:
    from data.masters.venue_master import get_venue_code_from_race_id
    from data.masters.venue_master import is_jra as _is_jra_venue
except Exception:
    get_venue_code_from_race_id = lambda rid: rid[4:6] if len(rid) >= 6 else "00"
    _is_jra_venue = lambda vc: vc in ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10")

from src.scraper.keibabook_training import (
    NAR_VENUE_TO_KB,
    KeibabookClient,
    jra_netkeiba_to_kb_id,
)

# URL定数
KB_CYUOU_SEISEKI = "https://s.keibabook.co.jp/cyuou/seiseki"
KB_CHIHOU_SEISEKI = "https://s.keibabook.co.jp/chihou/seiseki"
KB_CHIHOU_NITTEI = "https://s.keibabook.co.jp/chihou/nittei"
KB_UMA_DB = "https://s.keibabook.co.jp/db/uma"

# 券種名 → キー変換
_KB_TICKET_MAP = {
    "単勝": "tansho",
    "複勝": "fukusho",
    "枠連": "wakuren",
    "馬連": "umaren",
    "ワイド": "wide",
    "馬単": "umatan",
    "三連複": "sanrenpuku",
    "三連単": "sanrentan",
}


class KeibabookResultScraper:
    """競馬ブック 結果取得 + 過去走取得スクレイパー"""

    def __init__(self, client: KeibabookClient):
        self.client = client

    # ----------------------------------------------------------
    # 結果取得
    # ----------------------------------------------------------

    def fetch_result(
        self,
        netkeiba_race_id: str,
        race_date: Optional[Union[date, str]] = None,
    ) -> Optional[dict]:
        """
        netkeiba race_id からレース結果（着順・払戻・通過順）を取得する。
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

        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "/seiseki/" not in href and "/syutuba/" not in href:
                continue
            link_text = a.get_text(strip=True)
            m = re.search(r"(\d+)\s*R", link_text)
            if m and m.group(1).zfill(2) == race_no:
                parts = href.rstrip("/").split("/")
                if parts:
                    return parts[-1]

        return None

    def _parse_result_table(self, soup: "BeautifulSoup") -> list:
        """着順テーブル (table.default.seiseki) をパース"""
        table = soup.select_one("table.default.seiseki")
        if not table:
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
                finish_cell = row.select_one("td.cyakujun")
                if not finish_cell:
                    finish_cell = cells[0] if cells else None
                if not finish_cell:
                    continue
                finish_text = finish_cell.get_text(strip=True)
                if not finish_text or not finish_text.isdigit():
                    continue
                finish = int(finish_text)

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

                left_cell = row.select_one("td.left")
                last_3f = None
                corners = []

                if left_cell:
                    tuka = left_cell.select_one("ul.tuka")
                    if tuka:
                        tuka_text = tuka.get_text(strip=True)
                        if tuka_text and "**" not in tuka_text and "※" not in tuka_text:
                            corners = [int(x) for x in re.findall(r"\d+", tuka_text)]

                time_sec = 0.0
                for c in cells:
                    ct = c.get_text(strip=True)
                    tm = re.match(r"^(\d):(\d{2})\.(\d)$", ct)
                    if tm:
                        time_sec = int(tm.group(1)) * 60 + int(tm.group(2)) + int(tm.group(3)) * 0.1
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

            midasi_cell = row.select_one("td.midasi")
            if not midasi_cell:
                midasi_cell = cells[0]
            ticket_name = midasi_cell.get_text(strip=True)
            key = _KB_TICKET_MAP.get(ticket_name)
            if not key:
                continue

            combo_cell = cells[1] if len(cells) > 1 else None
            payout_cell = cells[2] if len(cells) > 2 else None
            if not combo_cell or not payout_cell:
                continue

            combo_parts = re.split(r"<br\s*/?>", str(combo_cell))
            payout_parts = re.split(r"<br\s*/?>", str(payout_cell))

            entries = []
            for i, (cp, pp) in enumerate(zip(combo_parts, payout_parts)):
                combo_text = BeautifulSoup(cp, "lxml").get_text(strip=True)
                payout_text = BeautifulSoup(pp, "lxml").get_text(strip=True)

                combo = re.sub(r"[^\d\-]", "", combo_text).strip("-")
                if not combo:
                    continue

                payout_val = re.sub(r"[^\d]", "", payout_text)
                if not payout_val:
                    continue

                entries.append({
                    "combo": combo,
                    "payout": int(payout_val),
                    "popularity": i + 1,
                })

            if entries:
                payouts[key] = entries

        return payouts

    # ----------------------------------------------------------
    # 過去走取得
    # ----------------------------------------------------------

    def fetch_horse_history(
        self,
        kb_horse_id: str,
    ) -> list:
        """競馬ブック馬詳細ページから過去走データを取得する。"""
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

                dt0 = dls[0].select_one("dt")
                if dt0:
                    negahi = dt0.select_one("span.negahi")
                    if negahi:
                        entry["venue_race"] = negahi.get_text(strip=True).replace("\xa0", " ")

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
        """結果ページから馬番→KB horse_id のマッピングを取得する。"""
        mapping = {}
        table = soup_result.select_one("table.default.seiseki")
        if not table:
            return mapping

        for row in table.select("tbody tr"):
            try:
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

                for a in row.select("a[href]"):
                    href = a.get("href", "")
                    m = re.search(r"/db/uma/(\d+)", href)
                    if m:
                        mapping[horse_no] = m.group(1)
                        break
            except Exception:
                continue

        return mapping
