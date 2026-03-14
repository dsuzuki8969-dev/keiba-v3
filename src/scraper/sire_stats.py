"""
父馬・母父馬の産駒成績スクレイパー

type=2: 距離別 (sprint/mile/middle/long × 芝/ダート)
type=1: コース・馬場別 (芝/ダート × 良/稍重/重/不良)

horse/sire.html?id=XXX&course=1&mode=1&type=2 or type=1
種牡馬(type=sire)と母父(BMS, type=bms)の両方のテーブルをパース
"""

from typing import Dict, Tuple

try:
    from bs4 import BeautifulSoup

    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

# netkeiba: -1400, -1800, -2200, -2600, 2600- (各4列)
# 自前: sprint, mile, middle, long
BUCKETS = ["sprint", "mile", "middle", "long"]


def _parse_int(text) -> int:
    t = (text or "").strip() if hasattr(text, "strip") else str(text)
    return int(t) if str(t).isdigit() else 0


def parse_sire_course_condition_page(soup) -> Dict[str, Dict[Tuple[str, str], dict]]:
    """
    産駒コース・馬場別ページのHTMLをパース (type=1)
    テーブル: 芝・良/稍重/重/不良、ダート・良/稍重/重/不良（各4列: 1着2着3着着外）
    障害はスキップ（芝4+ダート4=8ブロック×4列=32列）
    Returns: {"sire": {(surface, condition): stats}, "bms": {...}}
    """
    if not HAS_DEPS or not soup:
        return {"sire": {}, "bms": {}}

    result = {"sire": {}, "bms": {}}
    conditions = ["良", "稍重", "重", "不良"]
    surfaces = ["芝", "ダート"]

    for stat_type in ("sire", "bms"):
        for row in soup.select("tr"):
            cells = row.select("td")
            if len(cells) < 20:
                continue
            if cells[0].get_text(strip=True) != "累計":
                continue
            if not row.select_one(f"a[href*='type={stat_type}']"):
                continue
            idx = 1
            for surf in surfaces:
                for cond in conditions:
                    if idx + 4 > len(cells):
                        break
                    w = _parse_int(cells[idx].get_text())
                    p2 = _parse_int(cells[idx + 1].get_text())
                    p3 = _parse_int(cells[idx + 2].get_text())
                    o = _parse_int(cells[idx + 3].get_text())
                    runs = w + p2 + p3 + o
                    if runs > 0:
                        key = (surf, cond)
                        result[stat_type][key] = {
                            "wins": w,
                            "places": w + p2 + p3,
                            "runs": runs,
                            "win_rate": w / runs,
                            "place_rate": (w + p2 + p3) / runs,
                        }
                    idx += 4
            break
    return result


def parse_sire_page(soup) -> Dict[str, Dict[Tuple[str, str], dict]]:
    """
    産駒距離別ページのHTMLをパース
    Returns: {"sire": {(bucket, surface): stats}, "bms": {(bucket, surface): stats}}
    """
    if not HAS_DEPS or not soup:
        return {"sire": {}, "bms": {}}

    result = {"sire": {}, "bms": {}}
    for stat_type in ("sire", "bms"):
        for row in soup.select("tr"):
            cells = row.select("td")
            if len(cells) < 21:
                continue
            if cells[0].get_text(strip=True) != "累計":
                continue
            if not row.select_one(f"a[href*='type={stat_type}']"):
                continue

            # 芝: 5バケット×4列 (-1400,-1800,-2200,-2600,2600-)
            # 自前: sprint=-1400, mile=-1800, middle=-2200+-2600, long=2600-
            def add_bucket(surf: str, idx: int, bucket: str):
                if idx + 4 > len(cells):
                    return idx + 4
                w = _parse_int(cells[idx].get_text())
                p2 = _parse_int(cells[idx + 1].get_text())
                p3 = _parse_int(cells[idx + 2].get_text())
                o = _parse_int(cells[idx + 3].get_text())
                runs = w + p2 + p3 + o
                if runs > 0:
                    result[stat_type][(bucket, surf)] = {
                        "wins": w,
                        "places": w + p2 + p3,
                        "runs": runs,
                        "win_rate": w / runs,
                        "place_rate": (w + p2 + p3) / runs,
                    }
                return idx + 4

            idx = 1
            for surf in ("芝", "ダート"):
                idx = add_bucket(surf, idx, "sprint")  # -1400
                idx = add_bucket(surf, idx, "mile")  # -1800
                # -2200 + -2600 → middle
                if idx + 8 <= len(cells):
                    w1 = _parse_int(cells[idx].get_text()) + _parse_int(cells[idx + 4].get_text())
                    p1 = _parse_int(cells[idx + 1].get_text()) + _parse_int(
                        cells[idx + 5].get_text()
                    )
                    p2 = _parse_int(cells[idx + 2].get_text()) + _parse_int(
                        cells[idx + 6].get_text()
                    )
                    o1 = _parse_int(cells[idx + 3].get_text()) + _parse_int(
                        cells[idx + 7].get_text()
                    )
                    runs = w1 + p1 + p2 + o1
                    if runs > 0:
                        result[stat_type][("middle", surf)] = {
                            "wins": w1,
                            "places": w1 + p1 + p2,
                            "runs": runs,
                            "win_rate": w1 / runs,
                            "place_rate": (w1 + p1 + p2) / runs,
                        }
                idx += 8
                idx = add_bucket(surf, idx, "long")  # 2600-
            break
    return result


def fetch_sire_distance_stats(sire_id: str, client) -> Dict[str, Dict[Tuple[str, str], dict]]:
    """父馬/母父馬の産駒距離別ページを取得してパース (type=2)"""
    if not sire_id or not client:
        return {"sire": {}, "bms": {}}
    try:
        from src.scraper.netkeiba import BASE_URL

        url = f"{BASE_URL}/horse/sire.html?id={sire_id}&course=1&mode=1&type=2"
        soup = client.get(url)
        return parse_sire_page(soup) if soup else {"sire": {}, "bms": {}}
    except Exception:
        return {"sire": {}, "bms": {}}


def fetch_sire_course_condition_stats(
    sire_id: str, client
) -> Dict[str, Dict[Tuple[str, str], dict]]:
    """父馬/母父馬の産駒コース・馬場別ページを取得してパース (type=1)"""
    if not sire_id or not client:
        return {"sire": {}, "bms": {}}
    try:
        from src.scraper.netkeiba import BASE_URL

        url = f"{BASE_URL}/horse/sire.html?id={sire_id}&course=1&mode=1&type=1"
        soup = client.get(url)
        return parse_sire_course_condition_page(soup) if soup else {"sire": {}, "bms": {}}
    except Exception:
        return {"sire": {}, "bms": {}}


def fetch_sire_all_stats(sire_id: str, client) -> dict:
    """
    距離別(type=2)とコース・馬場別(type=1)の両方を取得
    Returns: {"distance": {sire/bms}, "course_condition": {sire/bms}}
    """
    dist = fetch_sire_distance_stats(sire_id, client)
    cc = fetch_sire_course_condition_stats(sire_id, client)
    return {"distance": dist, "course_condition": cc}
