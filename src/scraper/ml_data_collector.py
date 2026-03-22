"""
ML学習データの軽量コレクター

結果ページ(result.html)から全馬のデータを収集し、
LightGBM等の機械学習に使えるデータセットを構築する。

特徴:
  - 1レース = 1リクエスト（結果ページだけで全馬分取れる）
  - レジューム対応（中断→再開で重複なし）
  - 日単位でJSON保存（data/ml/{YYYYMMDD}.json）
"""

import json
import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from src.log import get_logger

logger = get_logger(__name__)

from data.masters.venue_master import (
    JRA_CODES,
    get_venue_code_from_race_id,
    get_venue_name,
    is_banei,
)

ML_DATA_DIR = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")),
    "data",
    "ml",
)
STATE_PATH = os.path.join(ML_DATA_DIR, "_collector_state.json")


def _parse_time(time_str: str) -> float:
    """'1:34.5' -> 94.5"""
    try:
        if ":" in time_str:
            parts = time_str.split(":")
            return int(parts[0]) * 60 + float(parts[1])
        return float(time_str)
    except Exception:
        return 0.0


def _parse_corners(pos_text: str) -> list:
    """'05-05-04-03' -> [5, 5, 4, 3]"""
    result = []
    for part in str(pos_text).split("-"):
        p = part.strip()
        if p.isdigit():
            result.append(int(p))
    return result


def _parse_weight_str(text: str) -> tuple:
    """'486(+4)' -> (486, 4), '486(-8)' -> (486, -8)"""
    text = text.strip()
    m = re.match(r"(\d+)\s*\(([+\-]?\d+)\)", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    m2 = re.match(r"(\d+)", text)
    if m2:
        return int(m2.group(1)), None
    return None, None


def _parse_sex_age(text: str) -> tuple:
    """'牡5' -> ('牡', 5)"""
    text = text.strip()
    if len(text) >= 2:
        sex = text[0]
        try:
            age = int(text[1:])
        except ValueError:
            age = None
        return sex, age
    return text, None


def _extract_id_from_link(tag, pattern: str) -> str:
    if not tag:
        return ""
    a = tag.find("a") if tag.name != "a" else tag
    if not a:
        a = tag.select_one("a")
    if a:
        href = a.get("href", "")
        m = re.search(pattern, href)
        if m:
            return m.group(1)
    return ""


def _infer_grade(race_name: str, is_jra: bool) -> str:
    rn = race_name
    if re.search(r"[GＧ][1１]", rn):
        return "G1"
    if re.search(r"[GＧ][2２]", rn):
        return "G2"
    if re.search(r"[GＧ][3３]", rn):
        return "G3"
    if re.search(r"[（(][LＬ][）)]", rn):
        return "L"
    if "新馬" in rn:
        return "新馬"
    if "未勝利" in rn:
        return "未勝利"
    if is_jra:
        if "1勝" in rn or "500万" in rn:
            return "1勝"
        if "2勝" in rn or "1000万" in rn:
            return "2勝"
        if "3勝" in rn or "1600万" in rn:
            return "3勝"
        if "OP" in rn or "オープン" in rn:
            return "OP"
        return "OP"
    if re.search(r"Jpn\s*[123]", rn, re.IGNORECASE) or "交流" in rn:
        return "交流重賞"
    return "その他"


def _parse_full_lap_data(soup: BeautifulSoup, distance: int) -> dict:
    """ラップタイムセクションから前半3F・ペース文字を抽出"""
    first_3f, pace_letter = None, ""

    # --- ペース文字 (S/M/H) ---
    # 方法1: .RapPace_Title 内の <span> から直接取得（JRA結果ページ）
    rap_title = soup.select_one(".RapPace_Title")
    if rap_title:
        span = rap_title.select_one("span")
        if span:
            t = span.get_text(strip=True).upper()
            if t in ("S", "M", "H"):
                pace_letter = t

    # 方法2: フォールバック — テキストノードで "ペース" + S/M/H を探す
    if not pace_letter:
        for el in soup.find_all(string=lambda t: t and "ペース" in str(t)):
            m = re.search(r"ペース[：:]\s*([SMH])", str(el))
            if m:
                pace_letter = m.group(1).strip()
                break

    # 方法3: ペース文字がない場合、前半3Fと後半3Fの差から推定
    # (first_3f 取得後に実施)

    # --- 前半3F ---
    for header in soup.find_all(["h2", "h3", "h4", "th", "dt"]):
        if "ラップ" not in header.get_text() and "lap" not in header.get_text().lower():
            continue
        table = header.find_next("table")
        if not table:
            continue
        rows = table.select("tr")
        if len(rows) < 3:
            continue
        lap_row = rows[2]
        rcells = lap_row.select("td") or lap_row.find_all("td")
        if len(rcells) >= 3:
            try:
                first_3f = sum(
                    float(c.get_text(strip=True).replace(",", ".")) for c in rcells[:3]
                )
            except ValueError:
                pass
        break

    # 方法3: ペース推定 — ラップの前半合計と後半合計を比較
    if not pace_letter and first_3f is not None:
        # 全馬の上がり3F平均をレース全体の後半3F近似に使わず、
        # ラップテーブルの後半3ハロンを使う
        for header in soup.find_all(["h2", "h3", "h4", "th", "dt"]):
            if "ラップ" not in header.get_text() and "lap" not in header.get_text().lower():
                continue
            table = header.find_next("table")
            if not table:
                continue
            rows = table.select("tr")
            if len(rows) < 3:
                continue
            lap_row = rows[2]
            rcells = lap_row.select("td") or lap_row.find_all("td")
            if len(rcells) >= 6:
                try:
                    last_3f = sum(
                        float(c.get_text(strip=True).replace(",", "."))
                        for c in rcells[-3:]
                    )
                    diff = first_3f - last_3f
                    if diff <= -1.0:
                        pace_letter = "S"
                    elif diff >= 1.0:
                        pace_letter = "H"
                    else:
                        pace_letter = "M"
                except (ValueError, TypeError):
                    pass
            break

    return {"first_3f": first_3f, "pace": pace_letter}


def parse_result_page(soup: BeautifulSoup, race_id: str) -> Optional[dict]:
    """
    結果ページ1枚から、1レースの全データを抽出する。

    Returns: {
        "race_id", "race_name", "date", "venue", "venue_code",
        "surface", "distance", "direction", "condition", "weather",
        "field_count", "grade", "is_jra",
        "first_3f", "pace",
        "horses": [ {...全馬分} ],
        "payouts": { "単勝": {...}, ... }
    }
    """
    venue_code = get_venue_code_from_race_id(race_id)
    is_jra = venue_code in JRA_CODES

    # --- レースヘッダー ---
    race_name = ""
    name_el = soup.select_one(".RaceName")
    if name_el:
        race_name = name_el.get_text(strip=True)

    data1 = soup.select_one(".RaceData01")
    surface, distance, direction, condition, weather = "芝", 1600, "右", "良", ""
    water_content = None  # ばんえい: 馬場水分量(%)
    _is_banei = is_banei(venue_code)
    if data1:
        txt = data1.get_text()
        sm = re.search(r"(芝|ダ|障|直)", txt)
        dm = re.search(r"(\d{3,4})m", txt)
        dr = re.search(r"\((右|左|直線)\)", txt)
        bm = re.search(r"馬場[：:]\s*([良稍重不]+)", txt)
        wm = re.search(r"天候[：:]\s*(\S+)", txt)
        # ばんえい: 水分量を取得
        water_m = re.search(r"水分量[：:]\s*([\d.]+)", txt)
        if water_m:
            water_content = float(water_m.group(1))
        if sm:
            s = sm.group(1)
            if s == "芝":
                surface = "芝"
            elif s == "障":
                surface = "障害"
            else:
                surface = "ダート"  # 「ダ」「直」→ダート
        if dm:
            distance = int(dm.group(1))
        if dr:
            direction = dr.group(1)
        if bm:
            condition = bm.group(1)
        if wm:
            weather = wm.group(1)
        # ばんえい固有: direction/conditionの補正
        if _is_banei:
            direction = "直"
            # 水分量からconditionを推定（馬場:良/稍重等がないため）
            if water_content is not None and not bm:
                if water_content <= 1.5:
                    condition = "良"
                elif water_content <= 2.5:
                    condition = "稍重"
                elif water_content <= 3.5:
                    condition = "重"
                else:
                    condition = "不良"

    data2 = soup.select_one(".RaceData02")
    field_count = 0
    if data2:
        fm = re.search(r"(\d+)頭", data2.get_text())
        if fm:
            field_count = int(fm.group(1))

    # 日付（HTMLメタ → race_id フォールバック）
    race_date = ""
    for el in soup.select('meta[property="og:description"], meta[property="og:title"]'):
        cnt = el.get("content", "")
        m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", cnt)
        if m:
            race_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            break
    if not race_date:
        title = soup.find("title")
        if title:
            m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", title.get_text())
            if m:
                race_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    if not race_date and len(race_id) >= 10:
        if not is_jra:
            race_date = f"{race_id[:4]}-{race_id[6:8]}-{race_id[8:10]}"
        else:
            race_date = f"{race_id[:4]}-01-01"

    grade = _infer_grade(race_name, is_jra)
    lap_data = _parse_full_lap_data(soup, distance)

    # --- 結果テーブル（全馬） ---
    table = soup.select_one(".ResultTableWrap table")
    if not table:
        return None

    # ヘッダーから列位置を動的に特定
    col_map = {}
    header_row = table.select_one("thead tr")
    if header_row:
        for i, th in enumerate(header_row.select("th")):
            t = th.get_text(strip=True)
            if t == "着順":
                col_map["finish"] = i
            elif t == "枠番" or t == "枠":
                col_map["gate"] = i
            elif t == "馬番":
                col_map["horse_no"] = i
            elif t == "馬名":
                col_map["horse_name"] = i
            elif t == "性齢":
                col_map["sex_age"] = i
            elif t == "斤量" or t == "斤":
                col_map["weight_kg"] = i
            elif t == "騎手":
                col_map["jockey"] = i
            elif t == "タイム":
                col_map["time"] = i
            elif t == "着差":
                col_map["margin"] = i
            elif t == "通過" or t == "コーナー通過順":
                col_map["corners"] = i
            elif "上り" in t or "上がり" in t or "3F" in t or "ﾀｲﾑ" in t:
                col_map["last3f"] = i
            elif "単勝" in t or t == "オッズ":
                col_map["odds"] = i
            elif t == "人気":
                col_map["popularity"] = i
            elif "馬体重" in t or t == "体重" or "馬重" in t:
                col_map["horse_weight"] = i
            elif "調教師" in t or "厩舎" in t:
                col_map["trainer"] = i

    # フォールバック（JRA標準配置: 着順/枠/馬番/馬名/性齢/斤量/騎手/タイム/着差/通過/上り/単勝/人気/馬体重/調教師）
    col_map.setdefault("finish", 0)
    col_map.setdefault("gate", 1)
    col_map.setdefault("horse_no", 2)
    col_map.setdefault("horse_name", 3)
    col_map.setdefault("sex_age", 4)
    col_map.setdefault("weight_kg", 5)
    col_map.setdefault("jockey", 6)
    col_map.setdefault("time", 7)
    col_map.setdefault("margin", 8)
    col_map.setdefault("corners", 9)
    col_map.setdefault("last3f", 10)
    col_map.setdefault("odds", 11)
    col_map.setdefault("popularity", 12)
    col_map.setdefault("horse_weight", 13)

    horses = []
    for row in table.select("tbody tr"):
        cells = row.select("td")
        if len(cells) < 8:
            continue

        finish_text = cells[col_map["finish"]].get_text(strip=True)
        if not finish_text.isdigit():
            # 除外・中止・取消
            finish = None
            status = finish_text
        else:
            finish = int(finish_text)
            status = None

        gate_text = cells[col_map["gate"]].get_text(strip=True)
        gate_no = int(gate_text) if gate_text.isdigit() else None

        horse_no_text = cells[col_map["horse_no"]].get_text(strip=True)
        horse_no = int(horse_no_text) if horse_no_text.isdigit() else None

        horse_name_cell = cells[col_map["horse_name"]]
        horse_name = horse_name_cell.get_text(strip=True)
        horse_id = _extract_id_from_link(horse_name_cell, r"/horse/(\w+)")

        sex, age = _parse_sex_age(cells[col_map["sex_age"]].get_text(strip=True))

        weight_kg_text = cells[col_map["weight_kg"]].get_text(strip=True)
        try:
            weight_kg = float(weight_kg_text)
        except ValueError:
            weight_kg = None

        jockey_cell = cells[col_map["jockey"]]
        jockey_name = jockey_cell.get_text(strip=True)
        jockey_id = _extract_id_from_link(jockey_cell, r"/jockey/(?:result/recent/)?(\w+)")

        # タイム（列位置 or 全セル走査）
        finish_time_sec = 0.0
        if "time" in col_map and col_map["time"] < len(cells):
            finish_time_sec = _parse_time(cells[col_map["time"]].get_text(strip=True))
        if finish_time_sec <= 0:
            for c in cells:
                t = c.get_text(strip=True)
                if re.match(r"\d+:\d{2}\.\d", t):
                    finish_time_sec = _parse_time(t)
                    break

        margin = ""
        if "margin" in col_map and col_map["margin"] < len(cells):
            margin = cells[col_map["margin"]].get_text(strip=True)

        # 通過順
        corners = []
        if "corners" in col_map and col_map["corners"] < len(cells):
            corners = _parse_corners(cells[col_map["corners"]].get_text(strip=True))

        # 上がり3F
        last3f = None
        if "last3f" in col_map and col_map["last3f"] < len(cells):
            t = cells[col_map["last3f"]].get_text(strip=True)
            try:
                v = float(t)
                if 25.0 <= v <= 50.0:
                    last3f = v
            except ValueError:
                pass
        if last3f is None:
            for ci in range(8, min(len(cells), 14)):
                t = cells[ci].get_text(strip=True)
                try:
                    v = float(t)
                    if 28.0 <= v <= 48.0:
                        last3f = v
                        break
                except ValueError:
                    pass

        # 単勝オッズ・人気
        odds, popularity = None, None
        if "odds" in col_map and col_map["odds"] < len(cells):
            ot = cells[col_map["odds"]].get_text(strip=True).replace(",", "")
            try:
                odds = float(ot)
            except ValueError:
                pass
        if "popularity" in col_map and col_map["popularity"] < len(cells):
            pt = cells[col_map["popularity"]].get_text(strip=True)
            if pt.isdigit():
                popularity = int(pt)

        # ヘッダーで見つからなかった場合: 後方列を走査
        if odds is None or popularity is None:
            for ci in range(8, len(cells)):
                t = cells[ci].get_text(strip=True).replace(",", "")
                if odds is None:
                    try:
                        v = float(t)
                        if 1.0 <= v <= 9999:
                            odds = v
                            continue
                    except ValueError:
                        pass
                if popularity is None and t.isdigit():
                    v = int(t)
                    if 1 <= v <= 30:
                        popularity = v

        # 馬体重
        horse_weight, weight_change = None, None
        if "horse_weight" in col_map and col_map["horse_weight"] < len(cells):
            horse_weight, weight_change = _parse_weight_str(
                cells[col_map["horse_weight"]].get_text(strip=True)
            )
        if horse_weight is None:
            for ci in range(len(cells) - 1, 7, -1):
                hw, wc = _parse_weight_str(cells[ci].get_text(strip=True))
                if hw and 300 <= hw <= 700:
                    horse_weight, weight_change = hw, wc
                    break

        # 調教師
        trainer_name, trainer_id = "", ""
        if "trainer" in col_map and col_map["trainer"] < len(cells):
            tc = cells[col_map["trainer"]]
            trainer_name = tc.get_text(strip=True)
            trainer_id = _extract_id_from_link(tc, r"/trainer/(?:result/recent/)?(\w+)")
        if not trainer_id:
            trainer_a = row.select_one("a[href*='/trainer/']")
            if trainer_a:
                trainer_name = trainer_a.get_text(strip=True)
                tm = re.search(r"/trainer/(?:result/recent/)?(\w+)", trainer_a.get("href", ""))
                if tm:
                    trainer_id = tm.group(1)

        entry = {
            "finish_pos": finish,
            "status": status,
            "gate_no": gate_no,
            "horse_no": horse_no,
            "horse_id": horse_id,
            "horse_name": horse_name,
            "sex": sex,
            "age": age,
            "weight_kg": weight_kg,
            "jockey": jockey_name,
            "jockey_id": jockey_id,
            "trainer": trainer_name,
            "trainer_id": trainer_id,
            "finish_time_sec": finish_time_sec if finish_time_sec > 0 else None,
            "margin": margin,
            "last_3f_sec": last3f,
            "first_3f_sec": round(finish_time_sec - last3f, 2) if (finish_time_sec and finish_time_sec > 0 and last3f and last3f > 0) else None,
            "positions_corners": corners,
            "odds": odds,
            "popularity": popularity,
            "horse_weight": horse_weight,
            "weight_change": weight_change,
        }
        horses.append(entry)

    if not horses:
        return None

    if field_count == 0:
        field_count = len([h for h in horses if h["finish_pos"] is not None])

    # --- 払戻 ---
    payouts = _parse_payouts(soup)

    result = {
        "race_id": race_id,
        "race_name": race_name,
        "date": race_date,
        "venue": get_venue_name(venue_code),
        "venue_code": venue_code,
        "surface": surface,
        "distance": distance,
        "direction": direction,
        "condition": condition,
        "weather": weather,
        "field_count": field_count,
        "grade": grade,
        "is_jra": is_jra,
        "first_3f": lap_data["first_3f"],
        "pace": lap_data["pace"],
        "horses": horses,
        "payouts": payouts,
    }
    # ばんえい: 水分量を追加
    if water_content is not None:
        result["water_content"] = water_content
    return result


def _parse_payouts(soup: BeautifulSoup) -> dict:
    payouts = {}
    for tbl_sel in [".Payout_Detail_Table", "table.pay_table_01", "table.payout"]:
        payout_table = soup.select_one(tbl_sel)
        if payout_table:
            break
    if not payout_table:
        return payouts

    for tr in payout_table.select("tr"):
        cells = tr.select("td, th")
        if len(cells) < 2:
            continue
        label = cells[0].get_text(strip=True)
        targets = ("馬連", "馬単", "ワイド", "三連複", "三連単", "複勝", "単勝")
        if label not in targets:
            continue
        combo = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        payout_text = cells[2].get_text(strip=True).replace(",", "") if len(cells) > 2 else ""
        try:
            payout_val = int(re.sub(r"[^\d]", "", payout_text)) if payout_text else 0
        except ValueError:
            payout_val = 0
        payouts[label] = {"combo": combo, "payout": payout_val}

    return payouts


# ============================================================
# レジューム付きコレクター
# ============================================================


def _load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _date_output_path(date_str: str) -> str:
    return os.path.join(ML_DATA_DIR, f"{date_str.replace('-', '')}.json")


def _save_day_data(date_str: str, races: list):
    os.makedirs(ML_DATA_DIR, exist_ok=True)
    path = _date_output_path(date_str)
    data = {"date": date_str, "race_count": len(races), "races": races}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def collect_ml_data(
    client,
    race_list_scraper,
    start_date: str,
    end_date: str,
    jra_only: bool = False,
    nar_only: bool = False,
    resume: bool = True,
) -> dict:
    """
    指定期間のレース結果を収集し、ML用データとして日別JSONに保存する。

    Args:
        client: NetkeibaClient
        race_list_scraper: RaceListScraper
        start_date: "YYYY-MM-DD"
        end_date: "YYYY-MM-DD"
        jra_only: JRAのみ
        nar_only: NARのみ
        resume: True=前回の続きから

    Returns:
        {"total_days": N, "total_races": N, "total_horses": N, "skipped_days": N}
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    state = _load_state() if resume else {}
    last_completed = state.get("last_completed_date")

    if resume and last_completed:
        resume_dt = datetime.strptime(last_completed, "%Y-%m-%d") + timedelta(days=1)
        if resume_dt > start_dt:
            start_dt = resume_dt
        if start_dt > end_dt:
            logger.info(f"既に完了済み (最終: {last_completed})")
            return {
                "total_days": 0,
                "total_races": state.get("total_races", 0),
                "total_horses": state.get("total_horses", 0),
                "skipped_days": 0,
            }

    dates = []
    d = start_dt
    while d <= end_dt:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    total_races = state.get("total_races", 0)
    total_horses = state.get("total_horses", 0)
    processed_days = state.get("processed_days", 0)
    skipped_days = 0
    total_dates = len(dates)

    scope = "JRA" if jra_only else ("NAR" if nar_only else "JRA+NAR")
    logger.info(f"{dates[0]} 〜 {dates[-1]}  ({total_dates}日間)  対象: {scope}")
    if resume and last_completed:
        logger.info(f"レジューム: {last_completed} の翌日から再開")
        logger.info(f"累計: {total_races}レース / {total_horses}頭")

    try:
        for i, date_str in enumerate(dates):
            # 既に保存済みの日はスキップ
            if os.path.exists(_date_output_path(date_str)):
                skipped_days += 1
                continue

            pct = 100 * (i + 1) // total_dates

            race_ids = race_list_scraper.get_race_ids(date_str)
            if not race_ids:
                logger.info(f"  [{i + 1}/{total_dates}] {date_str}  ({pct}%)  累計{total_races}R/{total_horses}頭 ... レースなし")
                _update_state(state, date_str, total_races, total_horses, processed_days)
                continue

            # フィルタ
            if jra_only:
                race_ids = [r for r in race_ids if get_venue_code_from_race_id(r) in JRA_CODES]
            elif nar_only:
                race_ids = [r for r in race_ids if get_venue_code_from_race_id(r) not in JRA_CODES]

            if not race_ids:
                logger.info(f"  [{i + 1}/{total_dates}] {date_str}  ({pct}%)  累計{total_races}R/{total_horses}頭 ... 対象レースなし")
                _update_state(state, date_str, total_races, total_horses, processed_days)
                continue

            day_races = []
            day_horses = 0
            for rid in race_ids:
                vc = get_venue_code_from_race_id(rid)
                base = (
                    "https://nar.netkeiba.com" if vc not in JRA_CODES else "https://race.netkeiba.com"
                )
                url = f"{base}/race/result.html"
                soup = client.get(url, params={"race_id": rid})
                if not soup:
                    continue

                parsed = parse_result_page(soup, rid)
                if not parsed:
                    continue

                day_races.append(parsed)
                day_horses += len(parsed["horses"])

            if day_races:
                _save_day_data(date_str, day_races)
                total_races += len(day_races)
                total_horses += day_horses
                processed_days += 1
                venues = sorted(set(r["venue"] or "不明" for r in day_races))
                logger.info(f"  [{i + 1}/{total_dates}] {date_str}  ({pct}%)  累計{total_races}R/{total_horses}頭 ... {len(day_races)}R / {day_horses}頭  [{', '.join(venues)}]")
            else:
                logger.info(f"  [{i + 1}/{total_dates}] {date_str}  ({pct}%)  累計{total_races}R/{total_horses}頭 ... 取得0件")

            _update_state(state, date_str, total_races, total_horses, processed_days)

    except KeyboardInterrupt:
        logger.warning("Ctrl+C で停止しました。次回 --resume で再開できます。")
        logger.warning(f"最終完了日: {state.get('last_completed_date', '未開始')}")
        logger.warning(f"累計: {total_races}レース / {total_horses}頭")

    logger.info("収集完了")
    logger.info(f"保存先: {ML_DATA_DIR}")
    logger.info(f"累計: {total_races}レース / {total_horses}頭 / {processed_days}日")
    if skipped_days:
        logger.info(f"スキップ: {skipped_days}日 (保存済み)")

    return {
        "total_days": processed_days,
        "total_races": total_races,
        "total_horses": total_horses,
        "skipped_days": skipped_days,
    }


def _update_state(state: dict, date_str: str, races: int, horses: int, days: int):
    state["last_completed_date"] = date_str
    state["total_races"] = races
    state["total_horses"] = horses
    state["processed_days"] = days
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _save_state(state)


# ============================================================
# 統計表示
# ============================================================


def ml_data_stats():
    """収集済みデータの統計を表示"""
    if not os.path.exists(ML_DATA_DIR):
        logger.info("データなし")
        return

    state = _load_state()
    files = [f for f in os.listdir(ML_DATA_DIR) if f.endswith(".json") and not f.startswith("_")]

    if not files:
        logger.info("データなし")
        return

    total_races, total_horses = 0, 0
    jra_races, nar_races = 0, 0
    date_range = []

    for fname in sorted(files):
        fpath = os.path.join(ML_DATA_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            date_range.append(data.get("date", fname))
            for race in data.get("races", []):
                total_races += 1
                total_horses += len(race.get("horses", []))
                if race.get("is_jra"):
                    jra_races += 1
                else:
                    nar_races += 1
        except Exception:
            logger.debug("ML stats file read failed", exc_info=True)
            continue

    logger.info(f"期間: {date_range[0]} 〜 {date_range[-1]}")
    logger.info(f"ファイル数: {len(files)}日分")
    logger.info(f"レース数: {total_races} (JRA: {jra_races} / NAR: {nar_races})")
    logger.info(f"延べ出走数: {total_horses}")
    if state:
        logger.info(f"最終更新: {state.get('updated_at', '不明')}")
    size_mb = sum(
        os.path.getsize(os.path.join(ML_DATA_DIR, f))
        for f in files
    ) / (1024 * 1024)
    logger.info(f"データサイズ: {size_mb:.1f} MB")
