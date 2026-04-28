"""
基準タイムDBの事前収集
過去のレース結果（1〜3着）をスクレイピングし、course_db を事前に蓄積する。
分析開始時に全レースで十分なサンプルが使えるようにする。
"""

import json
import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

try:
    import orjson as _json_fast

    def _fast_load(f):
        return _json_fast.loads(f.read())
except ImportError:
    import json as _json_fast

    def _fast_load(f):
        return json.load(f)

from bs4 import BeautifulSoup

from src.log import get_logger
from src.models import PaceType, PastRun

logger = get_logger(__name__)

# パース結果キャッシュ（レースID単位）
_PARSED_CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "cache", "course_db_parsed")


def _load_parsed_cache(race_id: str) -> Optional[List[dict]]:
    """パース済みPastRunリストをキャッシュから読み込む"""
    path = os.path.join(_PARSED_CACHE_DIR, f"{race_id}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            return _fast_load(f)
    except Exception:
        return None


def _save_parsed_cache(race_id: str, runs: List[dict]):
    """パース済みPastRunリストをキャッシュに保存"""
    os.makedirs(_PARSED_CACHE_DIR, exist_ok=True)
    path = os.path.join(_PARSED_CACHE_DIR, f"{race_id}.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(runs, f, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        logger.debug("パースキャッシュ保存失敗: %s", race_id, exc_info=True)


def _parse_time(time_str: str) -> float:
    """ "1:34.5" -> 94.5秒"""
    try:
        if ":" in time_str:
            parts = time_str.split(":")
            return int(parts[0]) * 60 + float(parts[1])
        return float(time_str)
    except Exception:
        return 0.0


def _parse_last3f(text: str) -> float:
    """上がり3F文字列を秒に"""
    try:
        t = re.sub(r"[^\d.]", "", text)
        return float(t) if t else 35.5
    except Exception:
        return 35.5


def _parse_corners(pos_text: str) -> list:
    """ "8-6" -> [8,6], "05-05-04-03" -> [5,5,4,3]。全コーナー通過順位"""
    result = []
    for part in str(pos_text).split("-"):
        p = part.strip()
        if p.isdigit():
            result.append(int(p))
    return result


def _parse_4c(pos_text: str) -> int:
    """ "8-6" -> 6 (最後角)。コーナー無しなら4"""
    corners = _parse_corners(pos_text)
    return corners[-1] if corners else 4


def _parse_full_lap_data(soup: BeautifulSoup, distance: int) -> tuple:
    """
    レース結果ページのラップタイムセクションから抽出。
    Returns: (first_3f_sec, last_3f_sec, pace_letter) 取得できなかったものは None / ""
    """
    first_3f, last_3f, pace_letter = None, None, ""
    # ペース:H/M/S を探す
    for el in soup.find_all(string=lambda t: t and "ペース" in str(t)):
        m = re.search(r"ペース[：:]\s*([HMS])", str(el))
        if m:
            pace_letter = m.group(1).strip() or ""
            break

    for header in soup.find_all(["h2", "h3", "h4", "th", "dt"]):
        if "ラップ" not in header.get_text() and "lap" not in header.get_text().lower():
            continue
        table = header.find_next("table")
        if not table:
            continue
        rows = table.select("tr") or table.select("tbody tr")
        if not rows:
            continue
        header_row = rows[0] if rows else None
        if not header_row:
            continue
        cells = (
            header_row.select("th") or header_row.select("td") or header_row.find_all(["th", "td"])
        )
        col_600 = None
        col_last = None
        for i, c in enumerate(cells):
            txt = c.get_text(strip=True)
            if txt in ("600m", "600"):
                col_600 = i
            if (
                distance
                and txt.replace("m", "").isdigit()
                and int(txt.replace("m", "")) == distance
            ):
                col_last = i
        if col_600 is None and len(cells) >= 3:
            col_600 = 2
        if col_last is None:
            col_last = len(cells) - 1

        def _parse_sec(val: str) -> Optional[float]:
            if not val:
                return None
            try:
                if ":" in val:
                    parts = val.split(":")
                    return int(parts[0]) * 60 + float(parts[1].replace(",", "."))
                return float(val.replace(",", "."))
            except ValueError:
                return None

        for row in rows[1:4]:
            rcells = row.select("td") or row.find_all("td")
            if col_600 is not None and len(rcells) > col_600:
                v = _parse_sec(rcells[col_600].get_text(strip=True))
                if v is not None:
                    first_3f = v
                    break

        if len(rows) >= 3 and first_3f is None:
            lap_row = rows[2]
            rcells = lap_row.select("td") or lap_row.find_all("td")
            if len(rcells) >= 3:
                try:
                    first_3f = sum(
                        float(c.get_text(strip=True).replace(",", ".")) for c in rcells[:3]
                    )
                except ValueError:
                    pass

        for row in rows[1:4]:
            rcells = row.select("td") or row.find_all("td")
            if len(rcells) >= 3 and col_last is not None and col_last < len(rcells):
                cumul = _parse_sec(rcells[col_last].get_text(strip=True))
                if cumul is not None and first_3f is not None:
                    last_3f = cumul - first_3f if first_3f and cumul > first_3f else None
                    break
        if last_3f is None and len(rows) >= 3:
            lap_row = rows[2]
            rcl = lap_row.select("td") or lap_row.find_all("td")
            if len(rcl) >= 6:
                try:
                    last_3f = sum(float(c.get_text(strip=True).replace(",", ".")) for c in rcl[-3:])
                except ValueError:
                    pass
        break

    return (first_3f, last_3f, pace_letter)


def _parse_first3f_from_lap(soup: BeautifulSoup, distance: int) -> Optional[float]:
    """_parse_full_lap_data のラッパー。first_3f のみ返す"""
    first_3f, _, _ = _parse_full_lap_data(soup, distance)
    return first_3f


_RE_JPN_GRADE = re.compile(r"Jpn\s*([123])", re.IGNORECASE)
_RE_PAREN = re.compile(r"[（(]([^）)]+)[）)]")

# 世代限定戦キーワード
_GEN_KW = re.compile(r"2歳|新馬|デビュー|初出走|(?<![3-9])3歳(?!以上|上)")

JRA_CODES_SET = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}


def _is_generation_race(race_name: str) -> bool:
    """2歳・3歳限定戦かどうかを判定"""
    return bool(_GEN_KW.search(race_name))


def _infer_grade(race_name: str) -> str:
    """JRAレース名からグレードを推定（全角・半角両対応）"""
    rn = race_name
    # G1（全角・半角混在対応）
    if re.search(r"[GＧ][1１]", rn) or "一级" in rn:
        return "G1"
    if re.search(r"[GＧ][2２]", rn):
        return "G2"
    if re.search(r"[GＧ][3３]", rn):
        return "G3"
    # Listed
    if re.search(r"[（(][LＬ][）)]", rn):
        return "L"
    if "新馬" in rn:
        return "新馬"
    if "未勝利" in rn:
        return "未勝利"
    if "1勝" in rn or "500万" in rn or "５００万" in rn:
        return "1勝"
    if "2勝" in rn or "1000万" in rn or "１０００万" in rn:
        return "2勝"
    if "3勝" in rn or "1600万" in rn or "１６００万" in rn:
        return "3勝"
    if "OP" in rn or "オープン" in rn:
        return "OP"
    return "OP"


def _token_to_nar_grade(token: str) -> str:
    """トークンからNARクラスを返す（空文字=不明）
    ※漢字数字・ーつなぎの組名（C2二、C3ー4、Aー2）にも対応するため
      \b 境界を使わず 単純 in 検索 + 最短パターンで判定する。
    """
    t = token.strip()
    # A系 (A1 > A2)
    if "A1" in t or "Ａ１" in t:
        return "A1"
    if re.search(r"A[2-9ーー\-]|Ａ[２-９]|^A$|A\d", t) or "Aクラス" in t:
        return "A2"
    # B系 (B1 > B2 > B3)
    if "B1" in t or "Ｂ１" in t:
        return "B1"
    if "B2" in t or "Ｂ２" in t:
        return "B2"
    if re.search(r"B[3-9ーー\-]|Ｂ[３-９]|B\d|B級|Bクラス", t) or t in ("B",):
        return "B3"
    # C系 (C1 > C2 > C3)
    if "C1" in t or "Ｃ１" in t:
        return "C1"
    if "C2" in t or "Ｃ２" in t:
        return "C2"
    if re.search(r"C[3-9ーー\-]|Ｃ[３-９]|C\d|C級|Cクラス|AB混合", t) or t in ("C",):
        return "C3"
    # OP
    if t in ("OP", "オープン", "一般", "オー", "4上", "4歳上") or "OP" in t:
        return "OP"
    return ""


def _infer_grade_nar(race_name: str) -> str:
    """NARレース名からグレードを推定"""
    cn = race_name.strip()

    # 交流重賞
    if _RE_JPN_GRADE.search(cn):
        return "交流重賞"
    if "交流" in cn:
        return "交流重賞"

    # 括弧内クラスを先に評価
    paren_grade = ""
    for m in _RE_PAREN.finditer(cn):
        inner = m.group(1).strip()
        if _RE_JPN_GRADE.match(inner):
            return "交流重賞"
        g = _token_to_nar_grade(inner)
        if g:
            paren_grade = g
            break
        # OP系括弧
        if inner in ("OP", "オープン", "一般", "オー", "4上", "4歳上"):
            paren_grade = "OP"
            break
        # 世代表示の括弧（'2歳', '3歳' など）はスキップして続ける

    # 本文（括弧除き）からクラス検索
    cn_bare = _RE_PAREN.sub(" ", cn)
    direct_grade = _token_to_nar_grade(cn_bare) or _token_to_nar_grade(cn)

    # 括弧内クラス > 本文クラス
    detected = paren_grade or direct_grade
    if detected:
        return detected

    # OP明示
    if re.search(r"\bOP\b|オープン", cn):
        return "OP"

    # 重賞・特別系
    if re.search(r"グランプリ|大賞典|大賞|優駿|ダービー|オークス|皐月|菊花|天皇賞|有馬", cn):
        return "重賞"
    if re.search(r"記念|賞|杯|盃|カップ|トロフィー|チャンピオン|スプリント特別", cn):
        return "重賞"

    # 新馬・未格付
    if "新馬" in cn or "デビュー" in cn or "初出走" in cn:
        return "新馬"
    if "未勝利" in cn or "未格付" in cn or "格付" in cn:
        return "未格付"

    # 世代戦（組分け等、クラス不明）→ 条件戦相当でC3
    if re.search(r"[23]歳", cn):
        return "C3"

    # 括弧内 CC/AB/BC/BB 系（笠松等の複合クラス表記）
    if re.search(r"[（(]CC[）)]", cn):
        return "C3"
    if re.search(r"[（(]BB[）)]", cn):
        return "B3"
    if re.search(r"[（(][AB]C[）)]|[（(]BC[）)]", cn):
        return "C3"
    if re.search(r"[（(]AB[）)]", cn):
        return "B3"

    # JRA認定・地方OP相当
    if "JRA認定" in cn:
        return "OP"

    # 特別単体・ダッシュ系→重賞/OP
    if re.search(r"ダッシュ|スプリント|スピード", cn):
        return "重賞"
    if "特別" in cn:
        return "OP"

    return "その他"


def _parse_result_to_past_runs(
    soup: BeautifulSoup,
    race_id: str,
    venue_code: str,
    surface: str,
    distance: int,
    condition: str,
    race_name: str,
    field_count: int,
    first_3f_sec: Optional[float] = None,
    race_pace: Optional[PaceType] = None,
) -> List[PastRun]:
    """
    結果ページから1〜3着のPastRunを生成。
    .ResultTableWrap table の tbody tr をパース。列: 0=着順,1=枠,2=馬番,7=タイム,10=後3Fなど
    """
    runs = []
    table = soup.select_one(".ResultTableWrap table")
    if not table:
        return runs

    for row in table.select("tbody tr"):
        cells = row.select("td")
        if len(cells) < 8:
            continue
        finish_text = cells[0].get_text(strip=True)
        if not finish_text.isdigit():
            continue
        finish = int(finish_text)
        if finish > 3:
            continue

        gate_no = (
            int(cells[1].get_text(strip=True)) if cells[1].get_text(strip=True).isdigit() else 1
        )
        horse_no = (
            int(cells[2].get_text(strip=True))
            if cells[2].get_text(strip=True).isdigit()
            else finish
        )

        time_sec = 0.0
        for c in cells:
            t = c.get_text(strip=True)
            if re.match(r"\d+:\d{2}\.\d", t):
                time_sec = _parse_time(t)
                break
        if time_sec <= 0:
            continue

        last3f = 35.5
        if len(cells) > 10:
            t = cells[10].get_text(strip=True)
            if re.match(r"^\d{2}\.\d$", t):
                try:
                    last3f = float(t)
                except ValueError:
                    pass

        # 日付: HTML meta タグから取得を試みる
        race_date = ""
        for el in soup.select('meta[property="og:description"], meta[property="og:title"]'):
            cnt = el.get("content", "")
            m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", cnt)
            if m:
                race_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                break
        if not race_date and venue_code not in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"] and len(race_id) >= 10:
            # NAR: race_id[6:8]=MM, [8:10]=DD の構造的日付を使用
            mm, dd = race_id[6:8], race_id[8:10]
            if mm.isdigit() and dd.isdigit() and 1 <= int(mm) <= 12 and 1 <= int(dd) <= 31:
                race_date = f"{race_id[:4]}-{mm}-{dd}"
        # JRA は HTML 抽出失敗時に race_id から日付を取得できない
        # YYYY-01-01 フォールバック禁止: 汚染日付を DB に混入させない
        if not race_date:
            logger.warning("日付抽出失敗のため row skip: race_id=%s", race_id)
            continue

        pos_text = cells[11].get_text() if len(cells) > 11 else ""
        corners = _parse_corners(pos_text)
        pos4c = corners[-1] if corners else _parse_4c(pos_text)

        # 騎手・調教師ID（馬場状態別集計用）
        jockey_id, trainer_id = "", ""
        jockey_a = row.select_one("a[href*='/jockey/']")
        if jockey_a:
            jm = re.search(r"/jockey/result/recent/(\w+)", jockey_a.get("href", ""))
            if jm:
                jockey_id = jm.group(1)
        trainer_a = row.select_one("a[href*='/trainer/']")
        if trainer_a:
            tm = re.search(r"/trainer/result/recent/(\w+)", trainer_a.get("href", ""))
            if tm:
                trainer_id = tm.group(1)

        is_jra = venue_code in JRA_CODES_SET
        grade = _infer_grade(race_name) if is_jra else _infer_grade_nar(race_name)
        is_gen = _is_generation_race(race_name)
        course_id = f"{venue_code}_{surface}_{distance}"
        runs.append(
            PastRun(
                race_date=race_date,
                venue=venue_code,
                course_id=course_id,
                distance=distance,
                surface=surface,
                condition=condition,
                class_name=race_name,
                grade=grade,
                field_count=field_count,
                gate_no=gate_no,
                horse_no=horse_no,
                jockey=jockey_a.get_text(strip=True) if jockey_a else "",
                jockey_id=jockey_id,
                trainer_id=trainer_id,
                weight_kg=55.0,
                position_4c=pos4c,
                positions_corners=corners,
                finish_pos=finish,
                finish_time_sec=time_sec,
                last_3f_sec=last3f,
                margin_behind=0.0,
                margin_ahead=0.0,
                pace=race_pace,
                first_3f_sec=first_3f_sec,
                is_generation=is_gen,
            )
        )
    return runs


# 1日あたり最大収集レース数（JRA〜24 + 地方複数場で全日程網羅）
MAX_RACES_PER_DAY = 200


def _load_collector_state(state_path: str) -> dict:
    if not state_path or not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_collector_state(state_path: str, state: dict):
    """アトミック書き込み（中断耐性）"""
    if not state_path:
        return
    os.makedirs(os.path.dirname(state_path) or ".", exist_ok=True)
    tmp_path = state_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, state_path)


def collect_course_db_from_results(
    client,
    race_list_scraper,
    start_date: str,
    end_date: str,
    output_path: str,
    days_back: int = 90,
    max_races_per_day: int = None,
    state_path: str = None,
    mode: str = "full",
    progress_callback=None,
) -> int:
    """
    指定期間のレース結果を収集し、基準タイムDBを構築してファイルに保存する。

    Args:
        client: NetkeibaClient (get用)
        race_list_scraper: RaceListScraper (get_race_ids用)
        start_date: 収集開始日 "YYYY-MM-DD"
        end_date: 収集終了日 "YYYY-MM-DD"
        output_path: 保存先JSON path
        days_back: start_date未指定時の遡り日数
        max_races_per_day: 1日あたりの最大収集レース数（Noneでデフォルト200）
        state_path: 状態保存先（途中再開・新規追加用）
        mode: "full"=全件, "resume"=途中再開, "append"=新規分のみ
        progress_callback: (day_index, total_days, total_runs, current_date, status) 進捗通知

    Returns:
        収集したPastRunの総数（今回の実行分）
    """
    from data.masters.venue_master import JRA_CODES, get_venue_code_from_race_id

    max_races_per_day = max_races_per_day or MAX_RACES_PER_DAY
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    start_dt = (
        datetime.strptime(start_date, "%Y-%m-%d")
        if start_date
        else end_dt - timedelta(days=days_back)
    )
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    all_runs: Dict[str, List[dict]] = {}
    # 既存データの重複チェック用インデックス {cid: set(key)}
    existing_keys: Dict[str, set] = {}
    start_from = start_dt

    if mode in ("resume", "append") and state_path:
        st = _load_collector_state(state_path)
        last = st.get("last_date")
        if last:
            last_dt = datetime.strptime(last, "%Y-%m-%d")
            start_from = last_dt + timedelta(days=1)
            if start_from > end_dt:
                if progress_callback:
                    progress_callback(0, 0, st.get("total_runs", 0), last, "already_done")
                return 0
        elif mode == "append":
            start_from = end_dt - timedelta(days=7)
        if os.path.exists(output_path):
            try:
                with open(output_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                all_runs = data.get("course_db", {})
            except Exception:
                logger.debug("resume/append data load failed", exc_info=True)
    elif mode == "merge":
        # 指定期間を収集して既存データにマージ（state依存なし）
        start_from = start_dt
        if os.path.exists(output_path):
            try:
                with open(output_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                all_runs = data.get("course_db", {})
            except Exception:
                logger.debug("merge data load failed", exc_info=True)

    # 既存データの重複チェック用インデックスを構築
    for cid, runs in all_runs.items():
        existing_keys[cid] = set()
        for r in runs:
            key = (
                r.get("race_date", ""),
                r.get("finish_pos", 0),
                r.get("horse_no", 0),
                round(r.get("finish_time_sec", 0), 1),
            )
            existing_keys[cid].add(key)

    date_strs = []
    d = start_from
    while d <= end_dt:
        date_strs.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    total_runs = sum(sum(1 for _ in v) for v in all_runs.values()) if all_runs else 0
    base_runs = total_runs

    def _report(i: int, cur_date: str, added: int, st: str = "running"):
        if progress_callback:
            progress_callback(i, len(date_strs), base_runs + added, cur_date, st)
        elif (i % 7 == 0 or i == len(date_strs)) and date_strs:
            logger.info("収集: %d/%d日 (%d走)", i, len(date_strs), base_runs + added)

    if progress_callback:
        progress_callback(
            0, len(date_strs), base_runs, date_strs[0] if date_strs else "", "starting"
        )

    for i, d in enumerate(date_strs):
        ids = race_list_scraper.get_race_ids(d)
        day_runs = 0
        if ids:
            ids = ids[:max_races_per_day]
            for rid in ids:
                vc = get_venue_code_from_race_id(rid)

                # パース結果キャッシュを確認（HTMLパースをスキップ）
                cached_dicts = _load_parsed_cache(rid)
                if cached_dicts is not None:
                    for rd in cached_dicts:
                        cid = rd.get("course_id", "")
                        dup_key = (
                            rd.get("race_date", ""),
                            rd.get("finish_pos", 0),
                            rd.get("horse_no", 0),
                            round(rd.get("finish_time_sec", 0), 1),
                        )
                        if cid not in existing_keys:
                            existing_keys[cid] = set()
                        if dup_key in existing_keys[cid]:
                            continue
                        existing_keys[cid].add(dup_key)
                        if cid not in all_runs:
                            all_runs[cid] = []
                        all_runs[cid].append(rd)
                        day_runs += 1
                        total_runs += 1
                    continue

                # キャッシュなし → HTMLパース
                base = (
                    "https://nar.netkeiba.com"
                    if vc not in JRA_CODES
                    else "https://race.netkeiba.com"
                )
                url = f"{base}/race/result.html"
                soup = client.get(url, params={"race_id": rid})
                if not soup:
                    continue
                data_el = soup.select_one(".RaceData01")
                surface, distance = "芝", 1600
                if data_el:
                    txt = data_el.get_text()
                    sm = re.search(r"(芝|ダ|障)", txt)
                    dm = re.search(r"(\d{3,4})m", txt)
                    if sm:
                        surface = "芝" if sm.group(1) == "芝" else "ダート"
                    if dm:
                        distance = int(dm.group(1))
                cond_el = re.search(r"馬場[：:]([良稍重不])", soup.get_text())
                condition = cond_el.group(1) if cond_el else "良"
                name_el = soup.select_one(".RaceName")
                race_name = name_el.get_text(strip=True) if name_el else ""
                venue_code = get_venue_code_from_race_id(rid)
                field_count = 16
                fc_el = soup.select_one(".RaceData02")
                if fc_el:
                    fm = re.search(r"(\d+)頭", fc_el.get_text())
                    if fm:
                        field_count = int(fm.group(1))
                first_3f, race_last3f, pace_letter = _parse_full_lap_data(soup, distance)
                pace_from_lap = {"H": PaceType.H, "M": PaceType.M, "S": PaceType.S}.get(
                    pace_letter
                )
                past_runs = _parse_result_to_past_runs(
                    soup,
                    rid,
                    venue_code,
                    surface,
                    distance,
                    condition,
                    race_name,
                    field_count,
                    first_3f_sec=first_3f,
                    race_pace=pace_from_lap,
                )
                # パース結果をキャッシュ保存
                run_dicts = [_past_run_to_dict(pr) for pr in past_runs]
                _save_parsed_cache(rid, run_dicts)

                for rd in run_dicts:
                    cid = rd.get("course_id", "")
                    dup_key = (
                        rd.get("race_date", ""),
                        rd.get("finish_pos", 0),
                        rd.get("horse_no", 0),
                        round(rd.get("finish_time_sec", 0), 1),
                    )
                    if cid not in existing_keys:
                        existing_keys[cid] = set()
                    if dup_key in existing_keys[cid]:
                        continue  # 重複スキップ
                    existing_keys[cid].add(dup_key)
                    if cid not in all_runs:
                        all_runs[cid] = []
                    all_runs[cid].append(rd)
                    day_runs += 1
                    total_runs += 1
                time.sleep(1.5)

        _report(i + 1, d, total_runs - base_runs)
        # 状態は毎日保存（再開ポイント確保）
        if state_path:
            _save_collector_state(
                state_path,
                {
                    "last_date": d,
                    "total_runs": total_runs,
                    "total_days": i + 1,
                    "start_date": date_strs[0],
                    "end_date": d,
                    "status": "running",
                },
            )
        # JSONは7日ごと or 最終日のみ書き込み（巨大ファイルの毎日書き込みを回避）
        is_last_day = i + 1 >= len(date_strs)
        should_write = is_last_day or ((i + 1) % 7 == 0)
        if should_write:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            tmp_path = output_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump({"version": 1, "course_db": all_runs}, f, ensure_ascii=False, indent=0)
            # Windowsではos.replaceが他プロセスのファイルロックで失敗する場合があるためリトライ
            import time as _time

            for _retry in range(5):
                try:
                    os.replace(tmp_path, output_path)
                    break
                except PermissionError:
                    if _retry == 4:
                        import shutil as _shutil

                        _shutil.copy2(tmp_path, output_path)
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            logger.debug("tmp file cleanup failed", exc_info=True)
                    else:
                        _time.sleep(2)

    if state_path:
        _save_collector_state(
            state_path,
            {
                "last_date": date_strs[-1] if date_strs else "",
                "total_runs": total_runs,
                "total_days": len(date_strs),
                "start_date": date_strs[0] if date_strs else "",
                "end_date": date_strs[-1] if date_strs else "",
                "status": "completed",
            },
        )
    _report(len(date_strs), date_strs[-1] if date_strs else "", total_runs - base_runs, "completed")
    return total_runs - base_runs


def _past_run_to_dict(pr: PastRun) -> dict:
    return {
        "race_date": pr.race_date,
        "venue": pr.venue,
        "course_id": pr.course_id,
        "distance": pr.distance,
        "surface": pr.surface,
        "condition": pr.condition,
        "class_name": pr.class_name,
        "grade": pr.grade,
        "field_count": pr.field_count,
        "gate_no": pr.gate_no,
        "horse_no": pr.horse_no,
        "jockey": pr.jockey,
        "jockey_id": getattr(pr, "jockey_id", ""),
        "trainer_id": getattr(pr, "trainer_id", ""),
        "weight_kg": pr.weight_kg,
        "position_4c": pr.position_4c,
        "positions_corners": getattr(pr, "positions_corners", None) or [],
        "finish_pos": pr.finish_pos,
        "finish_time_sec": pr.finish_time_sec,
        "last_3f_sec": pr.last_3f_sec,
        "margin_behind": pr.margin_behind,
        "margin_ahead": pr.margin_ahead,
        "first_3f_sec": getattr(pr, "first_3f_sec", None),
        "pace": pr.pace.value if pr.pace else None,
        "is_generation": getattr(pr, "is_generation", False),
        "win_odds": getattr(pr, "tansho_odds", None),
    }


def load_preload_course_db(path: str, target_date: str = None) -> Dict[str, List[PastRun]]:
    """事前収集した course_db を読み込む。帯広（ばんえい）データは除外する。

    Args:
        path: course_db_preload.json のパス
        target_date: ローリングウィンドウ基準日 (YYYY-MM-DD)。
                     指定時は基準日の1年前〜前日のデータのみロード。
    """
    if not path or not os.path.exists(path):
        return {}

    try:
        with open(path, "rb") as f:
            data = _fast_load(f)
    except Exception:
        return {}

    # ローリングウィンドウ日付範囲
    cutoff_date = None
    if target_date:
        from datetime import datetime, timedelta
        cutoff_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")

    raw = data.get("course_db", {})
    result = {}
    banei_skipped = 0
    date_filtered = 0
    for cid, runs in raw.items():
        # 帯広（場コード"52"）のコースIDを除外
        if cid.startswith("65_"):
            banei_skipped += len(runs) if isinstance(runs, list) else 1
            continue
        if isinstance(runs, list):
            parsed = [_dict_to_past_run(r) for r in runs]
            if target_date and cutoff_date:
                before = len(parsed)
                parsed = [pr for pr in parsed if cutoff_date <= pr.race_date < target_date]
                date_filtered += before - len(parsed)
            if parsed:
                result[cid] = parsed
    if banei_skipped:
        logger.info("帯広（ばんえい）データ %d走 を除外しました", banei_skipped)
    if date_filtered:
        logger.info("ローリングウィンドウ: %d走 を日付フィルタで除外 (基準日: %s)", date_filtered, target_date)
    return result


def _safe_float(v) -> float:
    """馬身表記("1/2", "3/4"等)や文字列を安全にfloatに変換"""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        # "1/2" → 0.5, "1 1/2" → 1.5 等
        try:
            if "/" in s:
                parts = s.split()
                if len(parts) == 2:
                    whole = float(parts[0])
                    num, den = parts[1].split("/")
                    return whole + float(num) / float(den)
                else:
                    num, den = s.split("/")
                    return float(num) / float(den)
        except Exception:
            pass
        return 0.0


def _dict_to_past_run(d: dict) -> PastRun:
    corners = d.get("positions_corners", [])
    if not isinstance(corners, list):
        corners = []
    pos4c = d.get("position_4c")
    if pos4c is None and corners:
        pos4c = corners[-1]
    first_3f = d.get("first_3f_sec")
    pace_val = d.get("pace")
    pace_obj = PaceType(pace_val) if pace_val and pace_val in [e.value for e in PaceType] else None
    return PastRun(
        race_date=d.get("race_date", ""),
        venue=d.get("venue", ""),
        course_id=d.get("course_id", ""),
        distance=int(d.get("distance", 1600)),
        surface=d.get("surface", "芝"),
        condition=d.get("condition", "良"),
        class_name=d.get("class_name", ""),
        grade=d.get("grade", "OP"),
        field_count=int(d.get("field_count", 16)),
        gate_no=int(d.get("gate_no", 1)),
        horse_no=int(d.get("horse_no", 1)),
        jockey=d.get("jockey", ""),
        jockey_id=d.get("jockey_id", ""),
        trainer_id=d.get("trainer_id", ""),
        weight_kg=float(d.get("weight_kg", 55)),
        position_4c=int(pos4c or 4),
        positions_corners=corners,
        finish_pos=int(d.get("finish_pos", 1)),
        finish_time_sec=float(d.get("finish_time_sec") or 0),
        last_3f_sec=float(d.get("last_3f_sec") or 35.5),
        margin_behind=_safe_float(d.get("margin_behind")),
        margin_ahead=_safe_float(d.get("margin_ahead")),
        pace=pace_obj,
        first_3f_sec=float(first_3f) if first_3f is not None else None,
        is_generation=bool(d.get("is_generation", False)),
    )
