"""
馬別過去走DB構築
result.htmlキャッシュ（75,000件以上）から horse_id 別の過去走データを構築する。
db.netkeiba.com への直接アクセスが制限された場合のフォールバック用。

出力 horse_db.json 構造:
{
  "horse_id": {
    "horse_name": "...",
    "sex": "牡",
    "runs": [
      {  # 最新順
        "race_id": "...",  # 重複排除用
        "race_date": "YYYY-MM-DD",
        "venue": "05",
        "course_id": "05_芝_2400",
        ...PastRun 相当フィールド...
      }
    ]
  }
}
"""

import json
import os
import re
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from src.log import get_logger

logger = get_logger(__name__)

# ============================================================
# 定数
# ============================================================

JRA_CODES_SET = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
BANEI_CODE = "65"

_RE_JPN_GRADE = re.compile(r"Jpn\s*([123])", re.IGNORECASE)
_GEN_KW = re.compile(r"2歳|新馬|デビュー|初出走|(?<![3-9])3歳(?!以上|上)")

# 着差 文字列 → 秒換算（概算）
_MARGIN_MAP = {
    "ハナ": 0.1,
    "短頭": 0.15,
    "頭": 0.2,
    "クビ": 0.3,
    "1/2": 0.3,
    "3/4": 0.4,
    "1": 0.6,
    "1.1/4": 0.75,
    "1.1/2": 0.9,
    "1.3/4": 1.05,
    "2": 1.2,
    "2.1/2": 1.5,
    "3": 1.8,
    "3.1/2": 2.1,
    "4": 2.4,
    "5": 3.0,
    "6": 3.6,
    "7": 4.2,
    "8": 4.8,
    "10": 6.0,
    "大差": 10.0,
}


# ============================================================
# ヘルパー関数
# ============================================================


def _parse_finish_time(s: str) -> float:
    """'1:08.8' → 68.8秒"""
    try:
        if ":" in s:
            p = s.split(":")
            return int(p[0]) * 60 + float(p[1])
        return float(s)
    except Exception:
        return 0.0


def _parse_corners(pos_text: str) -> List[int]:
    """'05-05-04-03' → [5,5,4,3]"""
    result = []
    for part in str(pos_text).split("-"):
        p = part.strip()
        if p.isdigit():
            result.append(int(p))
    return result


def _parse_margin_to_sec(text: str) -> float:
    """'1.1/2' → 0.9秒"""
    t = text.strip().replace("　", "").replace(" ", "")
    if not t:
        return 0.0
    if t in _MARGIN_MAP:
        return _MARGIN_MAP[t]
    try:
        return float(t)
    except ValueError:
        pass
    # 複合パターン ('2.1/2' など) の最長マッチ
    for key, val in sorted(_MARGIN_MAP.items(), key=lambda x: -len(x[0])):
        if key in t:
            return val
    return 0.5


def _find_last3f(cells) -> float:
    """セル群から上がり3F(秒)を探す。28-50秒の範囲の XX.X 形式を採用"""
    for c in cells:
        t = c.get_text(strip=True)
        if re.match(r"^\d{2}\.\d$", t):
            val = float(t)
            if 25.0 <= val <= 55.0:
                return val
    return 35.5


def _find_corners(cells) -> List[int]:
    """セル群からコーナー通過順位を探す ('1-2-3-4' 形式)"""
    for c in cells:
        t = c.get_text(strip=True)
        if re.match(r"^\d+(-\d+)+$", t):
            return _parse_corners(t)
    return []


def _infer_grade_jra(race_name: str) -> str:
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
    if "1勝" in rn or "500万" in rn:
        return "1勝"
    if "2勝" in rn or "1000万" in rn:
        return "2勝"
    if "3勝" in rn or "1600万" in rn:
        return "3勝"
    if "OP" in rn or "オープン" in rn:
        return "OP"
    return "OP"


def _infer_grade_nar(race_name: str) -> str:
    rn = race_name
    if _RE_JPN_GRADE.search(rn):
        return "交流重賞"
    if re.search(r"記念|大賞|杯|盃|カップ|グランプリ|ダービー", rn):
        return "重賞"
    if "新馬" in rn or "デビュー" in rn:
        return "新馬"
    if "未勝利" in rn or "未格付" in rn:
        return "未格付"
    for cls in ("A1", "A2", "B1", "B2", "B3", "C1", "C2", "C3"):
        if cls in rn:
            return cls
    if "OP" in rn or "オープン" in rn:
        return "OP"
    return "その他"


# ============================================================
# result.html パーサー
# ============================================================


def parse_result_page(
    soup: BeautifulSoup,
    race_id: str,
) -> Tuple[Optional[dict], List[dict]]:
    """
    result.html から全出走馬の過去走データを抽出。

    Returns:
        (race_meta, [run_dict]) or (None, []) on failure
        run_dict は PastRun フィールドに horse_id/horse_name/sex/age/race_id を追加したもの
    """
    try:
        from data.masters.venue_master import get_venue_code_from_race_id
    except ImportError:

        def get_venue_code_from_race_id(rid):
            return rid[4:6] if len(rid) >= 6 else "05"

    venue_code = get_venue_code_from_race_id(race_id)
    is_jra = venue_code in JRA_CODES_SET

    # --- 開催日取得 ---
    race_date = None
    for meta in soup.select(
        'meta[property="og:description"], meta[property="og:title"], meta[name="description"]'
    ):
        cnt = meta.get("content", "")
        m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", cnt)
        if m:
            race_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            break
    if not race_date:
        if not is_jra and len(race_id) >= 10:
            mm, dd = race_id[6:8], race_id[8:10]
            if mm.isdigit() and dd.isdigit() and 1 <= int(mm) <= 12 and 1 <= int(dd) <= 31:
                race_date = f"{race_id[:4]}-{mm}-{dd}"
        if not race_date:
            race_date = f"{race_id[:4]}-01-01"

    # --- コース情報 ---
    surface, distance, condition = "芝", 2000, "良"
    data1 = soup.select_one(".RaceData01")
    if data1:
        text = data1.get_text()
        sm = re.search(r"(芝|ダ|障)", text)
        nm = re.search(r"(\d{3,4})m", text)
        bm = re.search(r"馬場[：:]\s*([良稍重不]+)", text)
        if sm:
            surface = {"芝": "芝", "ダ": "ダート", "障": "障害"}.get(sm.group(1), "芝")
        if nm:
            distance = int(nm.group(1))
        if bm:
            condition = bm.group(1)

    # --- レース名・グレード ---
    race_name = ""
    rn_el = soup.select_one(".RaceName")
    if rn_el:
        race_name = rn_el.get_text(strip=True)
    grade = _infer_grade_jra(race_name) if is_jra else _infer_grade_nar(race_name)
    is_gen = bool(_GEN_KW.search(race_name))
    course_id = f"{venue_code}_{surface}_{distance}"

    # --- 結果テーブル ---
    table = soup.select_one(".ResultTableWrap table")
    if not table:
        return None, []

    rows = table.select("tbody tr")
    field_count = len(rows)
    if field_count == 0:
        return None, []

    race_meta = {
        "race_id": race_id,
        "race_date": race_date,
        "venue_code": venue_code,
        "surface": surface,
        "distance": distance,
        "condition": condition,
        "class_name": race_name,
        "grade": grade,
        "field_count": field_count,
        "course_id": course_id,
    }

    # --- 全馬パース ---
    runs_raw = []

    for row in rows:
        cells = row.select("td")
        if len(cells) < 7:
            continue
        finish_text = cells[0].get_text(strip=True)
        if not finish_text.isdigit():
            continue
        finish_pos = int(finish_text)

        # horse_id / name
        horse_a = row.select_one('a[href*="/horse/"]')
        if not horse_a:
            continue
        hm = re.search(r"/horse/(\d+)", horse_a.get("href", ""))
        if not hm:
            continue
        horse_id = hm.group(1)
        horse_name = horse_a.get_text(strip=True)

        # 枠・馬番
        gate_no = (
            int(cells[1].get_text(strip=True))
            if len(cells) > 1 and cells[1].get_text(strip=True).isdigit()
            else 0
        )
        horse_no = (
            int(cells[2].get_text(strip=True))
            if len(cells) > 2 and cells[2].get_text(strip=True).isdigit()
            else finish_pos
        )

        # 性齢
        sex, age = "牡", 4
        if len(cells) > 4:
            m2 = re.match(r"^([牡牝セ])(\d+)$", cells[4].get_text(strip=True))
            if m2:
                sex = m2.group(1)
                age = int(m2.group(2))

        # 斤量
        weight_kg = 55.0
        if len(cells) > 5:
            wt = cells[5].get_text(strip=True)
            try:
                weight_kg = float(wt)
            except ValueError:
                pass

        # 騎手
        jockey_a = row.select_one('a[href*="/jockey/"]')
        jockey_id, jockey_name = "", ""
        if jockey_a:
            jm = re.search(r"/jockey/result/recent/(\w+)", jockey_a.get("href", ""))
            if jm:
                jockey_id = jm.group(1)
            jockey_name = jockey_a.get_text(strip=True)

        # 調教師
        trainer_a = row.select_one('a[href*="/trainer/"]')
        trainer_id = ""
        if trainer_a:
            tm = re.search(r"/trainer/result/recent/(\w+)", trainer_a.get("href", ""))
            if tm:
                trainer_id = tm.group(1)

        # 走破タイム
        finish_time_sec = 0.0
        for c in cells:
            t = c.get_text(strip=True)
            if re.match(r"\d+:\d{2}\.\d", t):
                finish_time_sec = _parse_finish_time(t)
                break
        if finish_time_sec <= 0:
            continue

        # 着差 (cells[8] が一般的)
        margin_ahead_raw = cells[8].get_text(strip=True) if len(cells) > 8 else ""

        # 上がり3F
        last_3f = _find_last3f(cells)

        # コーナー通過順位
        corners = _find_corners(cells)
        pos4c = corners[-1] if corners else gate_no or 4

        # 馬体重・増減 (最後のセル)
        horse_weight, weight_change = None, None
        for c in reversed(cells):
            ct = c.get_text(strip=True)
            whm = re.search(r"(\d{3,4})\(([+-]?\d+)\)", ct)
            if whm:
                horse_weight = int(whm.group(1))
                weight_change = int(whm.group(2))
                break

        runs_raw.append(
            {
                "horse_id": horse_id,
                "horse_name": horse_name,
                "sex": sex,
                "age": age,
                "finish_pos": finish_pos,
                "finish_time_sec": finish_time_sec,
                "gate_no": gate_no,
                "horse_no": horse_no,
                "jockey": jockey_name,
                "jockey_id": jockey_id,
                "trainer_id": trainer_id,
                "weight_kg": weight_kg,
                "horse_weight": horse_weight,
                "weight_change": weight_change,
                "last_3f_sec": last_3f,
                "position_4c": pos4c,
                "positions_corners": corners,
                "margin_ahead_raw": margin_ahead_raw,
            }
        )

    if not runs_raw:
        return race_meta, []

    # --- マージン計算 (勝ち馬タイムからの差) ---
    winner_time = min(
        (r["finish_time_sec"] for r in runs_raw if r["finish_pos"] == 1),
        default=runs_raw[0]["finish_time_sec"],
    )
    # finish_pos 順でソートして隣接差を計算
    sorted_runs = sorted(runs_raw, key=lambda r: (r["finish_pos"], r["finish_time_sec"]))

    run_dicts = []
    for idx, r in enumerate(sorted_runs):
        margin_ahead = r["finish_time_sec"] - winner_time  # 1着からの時間差
        # margin_behind: 次の着順との時間差
        margin_behind = 0.0
        if idx + 1 < len(sorted_runs):
            next_t = sorted_runs[idx + 1]["finish_time_sec"]
            if next_t > r["finish_time_sec"]:
                margin_behind = next_t - r["finish_time_sec"]

        run_dict = {
            "race_id": race_id,
            "race_date": race_date,
            "venue": venue_code,
            "course_id": course_id,
            "distance": distance,
            "surface": surface,
            "condition": condition,
            "class_name": race_meta["class_name"],
            "grade": grade,
            "field_count": field_count,
            "gate_no": r["gate_no"],
            "horse_no": r["horse_no"],
            "jockey": r["jockey"],
            "jockey_id": r["jockey_id"],
            "trainer_id": r["trainer_id"],
            "weight_kg": r["weight_kg"],
            "horse_weight": r["horse_weight"],
            "weight_change": r["weight_change"],
            "position_4c": r["position_4c"],
            "positions_corners": r["positions_corners"],
            "finish_pos": r["finish_pos"],
            "finish_time_sec": r["finish_time_sec"],
            "last_3f_sec": r["last_3f_sec"],
            "margin_ahead": round(margin_ahead, 3),
            "margin_behind": round(margin_behind, 3),
            "first_3f_sec": None,
            "pace": None,
            "is_generation": is_gen,
            # horse 情報 (後で horse_db に格納時に使用)
            "_horse_id": r["horse_id"],
            "_horse_name": r["horse_name"],
            "_sex": r["sex"],
            "_age": r["age"],
        }
        run_dicts.append(run_dict)

    return race_meta, run_dicts


# ============================================================
# キャッシュ全件構築
# ============================================================


def build_horse_db_from_cache(
    cache_dir: str,
    output_path: str,
    progress_every: int = 2000,
    incremental: bool = True,
) -> Tuple[int, int]:
    """
    キャッシュされた result.html を全件再パースして horse_db.json を構築。

    Args:
        cache_dir: HTMLキャッシュディレクトリ
        output_path: 出力 JSON パス
        progress_every: 進捗表示間隔
        incremental: True の場合、既存データを保持して新規レースのみ追加

    Returns:
        (処理レース数, 総走数)
    """
    # --- 既存データ読み込み (増分モード) ---
    horse_db: Dict[str, dict] = {}
    seen_race_ids: set = set()

    if incremental and os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                horse_db = json.load(f)
            # 既存の race_id を収集
            for hid, hdata in horse_db.items():
                for run in hdata.get("runs", []):
                    rid = run.get("race_id", "")
                    if rid:
                        seen_race_ids.add(rid)
            logger.info(f"既存データ読み込み: {len(horse_db)}頭 / {len(seen_race_ids)}レース分")
        except Exception as e:
            logger.warning("horse_db load failed (fresh build): %s", e, exc_info=True)
            horse_db = {}
            seen_race_ids = set()

    # --- キャッシュファイル列挙 ---
    all_files = os.listdir(cache_dir)
    result_files = [
        f for f in all_files if f.endswith(".html") and "result.html" in f and "race_id=" in f
    ]
    result_files.sort()
    logger.info(f"対象 result.html: {len(result_files)} 件")

    processed = 0
    skipped = 0
    errors = 0
    total_runs = 0

    for i, fname in enumerate(result_files):
        if i % progress_every == 0:
            logger.info(f"  [{i:6d}/{len(result_files)}] 処理={processed} スキップ={skipped} 走={total_runs} エラー={errors}")

        # race_id 抽出
        m = re.search(r"race_id=(\d{12})", fname)
        if not m:
            continue
        race_id = m.group(1)

        # 帯広競馬(ばんえい)はスキップ
        if race_id[4:6] == BANEI_CODE:
            skipped += 1
            continue

        # 増分モード: 既知レースはスキップ
        if incremental and race_id in seen_race_ids:
            skipped += 1
            continue

        # HTML 読み込み
        try:
            with open(os.path.join(cache_dir, fname), "r", encoding="utf-8") as f:
                content = f.read()
            soup = BeautifulSoup(content, "lxml")
        except Exception:
            errors += 1
            continue

        # パース
        try:
            _, run_dicts = parse_result_page(soup, race_id)
        except Exception:
            errors += 1
            continue

        if not run_dicts:
            skipped += 1
            continue

        # horse_db に追加
        for run in run_dicts:
            horse_id = run.pop("_horse_id")
            horse_name = run.pop("_horse_name")
            sex = run.pop("_sex")
            run.pop("_age", None)  # age は race ごとに変わるので格納しない

            if horse_id not in horse_db:
                horse_db[horse_id] = {"horse_name": horse_name, "sex": sex, "runs": []}
            else:
                # 最新の名前・性別で更新
                horse_db[horse_id]["horse_name"] = horse_name
                horse_db[horse_id]["sex"] = sex

            horse_db[horse_id]["runs"].append(run)
            total_runs += 1

        seen_race_ids.add(race_id)
        processed += 1

    # --- 日付降順ソート（最新走が先頭）---
    logger.info("ソート中...")
    for hid in horse_db:
        horse_db[hid]["runs"].sort(key=lambda r: r.get("race_date", ""), reverse=True)

    # --- 保存 ---
    logger.info(f"保存中: {len(horse_db)}頭 / {total_runs}走 → {output_path}")
    tmp = output_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(horse_db, f, ensure_ascii=False)
    os.replace(tmp, output_path)
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info(f"保存完了: {size_mb:.1f} MB")
    logger.info(f"処理レース={processed}, スキップ={skipped}, エラー={errors}, 総走={total_runs}")

    return processed, total_runs


# ============================================================
# PastRun 復元 (horse_db.json のエントリから)
# ============================================================


def run_dict_to_past_run(run: dict):
    """
    horse_db.json の run dict を PastRun オブジェクトに変換。
    src.models は循環インポートを避けるため遅延インポート。
    """
    from src.models import PaceType, PastRun

    pace = None
    pace_raw = run.get("pace")
    if pace_raw:
        try:
            pace = PaceType(pace_raw)
        except ValueError:
            pass

    return PastRun(
        race_date=run.get("race_date", ""),
        venue=run.get("venue", ""),
        course_id=run.get("course_id", ""),
        distance=run.get("distance", 2000),
        surface=run.get("surface", "芝"),
        condition=run.get("condition", "良"),
        class_name=run.get("class_name", ""),
        grade=run.get("grade", "OP"),
        field_count=run.get("field_count", 10),
        gate_no=run.get("gate_no", 1),
        horse_no=run.get("horse_no", 1),
        jockey=run.get("jockey", ""),
        jockey_id=run.get("jockey_id", ""),
        trainer_id=run.get("trainer_id", ""),
        weight_kg=run.get("weight_kg", 55.0),
        horse_weight=run.get("horse_weight"),
        weight_change=run.get("weight_change"),
        position_4c=run.get("position_4c", 4),
        positions_corners=run.get("positions_corners", []),
        finish_pos=run.get("finish_pos", 10),
        finish_time_sec=run.get("finish_time_sec", 0.0),
        last_3f_sec=run.get("last_3f_sec", 35.5),
        margin_behind=run.get("margin_behind") or 0.0,
        margin_ahead=run.get("margin_ahead") or 0.0,
        pace=pace,
        first_3f_sec=run.get("first_3f_sec"),
        is_generation=run.get("is_generation", False),
    )


# ============================================================
# horse_db ローダー (シングルトン的に使う)
# ============================================================

_HORSE_DB_CACHE: Optional[dict] = None
_HORSE_DB_PATH: Optional[str] = None


def load_horse_db(path: str, force_reload: bool = False) -> dict:
    """horse_db.json をロードしてキャッシュする"""
    global _HORSE_DB_CACHE, _HORSE_DB_PATH
    if not force_reload and _HORSE_DB_CACHE is not None and path == _HORSE_DB_PATH:
        return _HORSE_DB_CACHE
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            _HORSE_DB_CACHE = json.load(f)
        _HORSE_DB_PATH = path
        return _HORSE_DB_CACHE
    except Exception:
        return {}


def get_past_runs_from_horse_db(horse_id: str, horse_db_path: str, max_runs: int = 30):
    """
    horse_id の過去走を horse_db.json から取得して PastRun リストを返す。
    見つからない場合は空リスト。
    """
    db = load_horse_db(horse_db_path)
    entry = db.get(horse_id)
    if not entry:
        return [], None  # (past_runs, horse_meta)

    runs_raw = entry.get("runs", [])[:max_runs]
    past_runs = []
    for r in runs_raw:
        try:
            pr = run_dict_to_past_run(r)
            past_runs.append(pr)
        except Exception:
            logger.debug("run_dict_to_past_run failed", exc_info=True)
            continue

    horse_meta = {
        "horse_name": entry.get("horse_name", ""),
        "sex": entry.get("sex", ""),
    }
    return past_runs, horse_meta
