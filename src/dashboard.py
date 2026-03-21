"""
統合ダッシュボード - ポートフォリオ・データ収集・レース分析を1つのWeb UIに
"""

import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, Response, jsonify, redirect, render_template, render_template_string, request, send_from_directory

from src.log import get_logger

logger = get_logger(__name__)

try:
    from config.settings import (
        COURSE_DB_COLLECTOR_STATE_PATH,
        COURSE_DB_PRELOAD_PATH,
        OUTPUT_DIR,
        SERVER_HOST,
        SERVER_PORT,
        AUTH_ENABLED,
        AUTH_USERNAME,
        AUTH_PASSWORD,
    )
except Exception:
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    COURSE_DB_PRELOAD_PATH = os.path.join(_root, "data", "course_db_preload.json")
    COURSE_DB_COLLECTOR_STATE_PATH = os.path.join(_root, "data", "course_db_collector_state.json")
    OUTPUT_DIR = os.path.join(_root, "output")
    SERVER_HOST = "0.0.0.0"
    SERVER_PORT = 5051
    AUTH_ENABLED = False
    AUTH_USERNAME = "admin"
    AUTH_PASSWORD = ""

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_collector_state = {
    "running": False,
    "day_index": 0,
    "total_days": 0,
    "total_runs": 0,
    "current_date": "",
    "status": "idle",
    "error": None,
    "elapsed_sec": 0,
    "start_time": 0,
}
_analyzer_state = {"running": False, "progress": "", "error": None, "done": False}
_analyzer_proc = None  # subprocess.Popen reference for cancel
_odds_state = {"running": False, "done": False, "error": None, "updated_at": None,
               "count": 0, "total": 0, "current": 0, "current_race": "", "started_at": 0}
_odds_cancel = False  # flag to cancel odds update loop
# ── オッズ自動取得スケジューラー ──
_ODDS_SCHEDULE_HOURS = [5, 7, 9, 11, 13, 15, 17, 19, 21, 23]
_odds_scheduler_running = False
_odds_last_auto_fetch = None   # datetime of last auto-fetch
# ── 予想自動生成スケジューラー ──
_PREDICT_SCHEDULE_HOUR = 17   # 前日17:00に翌日の予想を生成
_predict_scheduler_running = False
_predict_last_auto_run = None  # datetime of last auto-run
# ── 結果照合+DB更新 自動スケジューラー ──
_RESULTS_SCHEDULE_HOUR = 22   # 当日22:00に結果照合+DB更新
_results_scheduler_running = False
_results_last_auto_run = None  # datetime of last auto-run
_results_state = {"running": False, "done": False, "cancel": False, "progress": "", "error": None}
_db_update_state = {"running": False, "done": False, "cancel": False, "progress": "", "error": None}

# ── キャッシュ: 日付→(timestamp, データ) ──
_predictions_cache: dict = {}
_home_info_cache: dict = {}
_CACHE_TTL = 1800  # 秒（予想データ: 30分）
_WEATHER_CACHE_TTL = 1800  # 秒（天気データ: 30分）

# 競馬場コード → 緯度・経度（天気API用）
VENUE_COORDS = {
    "03": (43.06, 141.35),  # 札幌
    "04": (41.77, 140.73),  # 函館
    "01": (37.75, 140.47),  # 福島
    "02": (37.92, 139.04),  # 新潟
    "05": (35.66, 139.48),  # 東京
    "06": (35.72, 139.99),  # 中山
    "07": (35.13, 136.95),  # 中京
    "08": (34.99, 135.75),  # 京都
    "09": (34.82, 135.34),  # 阪神
    "10": (33.88, 130.88),  # 小倉
    "30": (42.29, 143.22),  # 門別
    "35": (39.70, 141.15),  # 盛岡
    "36": (39.14, 141.14),  # 水沢
    "42": (35.86, 139.66),  # 浦和
    "43": (35.69, 140.02),  # 船橋
    "44": (35.59, 139.78),  # 大井
    "45": (35.53, 139.70),  # 川崎
    "46": (36.56, 136.66),  # 金沢
    "47": (35.32, 136.70),  # 笠松
    "48": (35.18, 136.91),  # 名古屋
    "49": (34.73, 135.42),  # 園田（旧コード、実際は50が使用）
    "50": (34.73, 135.42),  # 園田
    "51": (34.82, 134.69),  # 姫路
    "52": (42.93, 143.20),  # 帯広（ばんえい）
    "54": (33.56, 133.53),  # 高知
    "55": (33.25, 130.30),  # 佐賀
}

# WMO天気コード → 表示用（Open-Meteo）
WMO_WEATHER = {
    0: "晴",
    1: "おおむね晴",
    2: "晴れ時々くもり",
    3: "くもり",
    45: "霧",
    48: "霧",
    51: "小雨",
    53: "小雨",
    55: "雨",
    56: "小雨",
    57: "雨",
    61: "雨",
    63: "雨",
    65: "大雨",
    66: "雨",
    67: "大雨",
    71: "雪",
    73: "雪",
    75: "大雪",
    77: "雪",
    80: "にわか雨",
    81: "にわか雨",
    82: "大雨",
    85: "にわか雪",
    86: "大雪",
    95: "雷雨",
    96: "雷雨",
    99: "雷雨",
}


def _get_todays_venues(date_str: str) -> list:
    """指定日の開催競馬場を取得（netkeiba + JRA/NAR公式補完）

    PremiumNetkeibaScraper.fetch_date() と同等のフォールバック構成:
    1. netkeiba (JRA + NAR)
    2. JRA公式 (netkeibaが空の場合)
    3. NAR公式 (常に補完 — ばんえい含む)
    4. ばんえい安全策 (NAR公式が帯広を返さない場合のみ)
    """
    try:
        from data.masters.venue_master import get_venue_code_from_race_id, get_venue_name, is_banei
        from src.scraper.netkeiba import NetkeibaClient, RaceListScraper

        client = NetkeibaClient(no_cache=True)
        scraper = RaceListScraper(client)
        ids = scraper.get_race_ids(date_str)
        existing = set(ids)

        # JRA公式で補完（netkeibaが空の場合のみ）
        if not ids:
            try:
                from src.scraper.official_odds import OfficialOddsScraper
                jra_scraper = OfficialOddsScraper()
                jra_ids = jra_scraper.get_jra_race_list(target_date=date_str)
                for rid in jra_ids:
                    if rid not in existing:
                        ids.append(rid)
                        existing.add(rid)
            except Exception as e:
                logger.debug("JRA公式補完失敗: %s", e)

        # NAR公式で補完（常に実行 — netkeibaが制限中でもNAR/ばんえいを確保）
        try:
            from src.scraper.official_nar import OfficialNARScraper
            nar = OfficialNARScraper()
            nar_ids = nar.get_race_ids(date_str)
            for rid in nar_ids:
                if rid not in existing:
                    ids.append(rid)
                    existing.add(rid)
        except Exception as e:
            logger.debug("NAR公式補完失敗: %s", e)

        # ばんえい安全策（NAR公式が帯広を返さなかった場合のみ発動）
        if not any(rid[4:6] == "65" for rid in ids):
            try:
                import glob as _glob_banei
                year = date_str[:4]
                mmdd = date_str[5:7] + date_str[8:10]
                # キャッシュにばんえいHTMLが存在すればID生成
                banei_pattern = os.path.join(
                    client.cache_dir, f"*race_id={year}65{mmdd}*"
                )
                cached = _glob_banei.glob(banei_pattern)
                if cached:
                    race_nos = set()
                    for path in cached:
                        m = re.search(rf"{year}65{mmdd}(\d{{2}})", os.path.basename(path))
                        if m:
                            race_nos.add(int(m.group(1)))
                    max_race = max(race_nos) if race_nos else 12
                    banei_ids = [f"{year}65{mmdd}{rno:02d}" for rno in range(1, max_race + 1)]
                    for rid in banei_ids:
                        if rid not in existing:
                            ids.append(rid)
                            existing.add(rid)
                    logger.info("ばんえいキャッシュ補完: %dR", max_race)
                else:
                    # nar.netkeiba.comで1R目を試行
                    from src.scraper.netkeiba import NAR_URL
                    probe_id = f"{year}65{mmdd}01"
                    probe_soup = client.get(
                        f"{NAR_URL}/race/shutuba.html",
                        params={"race_id": probe_id}
                    )
                    if probe_soup and probe_soup.select("table"):
                        banei_ids = [f"{year}65{mmdd}{rno:02d}" for rno in range(1, 13)]
                        for rid in banei_ids:
                            if rid not in existing:
                                ids.append(rid)
                                existing.add(rid)
                        logger.info("ばんえいプローブ補完: 12R")
            except Exception as e:
                logger.debug("ばんえい補完失敗: %s", e)

        seen = set()
        result = []
        for rid in ids:
            vc = get_venue_code_from_race_id(rid)
            if not vc or vc in seen:
                continue
            name = get_venue_name(vc)
            if not name:
                continue
            seen.add(vc)
            result.append({"code": vc, "name": name})
        if result:
            return result
    except Exception as e:
        logger.warning("venue fetch failed: %s", e)

    # フォールバック: 予想データ(outputディレクトリ)から会場を抽出
    try:
        from data.masters.venue_master import VENUE_NAME_TO_CODE
        date_key = date_str.replace("-", "")
        pred = _scan_today_predictions(date_str)
        if pred.get("order"):
            result = []
            for venue_name in pred["order"]:
                code = VENUE_NAME_TO_CODE.get(venue_name, "")
                result.append({"code": code, "name": venue_name})
            return result
    except Exception as e:
        logger.warning("venue fallback from predictions failed: %s", e)
    return []


def _fetch_weather(lat: float, lon: float) -> dict:
    """Open-Meteo API で天気・降水確率取得"""
    try:
        import urllib.request

        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=weather_code,precipitation_probability_max&timezone=Asia/Tokyo&forecast_days=1"
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read().decode())
        daily = data.get("daily", {})
        codes = daily.get("weather_code", [0])
        probs = daily.get("precipitation_probability_max", [0])
        wc = int(codes[0]) if codes else 0
        prob = int(probs[0]) if probs else 0
        return {"condition": WMO_WEATHER.get(wc, "—"), "precip_prob": prob}
    except Exception:
        return {"condition": "—", "precip_prob": None}


def _scan_output():
    date_files, single_files = [], []
    if os.path.isdir(OUTPUT_DIR):
        for name in sorted(os.listdir(OUTPUT_DIR), reverse=True):
            if not name.endswith(".html"):
                continue
            path = os.path.join(OUTPUT_DIR, name)
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            item = {"file": name, "mtime": mtime.strftime("%Y-%m-%d %H:%M")}
            if name.endswith("_全レース.html"):
                m = re.match(r"(\d{8})", name)
                if m:
                    ds = m.group(1)
                    item["date"] = f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}"
                    date_files.append(item)
            else:
                single_files.append(item)
    return date_files[:20], single_files[:30]


def _parse_race_html_meta(path: str) -> dict:
    """レースHTMLから race-meta JSONタグ、または正規表現で情報を抽出"""
    try:
        # バイト列で読み込み（mark文字検索のため）
        with open(path, "rb") as f:
            raw = f.read(50000)
        html = raw.decode("utf-8", errors="replace")
    except Exception:
        return {}

    # ① machine-readable JSON メタタグ（新形式）
    m = re.search(r'<script type="application/json" id="race-meta">(.*?)</script>', html)
    if m:
        try:
            import json as _json

            meta_json = _json.loads(m.group(1))
            # honmei_rentai_pct が未格納の既存HTMLファイル用フォールバック
            if "honmei_rentai_pct" not in meta_json and meta_json.get("honmei_name"):
                try:
                    with open(path, "r", encoding="utf-8", errors="replace") as ff:
                        full_html = ff.read()
                    body_start = full_html.find("</style>")
                    full_body = full_html[body_start:] if body_start >= 0 else full_html
                    wr = re.search(
                        r'class="hds-name">'
                        + re.escape(meta_json["honmei_name"])
                        + r"</span>.*?hds-wr-win[^>]*>(\d+\.?\d*)%.*?hds-wr-win[^>]*>(\d+\.?\d*)%.*?hds-wr-win[^>]*>(\d+\.?\d*)%",
                        full_body,
                        re.DOTALL,
                    )
                    if wr:
                        meta_json["honmei_rentai_pct"] = float(wr.group(2))
                except Exception:
                    pass
            # オッズ・人気を取得（JSONメタに含まれない場合、HTMLから抽出）
            if meta_json.get("honmei_name") and "honmei_odds" not in meta_json:
                try:
                    with open(path, "r", encoding="utf-8", errors="replace") as ff:
                        _full = ff.read()
                    _bs = _full.find("</style>")
                    _body = _full[_bs:] if _bs >= 0 else _full
                    _om = re.search(
                        r'class="hds-name">'
                        + re.escape(meta_json["honmei_name"])
                        + r'</span>.*?(\d+\.?\d*)倍\((\d+)人気\)',
                        _body,
                        re.DOTALL,
                    )
                    if _om:
                        meta_json["honmei_odds"] = float(_om.group(1))
                        meta_json["honmei_popularity"] = int(_om.group(2))
                except Exception:
                    pass
            return meta_json
        except Exception:
            logger.debug("race meta JSON parse failed", exc_info=True)

    # ② フォールバック：タイトルタグ・本文の正規表現解析（旧形式HTML対応）
    meta: dict = {}
    # タイトル: "{venue} {race_no}R {race_name} - D-AI競馬予想"
    t = re.search(r"<title>([^\s]+)\s+(\d+)R\s+(.+?)\s*-\s*D-AI", html)
    if t:
        meta["venue"] = t.group(1)
        meta["race_no"] = int(t.group(2))
        meta["race_name"] = t.group(3)
    # race-info-line から発走時刻・コース・距離を抽出
    # 例: "18:15発走 / ダート1400m (右な回り) / 天候:☀ / 馬場:ダ:良"
    ril = re.search(r'race-info-line">(.+?)</div>', html)
    if ril:
        line = ril.group(1)
        pt = re.search(r"(\d{1,2}:\d{2})発走", line)
        if pt:
            meta["post_time"] = pt.group(1)
        surf = re.search(r"(芝|ダート|障)(\d{3,4})m", line)
        if surf:
            meta["surface"] = surf.group(1)
            meta["distance"] = int(surf.group(2))
    # 出走頭数（sub div内: "出走11頭"）
    hc = re.search(r"出走(\d+)頭", html)
    if hc:
        meta["head_count"] = int(hc.group(1))
    # グレード（タイトル近辺）
    grade_m = re.search(r'badge b-S">(G[123]|L|OP)</span>', html)
    if grade_m:
        meta["grade"] = grade_m.group(1)
    # CSSより後のコンテンツ領域を取得
    body_start = html.find("</style>")
    body = html[body_start:] if body_start >= 0 else html

    # ■買い目 欄の「自信度：X」を最優先で取得（ユーザー可視の馬券自信度）
    conf_m = re.search(r"自信度[：:]\s*<[^>]*>(SS\+?|S\+?|A\+?|B\+?|C\+?|D\+?|E\+?|F)", body)
    if not conf_m:
        conf_m = re.search(r"自信度[：:]\s*(SS\+?|S\+?|A\+?|B\+?|C\+?|D\+?|E\+?|F)", body)
    if conf_m:
        meta["overall_confidence"] = conf_m.group(1)
    else:
        # フォールバック：展開予測の展開精度
        conf_m2 = re.search(r"展開精度\s+(SS|S|A|B|C|D)", body)
        if conf_m2:
            meta["overall_confidence"] = conf_m2.group(1)

    # 軸馬マーク + 馬名（バイト列で直接検索、優先度順）
    # ◉=U+25C9  ◎=U+25CE  ○=U+25CB  ▲=U+25B2  △=U+25B3  ☆=U+2606  ◉>◎>○>▲>△>☆
    MARK_PRIORITY = [
        (b"\xe2\x97\x89", "◉"),  # U+25C9 鉄板
        (b"\xe2\x97\x8e", "◎"),  # U+25CE 本命
        (b"\xe2\x97\x8b", "○"),  # U+25CB 対抗
        (b"\xe2\x96\xb2", "▲"),  # U+25B2 単穴
        (b"\xe2\x96\xb3", "△"),  # U+25B3 連下
    ]
    body_raw = raw[raw.find(b"</style>") :] if b"</style>" in raw else raw
    for mb, mark_char in MARK_PRIORITY:
        mk_key_b = b'class="m-' + mb + b'"'
        if mk_key_b not in body_raw:
            continue
        meta["honmei_mark"] = mark_char
        mk_key = f'class="m-{mark_char}"'
        # 新2行レイアウト: hds-row1 内の hds-name
        hm = re.search(
            re.escape(mk_key)
            + r"[^>]*>"
            + re.escape(mark_char)
            + r'</span>.*?<span class="hds-name">(.*?)</span>',
            body,
            re.DOTALL,
        )
        if hm:
            meta["honmei_name"] = hm.group(1).strip()
        else:
            # 旧フォーマット（hds-name-block内）
            hm2 = re.search(
                re.escape(mk_key) + r'.*?<span class="hds-name">(.*?)</span>', body, re.DOTALL
            )
            if hm2:
                meta["honmei_name"] = hm2.group(1).strip()
        break

    # 複勝率を取得（新フォーマット: hds-wr-win 複XX.X%）
    if meta.get("honmei_name"):
        # 新フォーマット: 馬名の後に 複XX.X% が teal色のスパンで来る
        wr_new = re.search(
            r'class="hds-name">'
            + re.escape(meta["honmei_name"])
            + r"</span>.*?hds-wr-win.*?(\d+\.?\d*)%.*?hds-wr-win.*?(\d+\.?\d*)%.*?hds-wr-win.*?(\d+\.?\d*)%",
            body,
            re.DOTALL,
        )
        if wr_new:
            meta["honmei_win_pct"] = float(wr_new.group(1))
            meta["honmei_rentai_pct"] = float(wr_new.group(2))
            meta["honmei_fukusho_pct"] = float(wr_new.group(3))
        else:
            # 旧フォーマット
            wr_old = re.search(
                r'class="hds-name">'
                + re.escape(meta["honmei_name"])
                + r'</span>.*?class="hds-winrate">勝(\d+)%[^<]*連(\d+)%[^<]*複(\d+)%</span>',
                body,
                re.DOTALL,
            )
            if wr_old:
                meta["honmei_win_pct"] = float(wr_old.group(1))
                meta["honmei_rentai_pct"] = float(wr_old.group(2))
                meta["honmei_fukusho_pct"] = float(wr_old.group(3))

    # オッズ・人気を取得（◉馬の行から "XX.X倍(N人気)" を抽出）
    if meta.get("honmei_name"):
        odds_m = re.search(
            r'class="hds-name">'
            + re.escape(meta["honmei_name"])
            + r'</span>.*?(\d+\.?\d*)倍\((\d+)人気\)',
            body,
            re.DOTALL,
        )
        if odds_m:
            meta["honmei_odds"] = float(odds_m.group(1))
            meta["honmei_popularity"] = int(odds_m.group(2))

    return meta


_VENUE_PRIORITY = [
    "東京",
    "中山",
    "京都",
    "阪神",
    "中京",
    "小倉",
    "新潟",
    "福島",
    "札幌",
    "函館",
    "門別",
    "盛岡",
    "水沢",
    "浦和",
    "大井",
    "船橋",
    "川崎",
    "名古屋",
    "笠松",
    "園田",
    "姫路",
    "金沢",
    "高知",
    "佐賀",
]
_VENUE_PRIO_MAP = {v: i for i, v in enumerate(_VENUE_PRIORITY)}


def _calc_betting_ev(horses: list) -> dict | None:
    """馬リストから単勝/馬連/三連複の的中率・期待値を計算（Harvilleモデル）

    期待値の計算:
    - 単勝: P(win) × 単勝オッズ
    - 馬連: P(的中) × 推定馬連オッズ  (推定 = sqrt(オッズA × オッズB) × 0.775)
    - 三連複: P(的中) × 推定三連複オッズ (推定 = cbrt(A×B×C) × 0.725)
    各点のEV合計を点数で割って1点あたりのEVとする
    """
    import math

    honmei = None
    others = []
    for h in horses:
        mk = h.get("mark", "")
        if mk in ("◉", "◎"):
            honmei = h
        elif mk in ("○", "▲", "△", "★", "☆"):
            others.append(h)
    if not honmei or not others:
        return None

    # モデル確率（正規化）
    raw = [(h.get("win_prob") or 0) for h in horses]
    m_sum = sum(raw) or 1.0
    def model_p(h):
        return (h.get("win_prob") or 0) / m_sum

    # オッズ取得（実オッズのみ — 推定オッズでは計算しない）
    def get_odds(h):
        return h.get("odds") or 0

    has_real_odds = any(h.get("odds") for h in horses)

    p_hon = model_p(honmei)
    hon_odds = get_odds(honmei)

    # Harville: P(A 1着, B 2着)
    def h2(pa, pb):
        return pa * pb / (1 - pa) if pa < 0.999 else 0

    # ── 単勝 ──
    tan_hit = p_hon * 100
    tan_ev = p_hon * hon_odds if hon_odds > 0 else 0

    # ── 馬連 (◎-X) ──
    # 推定馬連オッズ ≈ sqrt(odds_A × odds_B) × 控除率係数
    UMAREN_PR = 0.775
    umaren_hit = 0.0
    umaren_ev_total = 0.0
    for o in others:
        po = model_p(o)
        p_mod = h2(p_hon, po) + h2(po, p_hon)
        umaren_hit += p_mod
        o_odds = get_odds(o)
        if hon_odds > 0 and o_odds > 0:
            est_payout = math.sqrt(hon_odds * o_odds) * UMAREN_PR
            umaren_ev_total += p_mod * est_payout
    umaren_count = len(others)

    # ── 三連複 (◎ + othersから2頭)  Harvilleモデル ──
    sanren_hit = 0.0
    sanren_ev_total = 0.0
    sanren_count = 0
    # 全馬の単勝オッズリスト（Harville三連複オッズ推定用）
    all_odds_list = [get_odds(h) or 0 for h in horses]
    has_all_odds = all(o > 0 for o in all_odds_list)
    for i in range(len(others)):
        for j in range(i + 1, len(others)):
            pi, pj = model_p(others[i]), model_p(others[j])
            # 6通りの順列（Harville確率）
            p_mod = 0.0
            for a, b, c in [(p_hon, pi, pj), (p_hon, pj, pi),
                            (pi, p_hon, pj), (pi, pj, p_hon),
                            (pj, p_hon, pi), (pj, pi, p_hon)]:
                d1 = max(1 - a, 0.001)
                d2 = max(d1 - b, 0.001)
                p_mod += a * (b / d1) * (c / d2)
            sanren_hit += p_mod
            oi = get_odds(others[i])
            oj = get_odds(others[j])
            if hon_odds > 0 and oi > 0 and oj > 0:
                from src.calculator.betting import estimate_sanrenpuku_odds as _est_san
                est_odds = _est_san(
                    hon_odds, oi, oj, len(horses),
                    _all_odds=all_odds_list if has_all_odds else None,
                )
                sanren_ev_total += p_mod * est_odds
            sanren_count += 1

    # EV = 全点の期待リターン合計 / 点数
    umaren_ev = umaren_ev_total / umaren_count if umaren_count else 0
    sanren_ev = sanren_ev_total / sanren_count if sanren_count else 0

    if not has_real_odds:
        # 実オッズ未取得 → 的中率のみ返す（EVは計算不可）
        return {
            "bet_tan_hit": round(tan_hit, 1),
            "bet_tan_ev": None,
            "bet_umaren_hit": round(umaren_hit * 100, 1),
            "bet_umaren_ev": None,
            "bet_umaren_count": umaren_count,
            "bet_sanren_hit": round(sanren_hit * 100, 1),
            "bet_sanren_ev": None,
            "bet_sanren_count": sanren_count,
        }

    return {
        "bet_tan_hit": round(tan_hit, 1),
        "bet_tan_ev": round(tan_ev, 2),
        "bet_umaren_hit": round(umaren_hit * 100, 1),
        "bet_umaren_ev": round(umaren_ev, 2),
        "bet_umaren_count": umaren_count,
        "bet_sanren_hit": round(sanren_hit * 100, 1),
        "bet_sanren_ev": round(sanren_ev, 2),
        "bet_sanren_count": sanren_count,
    }


def _comp_gap(horses: list, honmei: dict) -> float:
    """◎の総合指数と2位との差を返す"""
    comps = sorted([h.get("composite", 0) for h in horses], reverse=True)
    if len(comps) >= 2:
        return round(comps[0] - comps[1], 1)
    return 0


def _scan_today_predictions(date_str: str) -> dict:
    """指定日の個別レースHTML (YYYYMMDD_場名XR.html) をスキャンして会場別に整理"""
    date_key = date_str.replace("-", "")
    races: dict = {}
    if not os.path.isdir(OUTPUT_DIR):
        return {"races": races, "order": []}

    # pred JSONから馬券自信度 + 馬データ + チケットデータを取得
    _pred_conf = {}  # {(venue, race_no): confidence}
    _pred_horses = {}  # {(venue, race_no): [horse_dict, ...]}
    _pred_tickets = {}  # {(venue, race_no): [ticket_dict, ...]}
    pred_json_path = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
    if os.path.isfile(pred_json_path):
        try:
            # UTF-8壊れ対策: バイナリ読み→不正バイト除去
            with open(pred_json_path, "rb") as pf:
                raw = pf.read()
            text = raw.decode("utf-8", errors="ignore")
            pred_data = json.loads(text)
            for pr in pred_data.get("races", []):
                venue = pr.get("venue", "")
                rno = pr.get("race_no")
                if not venue or rno is None:
                    continue
                key = (venue, rno)
                _pred_conf[key] = pr.get("confidence", "")
                _pred_horses[key] = pr.get("horses", [])
                _pred_tickets[key] = pr.get("tickets", [])
        except Exception:
            pass

    pat = re.compile(rf"^{re.escape(date_key)}_(.+?)(\d+)R\.html$")
    for name in sorted(os.listdir(OUTPUT_DIR)):
        m = pat.match(name)
        if not m:
            continue
        venue = m.group(1)
        if venue == "None" or venue.startswith("地方"):
            continue
        race_no = int(m.group(2))
        path = os.path.join(OUTPUT_DIR, name)
        html_meta = _parse_race_html_meta(path)

        # 馬券自信度: pred JSONから優先取得、なければHTMLパースを使用
        conf = _pred_conf.get((venue, race_no), "") or html_meta.get("overall_confidence", "")

        if venue not in races:
            races[venue] = []
        races[venue].append(
            {
                "race_no": race_no,
                "file": name,
                "url": f"/output/{name}",
                "name": html_meta.get("race_name") or f"{race_no}R",
                "post_time": html_meta.get("post_time", ""),
                "surface": html_meta.get("surface", ""),
                "distance": html_meta.get("distance", 0),
                "head_count": html_meta.get("head_count", 0),
                "grade": html_meta.get("grade", ""),
                "overall_confidence": conf,
                "honmei_no": html_meta.get("honmei_no", 0),
                "honmei_name": html_meta.get("honmei_name", ""),
                "honmei_mark": html_meta.get("honmei_mark", ""),
                "honmei_composite": html_meta.get("honmei_composite", 0),
                "composite_gap": _comp_gap(_pred_horses.get((venue, race_no), []), {}) if (venue, race_no) in _pred_horses else 0,
                "honmei_win_pct": html_meta.get("honmei_win_pct", 0),
                "honmei_rentai_pct": html_meta.get("honmei_rentai_pct", 0),
                "honmei_fukusho_pct": html_meta.get("honmei_fukusho_pct", 0),
                "honmei_odds": html_meta.get("honmei_odds"),
                "honmei_popularity": html_meta.get("honmei_popularity"),
            }
        )
        # pred.json の馬データから本命馬情報を上書き（オッズ更新後の最新印を反映）
        horses = _pred_horses.get((venue, race_no), [])
        if horses:
            # 本命馬を特定（◉ > ◎ の優先度）
            honmei = None
            for mk in ("◉", "◎"):
                for h in horses:
                    if h.get("mark") == mk:
                        honmei = h
                        break
                if honmei:
                    break
            if honmei:
                # 2位との総合指数差を計算
                honmei_comp = honmei.get("composite", 0)
                sorted_comps = sorted([h.get("composite", 0) for h in horses], reverse=True)
                comp_gap = round(sorted_comps[0] - sorted_comps[1], 1) if len(sorted_comps) >= 2 else 0
                races[venue][-1].update({
                    "honmei_no": honmei.get("horse_no", 0),
                    "honmei_name": honmei.get("horse_name", ""),
                    "honmei_mark": honmei.get("mark", ""),
                    "honmei_composite": honmei_comp,
                    "composite_gap": comp_gap,
                    "honmei_win_pct": round(honmei.get("win_prob", 0) * 100, 1),
                    "honmei_rentai_pct": round(honmei.get("place2_prob", 0) * 100, 1),
                    "honmei_fukusho_pct": round(honmei.get("place3_prob", 0) * 100, 1),
                })
                if honmei.get("odds") and honmei["odds"] > 0:
                    races[venue][-1]["honmei_odds"] = honmei["odds"]
                if honmei.get("popularity"):
                    races[venue][-1]["honmei_popularity"] = honmei["popularity"]
            # 馬券EV計算
            ev_data = _calc_betting_ev(horses)
            if ev_data:
                races[venue][-1].update(ev_data)
            # 買い目表示用: 印付き馬番
            _others = [{"no": h.get("horse_no", 0), "mark": h.get("mark", "")}
                       for h in horses if h.get("mark") in ("○", "▲", "△", "☆", "★")]
            races[venue][-1]["bet_others"] = _others
        # 三連複フォーメーションチケットの実データ
        tickets = _pred_tickets.get((venue, race_no), [])
        san_tickets = [t for t in tickets if t.get("type") == "三連複"]
        if san_tickets:
            san_count = len(san_tickets)
            san_probs = [t.get("prob", 0) for t in san_tickets]
            san_hit_pct = round(sum(san_probs) * 100, 1)
            san_evs = [t.get("ev", 0) for t in san_tickets if t.get("ev")]
            san_avg_ev = round(sum(san_evs) / len(san_evs) / 100, 2) if san_evs else None
            races[venue][-1].update({
                "fm_sanren_count": san_count,
                "fm_sanren_hit": san_hit_pct,
                "fm_sanren_ev": san_avg_ev,
            })

    for venue in races:
        races[venue].sort(key=lambda x: x["race_no"])
    order = sorted(races.keys(), key=lambda v: (_VENUE_PRIO_MAP.get(v, 999), v))

    # Fallback: HTMLがなければ pred JSON から読む
    if not races:
        pred_json_path = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
        if os.path.isfile(pred_json_path):
            try:
                with open(pred_json_path, "r", encoding="utf-8") as pf:
                    pred_data = json.load(pf)
                for pr in pred_data.get("races", []):
                    venue = pr.get("venue", "")
                    if not venue or venue == "None" or venue.startswith("地方"):
                        continue
                    race_no = pr.get("race_no", 0)
                    honmei = None
                    for h in pr.get("horses", []):
                        if h.get("mark") in ("◉", "◎"):
                            honmei = h
                            break
                    if venue not in races:
                        races[venue] = []
                    races[venue].append({
                        "race_no": race_no,
                        "file": "",
                        "url": "",
                        "name": pr.get("race_name") or f"{race_no}R",
                        "post_time": pr.get("post_time", ""),
                        "surface": pr.get("surface", ""),
                        "distance": pr.get("distance", 0),
                        "head_count": pr.get("field_count", 0),
                        "grade": pr.get("grade", ""),
                        "overall_confidence": pr.get("confidence", ""),
                        "honmei_no": honmei.get("horse_no", 0) if honmei else 0,
                        "honmei_name": honmei.get("horse_name", "") if honmei else "",
                        "honmei_mark": honmei.get("mark", "") if honmei else "",
                        "honmei_composite": honmei.get("composite", 0) if honmei else 0,
                        "composite_gap": _comp_gap(pr.get("horses", []), honmei) if honmei else 0,
                        "honmei_win_pct": round(honmei.get("win_prob", 0) * 100, 1) if honmei else 0,
                        "honmei_rentai_pct": round(honmei.get("place2_prob", 0) * 100, 1) if honmei else 0,
                        "honmei_fukusho_pct": round(honmei.get("place3_prob", 0) * 100, 1) if honmei else 0,
                    })
                    # 馬券EV計算
                    all_horses = pr.get("horses", [])
                    if all_horses:
                        ev_data = _calc_betting_ev(all_horses)
                        if ev_data:
                            races[venue][-1].update(ev_data)
                        _others = [{"no": h.get("horse_no", 0), "mark": h.get("mark", "")}
                                   for h in all_horses if h.get("mark") in ("○", "▲", "△", "☆", "★")]
                        races[venue][-1]["bet_others"] = _others
                    # 三連複フォーメーションチケットの実データ
                    _fb_tickets = pr.get("tickets", [])
                    _fb_san = [t for t in _fb_tickets if t.get("type") == "三連複"]
                    if _fb_san:
                        _sc = len(_fb_san)
                        _sp = [t.get("prob", 0) for t in _fb_san]
                        _se = [t.get("ev", 0) for t in _fb_san if t.get("ev")]
                        races[venue][-1].update({
                            "fm_sanren_count": _sc,
                            "fm_sanren_hit": round(sum(_sp) * 100, 1),
                            "fm_sanren_ev": round(sum(_se) / len(_se) / 100, 2) if _se else None,
                        })
                for venue in races:
                    races[venue].sort(key=lambda x: x["race_no"])
                order = sorted(races.keys(), key=lambda v: (_VENUE_PRIO_MAP.get(v, 999), v))
            except Exception:
                pass

    return {"races": races, "order": order}


def _get_db_state():
    st = {"course_runs": 0, "last_date": "", "total_runs": 0}
    if os.path.exists(COURSE_DB_PRELOAD_PATH):
        try:
            with open(COURSE_DB_PRELOAD_PATH, "r", encoding="utf-8") as f:
                d = json.load(f)
            st["course_runs"] = sum(len(v) for v in d.get("course_db", {}).values())
        except Exception:
            logger.debug("course_db preload read failed", exc_info=True)
    if os.path.exists(COURSE_DB_COLLECTOR_STATE_PATH):
        try:
            with open(COURSE_DB_COLLECTOR_STATE_PATH, "r", encoding="utf-8") as f:
                s = json.load(f)
            st["last_date"] = s.get("last_date", "")
            st["total_runs"] = s.get("total_runs", 0)
        except Exception:
            logger.debug("collector state read failed", exc_info=True)
    return st


BASE_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>D-AIkeiba - 統合ダッシュボード</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    :root{
      --bg:#F8FAFC;--surface:#FFFFFF;
      --primary:#059669;--primary-hover:#047857;--primary-light:#D1FAE5;
      --text:#111827;--text-muted:#6B7280;--text-xs:#9CA3AF;
      --border:#E5E7EB;--border-focus:#059669;
      --shadow:0 1px 3px rgba(0,0,0,.08),0 1px 2px rgba(0,0,0,.05);
      --shadow-md:0 4px 6px rgba(0,0,0,.07),0 2px 4px rgba(0,0,0,.05);
      --shadow-lg:0 10px 15px rgba(0,0,0,.08),0 4px 6px rgba(0,0,0,.04);
      --danger:#DC2626;--warning:#D97706;--info:#0369A1;
      --radius:12px;--radius-sm:8px;--radius-xs:6px;
    }
    body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Hiragino Sans','Noto Sans JP',sans-serif;font-size:14px;line-height:1.6}
    a{color:var(--primary);text-decoration:none}
    a:hover{color:var(--primary-hover)}
    .app-header{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 20px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:200;box-shadow:var(--shadow)}
    .app-header-title{font-size:18px;font-weight:700;color:var(--primary);display:flex;align-items:center;gap:8px}
    .app-header-sub{font-size:12px;color:var(--text-muted)}
    .tabs{display:flex;background:var(--surface);border-bottom:2px solid var(--border);position:sticky;top:57px;z-index:100;overflow-x:auto;-webkit-overflow-scrolling:touch}
    .tabs::-webkit-scrollbar{display:none}
    .tab{padding:14px 20px;font-size:14px;font-weight:500;color:var(--text-muted);border-bottom:2px solid transparent;margin-bottom:-2px;cursor:pointer;white-space:nowrap;transition:color .15s,border-color .15s;user-select:none;text-decoration:none;display:inline-block}
    .tab:hover{color:var(--text)}
    .tab.active{color:var(--primary);border-bottom-color:var(--primary);font-weight:600}
    .tab-panel{display:none;padding:16px 20px;max-width:960px;margin:0 auto}
    .tab-panel.active{display:block}
    .wrap{max-width:960px;margin:0 auto;padding:0 20px}
    .card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);padding:16px;margin-bottom:12px}
    .card h2,.card-title{font-size:13px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em;margin-bottom:10px}
    .card-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
    .race-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);padding:14px 16px;margin-bottom:8px;display:flex;align-items:center;gap:14px;cursor:pointer;transition:box-shadow .15s,transform .1s;text-decoration:none;color:inherit}
    .race-card:hover{box-shadow:var(--shadow-md);transform:translateY(-1px)}
    .race-card-time{min-width:52px;text-align:center}
    .race-card-time .time{font-size:18px;font-weight:700;color:var(--text);line-height:1}
    .race-card-time .rno{font-size:11px;color:var(--text-muted);margin-top:2px}
    .race-card-info{flex:1;min-width:0}
    .race-card-name{font-size:14px;font-weight:600;color:var(--text);margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .race-card-picks{font-size:13px;color:var(--text-muted)}
    .race-card-right{display:flex;flex-direction:column;align-items:flex-end;gap:6px;min-width:90px}
    .conf-bar{width:80px;height:5px;background:var(--border);border-radius:3px;overflow:hidden}
    .conf-fill{height:100%;background:var(--primary);border-radius:3px;transition:width .3s}
    .race-card-ev{font-size:12px;font-weight:600;color:var(--text-muted)}
    .badge{display:inline-block;font-size:11px;font-weight:600;padding:2px 8px;border-radius:20px;line-height:1.6}
    .badge-high{background:#D1FAE5;color:#065F46}
    .badge-mid{background:#FEF3C7;color:#92400E}
    .badge-low{background:#FEE2E2;color:#991B1B}
    .badge-blue{background:#DBEAFE;color:#1E40AF}
    .badge-gray{background:#F3F4F6;color:#6B7280}
    .badge-jra{background:#DBEAFE;color:#1E40AF}
    .badge-nar{background:#D1FAE5;color:#065F46}
    .venue-tabs{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}
    .venue-tab{padding:6px 12px;border:1px solid var(--border);border-radius:20px;font-size:12px;font-weight:500;color:var(--text-muted);cursor:pointer;background:var(--surface);transition:all .15s}
    .venue-tab:hover{border-color:var(--primary);color:var(--primary)}
    .venue-tab.active{background:var(--primary);border-color:var(--primary);color:#fff}
    .date-nav{display:flex;align-items:center;gap:8px;margin-bottom:16px;flex-wrap:wrap}
    .date-nav-btn{padding:6px 10px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--surface);color:var(--text);cursor:pointer;font-size:14px;transition:all .15s}
    .date-nav-btn:hover{border-color:var(--primary);color:var(--primary)}
    .date-display{font-size:16px;font-weight:700;color:var(--text);padding:4px 8px}
    input[type=date]{padding:6px 10px;border:1px solid var(--border);border-radius:var(--radius-sm);font-size:14px;color:var(--text);background:var(--surface)}
    input[type=date]:focus{outline:none;border-color:var(--border-focus)}
    .btn{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border-radius:var(--radius-sm);font-size:13px;font-weight:600;cursor:pointer;border:none;transition:all .15s;text-decoration:none}
    .btn-primary{background:var(--primary);color:#fff}
    .btn-primary:hover{background:var(--primary-hover)}
    .btn-outline{background:var(--surface);color:var(--primary);border:1px solid var(--primary)}
    .btn-outline:hover{background:var(--primary-light)}
    .btn-sm{padding:5px 12px;font-size:12px}
    .btn-ghost{background:transparent;color:var(--text-muted);border:1px solid var(--border)}
    .btn-ghost:hover{color:var(--text);border-color:var(--text-muted)}
    .btn-danger{background:var(--danger);color:#fff}
    button.btn{font-family:inherit}
    button.primary{background:var(--primary);color:#fff;padding:8px 16px;border:none;border-radius:var(--radius-sm);font-size:13px;font-weight:600;cursor:pointer;font-family:inherit}
    button.primary:hover{background:var(--primary-hover)}
    button.secondary{background:var(--primary-light);color:#065F46;padding:8px 16px;border:none;border-radius:var(--radius-sm);font-size:13px;font-weight:600;cursor:pointer;font-family:inherit}
    button.secondary:hover{background:#A7F3D0}
    button:disabled{opacity:0.6;cursor:not-allowed}
    .progress{height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin:8px 0}
    .progress-fill{height:100%;background:var(--primary);border-radius:4px;transition:width .4s}
    .progress-bar{height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin-top:12px}
    .stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:16px}
    .stat-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);padding:14px;text-align:center;box-shadow:var(--shadow)}
    .stat-value{font-size:24px;font-weight:700;color:var(--text);line-height:1}
    .stat-label{font-size:11px;color:var(--text-muted);margin-top:4px;text-transform:uppercase;letter-spacing:.04em}
    .stat-sub{font-size:12px;color:var(--text-muted);margin-top:2px}
    .stat-pos{color:var(--primary)}
    .stat-neg{color:var(--danger)}
    table{width:100%;border-collapse:collapse;font-size:13px}
    th{padding:8px 10px;text-align:left;font-size:11px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em;border-bottom:2px solid var(--border)}
    td{padding:9px 10px;border-bottom:1px solid var(--border);color:var(--text)}
    tr:last-child td{border-bottom:none}
    tr:hover td{background:#F9FAFB}
    .num{text-align:right;font-variant-numeric:tabular-nums}
    .subtabs{display:flex;gap:4px;margin-bottom:16px;border-bottom:1px solid var(--border);padding-bottom:0}
    .subtab{padding:8px 16px;font-size:13px;font-weight:500;color:var(--text-muted);border-bottom:2px solid transparent;margin-bottom:-1px;cursor:pointer;transition:all .15s}
    .subtab.active{color:var(--primary);border-bottom-color:var(--primary)}
    .sub-tabs{display:flex;gap:4px;margin-bottom:16px;border-bottom:1px solid var(--border)}
    .sub-tab{padding:8px 16px;font-size:13px;font-weight:500;color:var(--text-muted);border-bottom:2px solid transparent;margin-bottom:-1px;cursor:pointer;transition:all .15s;text-decoration:none;display:inline-block}
    .sub-tab:hover{color:var(--text)}
    .sub-tab.active{color:var(--primary);border-bottom-color:var(--primary)}
    .filter-bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px;padding:10px 12px;background:#F9FAFB;border-radius:var(--radius-sm);border:1px solid var(--border)}
    .filter-group{display:flex;gap:4px}
    .filter-btn{padding:4px 10px;border:1px solid var(--border);border-radius:20px;font-size:12px;background:var(--surface);color:var(--text-muted);cursor:pointer;transition:all .15s;font-family:inherit}
    .filter-btn.active{background:var(--primary);border-color:var(--primary);color:#fff}
    select.filter-select{padding:4px 8px;border:1px solid var(--border);border-radius:var(--radius-xs);font-size:12px;background:var(--surface);color:var(--text);cursor:pointer}
    .search-input{padding:6px 12px;border:1px solid var(--border);border-radius:20px;font-size:13px;background:var(--surface);color:var(--text);flex:1;min-width:150px}
    .search-input:focus{outline:none;border-color:var(--primary)}
    .chart-wrap{position:relative;height:200px;margin:12px 0}
    .modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:1000;align-items:center;justify-content:center}
    .modal-overlay.open{display:flex}
    .modal{background:var(--surface);border-radius:var(--radius);box-shadow:var(--shadow-lg);width:90%;max-width:640px;max-height:85vh;overflow-y:auto;padding:20px}
    .modal-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px}
    .modal-title{font-size:16px;font-weight:700}
    .modal-close{background:none;border:none;font-size:20px;color:var(--text-muted);cursor:pointer;padding:4px}
    .modal-close:hover{color:var(--text)}
    .analysis-panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px;margin-top:12px}
    .venue-check-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(80px,1fr));gap:6px;margin:10px 0}
    .venue-check-item{display:flex;align-items:center;gap:4px;font-size:12px;cursor:pointer;padding:4px 6px;border-radius:var(--radius-xs);border:1px solid var(--border);background:var(--surface);transition:all .15s}
    .venue-check-item:hover{border-color:var(--primary);background:var(--primary-light)}
    .venue-check-item input{accent-color:var(--primary)}
    .shimmer{background:linear-gradient(90deg,#f0f0f0 25%,#e0e0e0 50%,#f0f0f0 75%);background-size:200% 100%;animation:shimmer 1.5s infinite;border-radius:4px}
    @keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
    .shimmer-line{height:14px;margin-bottom:8px}
    .shimmer-card{height:80px;margin-bottom:8px;border-radius:var(--radius)}
    .about-body{line-height:1.9;color:#374151}
    .about-body h3{font-size:1rem;margin:22px 0 8px;color:#166534;border-bottom:1px solid #dcfce7;padding-bottom:4px}
    .about-body h3:first-child{margin-top:0}
    .about-body ul{margin:8px 0 12px;padding-left:22px}
    .about-body li{margin-bottom:6px}
    .about-body p{margin:8px 0}
    .about-body table th,.about-body table td{vertical-align:top}
    .fetch-mode-btn{padding:5px 14px;font-size:0.82rem;font-weight:600;border:1px solid var(--border);border-radius:20px;background:var(--surface);color:var(--text-muted);cursor:pointer;transition:.15s;font-family:inherit}
    .fetch-mode-btn.active{background:var(--primary);color:#fff;border-color:var(--primary)}
    .fetch-mode-btn:hover:not(.active){background:var(--primary-light)}
    #fetch-date-log{font-family:monospace;background:#f8fafc;border-radius:6px;padding:6px 10px;border:1px solid var(--border)}
    .result-date-row{display:flex;align-items:center;justify-content:space-between;background:#F9FAFB;border:1px solid var(--border);border-radius:var(--radius-sm);padding:10px 14px;flex-wrap:wrap;gap:8px;margin-bottom:6px}
    .result-date-row .rd-date{font-weight:700;color:var(--text);min-width:90px}
    .result-date-row .rd-stats{display:flex;gap:14px;font-size:0.85rem;color:var(--text);flex-wrap:wrap}
    .result-date-row .rd-roi{font-weight:700;color:var(--primary)}
    .result-date-row .rd-roi.loss{color:var(--danger)}
    .result-date-row .rd-btn{font-size:0.8rem;padding:4px 10px}
    .date-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
    .date-row input[type="date"]{margin-bottom:0;flex:0 0 auto}
    .btn-sm{padding:5px 12px;font-size:12px}
    .venue-badge{font-size:0.7rem;padding:1px 6px;border-radius:4px;margin-left:4px;font-weight:600}
    .venue-badge.jra{background:#DBEAFE;color:#1E40AF}
    .venue-badge.nar{background:#D1FAE5;color:#065F46}
    .select-btns{display:flex;gap:6px;margin-bottom:8px}
    .progress-detail{background:#F9FAFB;border:1px solid var(--border);border-radius:var(--radius-sm);padding:14px 16px;margin-top:12px;display:none}
    .progress-detail.show{display:block}
    .prog-race{font-size:0.9rem;color:var(--primary);font-weight:600;margin-bottom:6px;min-height:1.2em}
    .prog-bar-wrap{height:8px;background:var(--border);border-radius:4px;overflow:hidden;margin:6px 0}
    .prog-bar-fill{height:100%;background:var(--primary);transition:width .4s;border-radius:4px}
    .prog-meta{display:flex;gap:16px;font-size:0.82rem;color:var(--text-muted);margin-top:6px;flex-wrap:wrap}
    .prog-log{font-size:0.82rem;color:var(--text-muted);margin-top:8px;padding:8px;background:var(--surface);border-radius:5px;border:1px solid var(--border);min-height:32px;word-break:break-all}
    .loading-venues{color:var(--text-muted);font-style:italic;font-size:0.9rem;padding:8px 0}
    .analyze-result-link{margin-top:12px;padding:12px 16px;background:var(--primary-light);border-radius:var(--radius-sm);border:1px solid #86efac;display:none}
    .analyze-result-link.show{display:block}
    .analyze-result-link a{color:var(--primary);font-weight:600;text-decoration:none}
    .analyze-result-link a:hover{text-decoration:underline}
    .status{font-size:0.9rem;color:var(--text-muted);margin-top:8px}
    .error{color:var(--danger);margin-top:8px}
    .empty{color:var(--text-muted);font-style:italic}
    .muted{font-size:0.8rem;color:var(--text-muted);margin-left:6px}
    .h-venue-tabs{display:flex;gap:0;background:var(--primary);border-radius:var(--radius-sm) var(--radius-sm) 0 0;overflow-x:auto;flex-wrap:nowrap;scrollbar-width:none}
    .h-venue-tabs::-webkit-scrollbar{display:none}
    .h-vtab{flex:0 0 auto;padding:10px 20px;font-size:13px;font-weight:700;border:none;background:transparent;color:#a7d4b5;cursor:pointer;border-bottom:3px solid transparent;transition:.15s;white-space:nowrap;font-family:inherit}
    .h-vtab:hover{background:#047857;color:#fff}
    .h-vtab.active{background:#fff;color:var(--primary);border-bottom:3px solid #D97706}
    .h-vpanel{display:none;background:var(--surface);border:1px solid var(--border);border-top:none;border-radius:0 0 var(--radius-sm) var(--radius-sm);padding:14px}
    .h-vpanel.active{display:block}
    .vw-bar{font-size:12px;color:var(--text-muted);background:#F9FAFB;border:1px solid var(--border);border-radius:var(--radius-xs);padding:7px 14px;margin-bottom:14px;display:flex;align-items:center;gap:16px}
    .vw-name{font-weight:700;color:var(--primary);font-size:13px}
    .h-race-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:10px}
    .h-race-card{display:block;text-decoration:none;color:inherit;border:1px solid var(--border);border-radius:var(--radius-sm);padding:10px 12px;background:#fafbfc;transition:.15s}
    .h-race-card:hover{border-color:var(--primary);box-shadow:var(--shadow-md);transform:translateY(-2px);background:var(--surface)}
    .h-rc-top{display:flex;align-items:center;gap:5px;margin-bottom:5px}
    .h-rc-no{font-weight:700;font-size:17px;color:#0d2b5e;min-width:32px}
    .h-rc-grade{font-size:10px;font-weight:700;padding:1px 5px;border-radius:3px}
    .h-rc-G1{background:#c0392b;color:#fff}
    .h-rc-G2{background:#2c6dbf;color:#fff}
    .h-rc-G3{background:#27ae60;color:#fff}
    .h-rc-L,.h-rc-OP{background:#e67e22;color:#fff}
    .h-rc-nar{background:#5b21b6;color:#fff;font-size:9px}
    .h-rc-name{font-size:11px;font-weight:700;color:var(--text);margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .h-rc-meta{display:flex;gap:4px;font-size:11px;color:var(--text-muted);align-items:center;flex-wrap:wrap}
    .h-rc-surf-芝{color:#1a7a3a;font-weight:700}
    .h-rc-surf-ダ{color:#8b5e2a;font-weight:700}
    .h-rc-surf-障{color:#7c3aed;font-weight:700}
    .h-rc-time{font-size:11px;color:#2563eb;font-weight:600;margin-top:5px}
    .h-rc-see{font-size:10px;color:var(--primary);font-weight:700;text-align:right;margin-top:4px}
    .h-no-pred{color:var(--text-muted);font-style:italic;padding:24px;text-align:center;background:#F9FAFB;border-radius:var(--radius-sm);border:1px dashed var(--border)}
    .h-rc-axis{display:flex;align-items:center;gap:4px;margin-top:6px;padding:5px 8px;border-radius:var(--radius-xs);background:#f0f4ff;border:1px solid #c5d3eb}
    label{display:block;margin-bottom:4px;font-weight:500}
    .stat{display:flex;gap:16px;flex-wrap:wrap}
    .stat-item{background:var(--primary-light);padding:14px 18px;border-radius:var(--radius-sm);min-width:130px;border:1px solid #bbf7d0}
    .stat-item strong{font-size:1.2rem;color:var(--primary);display:block}
    .stat-item span{font-size:0.85rem;color:#065F46}
    .clock{font-size:2rem;font-weight:700;color:var(--primary);margin:24px 0;font-variant-numeric:tabular-nums}
    input[type="number"]{padding:8px;font-size:1rem;margin-bottom:12px;width:100%;max-width:200px;border:1px solid var(--border);border-radius:var(--radius-xs)}
    .flex{display:flex}.items-center{align-items:center}.gap-2{gap:8px}.gap-3{gap:12px}.flex-1{flex:1}.justify-between{justify-content:space-between}.flex-wrap{flex-wrap:wrap}
    .mt-2{margin-top:8px}.mt-3{margin-top:12px}.mb-2{margin-bottom:8px}.mb-3{margin-bottom:12px}
    .text-sm{font-size:12px}.text-xs{font-size:11px}.text-muted{color:var(--text-muted)}.font-bold{font-weight:700}.font-semibold{font-weight:600}
    .text-primary{color:var(--primary)}.text-danger{color:var(--danger)}.text-warning{color:var(--warning)}
    .hidden{display:none}
    @media(max-width:768px){
      .tab{padding:12px 14px;font-size:13px}
      .tab-panel{padding:12px}
      .wrap{padding:0 12px}
      .stat-grid{grid-template-columns:repeat(2,1fr)}
      .race-card{flex-wrap:wrap}
      .race-card-right{flex-direction:row;min-width:auto;width:100%}
      .filter-bar{padding:8px}
      .modal{width:95%;max-height:90vh}
      .venue-check-grid{grid-template-columns:repeat(auto-fill,minmax(70px,1fr))}
      .hide-mobile{display:none !important}
    }
    @media(max-width:480px){
      .stat-value{font-size:20px}
      .race-card-time .time{font-size:15px}
      .tab{padding:10px 10px;font-size:12px}
    }
  </style>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
</head>
<body>
  <div class="app-header">
    <div class="app-header-title">🏇 D-AI Keiba</div>
    <div class="app-header-sub" id="server-time"></div>
  </div>

  <div class="tabs">
    <a class="tab active" data-tab="today">🏇 今日</a>
    <a class="tab" data-tab="results">📊 成績</a>
    <a class="tab" data-tab="db">🔍 調べる</a>
    <a class="tab" data-tab="about">🤖 D-AIについて</a>
  </div>

    <div id="panel-today" class="tab-panel active">
      <!-- Date navigation -->
      <div class="date-nav">
        <button class="date-nav-btn" onclick="homeChangeDate(-1)">◀</button>
        <span class="date-display" id="home-date-label">—</span>
        <button class="date-nav-btn" onclick="homeChangeDate(+1)">▶</button>
        <span style="font-size:0.85rem;color:var(--text-muted);margin-left:4px" id="home-race-count"></span>
        <div style="display:flex;gap:6px;margin-left:8px;flex-wrap:wrap">
          <button class="btn btn-outline btn-sm" onclick="homeDataUpdate('odds')" id="btn-home-update-odds">⚡ オッズ更新</button>
          <button class="btn btn-ghost btn-sm" onclick="homeDataUpdate('results')" id="btn-home-update-results">📋 結果取得</button>
          <button class="btn btn-ghost btn-sm" onclick="toggleAnalysisPanelNew()">⚙ 分析設定</button>
        </div>
      </div>

      <!-- High confidence picks -->
      <div id="home-high-conf-card" class="card" style="margin-bottom:12px;display:none">
        <div class="card-title">⭐ 自信度SS・S レース <span id="high-conf-count" style="font-weight:400;text-transform:none"></span></div>
        <div id="home-high-conf-list"></div>
      </div>

      <!-- Venue tabs + race panels (existing style preserved) -->
      <div id="home-venue-tabs" class="h-venue-tabs"></div>
      <div id="home-race-panels"></div>
      <div id="home-no-pred" class="h-no-pred" style="display:none">この日の予想データがありません<br><span style="font-size:0.85rem">下の「分析設定」から分析を実行してください</span></div>

      <!-- Analysis panel (collapsible) -->
      <div id="analysis-panel-new" class="analysis-panel" style="display:none;margin-top:16px">
        <div class="card-title">🏟 開催場を選択して分析実行</div>
        <div class="venue-check-grid" id="venue-check-grid-new"></div>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:10px">
          <button class="btn btn-primary" id="btn-analyze-new" onclick="runAnalysisNew()">▶ 分析実行</button>
          <button class="btn btn-ghost btn-sm" onclick="loadVenueCheckboxesNew()">🔄 開催場再読込</button>
        </div>
        <div id="analyze-progress-new" class="progress-detail" style="margin-top:10px">
          <div class="prog-race" id="prog-race-new">準備中...</div>
          <div class="prog-bar-wrap"><div class="prog-bar-fill" id="prog-fill-new" style="width:0%"></div></div>
          <div class="prog-meta">
            <span>進捗: <b id="prog-count-new">0/0</b> レース</span>
            <span>経過: <b id="prog-elapsed-new">0秒</b></span>
            <span>残り推定: <b id="prog-remain-new">—</b></span>
          </div>
          <div class="prog-log" id="prog-log-new">—</div>
          <p class="error" id="analyze-err-new"></p>
        </div>
        <div class="analyze-result-link" id="analyze-result-new">
          ✅ 分析完了！ →
          <a id="analyze-result-link-new" href="#" target="_blank">全レース表示</a>
          <span style="margin:0 6px;color:var(--text-muted)">｜</span>
          <a id="analyze-simple-link-new" href="#" target="_blank">📤 配布用HTML</a>
        </div>
      </div>
      <p id="home-update-status" style="font-size:0.85rem;color:var(--text-muted);margin-top:8px"></p>

      <!-- Share URL section -->
      <div id="home-share-card" class="card" style="margin-top:14px;display:none">
        <div class="card-title">📤 配布用HTML <span id="share-size" style="font-weight:400;text-transform:none"></span></div>
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <input type="text" id="share-url-input" readonly
            style="flex:1;min-width:220px;padding:9px 12px;border:1px solid var(--border);
            border-radius:var(--radius-xs);font-size:12px;background:#f8f9fb;color:var(--text);font-family:monospace">
          <button class="btn btn-primary btn-sm" onclick="copyShareUrl()" id="share-copy-btn">📋 コピー</button>
          <a id="share-open-link" href="#" target="_blank" class="btn btn-outline btn-sm">🔗 開く</a>
        </div>
        <p style="font-size:11px;color:var(--text-muted);margin-top:8px">
          ※ ファイルで送る場合: <code id="share-file-path" style="background:#f3f4f6;padding:1px 5px;border-radius:3px;font-size:11px"></code>
        </p>
      </div>
      <div id="home-share-none" class="card" style="margin-top:14px;display:none;background:#fffbeb;border-color:#fbbf24">
        <div class="card-title" style="color:#92400e">📤 配布用HTML</div>
        <p style="font-size:12px;color:#92400e;margin-top:6px">この日の配布用HTMLがまだありません。「分析設定」から分析を実行すると自動生成されます。</p>
      </div>
    </div>

    <!-- Hidden inputs for legacy JS compatibility -->
    <div style="display:none">
      <input type="date" id="analyze_date" value="{{ today }}">
      <div id="venues-loading"></div>
      <div id="venues-area">
        <div id="venue-check-grid"></div>
      </div>
      <button id="btn_load_venues"></button>
      <button id="btn_all_select"></button>
      <button id="btn_all_clear"></button>
      <button id="btn_analyze" disabled></button>
      <div id="analyze-progress">
        <div id="prog-race"></div>
        <div id="prog-fill" style="width:0%"></div>
        <b id="prog-count"></b>
        <b id="prog-elapsed"></b>
        <b id="prog-remain"></b>
        <div id="prog-log"></div>
        <p id="analyze-err"></p>
      </div>
      <div id="analyze-result">
        <a id="analyze-result-link" href="#" target="_blank"></a>
        <a id="analyze-simple-link" href="#" target="_blank"></a>
      </div>
    </div>

    <!-- Hidden collect panel for legacy JS compatibility -->
    <div style="display:none">
      <strong id="stat-runs">—</strong>
      <strong id="stat-date">—</strong>
      <input type="date" id="start_date" value="{{ default_start }}">
      <input type="date" id="end_date" value="{{ today }}">
      <button id="btn_full"></button>
      <button id="btn_resume"></button>
      <button id="btn_append"></button>
      <button id="btn_run" disabled></button>
      <div id="collect-progress">
        <div class="progress-fill" id="fill" style="width:0%"></div>
        <p id="status"></p>
        <p id="err"></p>
      </div>
    </div>

    <div id="panel-results" class="tab-panel">

      <!-- 年別サブタブ -->
      <div class="sub-tabs">
        <a href="#" class="sub-tab active" data-year="2026">2026年成績</a>
      </div>

      <!-- サマリー stat cards (new design) -->
      <div class="stat-grid" style="margin-bottom:16px">
        <div class="stat-card"><div class="stat-value" id="rs-honmei-tansho-roi">—</div><div class="stat-label">◎単勝回収率</div></div>
        <div class="stat-card"><div class="stat-value" id="rs-roi">—</div><div class="stat-label">買い目回収率</div></div>
        <div class="stat-card"><div class="stat-value" id="rs-honmei-win">—</div><div class="stat-label">◎勝率</div></div>
        <div class="stat-card"><div class="stat-value" id="rs-honmei">—</div><div class="stat-label">◎複勝率</div></div>
        <div class="stat-card"><div class="stat-value" id="rs-races">—</div><div class="stat-label">予想レース数</div></div>
        <div class="stat-card"><div class="stat-value" id="rs-profit">—</div><div class="stat-label">収支（円）</div></div>
      </div>

      <div id="results-panel-content">
        <!-- Legacy hidden IDs for JS compatibility -->
        <div style="display:none">
          <h2 id="rs-panel-title">2026年成績</h2>
          <p id="rs-nodata"></p>
          <div id="rs-stat-row">
            <strong id="rs-hit-rate">—</strong>
            <strong id="rs-honmei-place2">—</strong>
          </div>
        </div>

        <!-- 印別・券種別（横並び） -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
          <!-- 印別成績 -->
          <div class="card">
            <h2 style="margin-bottom:4px">印別成績</h2>
            <p style="font-size:0.78rem;color:#64748b;margin:0 0 8px">印を付けた延べ頭数。小頭数レース（7頭以下）では△☆を省略するため◎より少なくなる。</p>
            <table style="width:100%;border-collapse:collapse;font-size:0.85rem">
              <thead><tr style="background:#eff6ff;color:#1e40af">
                <th style="padding:7px 8px;border:1px solid #bfdbfe;text-align:center">印</th>
                <th style="padding:7px 8px;border:1px solid #bfdbfe;text-align:right">延べ頭数</th>
                <th style="padding:7px 8px;border:1px solid #bfdbfe;text-align:right">勝率</th>
                <th style="padding:7px 8px;border:1px solid #bfdbfe;text-align:right">連対率</th>
                <th style="padding:7px 8px;border:1px solid #bfdbfe;text-align:right">複勝率</th>
                <th style="padding:7px 8px;border:1px solid #bfdbfe;text-align:right">単勝回収率</th>
              </tr></thead>
              <tbody id="results-mark-body"><tr><td colspan="6" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr></tbody>
            </table>
          </div>
          <!-- 券種別成績 -->
          <div class="card">
            <h2 style="margin-bottom:4px">券種別成績</h2>
            <p style="font-size:0.78rem;color:#64748b;margin:0 0 8px">◎or◉から○▲△☆に流しで馬連4点、三連複で流しで6点の計10点×100円購入した際の成績。</p>
            <table style="width:100%;border-collapse:collapse;font-size:0.85rem">
              <thead><tr style="background:#fdf4ff;color:#7e22ce">
                <th style="padding:7px 8px;border:1px solid #e9d5ff;text-align:center">券種</th>
                <th style="padding:7px 8px;border:1px solid #e9d5ff;text-align:right">レース数</th>
                <th style="padding:7px 8px;border:1px solid #e9d5ff;text-align:right">的中率</th>
                <th style="padding:7px 8px;border:1px solid #e9d5ff;text-align:right">回収率</th>
              </tr></thead>
              <tbody id="results-ticket-body"><tr><td colspan="4" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr></tbody>
            </table>
          </div>
        </div>

        <!-- 自信度×券種別 -->
        <div class="card">
          <h2 style="margin-bottom:10px">自信度×券種別成績</h2>
          <table style="width:100%;border-collapse:collapse;font-size:0.85rem">
            <thead><tr style="background:#dcfce7;color:#166534">
              <th style="padding:7px 8px;border:1px solid #bbf7d0;text-align:center">自信度</th>
              <th style="padding:7px 8px;border:1px solid #bbf7d0;text-align:center">券種</th>
              <th style="padding:7px 8px;border:1px solid #bbf7d0;text-align:right">レース数</th>
              <th style="padding:7px 8px;border:1px solid #bbf7d0;text-align:right">的中R</th>
              <th style="padding:7px 8px;border:1px solid #bbf7d0;text-align:right">的中率</th>
              <th style="padding:7px 8px;border:1px solid #bbf7d0;text-align:right">投資(円)</th>
              <th style="padding:7px 8px;border:1px solid #bbf7d0;text-align:right">回収(円)</th>
              <th style="padding:7px 8px;border:1px solid #bbf7d0;text-align:right">回収率</th>
            </tr></thead>
            <tbody id="results-conf-body"><tr><td colspan="8" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr></tbody>
          </table>
        </div>

        <!-- 穴馬・危険馬成績（横並び） -->
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
          <!-- 穴馬成績 -->
          <div class="card">
            <h2 style="margin-bottom:10px">穴馬成績</h2>
            <p style="font-size:0.78rem;color:#64748b;margin:0 0 8px">穴馬に選定された無印馬の実績（複勝100円想定）</p>
            <table style="width:100%;border-collapse:collapse;font-size:0.85rem">
              <thead><tr style="background:#fef3c7;color:#92400e">
                <th style="padding:7px 8px;border:1px solid #fde68a;text-align:center">区分</th>
                <th style="padding:7px 8px;border:1px solid #fde68a;text-align:right">頭数</th>
                <th style="padding:7px 8px;border:1px solid #fde68a;text-align:right">馬券内率</th>
                <th style="padding:7px 8px;border:1px solid #fde68a;text-align:right">複勝回収率</th>
              </tr></thead>
              <tbody id="results-ana-body"><tr><td colspan="4" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr></tbody>
            </table>
          </div>
          <!-- 危険馬成績 -->
          <div class="card">
            <h2 style="margin-bottom:10px">危険馬成績</h2>
            <p style="font-size:0.78rem;color:#64748b;margin:0 0 8px">危険馬フラグが付いた馬の着外率（4着以下）</p>
            <table style="width:100%;border-collapse:collapse;font-size:0.85rem">
              <thead><tr style="background:#fee2e2;color:#991b1b">
                <th style="padding:7px 8px;border:1px solid #fca5a5;text-align:center">区分</th>
                <th style="padding:7px 8px;border:1px solid #fca5a5;text-align:right">頭数</th>
                <th style="padding:7px 8px;border:1px solid #fca5a5;text-align:right">馬券外率</th>
                <th style="padding:7px 8px;border:1px solid #fca5a5;text-align:right">馬券外頭数</th>
              </tr></thead>
              <tbody id="results-kiken-body"><tr><td colspan="4" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr></tbody>
            </table>
          </div>
        </div>

        <!-- 日付別一覧 -->
        <div class="card">
          <h2 style="margin-bottom:10px">日付別成績</h2>
          <div id="results-date-list" style="display:flex;flex-direction:column;gap:6px">
            <p style="color:#9ca3af;font-style:italic;font-size:0.9rem">データなし</p>
          </div>
        </div>
      </div>

      <!-- ============================================ -->
      <!-- 詳細分析セクション（全体/JRA/NAR タブ）      -->
      <!-- ============================================ -->
      <div class="card" style="margin-top:16px">
        <h2 style="margin-bottom:12px">詳細分析（競馬場別・コース別・距離区分別）</h2>

        <!-- 全体/JRA/NAR タブ -->
        <div id="detail-cat-tabs" style="display:flex;gap:0;border-bottom:2px solid #e5e7eb;margin-bottom:16px">
          <button class="det-tab active" data-cat="all"  style="padding:8px 20px;border:none;background:none;font-size:0.9rem;font-weight:600;cursor:pointer;border-bottom:3px solid #2563eb;color:#2563eb;margin-bottom:-2px">全体</button>
          <button class="det-tab"        data-cat="jra"  style="padding:8px 20px;border:none;background:none;font-size:0.9rem;font-weight:600;cursor:pointer;color:#6b7280">中央競馬(JRA)</button>
          <button class="det-tab"        data-cat="nar"  style="padding:8px 20px;border:none;background:none;font-size:0.9rem;font-weight:600;cursor:pointer;color:#6b7280">地方競馬(NAR)</button>
        </div>

        <!-- 各カテゴリのコンテンツ -->
        <div id="det-content">
          <div id="det-loading" style="text-align:center;padding:24px;color:#9ca3af">読み込み中...</div>

          <!-- サマリー行 -->
          <div id="det-summary" style="display:none;background:#f8fafc;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:0.88rem;display:flex;gap:20px;flex-wrap:wrap">
            <span>レース数: <strong id="det-races">—</strong></span>
            <span>的中率: <strong id="det-hit-rate">—</strong></span>
            <span>回収率: <strong id="det-roi">—</strong></span>
            <span>馬連的中率: <strong id="det-u-hit">—</strong> / 回収率: <strong id="det-u-roi">—</strong></span>
            <span>三連複的中率: <strong id="det-s-hit">—</strong> / 回収率: <strong id="det-s-roi">—</strong></span>
          </div>

          <!-- 競馬場別 / 芝ダート / 距離区分 を3カラム -->
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px">

            <!-- 競馬場別 -->
            <div>
              <div style="font-weight:700;font-size:0.88rem;color:#1e40af;margin-bottom:6px;padding:5px 8px;background:#eff6ff;border-radius:4px">競馬場別</div>
              <table style="width:100%;border-collapse:collapse;font-size:0.78rem">
                <thead><tr style="background:#f1f5f9;color:#475569">
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:left">競馬場</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">R数</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">馬連<br>的中</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">馬連<br>回収</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">三連複<br>的中</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">三連複<br>回収</th>
                </tr></thead>
                <tbody id="det-venue-body"><tr><td colspan="6" style="text-align:center;padding:8px;color:#9ca3af">データなし</td></tr></tbody>
              </table>
            </div>

            <!-- 芝/ダート -->
            <div>
              <div style="font-weight:700;font-size:0.88rem;color:#166534;margin-bottom:6px;padding:5px 8px;background:#dcfce7;border-radius:4px">コース種別（芝・ダート）</div>
              <table style="width:100%;border-collapse:collapse;font-size:0.78rem">
                <thead><tr style="background:#f1f5f9;color:#475569">
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:left">種別</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">R数</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">馬連<br>的中</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">馬連<br>回収</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">三連複<br>的中</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">三連複<br>回収</th>
                </tr></thead>
                <tbody id="det-surface-body"><tr><td colspan="6" style="text-align:center;padding:8px;color:#9ca3af">データなし</td></tr></tbody>
              </table>
              <!-- 距離区分 -->
              <div style="font-weight:700;font-size:0.88rem;color:#7e22ce;margin:12px 0 6px;padding:5px 8px;background:#fdf4ff;border-radius:4px">距離区分（SS/S/M/I/L/E）</div>
              <table style="width:100%;border-collapse:collapse;font-size:0.78rem">
                <thead><tr style="background:#f1f5f9;color:#475569">
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:left">区分</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">R数</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">馬連<br>的中</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">馬連<br>回収</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">三連複<br>的中</th>
                  <th style="padding:5px 6px;border:1px solid #e2e8f0;text-align:right">三連複<br>回収</th>
                </tr></thead>
                <tbody id="det-dist-body"><tr><td colspan="6" style="text-align:center;padding:8px;color:#9ca3af">データなし</td></tr></tbody>
              </table>
            </div>

            <!-- TOP10 高額配当 -->
            <div>
              <div style="font-weight:700;font-size:0.88rem;color:#92400e;margin-bottom:6px;padding:5px 8px;background:#fef3c7;border-radius:4px">高額配当 馬連 TOP10</div>
              <div id="det-top10-umaren" style="font-size:0.76rem"></div>
              <div style="font-weight:700;font-size:0.88rem;color:#7c3aed;margin:12px 0 6px;padding:5px 8px;background:#ede9fe;border-radius:4px">高額配当 三連複 TOP10</div>
              <div id="det-top10-sanrenpuku" style="font-size:0.76rem"></div>
            </div>

          </div>
        </div>
      </div>

      <!-- 配布用HTML一括生成エリア -->
      <div class="card">
        <h2>配布用HTML生成（印・買い目のみ）</h2>
        <p style="font-size:0.82rem;color:#64748b;margin:0 0 10px">過去の予想データから、印と買い目だけの軽量HTMLを生成します。LINEなどでのシェア用です。</p>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px">
          <span style="font-size:0.82rem;color:#374151;font-weight:600">期間:</span>
          <input type="date" id="simple-from-date" style="padding:8px;border:1px solid #bbf7d0;border-radius:6px;font-size:0.92rem;width:auto;margin:0">
          <span style="font-size:0.9rem;color:#374151;font-weight:700">〜</span>
          <input type="date" id="simple-to-date" style="padding:8px;border:1px solid #bbf7d0;border-radius:6px;font-size:0.92rem;width:auto;margin:0">
          <button id="btn-simple-all" class="btn-sm" style="background:#f0fdf4;border:1px solid #86efac;color:#166534;border-radius:6px;padding:7px 12px;cursor:pointer;font-size:0.85rem;font-weight:600">全期間</button>
          <button id="btn-gen-simple" class="primary btn-sm">配布用HTML生成</button>
        </div>
        <div id="simple-progress" style="display:none;margin-top:8px">
          <div class="prog-bar-wrap"><div class="prog-bar-fill" id="simple-fill" style="width:0%"></div></div>
          <p class="status" id="simple-status" style="font-size:0.85rem;margin-top:6px"></p>
          <div id="simple-date-log" style="display:none;margin-top:8px;max-height:130px;overflow-y:auto"></div>
        </div>
        <p class="error" id="simple-err" style="margin-top:6px"></p>
      </div>

      <!-- 結果照合エリア -->
      <div class="card">
        <h2>結果照合（着順取得）</h2>
        <p style="font-size:0.82rem;color:#64748b;margin:0 0 10px">予想した日付の着順をネットケイバから取得し、的中・収支を自動計算します。レース終了後に実行してください。</p>
        <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:10px">
          <span style="font-size:0.82rem;color:#374151;font-weight:600">照合モード:</span>
          <button class="fetch-mode-btn active" id="fetch-mode-single" onclick="setFetchMode('single')">単一日付</button>
          <button class="fetch-mode-btn hide-mobile" id="fetch-mode-range" onclick="setFetchMode('range')">期間指定</button>
        </div>
        <!-- 単一日付モード -->
        <div id="fetch-single-area" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <select id="fetch-date-select" style="padding:8px;border:1px solid #bbf7d0;border-radius:6px;font-size:0.95rem">
            <option value="">-- 日付を選択 --</option>
          </select>
          <button id="btn-fetch-results" class="primary btn-sm">着順を取得して照合</button>
        </div>
        <!-- 期間指定モード -->
        <div id="fetch-range-area" style="display:none;gap:8px;align-items:center;flex-wrap:wrap">
          <input type="date" id="fetch-from-date" style="padding:8px;border:1px solid #bbf7d0;border-radius:6px;font-size:0.92rem;width:auto;margin:0">
          <span style="font-size:0.9rem;color:#374151;font-weight:700">〜</span>
          <input type="date" id="fetch-to-date" style="padding:8px;border:1px solid #bbf7d0;border-radius:6px;font-size:0.92rem;width:auto;margin:0">
          <button id="btn-fetch-unmatched" class="btn-sm" style="background:#f0fdf4;border:1px solid #86efac;color:#166534;border-radius:6px;padding:7px 12px;cursor:pointer;font-size:0.85rem;font-weight:600">未照合のみ</button>
          <button id="btn-fetch-range" class="primary btn-sm">範囲一括照合</button>
        </div>
        <div id="fetch-progress" style="display:none;margin-top:12px">
          <div class="prog-bar-wrap"><div class="prog-bar-fill" id="fetch-fill" style="width:0%"></div></div>
          <p class="status" id="fetch-status" style="font-size:0.85rem;margin-top:6px"></p>
          <div id="fetch-date-log" style="display:none;margin-top:8px;max-height:130px;overflow-y:auto"></div>
        </div>
        <p class="error" id="fetch-err" style="margin-top:6px"></p>
      </div>

      <!-- Chart: 累積回収率推移 -->
      <div class="card" style="margin-top:16px">
        <div class="card-title">累積回収率推移</div>
        <div class="chart-wrap"><canvas id="roi-chart"></canvas></div>
      </div>

      <!-- Chart: 月別収支 -->
      <div class="card" style="margin-top:12px">
        <div class="card-title">月別収支（円）</div>
        <div class="chart-wrap"><canvas id="monthly-chart"></canvas></div>
      </div>
    </div>

    <!-- Hidden legacy panel-past IDs -->
    <div style="display:none">
      <input type="date" id="past-create-date">
      <button id="btn-past-single"></button>
      <input type="date" id="past-range-start">
      <input type="date" id="past-range-end">
      <button id="btn-past-range"></button>
      <div id="past-create-status">
        <span id="past-create-progress"></span>
        <span id="past-create-elapsed"></span>
      </div>
      <input type="date" id="past-date-input">
      <span id="past-loading"></span>
      <div id="past-result"></div>
    </div>

    <!-- データベース パネル (redesigned) -->
    <div id="panel-db" class="tab-panel">
      <!-- Search bar -->
      <div class="filter-bar" style="margin-bottom:12px">
        <input type="text" class="search-input" id="db-search" placeholder="🔍 騎手・調教師名で検索..." oninput="onDbSearch()">
      </div>

      <!-- Subtabs -->
      <div class="subtabs" id="db-subtabs">
        <span class="subtab active" onclick="switchDbTab(this,'jockey')">騎手</span>
        <span class="subtab" onclick="switchDbTab(this,'trainer')">調教師</span>
        <span class="subtab" onclick="switchDbTab(this,'course')">コース</span>
      </div>

      <!-- Jockey panel -->
      <div id="db-panel-jockey">
        <div class="filter-bar">
          <div class="filter-group">
            <button class="filter-btn active" id="db-j-jn-all" onclick="dbSetJraNar('jockey','')">全体</button>
            <button class="filter-btn" id="db-j-jn-jra" onclick="dbSetJraNar('jockey','JRA')">JRA</button>
            <button class="filter-btn" id="db-j-jn-nar" onclick="dbSetJraNar('jockey','NAR')">NAR</button>
          </div>
          <div class="filter-group">
            <button class="filter-btn active" id="db-j-sf-all" onclick="dbSetSurface('jockey','')">総合</button>
            <button class="filter-btn" id="db-j-sf-t" onclick="dbSetSurface('jockey','芝')">芝</button>
            <button class="filter-btn" id="db-j-sf-d" onclick="dbSetSurface('jockey','ダート')">ダート</button>
          </div>
          <div class="filter-group">
            <button class="filter-btn active" id="db-j-sm-all" onclick="dbSetSmile('jockey','')">全</button>
            <button class="filter-btn" id="db-j-sm-SS" onclick="dbSetSmile('jockey','SS')">SS</button>
            <button class="filter-btn" id="db-j-sm-S" onclick="dbSetSmile('jockey','S')">S</button>
            <button class="filter-btn" id="db-j-sm-M" onclick="dbSetSmile('jockey','M')">M</button>
            <button class="filter-btn" id="db-j-sm-I" onclick="dbSetSmile('jockey','I')">I</button>
            <button class="filter-btn" id="db-j-sm-L" onclick="dbSetSmile('jockey','L')">L</button>
            <button class="filter-btn" id="db-j-sm-E" onclick="dbSetSmile('jockey','E')">E</button>
          </div>
          <select class="filter-select" id="db-jockey-sort" onchange="loadJockeyDB()">
            <option value="total">出走数順</option>
            <option value="wins">勝利数順</option>
            <option value="win_rate">勝率順</option>
            <option value="place2_rate">連対率順</option>
            <option value="place3_rate">複勝率順</option>
            <option value="roi">回収率順</option>
            <option value="dev">偏差値順</option>
          </select>
        </div>
        <div style="font-size:0.75rem;color:#f59e0b;background:#fffbeb;border:1px solid #fde68a;border-radius:4px;padding:4px 10px;margin-bottom:8px">
          ⚠ 集計対象：予想レース結果（全馬・着外含む）。行クリックで詳細。
        </div>
        <div id="db-jockey-table" style="overflow-x:auto">
          <p class="empty">読み込み中...</p>
        </div>
        <!-- legacy compat -->
        <span id="db-jockey-filter-label" style="display:none"></span>
        <input type="text" id="db-jockey-search" style="display:none">
      </div>

      <!-- Trainer panel -->
      <div id="db-panel-trainer" style="display:none">
        <div class="filter-bar">
          <div class="filter-group">
            <button class="filter-btn active" id="db-t-jn-all" onclick="dbSetJraNar('trainer','')">全体</button>
            <button class="filter-btn" id="db-t-jn-jra" onclick="dbSetJraNar('trainer','JRA')">JRA</button>
            <button class="filter-btn" id="db-t-jn-nar" onclick="dbSetJraNar('trainer','NAR')">NAR</button>
          </div>
          <div class="filter-group">
            <button class="filter-btn active" id="db-t-sf-all" onclick="dbSetSurface('trainer','')">総合</button>
            <button class="filter-btn" id="db-t-sf-t" onclick="dbSetSurface('trainer','芝')">芝</button>
            <button class="filter-btn" id="db-t-sf-d" onclick="dbSetSurface('trainer','ダート')">ダート</button>
          </div>
          <div class="filter-group">
            <button class="filter-btn active" id="db-t-sm-all" onclick="dbSetSmile('trainer','')">全</button>
            <button class="filter-btn" id="db-t-sm-SS" onclick="dbSetSmile('trainer','SS')">SS</button>
            <button class="filter-btn" id="db-t-sm-S" onclick="dbSetSmile('trainer','S')">S</button>
            <button class="filter-btn" id="db-t-sm-M" onclick="dbSetSmile('trainer','M')">M</button>
            <button class="filter-btn" id="db-t-sm-I" onclick="dbSetSmile('trainer','I')">I</button>
            <button class="filter-btn" id="db-t-sm-L" onclick="dbSetSmile('trainer','L')">L</button>
            <button class="filter-btn" id="db-t-sm-E" onclick="dbSetSmile('trainer','E')">E</button>
          </div>
          <select class="filter-select" id="db-trainer-sort" onchange="loadTrainerDB()">
            <option value="total">出走数順</option>
            <option value="wins">勝利数順</option>
            <option value="win_rate">勝率順</option>
            <option value="place2_rate">連対率順</option>
            <option value="place3_rate">複勝率順</option>
            <option value="roi">回収率順</option>
            <option value="dev">偏差値順</option>
          </select>
        </div>
        <div style="font-size:0.75rem;color:#f59e0b;background:#fffbeb;border:1px solid #fde68a;border-radius:4px;padding:4px 10px;margin-bottom:8px">
          ⚠ 集計対象：予想レース結果（全馬・着外含む）。行クリックで詳細。
        </div>
        <div id="db-trainer-table" style="overflow-x:auto">
          <p class="empty">読み込み中...</p>
        </div>
        <!-- legacy compat -->
        <span id="db-trainer-filter-label" style="display:none"></span>
        <input type="text" id="db-trainer-search" style="display:none">
      </div>

      <!-- 詳細モーダル -->
      <div id="db-detail-modal" style="display:none;position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,0.45);overflow-y:auto">
        <div style="background:#fff;max-width:720px;margin:40px auto;border-radius:var(--radius);padding:24px;position:relative;box-shadow:var(--shadow-lg)">
          <button onclick="document.getElementById('db-detail-modal').style.display='none'"
            style="position:absolute;top:12px;right:14px;background:none;border:none;font-size:1.3rem;cursor:pointer;color:var(--text-muted)">✕</button>
          <h3 id="db-detail-title" style="margin:0 0 4px;color:var(--primary)"></h3>
          <p id="db-detail-period" style="margin:0 0 12px;font-size:12px;color:var(--text-muted)"></p>
          <div id="db-detail-summary" style="margin-bottom:16px"></div>
          <div id="db-detail-devs" style="margin-bottom:16px"></div>
          <div id="db-detail-venue" style="overflow-x:auto;margin-bottom:16px"></div>
          <div id="db-detail-running-style" style="overflow-x:auto;margin-bottom:16px"></div>
          <div id="db-detail-dist" style="overflow-x:auto"></div>
        </div>
      </div>

      <!-- Course panel -->
      <div id="db-panel-course" style="display:none">
        <div class="filter-bar">
          <div class="filter-group">
            <button class="filter-btn active" id="btn-course-jra" onclick="courseSetRegion('JRA')">JRA（10場）</button>
            <button class="filter-btn" id="btn-course-nar" onclick="courseSetRegion('NAR')">NAR（16場）</button>
          </div>
          <span id="course-region-label" style="font-size:0.8rem;color:var(--text-muted);align-self:center;margin-left:8px">競馬場を選択してください</span>
        </div>
        <div id="course-venue-cards" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px"></div>
        <div id="course-course-list" style="margin-top:16px"></div>
      </div>
    </div>

    <div id="panel-about" class="tab-panel">
      <div class="card">
        <div class="about-body">

          <h3>処理パイプライン全体像</h3>
          <p>D-AIは1レースあたり以下の10ステップを順番に実行する。オッズは Step 1〜7 には一切渡さない。オッズを参照するのは Step 8（乖離検出）と Step 9（期待値算出）のみ。これにより「市場の歪み」を検出することが設計上可能になっている。</p>
          <table style="width:100%;border-collapse:collapse;font-size:0.83rem;margin:8px 0">
            <thead><tr style="background:#dcfce7;color:#166534">
              <th style="padding:6px 8px;border:1px solid #bbf7d0;white-space:nowrap">Step</th>
              <th style="padding:6px 8px;border:1px solid #bbf7d0">処理</th>
              <th style="padding:6px 8px;border:1px solid #bbf7d0">担当クラス / 関数</th>
            </tr></thead>
            <tbody>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">1</td><td style="padding:6px 8px;border:1px solid #e2e8f0">ペース予測 (5段階 HH〜SS)</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">PacePredictor.predict_pace()</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">2</td><td style="padding:6px 8px;border:1px solid #e2e8f0">換算定数 k の動的校正</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">calibrate_conversion_constant()</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">3</td><td style="padding:6px 8px;border:1px solid #e2e8f0">各馬の能力偏差値 (A〜E章)</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">calc_ability_deviation()</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">4</td><td style="padding:6px 8px;border:1px solid #e2e8f0">展開偏差値 (F章) + ML位置取り</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">PaceDeviationCalculator.calc()</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">5</td><td style="padding:6px 8px;border:1px solid #e2e8f0">コース適性偏差値 (G章) + 枠順バイアス</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">CourseAptitudeCalculator.calc()</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">6</td><td style="padding:6px 8px;border:1px solid #e2e8f0">騎手・厩舎・調教評価 (H〜J章)</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">JockeyChangeEvaluator / calc_shobu_score()</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">7</td><td style="padding:6px 8px;border:1px solid #e2e8f0">総合偏差値 (D指数) 集計・場内正規化</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">get_composite_weights() / _normalize_field_deviations()</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">8</td><td style="padding:6px 8px;border:1px solid #e2e8f0">ML三連率推定 + 穴馬・危険馬検知 (I章)</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">ProbabilityPredictor / calc_ana_score()</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">9</td><td style="padding:6px 8px;border:1px solid #e2e8f0">印付け → 買い目生成 → 期待値算出</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">assign_marks() / generate_tickets() / calc_expected_value()</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;text-align:center">10</td><td style="padding:6px 8px;border:1px solid #e2e8f0">乖離検出・バリューベット + 資金配分</td><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.8rem">detect_value_bets() / allocate_stakes()</td></tr>
            </tbody>
          </table>

          <h3>基準タイムDBと馬場補正（A〜B章）</h3>
          <p>基準タイムの計算式は以下。対象は同コース1〜3着走のみ。信頼度は蓄積件数に依存し、30件以上でA、10〜29件でB、3〜9件でCとなる。3件未満の場合は同競馬場・同馬場種別の全距離データで回帰的に代替する。</p>
          <div style="background:#f8fafc;padding:10px 14px;border-radius:6px;border-left:3px solid #16a34a;font-family:monospace;font-size:0.82rem;line-height:1.8">
            dist_coeff = 1600 / distance_m<br>
            score_total = 馬場条件スコア + クラススコア + 芝/ダスコア + 頭数スコア + 性別スコア + 季節スコア<br>
            standard_time = mean(1〜3着タイム) − (mean(score_total) × dist_coeff)<br>
            run_deviation = 50 + (standard_time − corrected_time) × dist_coeff × k
          </div>
          <p style="margin-top:8px">各補正スコアの実値：</p>
          <table style="width:100%;border-collapse:collapse;font-size:0.82rem;margin:4px 0">
            <thead><tr style="background:#f1f5f9"><th style="padding:5px 8px;border:1px solid #e2e8f0;text-align:left">カテゴリ</th><th style="padding:5px 8px;border:1px solid #e2e8f0;text-align:left">値</th></tr></thead>
            <tbody>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">馬場状態</td><td style="padding:5px 8px;border:1px solid #e2e8f0">良=0 / 稍重=−0.5 / 重=−1.0 / 不良=−1.5</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">クラス</td><td style="padding:5px 8px;border:1px solid #e2e8f0">G1=+6 / G2=+5 / G3=+4 / OP=+3 / 3勝=+2 / 2勝=+1 / 1勝=0 / 未勝利=−1 / 新馬=−2</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">芝/ダート</td><td style="padding:5px 8px;border:1px solid #e2e8f0">芝=0 / ダート=−0.5</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">頭数</td><td style="padding:5px 8px;border:1px solid #e2e8f0">〜11頭=0 / 12〜15頭=+0.1 / 16頭以上=+0.3</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">季節（月別）</td><td style="padding:5px 8px;border:1px solid #e2e8f0">4月=+0.8 / 6月=+0.7 / 10月=+0.6 / 7月=−0.6 / 12月=−0.6（実データ検証値）</td></tr>
            </tbody>
          </table>
          <p style="margin-top:8px">馬場補正はCV値・含水率は使わず、実データ実測値（秒/200m）をテーブルで管理する。距離帯（short/mile/mid/long）と主催（JRA/NAR）の組み合わせ別に保持。代表値：</p>
          <table style="width:100%;border-collapse:collapse;font-size:0.82rem;margin:4px 0">
            <thead><tr style="background:#f1f5f9"><th style="padding:5px 8px;border:1px solid #e2e8f0">馬場</th><th style="padding:5px 8px;border:1px solid #e2e8f0">JRA芝 mile(補正 s/200m)</th><th style="padding:5px 8px;border:1px solid #e2e8f0">JRAダート mile</th></tr></thead>
            <tbody>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">稍重</td><td style="padding:5px 8px;border:1px solid #e2e8f0">+0.126（遅い）</td><td style="padding:5px 8px;border:1px solid #e2e8f0">−0.072（速い）</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">重</td><td style="padding:5px 8px;border:1px solid #e2e8f0">+0.262</td><td style="padding:5px 8px;border:1px solid #e2e8f0">−0.243</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">不良</td><td style="padding:5px 8px;border:1px solid #e2e8f0">+0.434</td><td style="padding:5px 8px;border:1px solid #e2e8f0">−0.229</td></tr>
            </tbody>
          </table>
          <p style="margin-top:6px">競馬場別時計レベルも補正テーブルで持つ（東京芝=−0.073 s/200m で最速、新潟=+0.047 で最遅）。改修後コースのみ使う <code>filter_post_renovation_runs()</code> が適用されるため、京都改修前（〜2022年）のデータは除外される。</p>

          <h3>加重平均偏差値とα可変ロジック（C〜E章）</h3>
          <p>過去走の参照範囲：同系統（芝/ダ）かつ直近1年以内・最大5走。長期休養明け（直前走から90日以上）の場合は参照範囲を2年まで拡張し、算出した加重平均に対して以下の減衰をかける：</p>
          <div style="background:#f8fafc;padding:8px 14px;border-radius:6px;border-left:3px solid #16a34a;font-family:monospace;font-size:0.82rem;line-height:1.8">
            WA_WEIGHTS = [0.35, 0.25, 0.20, 0.12, 0.08]  # 最新走→5走前<br>
            corrected_dev[i] = run_deviation[i] + chakusa_index[i] × CHAKUSA_INDEX_WEIGHT<br>
            wa_dev = Σ(corrected_dev[i] × weight[i]) / Σweight[i]<br>
            if is_long_break: wa_dev = 50 + (wa_dev − 50) × 0.5  # RACE_HISTORY_休養DECAY
          </div>
          <p style="margin-top:8px">最新走偏差値（max_dev）と加重平均偏差値（wa_dev）の差が大きい場合、α値を動的に調整してピーク方向にバイアスをかける：</p>
          <div style="background:#f8fafc;padding:8px 14px;border-radius:6px;border-left:3px solid #16a34a;font-family:monospace;font-size:0.82rem;line-height:1.8">
            divergence = |max_dev − wa_dev|<br>
            if divergence &gt; threshold×2: α ± 0.15<br>
            if is_declining: α −= ALPHA_DECLINE_PENALTY<br>
            ability_score = wa_dev × (1 − α) + max_dev × α  # α ∈ [0.1, 0.9]
          </div>
          <p style="margin-top:8px">トレンド判定（E-1）は偏差値傾き（60%）と着順傾向（40%）の複合スコアで決まり、G1/G2直近1着に+4pt・3着以内に+2ptのボーナスが乗る。換算定数 k はレースごとに <code>calibrate_conversion_constant()</code> で動的校正（課題DBの蓄積が30件以上の場合のみ適用）。</p>

          <h3>ペース予測と展開偏差値（F章）</h3>
          <p>PacePredictorはベーススコア50（MM相当）から出発し、以下の加算で5段階に分類する：</p>
          <table style="width:100%;border-collapse:collapse;font-size:0.82rem;margin:4px 0 8px">
            <thead><tr style="background:#f1f5f9"><th style="padding:5px 8px;border:1px solid #e2e8f0">要因</th><th style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">調整値</th></tr></thead>
            <tbody>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">逃げ馬3頭以上</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+8</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">逃げ馬2頭</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+4</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">逃げ馬0頭</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−4</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">先行馬密度≥50%</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+5</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">先行馬密度≤15%</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−3</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">直線≥400m（長い）</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−2</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">小回りコース</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+3</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">逃げ馬の過去前半3F平均&lt;35.5s</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+2</td></tr>
            </tbody>
          </table>
          <p>スコア閾値：HH≥62 / HM≥56 / MM≥44 / MS≥38 / SS&lt;38。脚質はコーナー通過順位（StyleClassifier）で分類、コーナーデータ不足時は4角相対位置（≤15%→逃げ、≤35%→先行、≤60%→差し）で代替。末脚タイプは上がり3Fを平均より0.5s速ければ爆発型、0.5s遅ければ末脚非依存型に分類し位置取り×末脚の9分類+マクリとする。展開偏差値の計算ではML位置取りモデル（PositionPredictor）による予測も利用する。枠順バイアスは gate_bias_db（枠別過去成績）から取得。</p>

          <h3>騎手・厩舎・調教評価（H〜J章）</h3>
          <p>乗り替わりは6パターンに分類し、展開偏差値と勝負気配スコアに加算する：</p>
          <table style="width:100%;border-collapse:collapse;font-size:0.82rem;margin:4px 0 8px">
            <thead><tr style="background:#f1f5f9"><th style="padding:5px 8px;border:1px solid #e2e8f0">パターン</th><th style="padding:5px 8px;border:1px solid #e2e8f0">判定基準</th><th style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">展開影響</th><th style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">勝負気配</th></tr></thead>
            <tbody>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">A: 戦略的強化</td><td style="padding:5px 8px;border:1px solid #e2e8f0">上位騎手（偏差値≥60）への乗替</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+1.5</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+2.0</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">B: 戦術的</td><td style="padding:5px 8px;border:1px solid #e2e8f0">コース得意騎手等</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+0.5</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">C: ローテ都合</td><td style="padding:5px 8px;border:1px solid #e2e8f0">騎手スケジュール調整</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">0</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">D: 調教目的</td><td style="padding:5px 8px;border:1px solid #e2e8f0">若手・調教専任等</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−0.5</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">E: 見切り</td><td style="padding:5px 8px;border:1px solid #e2e8f0">前走大敗後の降格乗替</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−2.0</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">F: 事情不明</td><td style="padding:5px 8px;border:1px solid #e2e8f0">判定不可</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">0</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">−</td></tr>
            </tbody>
          </table>
          <p>テン乗り（騎手×馬の初コンビ）には−1.0のペナルティを追加。勝負気配スコアは以下の合算：格上げ出走+1.5 / 厩舎短期好調+1.5 / 休み明け高回収率厩舎+1.5 / 休養日数帯×厩舎初戦型補正（<code>calc_break_adjustment()</code>）。スコア≥4で「勝負気配」フラグが立つ。</p>

          <h3>総合偏差値（D指数）と競馬場別重み</h3>
          <p>D指数は競馬場別の最適重みで3軸を合成し、体重変動補正とオッズ整合性スコアを加算する：</p>
          <div style="background:#f8fafc;padding:8px 14px;border-radius:6px;border-left:3px solid #16a34a;font-family:monospace;font-size:0.82rem;line-height:1.8">
            w = get_composite_weights(venue)  # 競馬場別の能力/展開/適性の重み<br>
            base = ability × w["ability"] + pace × w["pace"] + course × w["course"]<br>
            base += calc_weight_change_adjustment(weight_change, horse_weight)<br>
            composite = base + odds_consistency_adj  # 市場オッズとの整合性補正
          </div>
          <p style="margin-top:8px">場内正規化（<code>_normalize_field_deviations()</code>）により展開・能力の各偏差値はレース内で 50 中心に補正される。印付け・買い目生成はこの正規化後の composite を使う。代表的な競馬場別重み：</p>
          <table style="width:100%;border-collapse:collapse;font-size:0.82rem;margin:4px 0">
            <thead><tr style="background:#dcfce7;color:#166534">
              <th style="padding:6px 8px;border:1px solid #bbf7d0">競馬場</th>
              <th style="padding:6px 8px;border:1px solid #bbf7d0;text-align:center">能力</th>
              <th style="padding:6px 8px;border:1px solid #bbf7d0;text-align:center">展開</th>
              <th style="padding:6px 8px;border:1px solid #bbf7d0;text-align:center">適性</th>
              <th style="padding:6px 8px;border:1px solid #bbf7d0">特徴</th>
            </tr></thead>
            <tbody>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0">東京</td><td style="padding:6px 8px;text-align:center;border:1px solid #e2e8f0">61.4%</td><td style="padding:6px 8px;text-align:center;border:1px solid #e2e8f0">12.5%</td><td style="padding:6px 8px;text-align:center;border:1px solid #e2e8f0">26.0%</td><td style="padding:6px 8px;border:1px solid #e2e8f0">長い直線で展開影響少、純粋な能力差が出る</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0">札幌</td><td style="padding:6px 8px;text-align:center;border:1px solid #e2e8f0">66.1%</td><td style="padding:6px 8px;text-align:center;border:1px solid #e2e8f0">14.3%</td><td style="padding:6px 8px;text-align:center;border:1px solid #e2e8f0">19.7%</td><td style="padding:6px 8px;border:1px solid #e2e8f0">洋芝・平坦で能力重視</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0">高知</td><td style="padding:6px 8px;text-align:center;border:1px solid #e2e8f0">29.0%</td><td style="padding:6px 8px;text-align:center;border:1px solid #e2e8f0">16.7%</td><td style="padding:6px 8px;text-align:center;border:1px solid #e2e8f0">54.2%</td><td style="padding:6px 8px;border:1px solid #e2e8f0">コース適性が支配的（地方独特の馬場）</td></tr>
            </tbody>
          </table>

          <h3>穴馬・危険馬検知（I章）</h3>
          <p>穴馬スコアは三連率ギャップ（最大+6pt）＋13項目の加算式。8pt以上が穴A（能力込み）、5〜7ptが穴B。主な加点項目：</p>
          <table style="width:100%;border-collapse:collapse;font-size:0.82rem;margin:4px 0">
            <thead><tr style="background:#f1f5f9"><th style="padding:5px 8px;border:1px solid #e2e8f0">項目</th><th style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">加点</th></tr></thead>
            <tbody>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">MLルール複勝率乖離≥15% (ML&gt;ルール)</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+2.0</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">MLルール複勝率乖離≥8%</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+1.0</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">トレンド上昇（RAPID_UP / UP）</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+1.5</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">展開上位25%以内</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+1.5</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">勝負気配スコア≥4</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+1.5</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">着差評価指数avg&gt;0.5</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+1.0</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">騎手Aパターン乗替</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+1.0</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">休み明け×厩舎初戦型 (回収率≥100%)</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+1.0</td></tr>
              <tr><td style="padding:5px 8px;border:1px solid #e2e8f0">コース初出走×類似コース高実績</td><td style="padding:5px 8px;border:1px solid #e2e8f0;text-align:center">+1.0</td></tr>
            </tbody>
          </table>
          <p style="margin-top:6px">三連率ギャップは「理論複勝率 − (1/odds × 3.5)」で算出。市場が複勝率を過小評価している馬を炙り出す。危険馬スコアは逆の構造（過信人気×能力不足）で、危険馬と判定された馬には◎を打たない。</p>

          <h3>MLモデル構成（LightGBM + LambdaRank）</h3>
          <p>5種類のモデルを組み合わせてレース予測を行う。複勝予測モデル（LGBMPredictor）は <b>88次元特徴量</b> を使用し、<b>オッズ・人気を含まない</b>（市場の歪み検出のため意図的に除外）。JRAとNAR（地方）の両方に対応（<code>is_jra</code> フラグで識別）。Optuna（ベイズ最適化 50試行）でハイパーパラメータ最適化後、Platt Scalingで確率キャリブレーション済み。Walk-Forward CV 7フォールド評価: AUC=0.8064 / Top1率=71.6% / Top3率=95.3%。</p>
          <table style="width:100%;border-collapse:collapse;font-size:0.82rem;margin:4px 0">
            <thead><tr style="background:#dcfce7;color:#166534">
              <th style="padding:6px 8px;border:1px solid #bbf7d0">モデル</th>
              <th style="padding:6px 8px;border:1px solid #bbf7d0">出力</th>
              <th style="padding:6px 8px;border:1px solid #bbf7d0">用途 / 性能</th>
            </tr></thead>
            <tbody>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace"><b>LGBMPredictor</b></td><td style="padding:6px 8px;border:1px solid #e2e8f0">複勝確率（Platt補正）</td><td style="padding:6px 8px;border:1px solid #e2e8f0"><b>メイン</b>: 88特徴量、30+サブモデル自動選択。AUC=0.8064 / Top3率=95.3%</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace"><b>LGBMRanker</b></td><td style="padding:6px 8px;border:1px solid #e2e8f0">ランクスコア（LambdaRank）</td><td style="padding:6px 8px;border:1px solid #e2e8f0">LGBMPredictorに10%ウェイトでブレンド。NDCG@3=0.5976（三連複精度向上）</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace">Last3FPredictor</td><td style="padding:6px 8px;border:1px solid #e2e8f0">上がり3F予測タイム</td><td style="padding:6px 8px;border:1px solid #e2e8f0">展開偏差値の末脚評価に使用（→ <code>ml_l3f_est</code> 特徴量として再利用）</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace">PositionPredictor</td><td style="padding:6px 8px;border:1px solid #e2e8f0">レース内位置取り予測</td><td style="padding:6px 8px;border:1px solid #e2e8f0">展開偏差値計算に使用（→ <code>ml_pos_est</code> 特徴量として再利用）</td></tr>
              <tr><td style="padding:6px 8px;border:1px solid #e2e8f0;font-family:monospace">ProbabilityPredictor</td><td style="padding:6px 8px;border:1px solid #e2e8f0">win / top2 / top3 確率</td><td style="padding:6px 8px;border:1px solid #e2e8f0">予想オッズ算出・穴馬乖離検出に使用</td></tr>
            </tbody>
          </table>
          <p style="margin-top:6px">全モデルはプロセス起動時に1回のみロードし（モジュールレベルのキャッシュ）、コース・距離帯・JRA/NARで最適なサブモデルを自動選択する。</p>

          <h3 style="margin-top:18px">LGBMPredictor: 88特徴量一覧と寄与度</h3>
          <p style="margin:4px 0 10px">全サブモデル平均の情報利得（gain）ベース寄与度。<b>上位10特徴量で全体の約60%</b> を説明する。モデル再学習後も自動更新される。</p>
          <div id="feat-imp-container">
            <div style="text-align:center;padding:20px;color:#64748b">⌛ 特徴量データ読み込み中...</div>
          </div>
{% raw %}
          <script>
          (function(){
            var _featData=[], _curCat='all';
            var CAT_STYLE={
              '能力':  {bg:'#dcfce7',tc:'#166534',bc:'#16a34a'},
              '展開':  {bg:'#dbeafe',tc:'#1e40af',bc:'#2563eb'},
              '騎手':  {bg:'#ffedd5',tc:'#c2410c',bc:'#ea580c'},
              '調教師':{bg:'#ede9fe',tc:'#7c3aed',bc:'#7c3aed'},
              'コース':{bg:'#f1f5f9',tc:'#475569',bc:'#64748b'},
              '体型':  {bg:'#ccfbf1',tc:'#0f766e',bc:'#0d9488'},
              '血統':  {bg:'#fef9c3',tc:'#713f12',bc:'#a16207'}
            };
            var CAT_DESC={
              '能力':'過去走実績・フォーム・類似コース適性',
              '展開':'位置取り・脚質・スピード指数',
              '騎手':'騎手成績・乗り替わり評価',
              '調教師':'調教師成績・騎手コンビ',
              'コース':'レース条件・競馬場属性・枠バイアス',
              '体型':'馬体重・年齢・斤量・休養',
              '血統':'父馬・母父馬の産駒実績'
            };
            var FEAT_DESC={
              'horse_form_zscore_in_race':'レース内フォームZスコア（直近走の偏差がこのレースで何σ優れているか）',
              'venue_sim_rank_in_race':'レース内・類似コース適性ランク（0=最低〜1=最高）',
              'prev_odds_1':'前走単勝オッズ（市場が評価した前走時点の能力水準）',
              'venue_sim_place_rate':'類似コース（直線長・坂・コーナー形状が近い競馬場）での複勝率',
              'ml_pos_est':'ML推定4角位置取り（0=先頭〜1=最後方）',
              'horse_form_rank_in_race':'レース内・直近走偏差値ランク（0=最低〜1=最高）',
              'field_count':'出走頭数（少頭数は堅め、多頭数は波乱含み）',
              'jockey_place_zscore_in_race':'レース内・騎手複勝率Zスコア（このレースの騎手陣の中で何σ上か）',
              'venue_sim_avg_finish':'類似コースでの平均着順（小さいほど好成績）',
              'horse_place_rank_in_race':'レース内・通算複勝率ランク（0=最低〜1=最高）',
              'horse_place_rate':'通算複勝率（全コース・全距離の3着以内率）',
              'horse_last_finish':'直近1走着順（小さいほど直近で好走）',
              'dev_run1':'直近1走の正規化着順（1.0=1着、0.0=最下位、頭数補正済）',
              'venue_code':'競馬場コード（東京=05、中山=06など。コース別傾向を学習）',
              'jockey_place_rate':'騎手の通算複勝率（全コース・全距離）',
              'venue_sim_runs':'類似コースでの通算出走数（信頼度の指標）',
              'jockey_place_rank_in_race':'レース内・騎手複勝率ランク（0=最低〜1=最高）',
              'horse_runs':'通算出走数（経験値と安定性の指標）',
              'horse_avg_finish':'通算平均着順（全過去走の単純平均）',
              'dev_run2':'直近2走の正規化着順（頭数補正済）',
              'jockey_place_rate_90d':'騎手の直近90日複勝率（現在の調子・状態）',
              'horse_running_style':'脚質（直近5走4角平均相対位置: 0.0=逃げ〜1.0=追込）',
              'ml_l3f_est':'ML推定上がり3Fタイム（秒）',
              'prev_odds_2':'2走前の単勝オッズ',
              'jockey_venue_wr':'騎手の当競馬場での通算勝率',
              'jt_combo_wr':'この騎手×調教師コンビの過去勝率（相性・信頼度）',
              'horse_days_since':'前走からの休養日数（間隔ローテとリフレッシュ度）',
              'same_dir_place_rate':'同回り方向（右回り/左回り）での複勝率',
              'trainer_place_rate':'調教師の通算複勝率（全コース・全距離）',
              'horse_weight':'馬体重(kg)',
              'gate_venue_wr':'この枠番×競馬場での過去複勝率（枠順バイアス）',
              'relative_weight_kg':'レース内相対斤量（自馬斤量 − このレースの平均斤量）',
              'trainer_place_rank_in_race':'レース内・調教師複勝率ランク（0=最低〜1=最高）',
              'trend_position_slope':'直近3走の着順トレンド傾き（プラス=改善中）',
              'trainer_runs':'調教師の通算出走数（信頼度の指標）',
              'age':'年齢（2〜9歳以上）',
              'trainer_place_rate_90d':'調教師の直近90日複勝率（現在の厩舎の調子）',
              'speed_sec_per_m_est':'スピード指数（過去走破時計÷√距離の加重平均：絶対速度）',
              'style_surface_wr':'この脚質×馬場種別での過去複勝率（脚質×馬場適性）',
              'trainer_venue_wr':'調教師の当競馬場での通算勝率',
              'jockey_runs':'騎手の通算出走数（信頼度の指標）',
              'trainer_dist_wr':'調教師の該当距離帯（sprint/mile/middle/long）での勝率',
              'weight_change':'馬体重変動(kg)（前走比: プラス=増量）',
              'gate_style_wr':'この枠番×脚質での過去複勝率（枠順と脚質の組み合わせ適性）',
              'jockey_win_rate':'騎手の通算勝率（全コース・全距離）',
              'jockey_dist_wr':'騎手の該当距離帯での勝率',
              'venue_sim_win_rate':'類似コースでの通算勝率',
              'trainer_win_rate_90d':'調教師の直近90日勝率',
              'jockey_win_rate_90d':'騎手の直近90日勝率',
              'jockey_surface_wr':'騎手の当馬場種別（芝/ダート）での勝率',
              'same_dir_runs':'同回り方向での通算出走数（サンプル数）',
              'trainer_wp_ratio':'調教師の勝率÷複勝率（詰めの鋭さ・勝ちに来るかの指標）',
              'jockey_wp_ratio':'騎手の勝率÷複勝率（追込型か安定型かを表す）',
              'jt_combo_runs':'この騎手×調教師コンビの過去出走数（信頼度）',
              'trainer_win_rate':'調教師の通算勝率（全コース・全距離）',
              'distance':'距離(m)（スプリント1400以下〜長距離2200超）',
              'trainer_surface_wr':'調教師の当馬場種別での勝率',
              'horse_win_rate':'通算勝率（全コース・全距離）',
              'horse_no':'馬番号（ゲート内での番号）',
              'gate_no':'枠番（1〜8枠）',
              'month':'開催月（1〜12月。季節性を学習）',
              'horse_condition_match':'当日馬場状態（良/稍重/重/不良）での過去複勝率',
              'grade_code':'グレードコード（0=新馬、1=未勝利、…8=G1）',
              'class_change':'クラス変化（+1=昇格、0=同クラス、−1=降格）',
              'kishu_pattern_code':'乗り替わりパターン（0=継続、1=強化A〜5=不明F）',
              'is_jockey_change':'乗り替わりフラグ（1=乗替、0=継続）',
              'venue_sim_n_venues':'類似コースでの実績を持つ競馬場の数（汎用性）',
              'weight_kg':'斤量(kg)（55kg、57kgなど）',
              'prev_grade_code':'前走グレードコード（0=新馬〜8=G1）',
              'bms_win_rate':'母父馬産駒の通算勝率（母系血統の総合力）',
              'sire_smile_wr':'父馬産駒の当距離帯（SMILE区分）での勝率',
              'bms_place_rate':'母父馬産駒の複勝率',
              'sire_win_rate':'父馬産駒の通算勝率（父系血統の総合力）',
              'bms_surf_wr':'母父馬産駒の当馬場種別（芝/ダート）での勝率',
              'venue_straight_m':'この競馬場の直線距離(m)（長いほど末脚有利）',
              'sire_place_rate':'父馬産駒の複勝率',
              'sire_surf_wr':'父馬産駒の当馬場種別での勝率（芝向き/ダート向き判別）',
              'condition':'馬場状態（良=0/稍重=1/重=2/不良=3）',
              'venue_first_corner':'スタートから最初のコーナーまでの距離割合',
              'sex_code':'性別コード（牡=0、牝=1、セン=2）',
              'is_jra':'JRA/NAR区分（1=JRA、0=NAR地方）',
              'venue_direction':'回り方向コード（右回り/左回り）',
              'venue_corner_type':'コーナー形状コード（急カーブ/緩カーブ）',
              'is_long_break':'長期休養フラグ（90日以上の休養明け=1）',
              'venue_slope':'坂高度(m)（中山最終直線・阪神坂など）',
              'surface':'馬場種別（0=芝、1=ダート、2=障害）※サブモデルで固定のため寄与度低',
              'trend_deviation_slope':'偏差値の直近3走傾き（trend_position_slopeの偏差値版）',
              'chakusa_index_avg3':'直近3走の着差評価指数平均（勝ち馬との時計差を数値化）'
            };

            function buildUI(data) {
              _featData = data;
              var cats = {};
              data.forEach(function(d) { cats[d.cat] = (cats[d.cat]||0)+1; });
              var maxPct = data.length ? data[0].pct : 1;

              var html = '<div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:8px;align-items:center">';
              html += '<span style="font-size:0.79rem;color:#64748b;margin-right:2px">カテゴリ:</span>';
              html += '<button id="fc-btn-all" onclick="featFilter(this,\'all\')" style="font-size:0.73rem;padding:2px 8px;border:1px solid #94a3b8;border-radius:10px;cursor:pointer;background:#e2e8f0;font-weight:bold">全て('+data.length+')</button>';
              Object.keys(CAT_STYLE).forEach(function(cat) {
                if (!cats[cat]) return;
                var cs = CAT_STYLE[cat];
                html += '<button onclick="featFilter(this,\''+cat+'\')" title="'+CAT_DESC[cat]+'" style="font-size:0.73rem;padding:2px 8px;border:1px solid '+cs.bc+';border-radius:10px;cursor:pointer;background:'+cs.bg+';color:'+cs.tc+'">'+cat+'('+cats[cat]+')</button>';
              });
              html += '</div>';
              html += '<input type="text" id="fc-search" oninput="featSearch()" placeholder="特徴量名・説明で検索 (例: jockey, 騎手, 類似コース)..." style="width:100%;box-sizing:border-box;padding:5px 8px;font-size:0.82rem;border:1px solid #cbd5e1;border-radius:4px;margin-bottom:6px">';
              html += '<div style="max-height:540px;overflow-y:auto;border:1px solid #e2e8f0;border-radius:4px">';
              html += '<table id="fc-table" style="width:100%;border-collapse:collapse;font-size:0.76rem">';
              html += '<thead><tr style="background:#dcfce7;color:#166534;position:sticky;top:0;z-index:1">';
              html += '<th style="padding:5px 6px;border:1px solid #bbf7d0;text-align:center">#</th>';
              html += '<th style="padding:5px 6px;border:1px solid #bbf7d0;text-align:left;min-width:165px">特徴量名</th>';
              html += '<th style="padding:5px 6px;border:1px solid #bbf7d0;text-align:left">意味・説明</th>';
              html += '<th style="padding:5px 6px;border:1px solid #bbf7d0;text-align:center">カテゴリ</th>';
              html += '<th style="padding:5px 6px;border:1px solid #bbf7d0;text-align:left;min-width:110px">寄与度</th>';
              html += '</tr></thead><tbody id="fc-tbody"></tbody></table></div>';
              document.getElementById('feat-imp-container').innerHTML = html;
              renderRows(data, maxPct);
            }

            window.featFilter = function(btn, cat) {
              _curCat = cat;
              document.querySelectorAll('#feat-imp-container button').forEach(function(b) {
                b.style.fontWeight = ''; b.style.boxShadow = '';
              });
              btn.style.fontWeight = 'bold'; btn.style.boxShadow = '0 0 0 2px #16a34a';
              featSearch();
            };

            window.featSearch = function() {
              var q = (document.getElementById('fc-search')||{value:''}).value.toLowerCase();
              renderRows(_featData.filter(function(d) {
                return (_curCat==='all' || d.cat===_curCat) &&
                       (!q || d.name.toLowerCase().includes(q) || (FEAT_DESC[d.name]||'').toLowerCase().includes(q));
              }), _featData.length ? _featData[0].pct : 1);
            };

            function renderRows(rows, maxPct) {
              var tbodyEl = document.getElementById('fc-tbody');
              if (!tbodyEl) return;
              var html = '';
              rows.forEach(function(d) {
                var cs = CAT_STYLE[d.cat] || {bg:'#f1f5f9',tc:'#475569',bc:'#64748b'};
                var barW = Math.max(1, Math.round(d.pct / maxPct * 100));
                var desc = FEAT_DESC[d.name] || d.name;
                var pctStr = d.pct >= 0.01 ? (d.pct.toFixed(2)+'%') : '&lt;0.01%';
                html += '<tr data-cat="'+d.cat+'">';
                html += '<td style="padding:3px 5px;border:1px solid #e2e8f0;text-align:center;color:#94a3b8;font-size:0.71rem">'+d.rank+'</td>';
                html += '<td style="padding:3px 5px;border:1px solid #e2e8f0;font-family:monospace;font-size:0.69rem;white-space:nowrap">'+d.name+'</td>';
                html += '<td style="padding:3px 5px;border:1px solid #e2e8f0;line-height:1.35">'+desc+'</td>';
                html += '<td style="padding:3px 5px;border:1px solid #e2e8f0;text-align:center"><span style="font-size:0.68rem;padding:1px 5px;border-radius:9px;background:'+cs.bg+';color:'+cs.tc+';white-space:nowrap">'+d.cat+'</span></td>';
                html += '<td style="padding:3px 5px;border:1px solid #e2e8f0"><div style="display:flex;align-items:center;gap:3px"><div style="width:76px;background:#e2e8f0;border-radius:2px;height:5px;flex-shrink:0"><div style="width:'+barW+'%;height:5px;background:'+cs.bc+';border-radius:2px"></div></div><span style="font-size:0.7rem;color:#475569;white-space:nowrap">'+pctStr+'</span></div></td>';
                html += '</tr>';
              });
              tbodyEl.innerHTML = html;
            }

            function loadFeatImp() {
              if (_featData.length) return;
              fetch('/api/feature_importance').then(function(r){return r.json();}).then(function(data) {
                if (data.error) {
                  document.getElementById('feat-imp-container').innerHTML = '<p style="color:#dc2626;padding:12px">エラー: '+data.error+'</p>';
                  return;
                }
                buildUI(data);
              }).catch(function(e) {
                document.getElementById('feat-imp-container').innerHTML = '<p style="color:#dc2626;padding:12px">読み込み失敗: '+e.message+'</p>';
              });
            }

            // About タブクリック時に遅延ロード
            document.querySelectorAll('.tab[data-tab="about"]').forEach(function(t) {
              t.addEventListener('click', function(){ setTimeout(loadFeatImp, 100); });
            });
            // すでに About タブが表示されている場合は即ロード
            setTimeout(function() {
              var panel = document.getElementById('panel-about');
              if (panel && panel.classList.contains('active')) loadFeatImp();
            }, 300);
          })();
          </script>
{% endraw %}

          <h3>予想オッズと乖離検出（バリューベット）</h3>
          <p>MLモデルの win 確率からオッズ未確定馬を含む全馬の予想オッズを算出し、実オッズとの乖離率を計算する：</p>
          <div style="background:#f8fafc;padding:8px 14px;border-radius:6px;border-left:3px solid #16a34a;font-family:monospace;font-size:0.82rem;line-height:1.8">
            predicted_odds = (1 / ml_win_prob) × payout_rate  # JRA単勝: 0.80<br>
            divergence_ratio = real_odds / predicted_odds<br>
            S: ratio ≥ 2.0（超妙味）/ A: ≥ 1.5（妙味大）/ B: ≥ 1.2（妙味あり）<br>
            馬連予想オッズ = sqrt(pred_a × pred_b) × payout_rate(馬連: 0.775)<br>
            三連複予想オッズ = geomean(pred_a, pred_b, pred_c) × payout_rate(三連複: 0.725)
          </div>
          <p style="margin-top:8px">前日モード（実オッズ未確定）では composite のsoftmax（temperature=3.5）から予想オッズを生成して暫定表示する。</p>

          <h3>買い目生成・期待値・資金配分（第5章）</h3>
          <p>馬連5点（◎軸 + ○▲△☆/穴）をベースに、本命の信頼度A以上かつ相手が着拾い型・安定型の場合ワイドを追加する。期待値は実測または推定オッズを使用：</p>
          <div style="background:#f8fafc;padding:8px 14px;border-radius:6px;border-left:3px solid #16a34a;font-family:monospace;font-size:0.82rem;line-height:1.8">
            umaren_odds = odds_a × odds_b / head_factor × 0.97  # head_factor: 8h=3.0〜16h=4.0<br>
            wide_odds = umaren_odds × 0.35<br>
            P(馬連) = place2_prob_a × place2_prob_b × (n / (n−1))  # 相関補正<br>
            EV(%) = P × odds × 100<br>
            分類: ≥300% → 勝負 / ≥200% → ◎買 / ≥150% → ○買 / ≥100% → 検討 / &lt;100% → 見送り
          </div>
          <p style="margin-top:8px">資金配分は印の重み（◎−○: 50% / ◎−▲: 30% / ◎−△: 15% / ◎−☆: 5%）にEV補正（最大1.3倍）をかけ、「見送り」判定分を残り点に再配分する。総賭け金は自信度別（SS/S/A/B/C）のデフォルト額またはカスタム額を使用。</p>

          <h3>データソースとバックテスト</h3>
          <p>JRA10場＋NAR14場（計24場）に対応。netkeibaから出走表・過去走・着順・払戻金を取得し、1.5秒/リクエストのレート制限を遵守。NAR出走表はJavaScript動的描画のため <code>nar.netkeiba.com</code> を優先使用（失敗時 <code>race.netkeiba.com</code> にフォールバック）。予想結果は JSON で保存し、レース後に自動照合。「結果分析」タブで通算/年別/印別/券種別/自信度別の回収率を確認できる。</p>

        </div>
      </div>
    </div>

  <script>
    // ===== Tab switching (new 4-tab design) =====
    document.querySelectorAll('.tab').forEach(t=>{
      t.onclick=e=>{
        e.preventDefault();
        document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
        document.querySelectorAll('.tab-panel').forEach(x=>x.classList.remove('active'));
        t.classList.add('active');
        const tab = t.dataset.tab;
        const panel = document.getElementById('panel-'+tab);
        if(panel) panel.classList.add('active');
        if(tab==='today'){ loadHomeRaces(); loadHighConfidence(); loadShareUrl(); }
        else if(tab==='results'){ loadResultsSummary('all'); }
        else if(tab==='db'){ loadJockeyDB(); }
        else if(tab==='about'){
          setTimeout(function(){ if(typeof loadFeatImp==='function') loadFeatImp(); },100);
        }
      };
    });

    // Server time in header
    function updateClock(){
      const n=new Date();
      const st=document.getElementById('server-time');
      if(st) st.textContent=n.toLocaleString('ja-JP',{year:'numeric',month:'2-digit',day:'2-digit',weekday:'short',hour:'2-digit',minute:'2-digit',second:'2-digit'});
    }
    setInterval(updateClock,1000);
    updateClock();

    // ===== Admin access control (old UI) =====
    var _isAdmin = true;
    (async function(){
      try {
        const r = await fetch('/api/auth_mode');
        const d = await r.json();
        _isAdmin = !!d.admin;
      } catch(e) { _isAdmin = true; }
      if (!_isAdmin) {
        // Show badge
        var clock = document.getElementById('server-time');
        if (clock) {
          var badge = document.createElement('span');
          badge.textContent = '閲覧モード';
          badge.style.cssText = 'display:inline-block;margin-left:10px;padding:2px 10px;background:#fbbf24;color:#78350f;border-radius:12px;font-size:11px;font-weight:700;vertical-align:middle';
          clock.parentNode.insertBefore(badge, clock);
        }
        // Hide 結果取得 button on today tab
        var btnRes = document.getElementById('btn-home-update-results');
        if (btnRes) btnRes.style.display = 'none';
        // Hide 結果照合 section in results tab (cards with 結果照合)
        var cards = document.querySelectorAll('#panel-results .card');
        cards.forEach(function(c) {
          var h2 = c.querySelector('h2');
          if (h2 && (h2.textContent.indexOf('結果照合') >= 0 || h2.textContent.indexOf('配布用HTML生成') >= 0)) {
            c.style.display = 'none';
          }
        });
      }
    })();

    // New: toggle analysis panel
    function toggleAnalysisPanelNew(){
      const p=document.getElementById('analysis-panel-new');
      if(!p) return;
      if(p.style.display==='none'||!p.style.display){
        p.style.display='block';
        loadVenueCheckboxesNew();
      } else {
        p.style.display='none';
      }
    }

    async function loadVenueCheckboxesNew(){
      const grid=document.getElementById('venue-check-grid-new');
      if(!grid) return;
      grid.innerHTML='<span style="font-size:12px;color:var(--text-muted)">読み込み中...</span>';
      try{
        const r=await fetch('/api/home_info?date='+_homeDate);
        const j=await r.json();
        if(!j.venues||!j.venues.length){grid.innerHTML='<span style="font-size:12px;color:var(--text-muted)">開催情報なし</span>';return;}
        const JRA_CODES_SET2=new Set(['01','02','03','04','05','06','07','08','09','10']);
        grid.innerHTML=j.venues.map(v=>{
          const isJra=JRA_CODES_SET2.has(v.code);
          return `<label class="venue-check-item">
            <input type="checkbox" class="venue-cb-new" value="${v.code}" checked>
            <span>${v.name}</span>
            <span class="venue-badge ${isJra?'jra':'nar'}">${isJra?'中央':'地方'}</span>
          </label>`;
        }).join('');
      }catch(e){
        grid.innerHTML='<span style="font-size:12px;color:var(--danger)">取得エラー: '+e.message+'</span>';
      }
    }

    async function runAnalysisNew(){
      const checkedVenues=[...document.querySelectorAll('.venue-cb-new:checked')].map(cb=>cb.value);
      if(!checkedVenues.length){alert('競馬場を1つ以上選択してください');return;}
      const btn=document.getElementById('btn-analyze-new');
      btn.disabled=true;
      const prog=document.getElementById('analyze-progress-new');
      const resultEl=document.getElementById('analyze-result-new');
      prog.classList.add('show');
      resultEl.classList.remove('show');
      document.getElementById('analyze-err-new').textContent='';
      document.getElementById('prog-fill-new').style.width='2%';
      document.getElementById('prog-race-new').textContent='分析開始中...';
      document.getElementById('prog-log-new').textContent='—';
      const r=await fetch('/api/analyze',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({date:_homeDate,venues:checkedVenues})});
      const j=await r.json();
      if(!j.ok){
        document.getElementById('analyze-err-new').textContent=j.error||'エラーが発生しました';
        btn.disabled=false;return;
      }
      pollAnalyzeNew();
    }

    function pollAnalyzeNew(){
      fetch('/api/analyze_status').then(r=>r.json()).then(j=>{
        const done=j.done_races||0;const total=j.total_races||0;
        const pct=total>0?Math.min(98,done/total*100):(j.running?5:100);
        document.getElementById('prog-fill-new').style.width=pct+'%';
        document.getElementById('prog-count-new').textContent=done+'/'+(total||'?');
        const elapsed=j.elapsed_sec||0;
        const fmt=s=>s<60?s+'秒':Math.floor(s/60)+'分'+(s%60)+'秒';
        document.getElementById('prog-elapsed-new').textContent=fmt(elapsed);
        const remain=(done>0&&total>0)?Math.round((elapsed/done)*(total-done)):null;
        document.getElementById('prog-remain-new').textContent=remain!=null?fmt(remain):'—';
        if(j.progress) document.getElementById('prog-race-new').textContent=j.progress;
        if(j.current_race&&j.current_race!==j.progress) document.getElementById('prog-log-new').textContent=j.current_race;
        if(j.done){
          document.getElementById('prog-fill-new').style.width='100%';
          document.getElementById('btn-analyze-new').disabled=false;
          if(j.error){document.getElementById('analyze-err-new').textContent=j.error;}
          else{
            document.getElementById('prog-race-new').textContent='分析完了！';
            const resultEl=document.getElementById('analyze-result-new');
            resultEl.classList.add('show');
            const dateKey=_homeDate.replace(/-/g,'');
            document.getElementById('analyze-result-link-new').href='/output/'+dateKey+'_全レース.html';
            document.getElementById('analyze-result-link-new').textContent='全レース（'+dateKey+'）';
            document.getElementById('analyze-simple-link-new').href='/output/'+dateKey+'_配布用.html';
            document.getElementById('analyze-simple-link-new').textContent='📤 配布用HTML（'+dateKey+'）';
            loadHomeRaces(true);loadShareUrl();
          }
        }else{setTimeout(pollAnalyzeNew,1200);}
      });
    }

    // New DB search and tab switcher
    function onDbSearch(){
      const q=(document.getElementById('db-search')||{}).value||'';
      // sync to legacy search inputs
      const js=document.getElementById('db-jockey-search');
      const ts=document.getElementById('db-trainer-search');
      if(js) js.value=q;
      if(ts) ts.value=q;
      // reload active panel
      const activeSubtab=document.querySelector('#db-subtabs .subtab.active');
      if(!activeSubtab) return;
      const t=activeSubtab.textContent;
      if(t.includes('騎手')) loadJockeyDB();
      else if(t.includes('調教師')) loadTrainerDB();
    }

    function switchDbTab(el, tab){
      document.querySelectorAll('#db-subtabs .subtab').forEach(s=>s.classList.remove('active'));
      el.classList.add('active');
      document.getElementById('db-panel-jockey').style.display=tab==='jockey'?'':'none';
      document.getElementById('db-panel-trainer').style.display=tab==='trainer'?'':'none';
      document.getElementById('db-panel-course').style.display=tab==='course'?'':'none';
      if(tab==='jockey') loadJockeyDB();
      else if(tab==='trainer') loadTrainerDB();
    }

    // ===== ローカル日付ヘルパー（JST対応: toISOStringはUTCなので使わない） =====
    function _localDate(d){
      const dt=d||new Date();
      return dt.getFullYear()+'-'+String(dt.getMonth()+1).padStart(2,'0')+'-'+String(dt.getDate()).padStart(2,'0');
    }

    // ===== HOME タブ - ネット競馬風 =====
    let _homeDate = _localDate();

    function homeChangeDate(delta){
      const d=new Date(_homeDate+'T00:00:00');
      d.setDate(d.getDate()+delta);
      _homeDate=_localDate(d);
      // 日付変更時はキャッシュ消去
      try{sessionStorage.removeItem('home_'+_homeDate);}catch(e){}
      loadHomeRaces();
      loadShareUrl();
      loadHighConfidence();
    }

    async function loadHomeRaces(force=false){
      // sessionStorageキャッシュ（ページ遷移から戻っても即表示）
      const cacheKey='home_'+_homeDate;
      if(!force){
        try{
          const cached=sessionStorage.getItem(cacheKey);
          if(cached){
            const c=JSON.parse(cached);
            if(Date.now()-c.ts < 300000){  // 5分以内
              _renderHome(c.pred, c.info);
              return;
            }
          }
        }catch(e){}
      }
      document.getElementById('home-date-label').textContent=_homeDate;
      document.getElementById('home-race-panels').innerHTML='';
      document.getElementById('home-no-pred').style.display='none';
      document.getElementById('home-race-count').textContent='';
      document.getElementById('home-venue-tabs').innerHTML='<span style="padding:10px 16px;display:block">読み込み中...</span>';
      try{
        const nc=force?'&nocache=1':'';
        const [predRes,infoRes]=await Promise.all([
          fetch('/api/today_predictions?date='+_homeDate+nc),
          fetch('/api/home_info?date='+_homeDate)
        ]);
        const pred=await predRes.json();
        const info=await infoRes.json();
        // sessionStorageにキャッシュ保存
        try{sessionStorage.setItem('home_'+_homeDate,JSON.stringify({ts:Date.now(),pred,info}));}catch(e){}
        _renderHome(pred,info);
      }catch(e){
        document.getElementById('home-venue-tabs').innerHTML=`<span style="padding:10px 16px;display:block">エラー: ${e.message}</span>`;
      }
    }

    function _renderHome(pred,info){
      const tabBar=document.getElementById('home-venue-tabs');
      const panels=document.getElementById('home-race-panels');
      const noData=document.getElementById('home-no-pred');
      const cntEl=document.getElementById('home-race-count');
      document.getElementById('home-date-label').textContent=_homeDate;
      const order=pred.order||Object.keys(pred.races||{});
      if(!order.length){tabBar.innerHTML='';noData.style.display='block';return;}
      cntEl.textContent=pred.total+'レース';
      const wmap={};
      (info.venues||[]).forEach(v=>{wmap[v.name]=info.weather[v.name]||{};});
      tabBar.innerHTML=order.map((v,i)=>
        `<button class="h-vtab${i===0?' active':''}" onclick="showHomeVenue(${i})">${v}</button>`
      ).join('');
      panels.innerHTML=order.map((venue,vi)=>{
        const races=pred.races[venue]||[];
        const w=wmap[venue]||{};
        const wTxt=w.condition
          ?`<span class="vw-name">${venue}</span><span>${w.condition}</span><span style="color:#2563eb">降水 ${w.precip_prob!=null?w.precip_prob+'%':'—'}</span>`
          :`<span class="vw-name">${venue}</span>`;
        const cards=races.map(r=>{
          const gradeCls = r.grade && ['G1','G2','G3','L','OP'].includes(r.grade) ? `h-rc-${r.grade}` : (r.grade ? 'h-rc-nar' : '');
          const gCls = r.grade ? `<span class="h-rc-grade ${gradeCls}">${r.grade}</span>` : '';
          const surf=r.surface||'';
          const surfD=surf==='ダート'?'ダ':surf;
          const surfCls=surfD?`h-rc-surf-${surfD}`:'';
          const confRaw = r.overall_confidence || 'C';
          const conf = confRaw.replace(/\u207a/g, '+');  // ⁺ → +
          const confColor=
            conf==='SS'?'#16a34a':conf==='S'?'#1a6fa8':
            conf==='A'?'#c0392b':
            conf==='B'||conf==='C'?'#333':'#aaa';
          const axisHtml=`<div class="h-rc-axis" style="margin-top:6px">
            <span style="font-size:11px;color:#6b7280">馬券自信度</span>
            <span style="font-size:16px;font-weight:900;color:${confColor};margin-left:6px">${confRaw}</span>
          </div>`;
          return `<a href="${r.url}" class="h-race-card">
            <div class="h-rc-top">
              <span class="h-rc-no">${r.race_no}R</span>${gCls}
              ${r.post_time?`<span class="h-rc-time" style="margin-left:auto">${r.post_time}</span>`:''}
            </div>
            <div class="h-rc-name">${r.name||r.race_no+'R'}</div>
            <div class="h-rc-meta">
              ${surfD?`<span class="${surfCls}">${surfD}</span>`:''}
              ${r.distance?`<span>${r.distance}m</span>`:''}
              ${r.head_count?`<span>${r.head_count}頭</span>`:''}
            </div>
            ${axisHtml}
            <div class="h-rc-see">予想を見る →</div>
          </a>`;
        }).join('');
        return `<div class="h-vpanel${vi===0?' active':''}" data-vi="${vi}">
          <div class="vw-bar">${wTxt}</div>
          <div class="h-race-grid">${cards}</div>
        </div>`;
      }).join('');
    }

    function showHomeVenue(idx){
      document.querySelectorAll('.h-vtab').forEach((t,i)=>t.classList.toggle('active',i===idx));
      document.querySelectorAll('.h-vpanel').forEach((p,i)=>p.classList.toggle('active',i===idx));
    }

    async function loadShareUrl(){
      const card     = document.getElementById('home-share-card');
      const noneCard = document.getElementById('home-share-none');
      const urlInput = document.getElementById('share-url-input');
      const openLink = document.getElementById('share-open-link');
      const sizeLbl  = document.getElementById('share-size');
      const filePath = document.getElementById('share-file-path');
      card.style.display='none'; noneCard.style.display='none';
      try{
        const r = await fetch('/api/share_url?date='+_homeDate);
        const j = await r.json();
        if(j.exists){
          urlInput.value = j.url;
          openLink.href  = j.url;
          sizeLbl.textContent = j.size_kb.toLocaleString()+' KB';
          filePath.textContent = 'output/'+j.filename;
          card.style.display='block';
        } else {
          noneCard.style.display='block';
        }
      }catch(e){ noneCard.style.display='block'; }
    }

    function copyShareUrl(){
      const input = document.getElementById('share-url-input');
      navigator.clipboard.writeText(input.value).then(()=>{
        const btn = document.getElementById('share-copy-btn');
        const orig = btn.textContent;
        btn.textContent='✅ コピー完了!';
        btn.style.background='#1e8c4a';
        setTimeout(()=>{ btn.textContent=orig; btn.style.background='#0d2b5e'; }, 2000);
      }).catch(()=>{
        input.select();
        document.execCommand('copy');
        alert('URLをコピーしました');
      });
    }

    async function loadData(){
      try{
        const r=await fetch('/api/portfolio');
        const j=await r.json();
        const sr=document.getElementById('stat-runs');
        const sd=document.getElementById('stat-date');
        if(sr) sr.textContent=j.course_runs.toLocaleString();
        if(sd) sd.textContent=j.last_date||'—';
      }catch(e){ console.warn('loadData error',e); }
    }

    let _collectMode = 'full';

    function setCollectMode(mode){
      _collectMode = mode;
      const today = _localDate();
      if(mode === 'full'){
        document.getElementById('start_date').value = '2024-01-01';
        document.getElementById('end_date').value = today;
      } else if(mode === 'resume'){
        // 日付はそのまま（前回の続きはサーバー側で判断）
        document.getElementById('end_date').value = today;
      } else if(mode === 'append'){
        const lastDate = document.getElementById('stat-date').textContent;
        if(lastDate && lastDate !== '—'){
          const d = new Date(lastDate);
          d.setDate(d.getDate()+1);
          document.getElementById('start_date').value = _localDate(d);
        }
        document.getElementById('end_date').value = today;
      }
      document.getElementById('btn_run').disabled = false;
      document.getElementById('btn_run').textContent = '▶ 実行（' + {full:'全件収集',resume:'途中再開',append:'未収集分の追加'}[mode] + '）';
    }

    async function startCollect(){
      const mode = _collectMode;
      document.getElementById('btn_full').disabled=
      document.getElementById('btn_resume').disabled=
      document.getElementById('btn_append').disabled=
      document.getElementById('btn_run').disabled=true;
      document.getElementById('collect-progress').style.display='block';
      const r=await fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({mode,start_date:document.getElementById('start_date').value,end_date:document.getElementById('end_date').value})});
      const j=await r.json();
      if(!j.ok){document.getElementById('err').textContent=j.error||'エラー';
        document.getElementById('btn_full').disabled=document.getElementById('btn_resume').disabled=document.getElementById('btn_append').disabled=document.getElementById('btn_run').disabled=false;
        return;}
      pollCollect();
    }

    function pollCollect(){
      fetch('/api/status').then(r=>r.json()).then(j=>{
        if(j.running){
          const total=j.total_days||1;
          const pct=total?Math.min(100,j.day_index/total*100):0;
          document.getElementById('fill').style.width=pct+'%';
          const elapsed=j.elapsed_sec||0;
          const elStr=elapsed<60?elapsed+'秒':Math.floor(elapsed/60)+'分'+(elapsed%60)+'秒';
          const done=j.day_index||0;
          const remain=done>0&&total>0?Math.round((elapsed/done)*(total-done)):null;
          const remStr=remain!=null?(remain<60?remain+'秒':Math.floor(remain/60)+'分'+(remain%60)+'秒'):'—';
          const statusText=j.status==='starting'?'準備中...':
            j.status==='already_done'?'既に完了済み':
            done+'/'+j.total_days+'日 ('+(j.total_runs||0)+'走) '+j.current_date
            +'  経過:'+elStr+'  残り推定:'+remStr;
          document.getElementById('status').textContent=statusText;
          if(j.status!=='already_done')document.getElementById('err').textContent='';
          setTimeout(pollCollect,1500);
        }else{
          document.getElementById('err').textContent=j.error||'';
          document.getElementById('fill').style.width='100%';
          document.getElementById('collect-progress').style.display='none';
          document.getElementById('btn_full').disabled=
          document.getElementById('btn_resume').disabled=
          document.getElementById('btn_append').disabled=false;
          document.getElementById('btn_run').disabled=true;
          document.getElementById('btn_run').textContent='▶ 実行';
          loadData();
        }
      });
    }

    document.getElementById('btn_full').onclick=()=>setCollectMode('full');
    document.getElementById('btn_resume').onclick=()=>setCollectMode('resume');
    document.getElementById('btn_append').onclick=()=>setCollectMode('append');
    document.getElementById('btn_run').onclick=()=>startCollect();

    // ===== レース予想タブ =====
    const JRA_CODES_SET = new Set(['01','02','03','04','05','06','07','08','09','10']);

    document.getElementById('btn_load_venues').onclick = async () => {
      const dt = document.getElementById('analyze_date').value;
      if (!dt) { alert('日付を入力してください'); return; }
      const loadingEl = document.getElementById('venues-loading');
      const areaEl = document.getElementById('venues-area');
      const btnAnalyze = document.getElementById('btn_analyze');
      loadingEl.textContent = '読み込み中...';
      loadingEl.style.display = 'block';
      areaEl.style.display = 'none';
      btnAnalyze.disabled = true;
      try {
        const r = await fetch('/api/home_info?date=' + dt);
        const j = await r.json();
        const grid = document.getElementById('venue-check-grid');
        grid.innerHTML = '';
        if (!j.venues || j.venues.length === 0) {
          loadingEl.textContent = 'この日の開催情報が見つかりませんでした。';
          loadingEl.style.display = 'block';
          btnAnalyze.disabled = true;
          return;
        }
        j.venues.forEach(v => {
          const isJra = JRA_CODES_SET.has(v.code);
          const label = document.createElement('label');
          label.className = 'venue-check-item ' + (isJra ? 'jra' : 'nar');
          label.innerHTML = `<input type="checkbox" class="venue-cb" value="${v.code}" checked>
            <span>${v.name}</span>
            <span class="venue-badge ${isJra ? 'jra' : 'nar'}">${isJra ? '中央' : '地方'}</span>`;
          grid.appendChild(label);
        });
        loadingEl.style.display = 'none';
        areaEl.style.display = 'block';
        btnAnalyze.disabled = false;
      } catch(e) {
        loadingEl.textContent = '取得エラー: ' + e.message;
        loadingEl.style.display = 'block';
        btnAnalyze.disabled = true;
      }
    };

    document.getElementById('btn_all_select').onclick = () => {
      document.querySelectorAll('.venue-cb').forEach(cb => cb.checked = true);
    };
    document.getElementById('btn_all_clear').onclick = () => {
      document.querySelectorAll('.venue-cb').forEach(cb => cb.checked = false);
    };

    document.getElementById('btn_analyze').onclick = async () => {
      const dt = document.getElementById('analyze_date').value;
      const checkedVenues = [...document.querySelectorAll('.venue-cb:checked')].map(cb => cb.value);
      if (checkedVenues.length === 0) { alert('競馬場を1つ以上選択してください'); return; }
      document.getElementById('btn_analyze').disabled = true;
      document.getElementById('btn_load_venues').disabled = true;
      const prog = document.getElementById('analyze-progress');
      const resultEl = document.getElementById('analyze-result');
      prog.classList.add('show');
      resultEl.classList.remove('show');
      document.getElementById('analyze-err').textContent = '';
      document.getElementById('prog-fill').style.width = '2%';
      document.getElementById('prog-race').textContent = '分析開始中...';
      document.getElementById('prog-log').textContent = '—';
      const r = await fetch('/api/analyze', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({date: dt, venues: checkedVenues})
      });
      const j = await r.json();
      if (!j.ok) {
        // 「既に実行中」の場合はポーリングに切り替えて進捗を表示
        if (j.error === '既に実行中です') {
          pollAnalyze();
          return;
        }
        document.getElementById('analyze-err').textContent = j.error || 'エラーが発生しました';
        document.getElementById('btn_analyze').disabled = false;
        document.getElementById('btn_load_venues').disabled = false;
        return;
      }
      pollAnalyze();
    };

    function pollAnalyze() {
      fetch('/api/analyze_status').then(r => r.json()).then(j => {
        const done  = j.done_races  || 0;
        const total = j.total_races || 0;
        const pct   = total > 0 ? Math.min(98, done / total * 100) : (j.running ? 5 : 100);
        document.getElementById('prog-fill').style.width = pct + '%';
        document.getElementById('prog-count').textContent = done + '/' + (total || '?');
        const elapsed = j.elapsed_sec || 0;
        const fmt = s => s < 60 ? s + '秒' : Math.floor(s/60) + '分' + (s%60) + '秒';
        document.getElementById('prog-elapsed').textContent = fmt(elapsed);
        const remain = (done > 0 && total > 0) ? Math.round((elapsed / done) * (total - done)) : null;
        document.getElementById('prog-remain').textContent = remain != null ? fmt(remain) : '—';
        // メイン進捗テキスト
        if (j.progress) document.getElementById('prog-race').textContent = j.progress;
        // ログ欄: 最後の完了レース（progress と異なる場合のみ）
        if (j.current_race && j.current_race !== j.progress) {
          document.getElementById('prog-log').textContent = j.current_race;
        }
        if (j.done) {
          document.getElementById('prog-fill').style.width = '100%';
          document.getElementById('btn_analyze').disabled = false;
          document.getElementById('btn_load_venues').disabled = false;
          if (j.error) {
            document.getElementById('analyze-err').textContent = j.error;
          } else {
            document.getElementById('prog-race').textContent = '分析完了！';
            const resultEl = document.getElementById('analyze-result');
            resultEl.classList.add('show');
            const dt = document.getElementById('analyze_date').value;
            if (dt) {
              const dateKey = dt.replace(/-/g, '');
              const link = document.getElementById('analyze-result-link');
              link.href = '/output/' + dateKey + '_全レース.html';
              link.textContent = '全レース（' + dateKey + '）';
              const simpleLink = document.getElementById('analyze-simple-link');
              simpleLink.href = '/output/' + dateKey + '_配布用.html';
              simpleLink.textContent = '📤 配布用HTML（' + dateKey + '）';
            }
            loadData();
            // 分析完了後は force=true でサーバー・クライアントキャッシュを両方バイパス
            try { loadHomeRaces(true); loadShareUrl(); } catch(e){}
          }
        } else {
          setTimeout(pollAnalyze, 1200);
        }
      });
    }

    loadHomeRaces();
    loadShareUrl();
    loadHighConfidence();
    loadData();

    // ページロード時に分析が既に実行中なら進捗ポーリングを自動再開
    fetch('/api/analyze_status').then(r=>r.json()).then(j=>{
      if (j.running) {
        document.getElementById('analyze-progress').classList.add('show');
        document.getElementById('btn_analyze').disabled = true;
        document.getElementById('btn_load_venues').disabled = true;
        pollAnalyze();
      }
    }).catch(()=>{});

    // ===== 結果分析タブ - サブタブ切り替え =====
    document.querySelectorAll('.sub-tab').forEach(t => {
      t.onclick = e => {
        e.preventDefault();
        document.querySelectorAll('.sub-tab').forEach(x => x.classList.remove('active'));
        t.classList.add('active');
        loadResultsSummary(t.dataset.year);
      };
    });

    let _allResultData = null;
    let _datesFetched  = false;

    let _allPredDates = [];  // 予想済み日付リスト（キャッシュ）

    async function _ensurePredDates() {
      if (_allPredDates.length > 0) return _allPredDates;
      const dr = await fetch('/api/results/dates');
      const dj = await dr.json();
      _allPredDates = dj.dates || [];
      return _allPredDates;
    }

    async function loadResultsSummary(yearFilter) {
      try {
        // 日付セレクト・日付ピッカーは初回のみ構築
        if (!_datesFetched) {
          _datesFetched = true;
          const dates = await _ensurePredDates();
          const sel = document.getElementById('fetch-date-select');
          sel.innerHTML = '<option value="">-- 日付を選択 --</option>';
          dates.forEach(d => {
            const opt = document.createElement('option');
            opt.value = d; opt.textContent = d;
            sel.appendChild(opt);
          });
          // 期間ピッカーのデフォルト: 最古〜最新の予想日
          if (dates.length > 0) {
            const oldest = dates[dates.length - 1];
            const newest = dates[0];
            document.getElementById('fetch-from-date').value = oldest;
            document.getElementById('fetch-to-date').value = newest;
            // 配布用HTML生成の期間もデフォルト設定
            document.getElementById('simple-from-date').value = oldest;
            document.getElementById('simple-to-date').value = newest;
          }
        }

        // 年フィルタ付きでAPIから取得
        const yr = yearFilter || 'all';
        const sr = await fetch(`/api/results/summary?year=${yr}`);
        const sj = await sr.json();

        const titleEl = document.getElementById('rs-panel-title');
        titleEl.textContent = '2026年成績';

        if (sj.error || !sj.total_races) {
          document.getElementById('rs-nodata').style.display = 'block';
          document.getElementById('rs-stat-row').style.display = 'none';
          _renderResultStats(null, null);
          return;
        }
        document.getElementById('rs-nodata').style.display = 'none';
        document.getElementById('rs-stat-row').style.display = '';
        _renderResultStats(sj, sj.by_date || []);
        // 詳細分析・チャートも同時にロード（非同期、並行）
        loadDetailedAnalysis(yr);
        loadResultsCharts(yr);
      } catch(e) {
        console.error('結果サマリー取得エラー:', e);
        document.getElementById('rs-nodata').style.display = 'block';
        document.getElementById('rs-stat-row').style.display = 'none';
        _renderResultStats(null, null);
      }
    }

    function _renderResultStats(agg, byDate) {
      const nd = !agg;
      const _t = (id, v) => { document.getElementById(id).textContent = v; };
      const _c = (id, ok) => { document.getElementById(id).style.color = ok ? '#16a34a' : '#dc2626'; };

      _t('rs-races',      nd ? '—' : agg.total_races + ' R');
      _t('rs-hit-rate',   nd ? '—' : agg.hit_rate.toFixed(1) + '%');
      _t('rs-roi',        nd ? '—' : agg.roi.toFixed(1) + '%');
      if (!nd) _c('rs-roi', agg.roi >= 100);
      _t('rs-profit',     nd ? '—' : (agg.profit >= 0 ? '+' : '') + agg.profit.toLocaleString() + '円');
      if (!nd) _c('rs-profit', agg.profit >= 0);
      _t('rs-honmei-win',    nd ? '—' : (agg.honmei_win_rate ?? 0).toFixed(1) + '%');
      _t('rs-honmei-place2', nd ? '—' : (agg.honmei_place2_rate ?? 0).toFixed(1) + '%');
      _t('rs-honmei',        nd ? '—' : agg.honmei_rate.toFixed(1) + '%');
      const htRoi = nd ? null : (agg.honmei_tansho_roi ?? null);
      _t('rs-honmei-tansho-roi', htRoi != null ? htRoi.toFixed(1) + '%' : '—');
      if (htRoi != null) _c('rs-honmei-tansho-roi', htRoi >= 100);

      const td = (v, right, bold, color) =>
        `<td style="padding:7px 8px;border:1px solid #e2e8f0;text-align:${right?'right':'center'}${bold?';font-weight:700':''}${color?';color:'+color:''}">${v}</td>`;

      // 印別テーブル
      const markOrder = ['◉','◎','○','▲','△','★','☆','×'];
      const markData  = nd ? {} : (agg.by_mark || {});
      const markBody  = document.getElementById('results-mark-body');
      const markRows  = markOrder.filter(m => markData[m]);
      // 各列の値を算出して順位・平均ベースで色分け
      // 1位=緑, 2位=青, 3位=赤, 平均以上=黒, 平均以下=灰
      const _markRateVals = {};
      for (const m of markRows) {
        const s = markData[m];
        _markRateVals[m] = {
          wr:  s.total > 0 ? s.win/s.total*100 : 0,
          p2r: s.total > 0 && s.place2 != null ? s.place2/s.total*100 : 0,
          pr:  s.total > 0 ? s.placed/s.total*100 : 0,
        };
      }
      const _rankColor = (vals, key) => {
        const sorted = [...vals].sort((a,b) => b[key] - a[key]);
        const avg = sorted.reduce((s,v) => s + v[key], 0) / sorted.length;
        const colors = {};
        sorted.forEach((v, i) => {
          const m = v._mark;
          if (i === 0) colors[m] = '#16a34a';      // 1位: 緑
          else if (i === 1) colors[m] = '#2563eb';  // 2位: 青
          else if (i === 2) colors[m] = '#dc2626';  // 3位: 赤
          else if (v[key] >= avg) colors[m] = '#1f2937'; // 平均以上: 黒
          else colors[m] = '#9ca3af';               // 平均以下: 灰
        });
        return colors;
      };
      const _valsArr = markRows.map(m => ({_mark: m, ..._markRateVals[m]}));
      const wrColors  = _rankColor(_valsArr, 'wr');
      const p2rColors = _rankColor(_valsArr, 'p2r');
      const prColors  = _rankColor(_valsArr, 'pr');

      markBody.innerHTML = markRows.length > 0 ? markRows.map(m => {
        const s = markData[m];
        const v = _markRateVals[m];
        const wr   = s.total > 0 ? v.wr.toFixed(1) + '%' : '—';
        const p2r  = s.total > 0 ? v.p2r.toFixed(1) + '%' : '—';
        const pr   = s.total > 0 ? v.pr.toFixed(1) + '%' : '—';
        const roi  = s.tansho_stake > 0 ? (s.tansho_ret/s.tansho_stake*100).toFixed(1) + '%' : '—';
        const roiCol = s.tansho_stake > 0 && s.tansho_ret/s.tansho_stake >= 1.0 ? '#16a34a' : (s.tansho_stake > 0 ? '#dc2626' : '');
        return `<tr>${td(m,false,true)}${td(s.total,true)}${td(wr,true,false,wrColors[m])}${td(p2r,true,false,p2rColors[m])}${td(pr,true,false,prColors[m])}${td(roi,true,false,roiCol)}</tr>`;
      }).join('') : '<tr><td colspan="6" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr>';

      // 券種別テーブル
      const ticketData = nd ? {} : (agg.by_ticket_type || {});
      const ticketBody = document.getElementById('results-ticket-body');
      const ticketRows = Object.keys(ticketData);
      ticketBody.innerHTML = ticketRows.length > 0 ? ticketRows.map(tt => {
        const s = ticketData[tt];
        const hr = s.total > 0 ? (s.hits/s.total*100).toFixed(1) + '%' : '—';
        const roi = s.stake > 0 ? (s.ret/s.stake*100).toFixed(1) + '%' : '—';
        const roiCol = s.stake > 0 && s.ret/s.stake >= 1 ? '#16a34a' : '#dc2626';
        return `<tr>${td(tt,false,true)}${td(s.total,true)}${td(hr,true)}${td(roi,true,true,roiCol)}</tr>`;
      }).join('') : '<tr><td colspan="4" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr>';

      // 自信度×券種別テーブル
      const confOrder = ['SS','S','A','B','C','D','E'];
      const ticketOrder = ['馬連','三連複'];
      const ctData = nd ? {} : (agg.by_conf_ticket || {});
      const confBody = document.getElementById('results-conf-body');
      const ctRows = [];
      for (const conf of confOrder) {
        for (const tt of ticketOrder) {
          const key = `${conf}_${tt}`;
          const s = ctData[key];
          if (!s || s.total === 0) continue;
          const hr  = (s.hits/s.total*100).toFixed(1) + '%';
          const roi = s.stake > 0 ? (s.ret/s.stake*100).toFixed(1) + '%' : '—';
          const roiCol = s.stake > 0 && s.ret/s.stake >= 1 ? '#16a34a' : '#dc2626';
          const hrCol  = parseFloat(hr) >= (tt === '馬連' ? 15 : 10) ? '#16a34a' : '';
          ctRows.push(`<tr>
            ${td(conf,false,true)} ${td(tt,false)}
            ${td(s.total,true)} ${td(s.hits,true)}
            ${td(hr,true,false,hrCol)}
            ${td(s.stake.toLocaleString(),true)} ${td(s.ret.toLocaleString(),true)}
            ${td(roi,true,true,roiCol)}
          </tr>`);
        }
      }
      confBody.innerHTML = ctRows.length > 0 ? ctRows.join('') : '<tr><td colspan="8" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr>';

      // 穴馬テーブル（馬券内率・複勝回収率）
      const anaData = nd ? null : (agg.by_ana || null);
      const anaBody = document.getElementById('results-ana-body');
      if (anaData && anaData.total > 0) {
        const aPr    = (anaData.place_rate  != null ? anaData.place_rate  : anaData.placed/anaData.total*100).toFixed(1) + '%';
        const aFRoi  = anaData.fukusho_roi  != null ? anaData.fukusho_roi.toFixed(1) + '%' : '—';
        const aPrCol  = parseFloat(aPr)   >= 30 ? '#16a34a' : '#b91c1c';
        const aFRoiCol = parseFloat(aFRoi) >= 80 ? '#16a34a' : '#b91c1c';
        anaBody.innerHTML = `<tr>
          ${td('穴馬',false,true)}${td(anaData.total,true)}
          ${td(aPr,true,false,aPrCol)}${td(aFRoi,true,false,aFRoiCol)}
        </tr>`;
      } else {
        anaBody.innerHTML = '<tr><td colspan="4" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr>';
      }

      // 危険馬テーブル（着外頭数付き）
      const kikenData = nd ? null : (agg.by_kiken || null);
      const kikenBody = document.getElementById('results-kiken-body');
      if (kikenData && kikenData.total > 0) {
        const kFr = (kikenData.fell_rate != null ? kikenData.fell_rate : kikenData.fell_through/kikenData.total*100).toFixed(1) + '%';
        const kFrCol = parseFloat(kFr) >= 60 ? '#16a34a' : '#b91c1c';
        kikenBody.innerHTML = `<tr>
          ${td('危険馬',false,true)}${td(kikenData.total,true)}
          ${td(kFr,true,false,kFrCol)}${td(kikenData.fell_through,true)}
        </tr>`;
      } else {
        kikenBody.innerHTML = '<tr><td colspan="4" style="text-align:center;padding:10px;color:#9ca3af">データなし</td></tr>';
      }

      // 日付別一覧
      const dateList = document.getElementById('results-date-list');
      if (byDate && byDate.length > 0) {
        dateList.innerHTML = byDate.map(r => {
          const profit = r.profit || 0;
          const roi = r.roi != null ? r.roi : 0;
          const roiOk  = roi >= 100;
          const roiStr = r.roi != null ? r.roi + '%' : '—';
          return `<div class="result-date-row">
            <span class="rd-date">${r.date}</span>
            <div class="rd-stats">
              <span>${r.total_races}R</span>
              <span>的中 ${r.hit_tickets || 0}/${r.total_tickets || 0}</span>
              <span>◎勝 ${(r.honmei_win_rate||0).toFixed(1)}%</span>
              <span>◎複 ${(r.honmei_rate||0).toFixed(1)}%</span>
              <span style="color:${profit>=0?'#16a34a':'#dc2626'}">${(profit>=0?'+':'')+profit.toLocaleString()}円</span>
              <span class="rd-roi ${roiOk?'':'loss'}">回収 ${roiStr}</span>
            </div>
          </div>`;
        }).join('');
      } else {
        dateList.innerHTML = '<p style="color:#9ca3af;font-style:italic;font-size:0.9rem">データなし</p>';
      }
    }

    // ===== Chart.js ROI / Monthly charts =====
    let _roiChart = null, _monthlyChart = null;

    function initRoiChart(labels, umRoi, sanRoi) {
      const canvas = document.getElementById('roi-chart');
      if (!canvas) return;
      if (_roiChart) { _roiChart.destroy(); _roiChart = null; }
      if (!labels || labels.length === 0) return;
      _roiChart = new Chart(canvas, {
        type: 'line',
        data: {
          labels: labels,
          datasets: [
            {
              label: '買い目回収率(%)',
              data: umRoi,
              borderColor: '#059669',
              backgroundColor: 'rgba(5,150,105,.08)',
              borderWidth: 2,
              fill: true,
              tension: 0.3,
              pointRadius: labels.length > 60 ? 0 : 3,
            },
            {
              label: '◎単勝回収率(%)',
              data: sanRoi,
              borderColor: '#D97706',
              backgroundColor: 'rgba(217,119,6,.05)',
              borderWidth: 2,
              fill: false,
              tension: 0.3,
              pointRadius: labels.length > 60 ? 0 : 3,
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: 'top', labels: { font: { size: 11 } } },
            tooltip: { mode: 'index', intersect: false }
          },
          scales: {
            x: { ticks: { font: { size: 10 }, maxTicksLimit: 12, maxRotation: 45 } },
            y: {
              ticks: { font: { size: 10 }, callback: v => v + '%' },
              grid: { color: 'rgba(0,0,0,.06)' }
            }
          }
        }
      });
    }

    function initMonthlyChart(labels, values) {
      const canvas = document.getElementById('monthly-chart');
      if (!canvas) return;
      if (_monthlyChart) { _monthlyChart.destroy(); _monthlyChart = null; }
      if (!labels || labels.length === 0) return;
      _monthlyChart = new Chart(canvas, {
        type: 'bar',
        data: {
          labels: labels,
          datasets: [{
            label: '月別収支(円)',
            data: values,
            backgroundColor: values.map(v => v >= 0 ? 'rgba(5,150,105,.7)' : 'rgba(220,38,38,.7)'),
            borderColor: values.map(v => v >= 0 ? '#059669' : '#DC2626'),
            borderWidth: 1,
            borderRadius: 4,
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: { callbacks: { label: ctx => (ctx.raw >= 0 ? '+' : '') + ctx.raw.toLocaleString() + '円' } }
          },
          scales: {
            x: { ticks: { font: { size: 10 } } },
            y: { ticks: { font: { size: 10 }, callback: v => (v >= 0 ? '+' : '') + v.toLocaleString() } }
          }
        }
      });
    }

    async function loadResultsCharts(yearFilter) {
      try {
        const yr = yearFilter || 'all';
        const r = await fetch(`/api/results/trend?year=${yr}`);
        if (!r.ok) return;
        const d = await r.json();
        if (d.error || !d.labels) return;
        initRoiChart(d.labels, d.ticket_roi_cum, d.honmei_tansho_roi_cum);
        initMonthlyChart(d.monthly_labels, d.monthly_profit);
      } catch(e) {
        console.warn('チャートデータ取得エラー:', e);
      }
    }

    // ===== 詳細分析 (競馬場別/コース別/距離区分別) =====
    let _detData = null;

    async function loadDetailedAnalysis(yearFilter) {
      const yr = yearFilter || 'all';
      document.getElementById('det-loading').style.display = '';
      document.getElementById('det-summary').style.display = 'none';
      try {
        const r = await fetch(`/api/results/detailed?year=${yr}`);
        _detData = await r.json();
      } catch(e) {
        console.error('詳細分析取得エラー:', e);
        document.getElementById('det-loading').textContent = '取得エラー';
        return;
      }
      document.getElementById('det-loading').style.display = 'none';
      // アクティブなカテゴリタブを確認して描画
      const activeTab = document.querySelector('.det-tab.active');
      renderDetailedCat(activeTab ? activeTab.dataset.cat : 'all');
    }

    function renderDetailedCat(cat) {
      if (!_detData) return;
      const d = _detData[cat];
      if (!d) return;

      // det-tab ボタンのスタイル更新
      document.querySelectorAll('.det-tab').forEach(b => {
        const isActive = b.dataset.cat === cat;
        b.style.borderBottom  = isActive ? '3px solid #2563eb' : 'none';
        b.style.color         = isActive ? '#2563eb' : '#6b7280';
        b.style.marginBottom  = isActive ? '-2px' : '0';
        b.classList.toggle('active', isActive);
      });

      // サマリー行
      const s = d.stats;
      const sumEl = document.getElementById('det-summary');
      sumEl.style.display = 'flex';
      document.getElementById('det-races').textContent    = (s.total_races || 0) + 'R';
      document.getElementById('det-hit-rate').textContent = (s.hit_rate   || 0).toFixed(1) + '%';
      const roiEl = document.getElementById('det-roi');
      roiEl.textContent = (s.roi || 0).toFixed(1) + '%';
      roiEl.style.color = (s.roi || 0) >= 100 ? '#16a34a' : '#dc2626';
      document.getElementById('det-u-hit').textContent = s.umaren ? (s.umaren.hit_rate||0).toFixed(1)+'%' : '—';
      document.getElementById('det-u-roi').textContent = s.umaren ? (s.umaren.roi||0).toFixed(1)+'%' : '—';
      document.getElementById('det-s-hit').textContent = s.sanrenpuku ? (s.sanrenpuku.hit_rate||0).toFixed(1)+'%' : '—';
      document.getElementById('det-s-roi').textContent = s.sanrenpuku ? (s.sanrenpuku.roi||0).toFixed(1)+'%' : '—';

      function roiColor(v) { return v >= 100 ? '#16a34a' : (v >= 80 ? '#92400e' : '#dc2626'); }

      // 共通テーブル行生成
      function makeDetailRow(label, st) {
        const u = st.umaren     || {};
        const sn = st.sanrenpuku || {};
        const uHit  = u.total > 0 ? (u.hit_rate||0).toFixed(0)+'%' : '—';
        const uRoi  = u.stake > 0 ? (u.roi||0).toFixed(0)+'%'       : '—';
        const snHit = sn.total > 0 ? (sn.hit_rate||0).toFixed(0)+'%' : '—';
        const snRoi = sn.stake > 0 ? (sn.roi||0).toFixed(0)+'%'      : '—';
        const uRoiC  = u.stake  > 0 ? roiColor(u.roi  || 0) : '';
        const snRoiC = sn.stake > 0 ? roiColor(sn.roi || 0) : '';
        return `<tr>
          <td style="padding:4px 6px;border:1px solid #e2e8f0;font-size:0.75rem">${label}</td>
          <td style="padding:4px 6px;border:1px solid #e2e8f0;text-align:right">${st.total_races||0}</td>
          <td style="padding:4px 6px;border:1px solid #e2e8f0;text-align:right">${uHit}</td>
          <td style="padding:4px 6px;border:1px solid #e2e8f0;text-align:right;font-weight:600;color:${uRoiC}">${uRoi}</td>
          <td style="padding:4px 6px;border:1px solid #e2e8f0;text-align:right">${snHit}</td>
          <td style="padding:4px 6px;border:1px solid #e2e8f0;text-align:right;font-weight:600;color:${snRoiC}">${snRoi}</td>
        </tr>`;
      }

      // 競馬場別テーブル
      const venueBody = document.getElementById('det-venue-body');
      const venues = d.by_venue || {};
      const venueKeys = Object.keys(venues).sort((a,b) => (venues[b].total_races||0) - (venues[a].total_races||0));
      venueBody.innerHTML = venueKeys.length > 0
        ? venueKeys.map(v => makeDetailRow(v, venues[v])).join('')
        : '<tr><td colspan="6" style="text-align:center;padding:8px;color:#9ca3af">データなし</td></tr>';

      // 芝/ダートテーブル
      const surfaceBody = document.getElementById('det-surface-body');
      const surfaces = d.by_surface || {};
      const surfaceKeys = ['芝', 'ダート'].filter(k => surfaces[k]);
      surfaceBody.innerHTML = surfaceKeys.length > 0
        ? surfaceKeys.map(k => makeDetailRow(k, surfaces[k])).join('')
        : '<tr><td colspan="6" style="text-align:center;padding:8px;color:#9ca3af">データなし</td></tr>';

      // 距離区分テーブル
      const distBody = document.getElementById('det-dist-body');
      const dists = d.by_dist_zone || {};
      const distOrder = ['SS','S','M','I','L','E'];
      const distLabels = {SS:'SS(〜1000)',S:'S(1001-1400)',M:'M(1401-1800)',I:'I(1801-2200)',L:'L(2201-2600)',E:'E(2601+)'};
      const distKeys = distOrder.filter(k => dists[k]);
      distBody.innerHTML = distKeys.length > 0
        ? distKeys.map(k => makeDetailRow(distLabels[k]||k, dists[k])).join('')
        : '<tr><td colspan="6" style="text-align:center;padding:8px;color:#9ca3af">データなし</td></tr>';

      // TOP10 高額配当
      function renderTop10(list, elId, isUmaren) {
        const el = document.getElementById(elId);
        if (!list || list.length === 0) {
          el.innerHTML = '<p style="color:#9ca3af;padding:4px">データなし</p>';
          return;
        }
        // カテゴリフィルタ（all以外はis_jraで絞り込み）
        let filtered = list;
        if (cat === 'jra') filtered = list.filter(x => x.is_jra);
        if (cat === 'nar') filtered = list.filter(x => !x.is_jra);
        filtered = filtered.slice(0, 10);
        if (filtered.length === 0) {
          el.innerHTML = '<p style="color:#9ca3af;padding:4px">データなし</p>';
          return;
        }
        el.innerHTML = filtered.map((x, i) => `
          <div style="display:flex;gap:6px;align-items:baseline;padding:3px 0;border-bottom:1px solid #f3f4f6;flex-wrap:wrap">
            <span style="color:#9ca3af;min-width:16px;font-size:0.72rem">${i+1}.</span>
            <span style="font-weight:700;color:#92400e;min-width:68px">${x.payout.toLocaleString()}円</span>
            <span style="color:#374151">${x.date} ${x.venue}${x.race_no}R</span>
            <span style="color:#7c3aed;font-weight:600">${x.marks || x.combo}</span>
            <span style="color:#6b7280;font-size:0.72rem">${x.race_name||''}</span>
          </div>`).join('');
      }
      renderTop10(_detData.top10_umaren,     'det-top10-umaren',     true);
      renderTop10(_detData.top10_sanrenpuku, 'det-top10-sanrenpuku', false);
    }

    // 詳細タブ ボタン クリックハンドラ
    document.querySelectorAll('.det-tab').forEach(b => {
      b.onclick = () => renderDetailedCat(b.dataset.cat);
    });

    // ===== 結果照合モード切り替え =====
    function setFetchMode(mode) {
      document.querySelectorAll('.fetch-mode-btn').forEach(b => b.classList.remove('active'));
      document.getElementById('fetch-mode-' + mode).classList.add('active');
      document.getElementById('fetch-single-area').style.display = mode === 'single' ? 'flex' : 'none';
      document.getElementById('fetch-range-area').style.display = mode === 'range' ? 'flex' : 'none';
      document.getElementById('fetch-err').textContent = '';
      document.getElementById('fetch-progress').style.display = 'none';
    }

    // ===== 単一日付照合 =====
    document.getElementById('btn-fetch-results').onclick = async () => {
      const date = document.getElementById('fetch-date-select').value;
      if (!date) { alert('日付を選択してください'); return; }
      const btn = document.getElementById('btn-fetch-results');
      btn.disabled = true;
      document.getElementById('fetch-err').textContent = '';
      document.getElementById('fetch-date-log').style.display = 'none';
      document.getElementById('fetch-date-log').innerHTML = '';
      try {
        await _fetchOneDate(date, 0, 1);
        document.getElementById('fetch-fill').style.width = '100%';
        document.getElementById('fetch-status').textContent = '照合完了！成績を更新しました。';
        const activeYear = document.querySelector('.sub-tab.active');
        loadResultsSummary(activeYear ? activeYear.dataset.year : 'all');
        setTimeout(() => { document.getElementById('fetch-progress').style.display = 'none'; }, 2000);
      } catch(e) {
        document.getElementById('fetch-err').textContent = e.message;
        document.getElementById('fetch-progress').style.display = 'none';
      } finally {
        btn.disabled = false;
      }
    };

    // ===== 期間一括照合 =====
    document.getElementById('btn-fetch-range').onclick = async () => {
      const from = document.getElementById('fetch-from-date').value;
      const to   = document.getElementById('fetch-to-date').value;
      if (!from || !to) { alert('開始日と終了日を入力してください'); return; }
      if (from > to)    { alert('開始日は終了日以前にしてください'); return; }
      const dates = (await _ensurePredDates()).filter(d => d >= from && d <= to);
      if (dates.length === 0) {
        document.getElementById('fetch-err').textContent = '指定範囲に予想データがありません';
        return;
      }
      await _fetchDateList(dates);
    };

    // ===== 未照合のみ照合 =====
    document.getElementById('btn-fetch-unmatched').onclick = async () => {
      const from = document.getElementById('fetch-from-date').value;
      const to   = document.getElementById('fetch-to-date').value;
      document.getElementById('fetch-err').textContent = '';
      try {
        const ur = await fetch('/api/results/unmatched_dates');
        const uj = await ur.json();
        let unmatched = uj.dates || [];
        if (from) unmatched = unmatched.filter(d => d >= from);
        if (to)   unmatched = unmatched.filter(d => d <= to);
        if (unmatched.length === 0) {
          document.getElementById('fetch-progress').style.display = 'block';
          document.getElementById('fetch-date-log').style.display = 'none';
          document.getElementById('fetch-status').textContent = '指定範囲に未照合の日付はありません';
          document.getElementById('fetch-fill').style.width = '100%';
          setTimeout(() => { document.getElementById('fetch-progress').style.display = 'none'; }, 2000);
          return;
        }
        await _fetchDateList(unmatched);
      } catch(e) {
        document.getElementById('fetch-err').textContent = e.message;
      }
    };

    // ===== 配布用HTML生成 =====
    document.getElementById('btn-simple-all').onclick = async () => {
      const dates = await _ensurePredDates();
      if (dates.length > 0) {
        document.getElementById('simple-from-date').value = dates[dates.length - 1];
        document.getElementById('simple-to-date').value   = dates[0];
      }
    };

    document.getElementById('btn-gen-simple').onclick = async () => {
      const from = document.getElementById('simple-from-date').value;
      const to   = document.getElementById('simple-to-date').value;
      document.getElementById('simple-err').textContent = '';
      const dates = (await _ensurePredDates()).filter(d => (!from || d >= from) && (!to || d <= to));
      if (dates.length === 0) {
        document.getElementById('simple-err').textContent = '指定範囲に予想データがありません';
        return;
      }
      const btn = document.getElementById('btn-gen-simple');
      btn.disabled = true;
      const prog  = document.getElementById('simple-progress');
      const fill  = document.getElementById('simple-fill');
      const stat  = document.getElementById('simple-status');
      const log   = document.getElementById('simple-date-log');
      prog.style.display = 'block';
      log.style.display = 'block';
      log.innerHTML = '';
      let ok = 0, ng = 0;
      for (let i = 0; i < dates.length; i++) {
        const date = dates[i];
        fill.style.width = Math.round((i / dates.length) * 100) + '%';
        stat.textContent = `[${i+1}/${dates.length}] ${date} を生成中...`;
        try {
          const r = await fetch('/api/generate_simple_html', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({date})
          });
          const j = await r.json();
          const line = document.createElement('div');
          line.style.padding = '1px 0';
          if (j.ok) {
            ok++;
            line.style.color = '#166534';
            line.textContent = '✓ ' + date + ' → ' + (j.filename || '');
          } else {
            ng++;
            line.style.color = '#991b1b';
            line.textContent = '✗ ' + date + ': ' + (j.error || 'エラー');
          }
          log.appendChild(line);
          log.scrollTop = log.scrollHeight;
        } catch(e) {
          ng++;
          const line = document.createElement('div');
          line.style.cssText = 'padding:1px 0;color:#991b1b';
          line.textContent = '✗ ' + date + ': ' + e.message;
          log.appendChild(line);
        }
      }
      fill.style.width = '100%';
      stat.textContent = `完了: ${ok}件生成${ng > 0 ? ' / ' + ng + '件失敗' : ''} (全${dates.length}件)`;
      btn.disabled = false;
    };

    // ===== 共通: 1日分照合 =====
    async function _fetchOneDate(date, idx, total) {
      document.getElementById('fetch-progress').style.display = 'block';
      const pct = Math.round(idx / total * 100);
      document.getElementById('fetch-fill').style.width = pct + '%';
      document.getElementById('fetch-status').textContent =
        total > 1 ? `[${idx+1}/${total}] ${date} を照合中...` : `${date} の着順を取得中... (数分かかります)`;
      const r = await fetch('/api/results/fetch', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({date})
      });
      return await r.json();
    }

    // ===== 共通: 複数日まとめて照合 =====
    async function _fetchDateList(dates) {
      const rangeBtn    = document.getElementById('btn-fetch-range');
      const unmatchBtn  = document.getElementById('btn-fetch-unmatched');
      rangeBtn.disabled = true;
      unmatchBtn.disabled = true;
      document.getElementById('fetch-err').textContent = '';
      document.getElementById('fetch-progress').style.display = 'block';
      const log = document.getElementById('fetch-date-log');
      log.style.display = 'block';
      log.innerHTML = '';
      let ok = 0, ng = 0;
      for (let i = 0; i < dates.length; i++) {
        const date = dates[i];
        try {
          const j = await _fetchOneDate(date, i, dates.length);
          const line = document.createElement('div');
          line.style.padding = '1px 0';
          if (j.ok) {
            ok++;
            line.style.color = '#166534';
            line.textContent = '✓ ' + date;
          } else {
            ng++;
            line.style.color = '#991b1b';
            line.textContent = '✗ ' + date + ': ' + (j.error || 'エラー');
          }
          log.appendChild(line);
          log.scrollTop = log.scrollHeight;
        } catch(e) {
          ng++;
          const line = document.createElement('div');
          line.style.cssText = 'padding:1px 0;color:#991b1b';
          line.textContent = '✗ ' + date + ': ' + e.message;
          log.appendChild(line);
        }
      }
      document.getElementById('fetch-fill').style.width = '100%';
      document.getElementById('fetch-status').textContent =
        `完了: ${ok}日成功${ng > 0 ? ' / ' + ng + '日失敗' : ''} (全${dates.length}日)`;
      rangeBtn.disabled = false;
      unmatchBtn.disabled = false;
      const activeYear = document.querySelector('.sub-tab.active');
      loadResultsSummary(activeYear ? activeYear.dataset.year : 'all');
    }

    // ===== 過去の予想 =====
    let _pastAnalyzeRunning = false;

    async function pastRunSingle(){
      const dateVal = document.getElementById('past-create-date').value;
      if(!dateVal){ alert('日付を入力してください'); return; }
      if(_pastAnalyzeRunning){ alert('現在分析が実行中です'); return; }
      _pastAnalyzeRunning = true;
      document.getElementById('btn-past-single').disabled = true;
      document.getElementById('past-create-status').style.display = 'block';
      await _pastRunDate(dateVal, 1, 1);
      _pastAnalyzeRunning = false;
      document.getElementById('btn-past-single').disabled = false;
    }

    async function pastRunRange(){
      const start = document.getElementById('past-range-start').value;
      const end   = document.getElementById('past-range-end').value;
      if(!start||!end){ alert('開始日と終了日を入力してください'); return; }
      if(_pastAnalyzeRunning){ alert('現在分析が実行中です'); return; }
      const dates=[];
      const d=new Date(start+'T00:00:00'), endD=new Date(end+'T00:00:00');
      while(d<=endD){ dates.push(_localDate(d)); d.setDate(d.getDate()+1); }
      if(!dates.length){ alert('有効な期間を指定してください'); return; }
      if(dates.length>14){ alert(dates.length+'日分は範囲が大きすぎます（最大14日）'); return; }
      _pastAnalyzeRunning = true;
      document.getElementById('btn-past-range').disabled = true;
      document.getElementById('past-create-status').style.display = 'block';
      for(let i=0;i<dates.length;i++){
        await _pastRunDate(dates[i], i+1, dates.length);
      }
      document.getElementById('past-create-progress').textContent = '✓ '+dates.length+'日分の分析完了';
      document.getElementById('past-create-elapsed').textContent = '';
      _pastAnalyzeRunning = false;
      document.getElementById('btn-past-range').disabled = false;
    }

    async function _pastRunDate(dateVal, idx, total){
      const progressEl = document.getElementById('past-create-progress');
      const elapsedEl  = document.getElementById('past-create-elapsed');
      progressEl.textContent = '['+idx+'/'+total+'] '+dateVal+' 開始中...';
      elapsedEl.textContent = '';
      try{
        const r = await fetch('/api/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({date:dateVal})});
        const j = await r.json();
        if(!j.ok){ progressEl.textContent='['+idx+'/'+total+'] '+dateVal+' エラー: '+(j.error||'不明'); return; }
        const startTs = Date.now();
        await new Promise(resolve=>{
          const poll=()=>{
            fetch('/api/analyze_status').then(r=>r.json()).then(j2=>{
              const elapsed=Math.round((Date.now()-startTs)/1000);
              elapsedEl.textContent='('+elapsed+'秒)';
              if(j2.done){
                progressEl.textContent='['+idx+'/'+total+'] '+dateVal+(j2.error?' エラー':' ✓ 完了');
                resolve();
              } else {
                progressEl.textContent='['+idx+'/'+total+'] '+dateVal+' '+(j2.progress||'分析中...');
                setTimeout(poll,2000);
              }
            }).catch(()=>setTimeout(poll,3000));
          };
          poll();
        });
      }catch(e){ progressEl.textContent='エラー: '+e.message; }
    }

    async function loadPastPrediction(){
      const dateVal = document.getElementById('past-date-input').value;
      if(!dateVal){ alert('日付を入力してください'); return; }
      document.getElementById('past-loading').style.display='inline';
      const resultEl=document.getElementById('past-result');
      resultEl.innerHTML='';
      try{
        const res = await fetch('/api/today_predictions?date='+dateVal);
        const data = await res.json();
        if(!data.total){
          resultEl.innerHTML=`<div style="padding:16px;background:#fffbeb;border:1px solid #fde68a;border-radius:8px">
            <p style="color:#92400e;margin:0 0 10px;font-weight:600">この日（${dateVal}）の予想データがありません</p>
            <p style="color:#6b7280;font-size:0.85rem;margin:0">上の「分析作成」セクションで分析を実行してください。</p>
          </div>`;
          return;
        }
        const order=data.order||Object.keys(data.races||{});
        // 競馬場タブ
        let tabsHtml='<div style="display:flex;gap:2px;flex-wrap:wrap;border-bottom:2px solid #dcfce7;margin-bottom:12px">';
        for(let i=0;i<order.length;i++){
          const v=order[i], cnt=(data.races[v]||[]).length;
          const isFirst=i===0;
          const base='padding:8px 14px;text-decoration:none;border-radius:6px 6px 0 0;font-size:0.88rem;font-weight:600;cursor:pointer';
          const active=isFirst?base+';background:#dcfce7;color:#166534;border:1px solid #bbf7d0;border-bottom:2px solid #fff;margin-bottom:-2px':base+';color:#4b5563';
          tabsHtml+=`<a href="#" style="${active}" onclick="pastSwitchVenue(${i},event)" data-pastvidx="${i}">${v} <span style="font-size:0.75rem;color:#9ca3af;font-weight:400">${cnt}R</span></a>`;
        }
        tabsHtml+='</div>';
        // レースカードパネル
        let cardsHtml='';
        for(let i=0;i<order.length;i++){
          const v=order[i], races=data.races[v]||[];
          cardsHtml+=`<div id="past-venue-panel-${i}" style="display:${i===0?'block':'none'}"><div style="display:grid;gap:5px">`;
          for(const r of races){
            const c=(r.overall_confidence||'').replace('\u207a','+');
            const cBg=c==='SS'?'background:#f0fdf4;border:1.5px solid #86efac':c==='S'?'background:#eff6ff;border:1px solid #bfdbfe':c==='A'?'background:#fef2f2;border:1px solid #fecaca':'background:#fff;border:1px solid #e5e7eb';
            const badge=c?`<span style="background:${c==='SS'?'linear-gradient(135deg,#16a34a,#15803d)':c==='S'?'#1a6fa8':c==='A'?'#c0392b':c==='B'||c==='C'?'#333':'#aaa'};color:#fff;padding:1px 8px;border-radius:10px;font-size:0.78rem;font-weight:700">${c}</span>`:'';
            const _cd=(r.surface||'')+(r.distance?r.distance+'m':'');
            cardsHtml+=`<a href="${r.url}" target="_blank" style="display:flex;align-items:center;gap:8px;padding:8px 12px;${cBg};border-radius:7px;text-decoration:none;color:inherit">
              ${badge}
              <span style="color:#0d2b5e;font-weight:700;min-width:28px">${r.race_no}R</span>
              <span style="font-size:0.88rem;color:#1a1a2e;flex:1">${r.name||r.race_no+'R'}${_cd?`<span style="font-size:0.78rem;color:#9ca3af;margin-left:5px">${_cd}</span>`:''}</span>
              ${r.honmei_name?`<span style="font-size:0.82rem;color:#374151;white-space:nowrap">${r.honmei_mark||'◎'} ${r.honmei_name}</span>`:''}
            </a>`;
          }
          cardsHtml+='</div></div>';
        }
        resultEl.innerHTML=tabsHtml+cardsHtml;
      }catch(e){
        resultEl.innerHTML=`<p class="error">エラー: ${e.message}</p>`;
      }finally{
        document.getElementById('past-loading').style.display='none';
      }
    }

    function pastSwitchVenue(idx, e){
      e.preventDefault();
      const base='padding:8px 14px;text-decoration:none;border-radius:6px 6px 0 0;font-size:0.88rem;font-weight:600;cursor:pointer';
      document.querySelectorAll('[data-pastvidx]').forEach(el=>{
        const i=parseInt(el.dataset.pastvidx);
        el.style.cssText=i===idx?base+';background:#dcfce7;color:#166534;border:1px solid #bbf7d0;border-bottom:2px solid #fff;margin-bottom:-2px':base+';color:#4b5563';
      });
      document.querySelectorAll('[id^="past-venue-panel-"]').forEach(el=>{ el.style.display='none'; });
      const t=document.getElementById('past-venue-panel-'+idx);
      if(t) t.style.display='block';
    }

    // ===== HOME 注目レース（A以上ピックアップ） =====
    async function loadHighConfidence(){
      // _homeDate の日付で取得（日付ナビと連動）
      const targetDate = _homeDate;
      const today = _localDate();
      try{
        const el = document.getElementById('home-high-conf-list');
        const cnt = document.getElementById('high-conf-count');
        const card = document.getElementById('home-high-conf-card');
        // 今日以外の日付はスキャン結果から取得
        const predRes = await fetch('/api/today_predictions?date='+targetDate);
        const predData = await predRes.json();
        const order = predData.order||[];
        if(!order.length){
          if(card) card.style.display='none';
          cnt.textContent='';
          return;
        }
        // 自信度A以上をフィルタ
        const confLevel = {'SS':6,'S+':5,'S':4,'A+':3,'A':2,'B+':1,'B':0,'C':0,'D':0,'E':0};
        const getLevel = c=>(confLevel[c.replace('\u207a','+')]||0);
        let highRaces=[];
        for(const venue of order){
          for(const r of (predData.races[venue]||[])){
            const c=r.overall_confidence||'';
            if(getLevel(c)>=2) highRaces.push({...r,venue});
          }
        }
        if(!highRaces.length){
          if(card) card.style.display='none';
          cnt.textContent='';
          return;
        }
        if(card) card.style.display='block';
        cnt.textContent=highRaces.length+'件';
        // 会場別にグループ化
        const byVenue={};
        for(const r of highRaces){
          if(!byVenue[r.venue]) byVenue[r.venue]=[];
          byVenue[r.venue].push(r);
        }
        // order順に会場を並べる（自信度別スタイリング）
        const _confOrder={'SS':0,'S':1,'A':2,'B':3,'C':4,'D':5,'E':6};
        const _confBadge=(c)=>{
          if(c==='SS') return `<span style="background:linear-gradient(135deg,#16a34a,#15803d);color:#fff;padding:3px 10px;border-radius:12px;font-size:0.86rem;font-weight:800;white-space:nowrap;min-width:34px;text-align:center;box-shadow:0 2px 6px rgba(22,163,74,0.45);letter-spacing:0.5px">${c}</span>`;
          if(c==='S') return `<span style="background:#1a6fa8;color:#fff;padding:2px 9px;border-radius:12px;font-size:0.82rem;font-weight:700;white-space:nowrap;min-width:30px;text-align:center;box-shadow:0 1px 3px rgba(26,111,168,0.3)">${c}</span>`;
          if(c==='A') return `<span style="background:#c0392b;color:#fff;padding:2px 9px;border-radius:12px;font-size:0.82rem;font-weight:700;white-space:nowrap;min-width:30px;text-align:center">${c}</span>`;
          if(c==='B'||c==='C') return `<span style="background:#333;color:#fff;padding:1px 8px;border-radius:12px;font-size:0.78rem;font-weight:700;white-space:nowrap;min-width:28px;text-align:center">${c}</span>`;
          return `<span style="background:#aaa;color:#fff;padding:1px 8px;border-radius:12px;font-size:0.78rem;font-weight:700;white-space:nowrap;min-width:28px;text-align:center">${c||'—'}</span>`;
        };
        const _confCard=(c)=>{
          if(c==='SS') return 'background:#f0fdf4;border:1.5px solid #86efac';
          if(c==='S') return 'background:#eff6ff;border:1px solid #bfdbfe';
          if(c==='A') return 'background:#fef2f2;border:1px solid #fecaca';
          return 'background:#fff;border:1px solid #e5e7eb';
        };
        let html='';
        for(const venue of order){
          if(!byVenue[venue]) continue;
          // 自信度降順 → レース番号順でソート
          byVenue[venue].sort((a,b)=>{
            const ca=_confOrder[(a.overall_confidence||'').replace('\u207a','+')] ?? 99;
            const cb=_confOrder[(b.overall_confidence||'').replace('\u207a','+')] ?? 99;
            return ca!==cb ? ca-cb : a.race_no-b.race_no;
          });
          html+=`<div style="margin-bottom:10px"><div style="font-weight:700;color:#166534;font-size:0.88rem;margin-bottom:4px;padding:3px 8px;background:#dcfce7;border-radius:4px;display:inline-block">${venue}</div>`;
          html+='<div style="display:grid;gap:4px">';
          for(const r of byVenue[venue]){
            const c=(r.overall_confidence||'').replace('\u207a','+');
            const _courseDist=(r.surface||'')+(r.distance?r.distance+'m':'');
            html+=`<a href="${r.url}" target="_blank" style="display:flex;align-items:center;gap:8px;padding:7px 10px;${_confCard(c)};border-radius:6px;text-decoration:none;color:inherit">
              ${_confBadge(c)}
              <span style="color:#0d2b5e;font-weight:700;min-width:28px">${r.race_no}R</span>
              <span style="font-size:0.88rem;color:#1a1a2e;flex:1">${r.name||r.race_no+'R'}${_courseDist?`<span style="font-size:0.78rem;color:#9ca3af;margin-left:5px">${_courseDist}</span>`:''}</span>
              ${r.honmei_name?`<span style="font-size:0.82rem;color:#374151;white-space:nowrap">${r.honmei_mark||'◎'} ${r.honmei_name}</span>`:''}
            </a>`;
          }
          html+='</div></div>';
        }
        el.innerHTML=html;
      }catch(e){
        document.getElementById('home-high-conf-list').innerHTML=`<p class="empty">データ取得失敗: ${e.message}</p>`;
      }
    }

    // today タブ表示時にA以上ピックアップを読み込む
    const _todayTabEl = document.querySelector('[data-tab="today"]');
    if(_todayTabEl) _todayTabEl.addEventListener('click', loadHighConfidence);
    // loadHighConfidence() is called during initialization

    // ===== ⓪ HOME データ更新 =====
    async function homeDataUpdate(mode){
      const today = _localDate();
      const statusEl = document.getElementById('home-update-status');
      if(mode==='today'){
        document.getElementById('btn-home-update-today').disabled=true;
        statusEl.textContent='本日の予想を実行中…';
        try{
          const res=await fetch('/api/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({date:today})});
          const d=await res.json();
          statusEl.textContent=d.ok?'✓ 分析開始しました。下の分析設定パネルで進捗を確認してください。':'エラー: '+(d.error||'不明');
        }catch(e){statusEl.textContent='エラー: '+e.message;}
        finally{const b=document.getElementById('btn-home-update-today');if(b)b.disabled=false;}
      } else if(mode==='odds'){
        const selDate = document.getElementById('home-date-label')?.textContent||today;
        // home-date-label は "2026-03-01" 形式
        const dateVal = selDate.match(/\d{4}-\d{2}-\d{2}/) ? selDate.match(/\d{4}-\d{2}-\d{2}/)[0] : today;
        document.getElementById('btn-home-update-odds').disabled=true;
        statusEl.textContent=`${dateVal} のオッズを取得中…`;
        try{
          const res=await fetch('/api/odds_update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({date:dateVal})});
          const d=await res.json();
          if(d.ok){
            statusEl.textContent='オッズ取得中…';
            const poll=setInterval(async()=>{
              try{
                const s=await(await fetch('/api/odds_update_status')).json();
                if(s.done||!s.running){
                  clearInterval(poll);
                  if(s.error){statusEl.textContent='エラー: '+s.error;}
                  else{statusEl.textContent='✓ '+(s.count||0)+'R分オッズ更新完了 '+(s.updated_at||'');}
                  setTimeout(()=>loadHomeRaces(true),1000);
                } else {
                  statusEl.textContent='取得中…';
                }
              }catch(e){clearInterval(poll);}
            },1500);
          } else {
            statusEl.textContent='エラー: '+(d.error||'不明');
          }
        }catch(e){statusEl.textContent='エラー: '+e.message;}
        finally{document.getElementById('btn-home-update-odds').disabled=false;}
      } else {
        document.getElementById('btn-home-update-results').disabled=true;
        statusEl.textContent='未照合の結果を取得中…';
        try{
          const r1=await fetch('/api/results/unmatched_dates');
          const {dates}=await r1.json();
          if(!dates.length){ statusEl.textContent='未照合の日付はありません'; return; }
          statusEl.textContent=`${dates.length}日分を取得中…`;
          let ok=0;
          for(const d of dates){
            await fetch('/api/results/fetch',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({date:d})});
            ok++;
            statusEl.textContent=`${ok}/${dates.length}日完了…`;
          }
          statusEl.textContent=`✓ ${ok}日分の結果を取得しました`;
        }catch(e){statusEl.textContent='エラー: '+e.message;}
        finally{document.getElementById('btn-home-update-results').disabled=false;}
      }
    }

    // ===== ④ データベース =====
    // サブタブ切り替え
    document.querySelectorAll('.db-sub').forEach(t=>{
      t.addEventListener('click', e=>{
        e.preventDefault();
        document.querySelectorAll('.db-sub').forEach(x=>{
          x.style.background='transparent'; x.style.color='#4b5563'; x.style.fontWeight='normal';
        });
        t.style.background='#dcfce7'; t.style.color='#166534'; t.style.fontWeight='600';
        ['jockey','trainer','course'].forEach(s=>{
          document.getElementById('db-panel-'+s).style.display = s===t.dataset.dbsub ? 'block':'none';
        });
        // コースタブ初回表示時にJRAを自動ロード
        if(t.dataset.dbsub === 'course'){
          const cards = document.getElementById('course-venue-cards');
          if(cards && cards.children.length === 0) courseSetRegion('JRA');
        }
      });
    });

    /* ── 成績集計テーブル共通 ── */
    function _rateColor(rate){
      if(rate>=20) return '#15803d';
      if(rate>=10) return '#166534';
      if(rate>=5)  return '#374151';
      return '#9ca3af';
    }

    // DB フィルタ状態
    const _dbFilter = {
      jockey:  { jra_nar: '', surface: '', smile: '' },
      trainer: { jra_nar: '', surface: '', smile: '' },
    };

    function _dbFilterLabel(ptype){
      const f = _dbFilter[ptype];
      const parts = [];
      if(f.jra_nar) parts.push(f.jra_nar);
      if(f.surface) parts.push(f.surface);
      if(f.smile)   parts.push('距離:'+f.smile);
      return parts.length ? parts.join(' / ') : '全体';
    }

    function _dbUpdateFilterButtons(ptype){
      const f = _dbFilter[ptype];
      const p = ptype==='jockey' ? 'j' : 't';
      const activeStyle = 'padding:4px 12px;border-radius:20px;border:1px solid #166534;background:#166534;color:#fff;font-size:0.82rem;cursor:pointer';
      const inactStyle  = 'padding:4px 12px;border-radius:20px;border:1px solid #d1d5db;background:#fff;color:#374151;font-size:0.82rem;cursor:pointer';
      const actSmStyle  = 'padding:4px 10px;border-radius:20px;border:1px solid #166534;background:#166534;color:#fff;font-size:0.78rem;cursor:pointer';
      const inSmStyle   = 'padding:4px 10px;border-radius:20px;border:1px solid #d1d5db;background:#fff;color:#374151;font-size:0.78rem;cursor:pointer';
      // JRA/NAR
      document.getElementById(`db-${p}-jn-all`).style.cssText = !f.jra_nar ? activeStyle : inactStyle;
      document.getElementById(`db-${p}-jn-jra`).style.cssText = f.jra_nar==='JRA' ? activeStyle : inactStyle;
      document.getElementById(`db-${p}-jn-nar`).style.cssText = f.jra_nar==='NAR' ? activeStyle : inactStyle;
      // Surface
      document.getElementById(`db-${p}-sf-all`).style.cssText = !f.surface ? activeStyle : inactStyle;
      document.getElementById(`db-${p}-sf-t`).style.cssText   = f.surface==='芝' ? activeStyle : inactStyle;
      document.getElementById(`db-${p}-sf-d`).style.cssText   = f.surface==='ダート' ? activeStyle : inactStyle;
      // SMILE
      for(const sm of ['all','SS','S','M','I','L','E']){
        const val = sm==='all' ? '' : sm;
        const el = document.getElementById(`db-${p}-sm-${sm}`);
        if(el) el.style.cssText = f.smile===val ? actSmStyle : inSmStyle;
      }
      // フィルタラベル
      const lbl = document.getElementById(`db-${ptype}-filter-label`);
      if(lbl) lbl.textContent = _dbFilterLabel(ptype);
    }

    function dbSetJraNar(ptype, val){
      _dbFilter[ptype].jra_nar = val;
      _dbUpdateFilterButtons(ptype);
      if(ptype==='jockey') loadJockeyDB(); else loadTrainerDB();
    }
    function dbSetSurface(ptype, val){
      _dbFilter[ptype].surface = val;
      _dbUpdateFilterButtons(ptype);
      if(ptype==='jockey') loadJockeyDB(); else loadTrainerDB();
    }
    function dbSetSmile(ptype, val){
      _dbFilter[ptype].smile = val;
      _dbUpdateFilterButtons(ptype);
      if(ptype==='jockey') loadJockeyDB(); else loadTrainerDB();
    }

    // 所属バッジ
    const _locationBadge = (loc) => {
      if(!loc) return '';
      const colorMap = {
        '美浦':'#1d4ed8','栗東':'#b45309',
        '大井':'#0f766e','船橋':'#0f766e','川崎':'#0f766e','浦和':'#0f766e',
        '門別':'#4338ca','盛岡':'#4338ca','水沢':'#4338ca',
        '金沢':'#4338ca','笠松':'#4338ca','名古屋':'#4338ca',
        '園田':'#4338ca','姫路':'#4338ca','高知':'#4338ca','佐賀':'#4338ca','帯広':'#4338ca','帯広(ばんえい)':'#4338ca',
      };
      const c = colorMap[loc]||'#6b7280';
      return `<span style="background:${c};color:#fff;font-size:0.68rem;padding:1px 5px;border-radius:8px;margin-left:4px;vertical-align:middle;white-space:nowrap">${loc}</span>`;
    };

    function _buildAggTable(persons, label, ptype){
      if(!persons.length) return '<p class="empty">データがありません（指定条件で実績なし）</p>';
      // 複合偏差値計算（出走10以上を対象）
      // 勝率40% + 複勝率30% + 回収率30% の加重Z-score + サンプルサイズ補正
      const MIN_N = 10;
      const validForDev = persons.filter(p => (p.total||0) >= MIN_N);
      function _calcStats(arr, fn) {
        if (!arr.length) return {mean:0, std:1};
        const vals = arr.map(fn);
        const mean = vals.reduce((s,v)=>s+v,0)/vals.length;
        const variance = vals.reduce((s,v)=>s+(v-mean)**2,0)/vals.length;
        return {mean, std: Math.sqrt(variance)||1};
      }
      const wrStats  = _calcStats(validForDev, p => +p.win_rate||0);
      const p3Stats  = _calcStats(validForDev, p => +p.place3_rate||0);
      const roiStats = _calcStats(validForDev.filter(p=>p.roi!=null), p => +p.roi||0);
      function _calcDev(p) {
        if ((p.total||0) < MIN_N) return null;
        const wrZ  = ((+p.win_rate||0) - wrStats.mean) / wrStats.std;
        const p3Z  = ((+p.place3_rate||0) - p3Stats.mean) / p3Stats.std;
        const roiZ = p.roi != null ? ((+p.roi||0) - roiStats.mean) / roiStats.std : 0;
        const roiW = p.roi != null ? 0.20 : 0;
        const w1 = 0.30, w2 = 0.50, w3 = roiW;
        const wSum = w1 + w2 + w3;
        const composite = (w1*wrZ + w2*p3Z + w3*roiZ) / wSum;
        // サンプルサイズ補正: 出走数が少ないほど50寄りに (30騎乗で信頼度80%, 100騎乗で98%)
        const reliability = 1 - Math.exp(-p.total / 40);
        return Math.round(50 + 10 * composite * reliability);
      }
      const _devColor = d => d >= 60 ? '#7c3aed' : d >= 55 ? '#1d4ed8' : d >= 45 ? '#374151' : '#9ca3af';
      const rows = persons.map(p=>{
        const wr  = p.win_rate    != null ? (+p.win_rate).toFixed(1)+'%'    : '—';
        const p2r = p.place2_rate != null ? (+p.place2_rate).toFixed(1)+'%' : '—';
        const p3r = p.place3_rate != null ? (+p.place3_rate).toFixed(1)+'%' : '—';
        const roi = p.roi != null ? (+p.roi).toFixed(1)+'%' : '—';
        const lose = p.total - (p.place3||0);
        const record = `${p.win||0}-${(p.place2||0)-(p.win||0)}-${(p.place3||0)-(p.place2||0)}-${Math.max(0,lose)}`;
        const dev = _calcDev(p);
        const devStr = dev != null ? dev : '—';
        const roiColor = p.roi != null ? (p.roi >= 100 ? '#059669' : p.roi >= 80 ? '#374151' : '#9ca3af') : '#9ca3af';
        return `<tr style="cursor:pointer;transition:background 0.1s" onmouseover="this.style.background='#f0fdf4'" onmouseout="this.style.background=''" onclick="showPersonnelDetail('${ptype}','${p.id.replace(/'/g,"\\'")}','${p.name.replace(/'/g,"\\'")}',${dev!=null?dev:'null'})">
          <td style="padding:5px 8px;font-weight:600;color:#166534">${p.name}${_locationBadge(p.location||'')}</td>
          <td style="padding:5px 8px;text-align:center;color:#9ca3af;font-size:0.78rem">${p.id}</td>
          <td style="padding:5px 8px;text-align:right;font-weight:600">${p.total}</td>
          <td style="padding:5px 8px;text-align:right;font-size:0.8rem;color:#6b7280">${record}</td>
          <td style="padding:5px 8px;text-align:right;color:${_rateColor(p.win_rate)};font-weight:600">${wr}</td>
          <td style="padding:5px 8px;text-align:right;color:${_rateColor(p.place2_rate)}">${p2r}</td>
          <td style="padding:5px 8px;text-align:right;color:${_rateColor(p.place3_rate)}">${p3r}</td>
          <td style="padding:5px 8px;text-align:right;color:${roiColor}">${roi}</td>
          <td style="padding:5px 8px;text-align:right;color:${_devColor(dev||50)};font-size:0.82rem">${devStr}</td>
        </tr>`;
      }).join('');
      return `<table style="width:100%;border-collapse:collapse;font-size:0.86rem">
        <thead><tr style="background:#f0fdf4;color:#374151;font-size:0.82rem">
          <th style="padding:6px 8px;text-align:left">${label}名</th>
          <th style="padding:6px 8px;text-align:center;color:#9ca3af">ID</th>
          <th style="padding:6px 8px;text-align:right">出走</th>
          <th style="padding:6px 8px;text-align:right;color:#6b7280">1着-2着-3着-着外</th>
          <th style="padding:6px 8px;text-align:right">勝率</th>
          <th style="padding:6px 8px;text-align:right">連対率</th>
          <th style="padding:6px 8px;text-align:right">複勝率</th>
          <th style="padding:6px 8px;text-align:right">回収率</th>
          <th style="padding:6px 8px;text-align:right">偏差値</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table><p style="font-size:0.78rem;color:#9ca3af;margin-top:4px">${persons.length}件（全期間・行クリックで詳細）<span style="margin-left:8px">※回収率は単勝オッズデータ更新後に反映</span></p>`;
    }

    function _buildBreakdownTable(byMap, keyLabel, sortKeys){
      if(!byMap || !Object.keys(byMap).length) return '<p class="empty">データなし</p>';
      let entries;
      if(sortKeys){
        // 指定順でソート
        const seen = new Set();
        entries = [];
        for(const k of sortKeys){ if(byMap[k]){entries.push([k,byMap[k]]);seen.add(k);} }
        Object.entries(byMap).forEach(([k,v])=>{ if(!seen.has(k)) entries.push([k,v]); });
      } else {
        entries = Object.entries(byMap).sort((a,b)=>b[1].total-a[1].total);
      }
      const rows = entries.map(([key,st])=>{
        const t   = st.total||0;
        const wr  = t ? (st.win_rate!=null ? st.win_rate.toFixed(1) : (st.win/t*100).toFixed(1))+'%' : '—';
        const p2r = t ? (st.place2_rate!=null ? st.place2_rate.toFixed(1) : ((st.place2||0)/t*100).toFixed(1))+'%' : '—';
        const p3r = t ? (st.place3_rate!=null ? st.place3_rate.toFixed(1) : (st.place3/t*100).toFixed(1))+'%' : '—';
        const lose = t - (st.place3||0);
        return `<tr>
          <td style="padding:4px 8px;font-weight:600">${key}</td>
          <td style="padding:4px 8px;text-align:right">${t}</td>
          <td style="padding:4px 8px;text-align:right;color:#6b7280;font-size:0.8rem">${st.win||0}-${(st.place2||0)-(st.win||0)}-${(st.place3||0)-(st.place2||0)}-${Math.max(0,lose)}</td>
          <td style="padding:4px 8px;text-align:right;color:${_rateColor(parseFloat(wr))}">${wr}</td>
          <td style="padding:4px 8px;text-align:right;color:${_rateColor(parseFloat(p2r))}">${p2r}</td>
          <td style="padding:4px 8px;text-align:right;color:${_rateColor(parseFloat(p3r))}">${p3r}</td>
        </tr>`;
      }).join('');
      return `<table style="width:100%;border-collapse:collapse;font-size:0.84rem">
        <thead><tr style="background:#f8fafc;font-size:0.8rem">
          <th style="padding:5px 8px;text-align:left">${keyLabel}</th>
          <th style="padding:5px 8px;text-align:right">出走</th>
          <th style="padding:5px 8px;text-align:right;color:#6b7280">1-2-3-着外</th>
          <th style="padding:5px 8px;text-align:right">勝率</th>
          <th style="padding:5px 8px;text-align:right">連対率</th>
          <th style="padding:5px 8px;text-align:right">複勝率</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
    }

    async function showPersonnelDetail(ptype, pid, pname, devVal){
      const modal = document.getElementById('db-detail-modal');
      const title = document.getElementById('db-detail-title');
      title.textContent = `${pname}（${ptype==='jockey'?'騎手':'調教師'}）詳細`;
      document.getElementById('db-detail-summary').innerHTML = '<p style="color:#6b7280">読み込み中…</p>';
      document.getElementById('db-detail-devs').innerHTML = '';
      document.getElementById('db-detail-venue').innerHTML = '';
      document.getElementById('db-detail-running-style').innerHTML = '';
      document.getElementById('db-detail-dist').innerHTML = '';
      modal.style.display = 'block';

      try{
        const r1 = await fetch(`/api/db/personnel_agg?type=${ptype}&id=${encodeURIComponent(pid)}`);
        const agg = await r1.json();
        if(agg.error){ document.getElementById('db-detail-summary').innerHTML=`<p class="error">${agg.error}</p>`; return; }

        // 総合サマリー（回収率・偏差値付き）
        const roiStr = agg.roi != null ? (+agg.roi).toFixed(1)+'%' : '—';
        const roiColor = agg.roi != null ? (agg.roi >= 100 ? '#059669' : agg.roi >= 80 ? '#374151' : '#9ca3af') : '#9ca3af';
        const devStr = devVal != null ? devVal : '—';
        const devColor = devVal != null ? (devVal>=60?'#7c3aed':devVal>=55?'#1d4ed8':devVal>=45?'#374151':'#9ca3af') : '#9ca3af';
        const locBadge = agg.location ? `<span style="background:${({'美浦':'#1d4ed8','栗東':'#b45309'}[agg.location]||'#0f766e')};color:#fff;font-size:0.72rem;padding:2px 7px;border-radius:10px;margin-left:6px">${agg.location}</span>` : '';

        const _fmt=(st,label,showRoi)=>{
          const t=st.total||0;
          const lose=t-(st.place3||0);
          const roiPart = showRoi && agg.roi!=null ? ` 回収率 <b style="color:${roiColor}">${roiStr}</b>` : '';
          const devPart = showRoi && devVal!=null ? ` 偏差値 <b style="color:${devColor}">${devStr}</b>` : '';
          return `<div style="background:#f8fafc;border-radius:6px;padding:8px 12px;font-size:0.86rem;margin-bottom:6px">
            <b style="color:#166534">${label}</b>${showRoi?locBadge:''}: 出走 <b>${t}</b>
            成績 <b>${st.win||0}-${(st.place2||0)-(st.win||0)}-${(st.place3||0)-(st.place2||0)}-${Math.max(0,lose)}</b>
            勝率 <b style="color:${_rateColor(st.win_rate)}">${t?( +st.win_rate||0).toFixed(1)+'%':'—'}</b>
            連対 <b>${t?(+st.place2_rate||0).toFixed(1)+'%':'—'}</b>
            複勝 <b style="color:${_rateColor(st.place3_rate)}">${t?(+st.place3_rate||0).toFixed(1)+'%':'—'}</b>${roiPart}${devPart}
          </div>`;
        };
        document.getElementById('db-detail-summary').innerHTML =
          _fmt(agg, '全体', true) +
          _fmt(agg.jra||{}, 'JRA', false) +
          _fmt(agg.nar||{}, 'NAR', false);

        // 競馬場別（コード→名前変換）
        const byVenueNamed = {};
        for(const [vc, vs] of Object.entries(agg.by_venue||{})){
          byVenueNamed[VENUE_CODE_TO_NAME[vc]||vc] = vs;
        }
        const venueTable = _buildBreakdownTable(byVenueNamed, '競馬場');
        document.getElementById('db-detail-venue').innerHTML =
          `<h4 style="margin:0 0 6px;font-size:0.88rem;color:#374151">競馬場別</h4>${venueTable}`;

        // 脚質別
        const styleOrder=['逃げ','先行','好位','中団','差し','追込'];
        const styleTable = _buildBreakdownTable(agg.by_running_style||{}, '脚質', styleOrder);
        document.getElementById('db-detail-running-style').innerHTML =
          `<h4 style="margin:8px 0 6px;font-size:0.88rem;color:#374151">脚質別成績</h4>${styleTable}`;

        // SMILE別
        const smileOrder=['芝SS','芝S','芝M','芝I','芝L','芝E','ダートSS','ダートS','ダートM','ダートI','ダートL','ダートE','障害SS','障害S','障害M','障害I','障害L','障害E'];
        const smileTable = _buildBreakdownTable(agg.by_smile||{}, '距離区分', smileOrder);
        document.getElementById('db-detail-dist').innerHTML =
          `<h4 style="margin:8px 0 6px;font-size:0.88rem;color:#374151">馬場×距離区分別（SMILE）</h4>${smileTable}`;


      }catch(e){
        document.getElementById('db-detail-summary').innerHTML=`<p class="error">エラー: ${e.message}</p>`;
      }
    }

    // 偏差値ソート用: 全件取得してクライアント側でソート
    function _sortByDev(persons, limit){
      const valid = persons.filter(p=>(p.total||0)>=10);
      if(!valid.length) return persons.slice(0,limit);
      const meanWR = valid.reduce((s,p)=>s+(+p.win_rate||0),0)/valid.length;
      const varWR  = valid.length>1 ? valid.reduce((s,p)=>s+((+p.win_rate||0)-meanWR)**2,0)/valid.length : 1;
      const stdWR  = Math.sqrt(varWR)||1;
      const withDev = persons.map(p=>({
        ...p,
        _predev: (p.total||0)>=10 ? Math.round(10*((+p.win_rate||0)-meanWR)/stdWR+50) : null
      }));
      withDev.sort((a,b)=>{
        if(a._predev!=null && b._predev!=null) return b._predev-a._predev;
        if(a._predev!=null) return -1;
        if(b._predev!=null) return 1;
        return (b.total||0)-(a.total||0);
      });
      return withDev.slice(0,limit);
    }

    async function loadJockeyDB(){
      const q    = document.getElementById('db-jockey-search').value.trim();
      const sort = document.getElementById('db-jockey-sort').value;
      const f    = _dbFilter.jockey;
      const el   = document.getElementById('db-jockey-table');
      el.innerHTML='<p class="empty">読み込み中…</p>';
      try{
        const serverSort = sort==='dev' ? 'win_rate' : sort;
        const limit = sort==='dev' ? 500 : 200;
        const qs=`type=jockey&q=${encodeURIComponent(q)}&sort=${serverSort}&limit=${limit}&jra_nar=${encodeURIComponent(f.jra_nar)}&surface=${encodeURIComponent(f.surface)}&smile=${encodeURIComponent(f.smile)}`;
        const res=await fetch(`/api/db/personnel_agg?${qs}`);
        const data=await res.json();
        if(data.error){ el.innerHTML=`<p class="error">${data.error}</p>`; return; }
        document.getElementById('db-jockey-filter-label').textContent = data.period ? `集計期間: ${data.period}` : '';
        const persons = sort==='dev' ? _sortByDev(data.persons||[], 200) : (data.persons||[]);
        el.innerHTML=_buildAggTable(persons,'騎手','jockey');
      }catch(e){ el.innerHTML=`<p class="error">エラー: ${e.message}</p>`; }
    }

    async function loadTrainerDB(){
      const q    = document.getElementById('db-trainer-search').value.trim();
      const sort = document.getElementById('db-trainer-sort').value;
      const f    = _dbFilter.trainer;
      const el   = document.getElementById('db-trainer-table');
      el.innerHTML='<p class="empty">読み込み中…</p>';
      try{
        const serverSort = sort==='dev' ? 'win_rate' : sort;
        const limit = sort==='dev' ? 500 : 200;
        const qs=`type=trainer&q=${encodeURIComponent(q)}&sort=${serverSort}&limit=${limit}&jra_nar=${encodeURIComponent(f.jra_nar)}&surface=${encodeURIComponent(f.surface)}&smile=${encodeURIComponent(f.smile)}`;
        const res=await fetch(`/api/db/personnel_agg?${qs}`);
        const data=await res.json();
        if(data.error){ el.innerHTML=`<p class="error">${data.error}</p>`; return; }
        document.getElementById('db-trainer-filter-label').textContent = data.period ? `集計期間: ${data.period}` : '';
        const persons = sort==='dev' ? _sortByDev(data.persons||[], 200) : (data.persons||[]);
        el.innerHTML=_buildAggTable(persons,'調教師','trainer');
      }catch(e){ el.innerHTML=`<p class="error">エラー: ${e.message}</p>`; }
    }

    const VENUE_CODE_TO_NAME={'03':'札幌','04':'函館','01':'福島','02':'新潟','05':'東京','06':'中山','07':'中京','08':'京都','09':'阪神','10':'小倉','30':'門別','35':'盛岡','36':'水沢','42':'浦和','43':'船橋','44':'大井','45':'川崎','46':'金沢','47':'笠松','48':'名古屋','49':'園田','50':'園田','51':'姫路','52':'帯広','54':'高知','55':'佐賀','65':'帯広(ばんえい)'};

    async function loadCourseDB(){
      const surf=document.getElementById('db-course-surface').value;
      const venue=document.getElementById('db-course-venue').value.trim();
      const el=document.getElementById('db-course-list');
      el.innerHTML='<p class="empty">読み込み中…</p>';
      try{
        const qs=[surf?'surface='+encodeURIComponent(surf):'',venue?'venue='+encodeURIComponent(venue):''].filter(Boolean).join('&');
        const res=await fetch('/api/db/course'+(qs?'?'+qs:''));
        const data=await res.json();
        const keys=data.keys||[];
        if(!keys.length){ el.innerHTML='<p class="empty">該当コースがありません</p>'; return; }
        el.innerHTML=`<p style="font-size:0.88rem;color:#374151;margin-bottom:8px">${data.total_keys}件のコース（クリックで詳細）:</p>
          <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:6px">
            ${keys.map(k=>{
              const parts=k.split('_');
              const vn=VENUE_CODE_TO_NAME[parts[0]]||parts[0];
              const sf=parts[1]||'';
              const ds=parts[2]||'';
              return `<div onclick="showCourseDetail('${k}')" style="padding:8px 10px;background:#fff;border:1px solid #bbf7d0;border-radius:6px;cursor:pointer;font-size:0.85rem;display:flex;justify-content:space-between;align-items:center;transition:background 0.15s" onmouseover="this.style.background='#f0fdf4'" onmouseout="this.style.background='#fff'">
                <span><b>${vn}</b> <span style="color:#6b7280">${sf}</span> ${ds}m</span>
                <span style="color:#9ca3af;font-size:0.75rem">›</span>
              </div>`;
            }).join('')}
          </div>`;
      }catch(e){ el.innerHTML=`<p class="error">エラー: ${e.message}</p>`; }
    }

    // ===== コースタブ 静的データ =====
    const _COURSE_VENUE_INFO = {
      // JRA
      '03':{ name:'札幌', region:'JRA', surface:'芝・ダート', desc:'右回り大回り。北海道の洋芝コース。坂なし、直線266m。坂のない平坦コースで先行馬やスタミナよりもスピードが活きる。洋芝の影響でタイムは遅め。' },
      '04':{ name:'函館', region:'JRA', surface:'芝・ダート', desc:'右回り小回り。北海道の洋芝コース。直線262mとJRA最短クラス。コーナーきつく先行・逃げ馬が圧倒的有利。内枠が有利な傾向。' },
      '01':{ name:'福島', region:'JRA', surface:'芝・ダート', desc:'右回り小回り。直線292m。コーナーが小さく先行有利。内枠有利。スタミナよりスピードタイプが活躍。' },
      '02':{ name:'新潟', region:'JRA', surface:'芝・ダート', desc:'右回り外回り・直線コースあり。外回り芝は直線659mとJRA最長。差し・追込も十分通用。直線1000mコースはスプリンターの祭典。' },
      '05':{ name:'東京', region:'JRA', surface:'芝・ダート', desc:'左回り大回り。芝直線525.9m。日本最高峰の競走が集まる。長い直線で差し・追込が有効。坂はゴール前の急坂のみ。外枠でも差し馬なら対応可。' },
      '06':{ name:'中山', region:'JRA', surface:'芝・ダート', desc:'右回り内回り。直線310m。ゴール前の急坂（2m）が特徴。スタミナとパワーが必要。内回りは小回りで先行有利。外回りは差しも決まりやすい。' },
      '07':{ name:'中京', region:'JRA', surface:'芝・ダート', desc:'左回り大回り。芝直線412.5m。坂あり。バランス型のコース。先行〜差しが通用。ダート1800mは深砂でパワー型が有利。' },
      '08':{ name:'京都', region:'JRA', surface:'芝・ダート', desc:'右回り内外回り。直線329m（内回り）・404m（外回り）。3〜4コーナーの下り坂が特徴。展開次第で逃げ・差しどちらも可。' },
      '09':{ name:'阪神', region:'JRA', surface:'芝・ダート', desc:'右回り内外回り。芝直線473m（外回り）。急坂（1.15m）あり。スタミナ・パワーが重要。外回りは差し馬も活躍。内回りは先行有利。' },
      '10':{ name:'小倉', region:'JRA', surface:'芝・ダート', desc:'右回り。直線293m。先行有利傾向が強い。フルゲートが多くコーナーで差を広げにくいため逃げ・先行有利。洋芝気味で高速決着は少ない。' },
      // NAR
      '30':{ name:'門別', region:'NAR', surface:'ダート', desc:'右回り。直線330m。北海道の地方競馬。砂ダートのみ。先行有利。3〜4歳の若駒中心でホッカイドウ競馬の主要場。' },
      '35':{ name:'盛岡', region:'NAR', surface:'芝・ダート', desc:'右回り。直線300m（芝）・330m（ダート）。地方競馬で芝コースを持つ数少ない競馬場。芝とダートの2本立て。先行〜差しが通用。' },
      '36':{ name:'水沢', region:'NAR', surface:'ダート', desc:'右回り。直線220m。岩手競馬第2の競馬場。小回りで先行有利。コーナーがきつくスタミナも要求される。' },
      '42':{ name:'浦和', region:'NAR', surface:'ダート', desc:'右回り。直線220m。南関東最小の競馬場。小回りで先行馬が極端に有利。内枠有利傾向が強い。' },
      '43':{ name:'船橋', region:'NAR', surface:'ダート', desc:'右回り。直線308m。南関東の主要場。南関東では差し馬も決まりやすい中程度の直線。' },
      '44':{ name:'大井', region:'NAR', surface:'ダート', desc:'右回り内外回り。直線386m（内回り）〜400m（外回り）。南関東最大の競馬場。帝王賞・東京大賞典など重賞多数。差しも十分通用。' },
      '45':{ name:'川崎', region:'NAR', surface:'ダート', desc:'右回り。直線300m。南関東の主要場。平坦コースで先行〜差しが通用。川崎記念などGⅡ級重賞開催。' },
      '46':{ name:'金沢', region:'NAR', surface:'ダート', desc:'右回り。直線236m。北陸の地方競馬場。小回りで先行馬が有利。砂が深く時計がかかりやすい。' },
      '47':{ name:'笠松', region:'NAR', surface:'ダート', desc:'右回り。直線235m。東海地方の地方競馬。小回りで先行有利。かつてはオグリキャップが活躍した名門場。' },
      '48':{ name:'名古屋', region:'NAR', surface:'ダート', desc:'右回り。直線240m。東海地方最大の競馬場。先行有利傾向。名古屋グランプリなどダート重賞開催。' },
      '49':{ name:'園田', region:'NAR', surface:'ダート', desc:'右回り。直線215m。関西最大の地方競馬場。コーナーが小さく先行が圧倒的有利。小回りのため外枠は不利。' },
      '51':{ name:'姫路', region:'NAR', surface:'ダート', desc:'右回り。直線218m。園田競馬場の姉妹競馬場。レイアウトが類似し先行有利。' },
      '54':{ name:'高知', region:'NAR', surface:'ダート', desc:'右回り。直線200m。四国唯一の地方競馬場。直線が短く先行有利が顕著。砂が深い独特の馬場。' },
      '55':{ name:'佐賀', region:'NAR', surface:'ダート', desc:'右回り。直線295m。九州の地方競馬場。九州地方唯一の競馬場。先行有利傾向。' },
      '50':{ name:'園田', region:'NAR', surface:'ダート', desc:'右回り。直線215m。関西最大の地方競馬場。コーナーが小さく先行が圧倒的有利。小回りのため外枠は不利。' },
      '65':{ name:'帯広(ばんえい)', region:'NAR', surface:'ばんえい', desc:'直線200m。ばんえい競馬（帯広）。重量物を引いて200mの坂コースを走る独自の競馬形式。' },
    };
    const _JRA_CODES=['03','04','01','02','05','06','07','08','09','10'];
    const _NAR_CODES=['30','35','36','42','43','44','45','46','47','48','49','50','51','54','55','65'];

    let _courseRegion='JRA';
    let _courseVenueData = {};  // {course_key: {records_count, surface}} from api

    async function courseSetRegion(region){
      _courseRegion = region;
      document.getElementById('btn-course-jra').style.cssText=
        region==='JRA' ? 'padding:6px 20px;border-radius:20px;border:1px solid #166534;background:#166534;color:#fff;font-weight:600;cursor:pointer;font-size:0.9rem'
                       : 'padding:6px 20px;border-radius:20px;border:1px solid #d1d5db;background:#fff;color:#374151;font-weight:600;cursor:pointer;font-size:0.9rem';
      document.getElementById('btn-course-nar').style.cssText=
        region==='NAR' ? 'padding:6px 20px;border-radius:20px;border:1px solid #0f766e;background:#0f766e;color:#fff;font-weight:600;cursor:pointer;font-size:0.9rem'
                       : 'padding:6px 20px;border-radius:20px;border:1px solid #d1d5db;background:#fff;color:#374151;font-weight:600;cursor:pointer;font-size:0.9rem';
      const codes = region==='JRA' ? _JRA_CODES : _NAR_CODES;
      const label = document.getElementById('course-region-label');
      label.textContent='コースデータ読み込み中…';
      // コースDBキーを取得
      try{
        const res = await fetch('/api/db/course');
        const data = await res.json();
        _courseVenueData = {};
        for(const k of (data.keys||[])){
          const vc = k.split('_')[0];
          if(!_courseVenueData[vc]) _courseVenueData[vc]=[];
          _courseVenueData[vc].push(k);
        }
      }catch(e){ label.textContent='データ取得失敗'; return; }

      label.textContent=`${region} ${codes.length}場`;
      const grid = document.getElementById('course-venue-cards');
      grid.innerHTML = codes.map(vc=>{
        const info = _COURSE_VENUE_INFO[vc]||{name:vc,desc:'',surface:''};
        const keys = _courseVenueData[vc]||[];
        const hasCourse = keys.length > 0;
        return `<div style="border:1px solid ${hasCourse?'#bbf7d0':'#e5e7eb'};border-radius:8px;padding:12px;background:${hasCourse?'#fff':'#f9fafb'}">
          <div style="font-weight:700;font-size:1rem;color:${hasCourse?'#166534':'#9ca3af'};margin-bottom:4px">
            ${info.name}
            <span style="font-size:0.72rem;color:#9ca3af;font-weight:400;margin-left:6px">${info.surface||''}</span>
          </div>
          <div style="font-size:0.78rem;color:#6b7280;line-height:1.5;margin-bottom:8px">${info.desc||''}</div>
          ${hasCourse
            ? `<div style="display:flex;flex-wrap:wrap;gap:4px">
                ${keys.sort((a,b)=>{
                  const [,sa,da]=[...a.split('_')], [,sb,db]=[...b.split('_')];
                  if(sa!==sb) return sa<sb?-1:1;
                  return parseInt(da)-parseInt(db);
                }).map(k=>{
                  const [,sf,ds]=k.split('_');
                  const sfCol=sf==='芝'?'#166534':'#92400e';
                  return `<button onclick="showCourseDetail('${k}')" style="padding:3px 8px;border-radius:12px;border:1px solid ${sfCol};color:${sfCol};background:#fff;font-size:0.75rem;cursor:pointer;transition:background 0.15s" onmouseover="this.style.background='#f0fdf4'" onmouseout="this.style.background='#fff'">
                    ${sf} ${ds}m
                  </button>`;
                }).join('')}
              </div>`
            : `<span style="font-size:0.75rem;color:#b45309">統計未収集<br><span style="color:#9ca3af;font-size:0.7rem">収集ツールで取得後に表示</span></span>`
          }
        </div>`;
      }).join('');
      document.getElementById('course-course-list').innerHTML='';
    }

    async function showCourseDetail(courseKey){
      const modal=document.getElementById('db-detail-modal');
      const parts=courseKey.split('_');
      const vc=parts[0], sf=parts[1]||'', ds=parts[2]||'';
      const vn=VENUE_CODE_TO_NAME[vc]||vc;
      // コース詳細タイトル
      document.getElementById('db-detail-title').textContent=`${vn} ${sf} ${ds}m コース詳細`;
      document.getElementById('db-detail-summary').innerHTML='<p style="color:#6b7280">読み込み中…</p>';
      document.getElementById('db-detail-devs').innerHTML='';
      document.getElementById('db-detail-venue').innerHTML='';
      document.getElementById('db-detail-running-style').innerHTML='';
      document.getElementById('db-detail-dist').innerHTML='';
      modal.style.display='block';
      try{
        const res=await fetch(`/api/db/course_stats?key=${encodeURIComponent(courseKey)}`);
        const d=await res.json();
        if(d.error){ document.getElementById('db-detail-summary').innerHTML=`<p class="error">${d.error}</p>`; return; }

        // ── 基本情報 ──
        const recStr=d.record?`<b style="color:#dc2626">${d.record.time_str}</b> (${d.record.grade||''} ${d.record.date||''})`:' —';
        document.getElementById('db-detail-summary').innerHTML=`
          <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px;margin-bottom:8px;font-size:0.88rem">
            <div style="background:#f8fafc;border-radius:6px;padding:8px 10px"><span style="color:#6b7280">総データ数</span><br><b>${d.count}件</b></div>
            <div style="background:#fef2f2;border-radius:6px;padding:8px 10px"><span style="color:#6b7280">レコードタイム</span><br>${recStr}</div>
            <div style="background:#f0fdf4;border-radius:6px;padding:8px 10px"><span style="color:#6b7280">ペース区分</span><br><b>${(d.pace_avg||{}).pace_type||'—'}</b></div>
            <div style="background:#eff6ff;border-radius:6px;padding:8px 10px"><span style="color:#6b7280">前半3F平均</span><br><b>${(d.pace_avg||{}).first_3f!=null?(+d.pace_avg.first_3f).toFixed(2)+'秒':'—'}</b></div>
            <div style="background:#eff6ff;border-radius:6px;padding:8px 10px"><span style="color:#6b7280">上り3F平均</span><br><b>${(d.pace_avg||{}).last_3f!=null?(+d.pace_avg.last_3f).toFixed(2)+'秒':'—'}</b></div>
          </div>`;

        // ── クラス別平均走破タイム ──
        let classHtml='<p class="empty">データなし</p>';
        if(d.class_avg && Object.keys(d.class_avg).length){
          const rows=Object.entries(d.class_avg).map(([cls,v])=>
            `<tr><td style="padding:4px 8px;font-weight:600">${cls}</td>
             <td style="padding:4px 8px;text-align:right"><b style="color:#166534">${v.avg_str}</b></td>
             <td style="padding:4px 8px;text-align:right;color:#9ca3af">${v.n}件</td></tr>`
          ).join('');
          classHtml=`<table style="width:100%;border-collapse:collapse;font-size:0.83rem">
            <thead><tr style="background:#f0fdf4"><th style="padding:4px 8px;text-align:left">クラス</th><th style="padding:4px 8px;text-align:right">平均走破タイム</th><th style="padding:4px 8px;text-align:right">件数</th></tr></thead>
            <tbody>${rows}</tbody></table>`;
        }
        document.getElementById('db-detail-devs').innerHTML=
          `<h4 style="margin:0 0 6px;font-size:0.88rem;color:#374151">クラス別平均走破タイム（1〜3着馬）</h4>${classHtml}`;

        // ── 成績(X-X-X-X) ヘルパー ──
        const _fmtRec=(w,p2,p3,total)=>{
          const other=Math.max(0,total-p3);
          return `<span style="font-family:monospace">${w}-${p2-w}-${p3-p2}-${other}</span>`;
        };
        const _fmtRoi=(roi)=>{
          const c=roi>=100?'#166534':roi>=80?'#92400e':'#9ca3af';
          return `<span style="color:${c}">${roi}%</span>`;
        };

        // ── 枠順成績 ──
        let gateHtml='<p class="empty">データなし（3出走未満）</p>';
        if(d.gate_bias && Object.keys(d.gate_bias).length){
          const rows=Object.entries(d.gate_bias).map(([g,v])=>
            `<tr>
             <td style="padding:3px 6px;text-align:center;font-weight:600">${g}枠</td>
             <td style="padding:3px 6px;text-align:right">${v.runs}</td>
             <td style="padding:3px 6px;text-align:center">${_fmtRec(v.win,v.place2,v.place3,v.runs)}</td>
             <td style="padding:3px 6px;text-align:right;color:${_rateColor(v.win_rate)};font-weight:600">${v.win_rate}%</td>
             <td style="padding:3px 6px;text-align:right;color:${_rateColor(v.place2_rate)}">${v.place2_rate}%</td>
             <td style="padding:3px 6px;text-align:right;color:${_rateColor(v.place3_rate)}">${v.place3_rate}%</td>
             <td style="padding:3px 6px;text-align:right">${_fmtRoi(v.roi)}</td></tr>`
          ).join('');
          gateHtml=`<table style="width:100%;border-collapse:collapse;font-size:0.8rem">
            <thead><tr style="background:#dcfce7"><th style="padding:3px 6px">枠</th><th style="padding:3px 6px;text-align:right">出走</th><th style="padding:3px 6px;text-align:center">成績</th><th style="padding:3px 6px;text-align:right">勝率</th><th style="padding:3px 6px;text-align:right">連対率</th><th style="padding:3px 6px;text-align:right">複勝率</th><th style="padding:3px 6px;text-align:right">単回収</th></tr></thead>
            <tbody>${rows}</tbody></table>`;
        }
        document.getElementById('db-detail-venue').innerHTML=
          `<h4 style="margin:0 0 6px;font-size:0.88rem;color:#374151">枠順別成績（3出走以上）</h4>${gateHtml}`;

        // ── 脚質別成績 ──
        let styleHtml='<p class="empty">データなし</p>';
        const styleData=d.running_style||{};
        const styleKeys=['逃げ','先行','好位','中団','差し','追込'];
        const stylesAny=styleKeys.some(s=>styleData[s]&&styleData[s].total>0);
        if(stylesAny){
          const rows=styleKeys.map(s=>{
            const st=styleData[s]||{total:0,win:0,place2:0,place3:0,win_rate:0,place2_rate:0,place3_rate:0,roi:0};
            return `<tr>
              <td style="padding:3px 6px;font-weight:600">${s}</td>
              <td style="padding:3px 6px;text-align:right">${st.total}</td>
              <td style="padding:3px 6px;text-align:center">${_fmtRec(st.win,st.place2,st.place3,st.total)}</td>
              <td style="padding:3px 6px;text-align:right;color:${_rateColor(st.win_rate)};font-weight:600">${st.win_rate}%</td>
              <td style="padding:3px 6px;text-align:right;color:${_rateColor(st.place2_rate)}">${st.place2_rate}%</td>
              <td style="padding:3px 6px;text-align:right;color:${_rateColor(st.place3_rate)}">${st.place3_rate}%</td>
              <td style="padding:3px 6px;text-align:right">${_fmtRoi(st.roi)}</td></tr>`;
          }).join('');
          styleHtml=`<table style="width:100%;border-collapse:collapse;font-size:0.8rem">
            <thead><tr style="background:#dcfce7"><th style="padding:3px 6px">脚質</th><th style="padding:3px 6px;text-align:right">出走</th><th style="padding:3px 6px;text-align:center">成績</th><th style="padding:3px 6px;text-align:right">勝率</th><th style="padding:3px 6px;text-align:right">連対率</th><th style="padding:3px 6px;text-align:right">複勝率</th><th style="padding:3px 6px;text-align:right">単回収</th></tr></thead>
            <tbody>${rows}</tbody></table>`;
        }
        document.getElementById('db-detail-running-style').innerHTML=
          `<h4 style="margin:0 0 6px;font-size:0.88rem;color:#374151">脚質別成績（4角位置ベース）</h4>${styleHtml}`;

        // ── TOP5 騎手・調教師 ──
        const topPeriod = d.top_period||'過去1年';
        const fmtTop=(arr,label)=>{
          if(!arr||!arr.length) return `<div style="flex:1;min-width:260px"><div style="font-size:0.82rem;font-weight:600;color:#374151;margin-bottom:4px">${label}（${topPeriod} 勝利数）</div><p style="color:#9ca3af;font-size:0.8rem">データなし</p></div>`;
          const rows=arr.map((p,i)=>
            `<tr>
             <td style="padding:3px 5px;color:#9ca3af">${i+1}</td>
             <td style="padding:3px 5px;font-weight:600;max-width:80px;overflow:hidden;white-space:nowrap">${p.name||p.id||'—'}</td>
             <td style="padding:3px 5px;text-align:right">${p.total}</td>
             <td style="padding:3px 5px;text-align:center;font-size:0.75rem">${_fmtRec(p.wins,p.place2,p.place3,p.total)}</td>
             <td style="padding:3px 5px;text-align:right;color:${_rateColor(p.win_rate)};font-weight:600">${p.win_rate}%</td>
             <td style="padding:3px 5px;text-align:right;color:${_rateColor(p.place2_rate)}">${p.place2_rate}%</td>
             <td style="padding:3px 5px;text-align:right;color:${_rateColor(p.place3_rate)}">${p.place3_rate}%</td>
             <td style="padding:3px 5px;text-align:right">${_fmtRoi(p.roi)}</td></tr>`
          ).join('');
          return `<div style="flex:1;min-width:260px;overflow-x:auto">
            <div style="font-size:0.82rem;font-weight:600;color:#374151;margin-bottom:4px">${label}（${topPeriod} 勝利数）</div>
            <table style="width:100%;border-collapse:collapse;font-size:0.78rem;white-space:nowrap">
              <thead><tr style="background:#f8fafc"><th style="padding:3px 5px">#</th><th style="padding:3px 5px">名前</th><th style="padding:3px 5px;text-align:right">出走</th><th style="padding:3px 5px;text-align:center">成績</th><th style="padding:3px 5px;text-align:right">勝率</th><th style="padding:3px 5px;text-align:right">連対率</th><th style="padding:3px 5px;text-align:right">複勝率</th><th style="padding:3px 5px;text-align:right">単回収</th></tr></thead>
              <tbody>${rows}</tbody></table>
          </div>`;
        };
        document.getElementById('db-detail-dist').innerHTML=`
          <div style="display:flex;gap:16px;flex-wrap:wrap">
            ${fmtTop(d.top_jockeys,'TOP5 騎手')}
            ${fmtTop(d.top_trainers,'TOP5 調教師')}
          </div>`;

      }catch(e){ document.getElementById('db-detail-summary').innerHTML=`<p class="error">${e.message}</p>`; }
    }

    // ② 過去の予想の日付初期化（起動時）
    (function(){
      const today=_localDate();
      const yd=new Date(today+'T00:00:00'); yd.setDate(yd.getDate()-1);
      const yesterday=_localDate(yd);
      const inp=document.getElementById('past-date-input');
      if(inp&&!inp.value) inp.value=yesterday;
      const cd=document.getElementById('past-create-date');
      if(cd&&!cd.value) cd.value=yesterday;
      const rs=document.getElementById('past-range-start');
      const re=document.getElementById('past-range-end');
      if(rs&&!rs.value) rs.value=yesterday;
      if(re&&!re.value) re.value=yesterday;
    })();
  </script>
</body>
</html>
"""


def _check_auth(req) -> bool:
    """HTTP Basic 認証チェック。AUTH_ENABLED=False なら常に True。"""
    if not AUTH_ENABLED:
        return True
    auth = req.authorization
    if not auth:
        return False
    return auth.username == AUTH_USERNAME and auth.password == AUTH_PASSWORD


def _auth_required():
    """401 Unauthorized レスポンスを返す"""
    return Response(
        "認証が必要です",
        401,
        {"WWW-Authenticate": 'Basic realm="D-AIkeiba"'},
    )


def _is_admin(req) -> bool:
    """localhost (127.0.0.1 / ::1) からのアクセスなら admin とみなす"""
    addr = req.remote_addr or ""
    return addr in ("127.0.0.1", "::1")


# ── 認証済みクライアント（オッズAJAX API用）──
_auth_client = None
_auth_client_ts = 0.0          # 最終ログイン時刻
_AUTH_CLIENT_TTL = 1800        # 30分でセッション更新
_auth_client_lock = __import__("threading").Lock()

# ── 公式サイトオッズスクレーパー（JRA/NAR） ──
_official_odds_scraper = None

# ── 印固定: 発走N分前以降は印を変更しない ──
MARK_FREEZE_MINUTES = 10  # 発走何分前に印を固定するか

def _is_marks_frozen(race: dict) -> bool:
    """発走時刻の MARK_FREEZE_MINUTES 分前以降なら True を返す"""
    post_time = race.get("post_time", "")
    if not post_time:
        return False
    try:
        now = datetime.now()
        h, m = map(int, post_time.split(":"))
        race_start = now.replace(hour=h, minute=m, second=0, microsecond=0)
        diff = (race_start - now).total_seconds()
        # 発走時刻を過ぎている or N分前以内 → 固定
        return diff <= MARK_FREEZE_MINUTES * 60
    except Exception:
        return False


def _update_html_marks(date_key: str, venue: str, race_no: int, horses: list) -> bool:
    """pred.json の印変更をHTMLファイルに反映する。

    馬番に隣接する全ての印スパンを直接書き換える。
    パターン非依存: 馬番と印の隣接関係を複数方向で検索。
    Returns: 更新成功なら True
    """
    ALL_MARKS_STR = "◉◎○▲△★☆×"
    ALL_MARKS = set(ALL_MARKS_STR)
    html_path = os.path.join(OUTPUT_DIR, f"{date_key}_{venue}{race_no}R.html")
    if not os.path.isfile(html_path):
        return False

    # 馬番→新マーク
    new_map: dict[int, str] = {}
    for h in horses:
        m = h.get("mark", "")
        if m and m in ALL_MARKS:
            new_map[h.get("horse_no", 0)] = m
    if not new_map:
        return False

    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        original = html

        MK = f"[{ALL_MARKS_STR}]"

        # --- 馬番から印を特定して書き換える（全パターン対応） ---

        for hno, new_mark in new_map.items():
            # A: 馬カード — uma>馬番</span><span class="m-X"...>X</span>
            html = re.sub(
                rf'(<span class="uma [^"]*"[^>]*>{hno}</span>)'
                rf'<span class="m-{MK}"([^>]*)>{MK}</span>',
                rf'\1<span class="m-{new_mark}"\2>{new_mark}</span>',
                html,
            )
            # B: 確率テーブル — <span class="m-X">X</span> 馬番</td>
            html = re.sub(
                rf'<span class="m-{MK}">{MK}</span> {hno}</td>',
                rf'<span class="m-{new_mark}">{new_mark}</span> {hno}</td>',
                html,
            )
            # C: 印まとめ欄 — <span class="m-X"...>X</span>...<uma>馬番</span>
            html = re.sub(
                rf'<span class="m-{MK}"([^>]*)>{MK}</span>'
                rf'(\s*<div>\s*<strong[^>]*><span class="uma [^"]*">{hno}</span>)',
                rf'<span class="m-{new_mark}"\1>{new_mark}</span>\2',
                html,
                flags=re.DOTALL,
            )
            # D: 買い目欄 — <span class="m-X"...>X<span class="uma wkN">馬番</span></span>
            html = re.sub(
                rf'<span class="m-{MK}"([^>]*)>{MK}<span class="uma ([^"]*)">{hno}</span></span>',
                rf'<span class="m-{new_mark}"\1>{new_mark}<span class="uma \2">{hno}</span></span>',
                html,
            )
            # E: まとめテーブル — 馬番</span></td><td><span class="m-X">X</span>
            html = re.sub(
                rf'(>{hno}</span>\s*</td>\s*<td[^>]*>)'
                rf'<span class="m-{MK}">{MK}</span>',
                rf'\1<span class="m-{new_mark}">{new_mark}</span>',
                html,
            )
            # F: 大判印 — <div class="mlarge m-X">X</div>...data-live-odds="馬番"
            html = re.sub(
                rf'(<div class="mlarge) m-{MK}">{MK}(</div>.*?data-live-odds="{hno}")',
                rf'\1 m-{new_mark}">{new_mark}\2',
                html,
                flags=re.DOTALL,
            )
            # G: JSON埋込メタ — "honmei_mark": "X"...馬番一致時のみ
            # H: 印見解テキスト — 印+馬名
            # (テキスト内の印は_parse_race_html_metaに影響しないため省略)

        if html == original:
            return False

        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        return True
    except Exception as e:
        logger.warning("HTML印更新失敗 %s: %s", html_path, e)
        return False


def _get_official_odds_scraper():
    """OfficialOddsScraper のシングルトンを返す"""
    global _official_odds_scraper
    if _official_odds_scraper is None:
        try:
            from src.scraper.official_odds import OfficialOddsScraper
            _official_odds_scraper = OfficialOddsScraper()
            logger.info("公式オッズスクレーパー初期化完了")
        except Exception as e:
            logger.warning("公式オッズスクレーパー初期化失敗: %s", e)
    return _official_odds_scraper


def _get_auth_client(force_refresh: bool = False):
    """認証済み netkeiba クライアントを返す（ログイン済みセッション・自動更新）"""
    global _auth_client, _auth_client_ts
    import time as _t
    with _auth_client_lock:
        now = _t.time()
        if _auth_client is not None and not force_refresh and (now - _auth_client_ts) < _AUTH_CLIENT_TTL:
            return _auth_client
        # 新規ログインまたは更新
        try:
            from src.scraper.auth import AuthenticatedClient
            client = AuthenticatedClient()
            client.login()
            if client.session.cookies.get("nkauth") or client.session.cookies.get("netkeiba"):
                _auth_client = client
                _auth_client_ts = now
                logger.info("認証済みクライアント初期化/更新成功（オッズAPI用）")
                return _auth_client
        except Exception as e:
            logger.debug("認証クライアント初期化失敗: %s", e)
        return None


def _recalc_divergence(h: dict):
    """馬のdict内 odds と predicted_tansho_odds から乖離率を再計算"""
    pred_o = h.get("predicted_tansho_odds")
    real_o = h.get("odds")
    if not pred_o or pred_o <= 0 or not real_o or real_o <= 0:
        return
    ratio = round(real_o / pred_o, 2)
    # config/settings.py DIVERGENCE_SIGNAL 準拠
    signal = "×"
    for label, threshold in [("S", 2.0), ("A", 1.5), ("B", 1.2), ("C", 0.8)]:
        if ratio >= threshold:
            signal = label
            break
    h["odds_divergence"] = ratio
    h["divergence_signal"] = signal


def create_app():
    _frontend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
    app = Flask(
        __name__,
        template_folder=os.path.join(_frontend_dir, "templates"),
        static_folder=os.path.join(_frontend_dir, "static"),
    )
    today = datetime.now().strftime("%Y-%m-%d")
    default_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    @app.after_request
    def _compress_response(response):
        """JSON レスポンスを gzip 圧縮（1KB以上）"""
        if (
            "gzip" in request.headers.get("Accept-Encoding", "")
            and response.content_type
            and response.content_type.startswith("application/json")
            and response.content_length
            and response.content_length > 1024
        ):
            import gzip as _gzip
            response.data = _gzip.compress(response.data)
            response.headers["Content-Encoding"] = "gzip"
            response.headers["Content-Length"] = len(response.data)
            response.headers["Vary"] = "Accept-Encoding"
        return response

    @app.before_request
    def _require_auth():
        """全リクエストに Basic 認証を適用（/output/ は除外）"""
        if request.path.startswith("/output/"):
            return None  # 配布用HTMLは認証不要
        if not _check_auth(request):
            return _auth_required()

    # --- About ページのコンテンツを読み込み ---
    _about_html_path = os.path.join(_frontend_dir, "templates", "about_content.html")
    _about_content = ""
    try:
        with open(_about_html_path, encoding="utf-8") as f:
            _about_content = f.read()
    except FileNotFoundError:
        _about_content = "<p>About content not found.</p>"

    def _render_new_index():
        resp = Response(
            render_template("index.html", about_content=_about_content),
            mimetype="text/html; charset=utf-8",
        )
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    def _render_old_index():
        _today = datetime.now().strftime("%Y-%m-%d")
        _default_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        html = render_template_string(BASE_HTML, default_start=_default_start, today=_today)
        resp = Response(html, mimetype="text/html; charset=utf-8")
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    # --- React SPA (メインUI) ---
    _react_build_dir = os.path.join(PROJECT_ROOT, "frontend", "dist")

    @app.route("/")
    def index():
        """React SPAをルートで配信"""
        return send_from_directory(_react_build_dir, "index.html")

    @app.route("/assets/<path:path>")
    def serve_react_assets(path):
        """React SPAのビルドアセット配信"""
        return send_from_directory(os.path.join(_react_build_dir, "assets"), path)

    @app.route("/favicon.svg")
    @app.route("/favicon.ico")
    @app.route("/favicon-32.png")
    def serve_favicon():
        """favicon配信"""
        filename = request.path.lstrip("/")
        return send_from_directory(_react_build_dir, filename)

    @app.route("/new")
    @app.route("/new/<path:path>")
    def serve_react_legacy(path=""):
        """旧パス /new → ルートへリダイレクト"""
        return redirect("/")

    # --- 競馬場ロゴ配信 ---
    _logos_dir = os.path.join(PROJECT_ROOT, "frontend", "public", "logos")

    @app.route("/logos/<path:filename>")
    def serve_logo(filename):
        return send_from_directory(_logos_dir, filename)

    @app.route("/api/auth_mode")
    def api_auth_mode():
        """クライアントの admin/restricted 判定を返す"""
        return jsonify(admin=_is_admin(request))

    @app.route("/api/portfolio")
    def api_portfolio():
        date_files, single_files = _scan_output()
        st = _get_db_state()
        return jsonify(
            course_runs=st["course_runs"],
            last_date=st["last_date"],
            date_files=date_files,
            single_files=single_files,
        )

    @app.route("/api/today_predictions")
    def api_today_predictions():
        """指定日の生成済み個別レースHTMLを会場別に返す（60秒キャッシュ付き）"""
        date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
        nocache = request.args.get("nocache", "0") == "1"
        now = time.time()
        cached = _predictions_cache.get(date)
        if not nocache and cached and (now - cached[0]) < _CACHE_TTL:
            result = cached[1]
        else:
            result = _scan_today_predictions(date)
            _predictions_cache[date] = (now, result)
        total = sum(len(v) for v in result["races"].values())
        # pred.json から odds_updated_at を取得
        date_key = date.replace("-", "")
        _pf = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
        odds_ts = None
        if os.path.isfile(_pf):
            try:
                with open(_pf, "r", encoding="utf-8") as _f:
                    odds_ts = json.load(_f).get("odds_updated_at")
            except Exception:
                pass
        return jsonify(date=date, races=result["races"], order=result["order"],
                       total=total, odds_updated_at=odds_ts)

    @app.route("/api/race_prediction")
    def api_race_prediction():
        date = request.args.get("date", "")
        venue = request.args.get("venue", "")
        race_no = request.args.get("race_no", 0, type=int)
        if not date or not venue or not race_no:
            return jsonify({"ok": False, "error": "Missing params"})
        date_key = date.replace("-", "")
        pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
        if not os.path.exists(pred_file):
            # pred.json がない場合、個別レースHTMLがあるか確認し、
            # 予想を再生成してpred.jsonを作成する
            html_file = os.path.join(OUTPUT_DIR, f"{date_key}_{venue}{race_no}R.html")
            if os.path.isfile(html_file):
                # HTMLは存在するがpred.jsonがない（途中中断）→ 再生成を案内
                return jsonify({
                    "ok": False,
                    "error": "予想データが見つかりません。予想タブの「予想生成」ボタンで再生成してください。",
                    "html_exists": True,
                    "html_url": f"/output/{date_key}_{venue}{race_no}R.html"
                })
            return jsonify({"ok": False, "error": "Prediction file not found"})
        try:
            with open(pred_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            for race in data.get("races", []):
                if race.get("venue") == venue and race.get("race_no") == race_no:
                    # odds があるのに乖離率未計算の馬を補完
                    patched = False
                    for h in race.get("horses", []):
                        if h.get("odds") and h.get("predicted_tansho_odds") and h.get("odds_divergence") is None:
                            _recalc_divergence(h)
                            patched = True
                    # 馬個別見解・印見解・全頭診断が未生成の場合はオンデマンド生成
                    _needs_gen = race.get("horses") and (
                        not race["horses"][0].get("horse_comment")
                        or not race["horses"][0].get("horse_diagnosis")
                    )
                    if _needs_gen:
                        try:
                            from src.calculator.calibration import generate_horse_comment, generate_horse_diagnosis, generate_mark_comment_rich
                            all_composites = [hd.get("composite", 0) for hd in race["horses"]]
                            _rc = {
                                "field_count": race.get("field_count", 0),
                                "straight_m": race.get("straight_m", 0),
                                "slope_type": race.get("slope_type", ""),
                                "surface": race.get("surface", ""),
                                "pace_predicted": race.get("pace_predicted", "MM"),
                                "leading_horses": race.get("leading_horses", []),
                                "front_horses": race.get("front_horses", []),
                                "mid_horses": race.get("mid_horses", []),
                                "rear_horses": race.get("rear_horses", []),
                                "estimated_front_3f": race.get("estimated_front_3f"),
                                "all_composites": all_composites,
                            }
                            _mark_set = {"◉", "◎", "○", "▲"}
                            for hd in race["horses"]:
                                m = hd.get("mark", "-")
                                lvl = "full" if m in _mark_set else ("normal" if m in ("△", "★", "☆") else "short")
                                hd["horse_comment"] = generate_horse_comment(hd, _rc, lvl)
                                # 全頭診断用短評
                                if not hd.get("horse_diagnosis"):
                                    hd["horse_diagnosis"] = generate_horse_diagnosis(hd, _rc)
                            sorted_h = sorted(race["horses"], key=lambda x: x.get("composite", 0), reverse=True)
                            race["mark_comment_rich"] = generate_mark_comment_rich(sorted_h, _rc)
                            patched = True
                        except Exception:
                            pass

                    # JRA結果CNAME を動的に導出（未保存の場合）
                    if not race.get("result_cname") and race.get("race_id"):
                        official = _get_official_odds_scraper()
                        if official:
                            try:
                                rc = official.get_result_cname(race["race_id"])
                                if rc:
                                    race["result_cname"] = rc
                                    patched = True
                            except Exception:
                                pass
                    # ML予測確率をcompositeに反映し、印を再割り当て（発走10分前以降は印固定）
                    # reassign_marks_dict内部で_apply_ml_composite_adjも呼ばれるため個別呼び出し不要
                    _race_horses = race.get("horses", [])
                    if _race_horses and any(h.get("win_prob") for h in _race_horses):
                        try:
                            from src.calculator.popularity_blend import reassign_marks_dict
                            if not _is_marks_frozen(race):
                                reassign_marks_dict(_race_horses)
                            else:
                                logger.debug("印固定中（発走%d分前以内）: %s", MARK_FREEZE_MINUTES, race.get("race_id"))
                        except Exception:
                            pass

                    # フォーメーション買い目は無効化 — 既存データも除去
                    race["formation_tickets"] = []
                    race["formation_columns"] = {}
                    race["tickets"] = []

                    # チケットにmark情報を補完（既存JSONに未保存の場合）
                    _mark_by_no = {h.get("horse_no"): h.get("mark", "") for h in race.get("horses", [])}
                    for _t in race.get("tickets", []) + race.get("formation_tickets", []):
                        if not _t.get("mark_a"):
                            _combo = _t.get("combo", [])
                            if len(_combo) >= 2:
                                _t["mark_a"] = _mark_by_no.get(_combo[0], "")
                                _t["mark_b"] = _mark_by_no.get(_combo[1], "")
                            if len(_combo) >= 3:
                                _t["mark_c"] = _mark_by_no.get(_combo[2], "")

                    # formation_columnsが未保存の場合、印から排他列を構築
                    if race.get("formation_tickets") and not race.get("formation_columns"):
                        _col1, _col2, _col3 = [], [], []
                        for h in race.get("horses", []):
                            mk = h.get("mark", "")
                            no = h.get("horse_no")
                            if not no:
                                continue
                            if mk in ("◉", "◎"):
                                _col1.append(no)
                            elif mk in ("○", "▲"):
                                _col2.append(no)
                            elif mk in ("△", "★", "☆"):
                                _col3.append(no)
                        if _col1 or _col2 or _col3:
                            race["formation_columns"] = {
                                "col1": _col1,
                                "col2": _col2,
                                "col3": _col3,
                            }

                    # formation_columnsを排他化（旧累積列データ互換）
                    _fc = race.get("formation_columns", {})
                    if _fc:
                        _c1 = _fc.get("col1", [])
                        _c1s = set(_c1)
                        _c2 = [n for n in _fc.get("col2", []) if n not in _c1s]
                        _c2s = _c1s | set(_c2)
                        _c3 = [n for n in _fc.get("col3", []) if n not in _c2s]
                        race["formation_columns"] = {"col1": _c1, "col2": _c2, "col3": _c3}

                    # フォーメーションチケットを排他列でフィルタ + 低EVチケット除外
                    _fc = race.get("formation_columns", {})
                    if _fc and race.get("formation_tickets"):
                        _s1 = set(_fc.get("col1", []))
                        _s2 = set(_fc.get("col2", []))
                        _s3 = set(_fc.get("col3", []))
                        _valid = []
                        for _t in race["formation_tickets"]:
                            _c = _t.get("combo", [])
                            if len(_c) < 3:
                                continue
                            _cs = set(_c)
                            # 3頭のうち各列から最低1頭ずつ含まれるか
                            if not (_cs & _s1 and _cs & _s2 and _cs & _s3):
                                continue
                            # EV < 80% のチケットは除外（旧データ互換）
                            if _t.get("ev", 0) < 80:
                                _t["stake"] = 0
                            _valid.append(_t)
                        race["formation_tickets"] = _valid

                    if patched:
                        try:
                            with open(pred_file, "w", encoding="utf-8") as wf:
                                json.dump(data, wf, ensure_ascii=False, indent=2)
                        except Exception:
                            pass
                    return jsonify({"ok": True, "race": race})
            # pred.json にレースが見つからない場合、HTMLファイルにフォールバック
            html_file = os.path.join(OUTPUT_DIR, f"{date_key}_{venue}{race_no}R.html")
            if os.path.isfile(html_file):
                return jsonify({
                    "ok": False,
                    "error": "予想データが同期されていません。個別HTMLで表示します。",
                    "html_exists": True,
                    "html_url": f"/output/{date_key}_{venue}{race_no}R.html",
                })
            return jsonify({"ok": False, "error": "Race not found"})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})

    @app.route("/api/race_odds", methods=["POST"])
    def api_race_odds():
        """単一レースのオッズを取得して返す"""
        data = request.get_json(force=True, silent=True) or {}
        race_id = data.get("race_id", "")
        date = data.get("date", "")
        venue = data.get("venue", "")
        race_no = data.get("race_no", 0)
        if not race_id:
            return jsonify(ok=False, error="race_id が必要です")
        try:
            result = {}
            source = ""

            # 1) 公式サイト（JRA/NAR）を優先
            official = _get_official_odds_scraper()
            if official:
                try:
                    result = official.get_tansho(race_id)
                    if result:
                        source = "official"
                        logger.info("公式オッズ取得成功: %s (%d頭)", race_id, len(result))
                except Exception as e:
                    logger.warning("公式オッズ取得失敗: %s → netkeiba にフォールバック", e)

            # 2) 公式で取れなければ netkeiba にフォールバック
            if not result:
                from src.scraper.netkeiba import NetkeibaClient, OddsScraper
                client = _get_auth_client() or NetkeibaClient(no_cache=True)
                scraper = OddsScraper(client)
                result = scraper.get_tansho(race_id)
                if result:
                    source = "netkeiba"

            if not result:
                return jsonify(ok=False, error="オッズ未発売（発走時刻が近づくと取得できます）")

            # 3) 三連複オッズ取得（netkeiba → 公式サイトフォールバック）
            sanrenpuku_odds_map = {}
            _san_source = ""
            try:
                from src.scraper.netkeiba import NetkeibaClient, OddsScraper as _OS
                _nk_client = _get_auth_client() or NetkeibaClient(no_cache=True)
                _san_scraper = _OS(_nk_client)
                sanrenpuku_odds_map = _san_scraper.get_sanrenpuku_odds(race_id)
                if sanrenpuku_odds_map:
                    _san_source = "netkeiba"
            except Exception as _se:
                logger.debug("三連複オッズ netkeiba失敗: %s", _se)
            # 公式サイトフォールバック
            if not sanrenpuku_odds_map:
                try:
                    _off = _get_official_odds_scraper()
                    if _off:
                        sanrenpuku_odds_map = _off.get_sanrenpuku_odds(race_id)
                        if sanrenpuku_odds_map:
                            _san_source = "official"
                except Exception as _se2:
                    logger.debug("三連複オッズ 公式サイト失敗: %s", _se2)
            if sanrenpuku_odds_map:
                logger.info("三連複オッズ取得成功(%s): %s (%d組)", _san_source, race_id, len(sanrenpuku_odds_map))

            # {horse_no: [odds, rank]} に変換 → 実出走馬内で人気を再計算
            odds_map = {}
            for horse_no, (odds_val, rank) in result.items():
                odds_map[str(horse_no)] = {"odds": odds_val, "popularity": rank}
            # 実出走馬のオッズ昇順で人気を再割り当て
            _sorted_odds = sorted(
                [(hno, d["odds"]) for hno, d in odds_map.items() if d["odds"] and d["odds"] > 0],
                key=lambda x: x[1],
            )
            for _new_rank, (_hno, _) in enumerate(_sorted_odds, 1):
                odds_map[_hno]["popularity"] = _new_rank

            # 馬体重・馬主を取得（公式サイト優先 → netkeiba フォールバック）
            weight_map = {}
            weight_from_official = False
            if official:
                try:
                    wt_data = official.get_weights(race_id)
                    if wt_data:
                        for hno, info in wt_data.items():
                            weight_map[str(hno)] = {
                                "horse_weight": info["weight"],
                                "weight_change": info["weight_change"],
                                "owner": info.get("owner", ""),
                            }
                        weight_from_official = True
                        logger.info("公式馬体重取得: %s (%d頭)", race_id, len(weight_map))
                except Exception as e:
                    logger.debug("公式馬体重取得失敗: %s", e)
            if not weight_map:
                try:
                    from src.scraper.netkeiba import NetkeibaClient, OddsScraper as _OS
                    wc_client = _get_auth_client() or NetkeibaClient(no_cache=True)
                    w_scraper = _OS(wc_client)
                    weights = w_scraper.get_weights(race_id)
                    if weights:
                        for horse_no, (w, wc) in weights.items():
                            weight_map[str(horse_no)] = {"horse_weight": w, "weight_change": wc}
                except Exception:
                    pass

            # pred.json を更新（オッズ + 馬体重 + 馬主）
            if date:
                date_key = date.replace("-", "")
                pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
                if os.path.isfile(pred_file):
                    try:
                        with open(pred_file, "r", encoding="utf-8") as f:
                            pred = json.load(f)
                        for race in pred.get("races", []):
                            if race.get("race_id") == race_id:
                                for h in race.get("horses", []):
                                    hno = str(h.get("horse_no", ""))
                                    if hno in odds_map:
                                        h["odds"] = odds_map[hno]["odds"]
                                        h["popularity"] = odds_map[hno]["popularity"]
                                        _recalc_divergence(h)
                                    if hno in weight_map:
                                        h["horse_weight"] = weight_map[hno].get("horse_weight") or weight_map[hno].get("weight")
                                        h["weight_change"] = weight_map[hno].get("weight_change")
                                        if weight_from_official:
                                            h["weight_confirmed"] = True
                                        owner = weight_map[hno].get("owner", "")
                                        if owner and not h.get("owner"):
                                            h["owner"] = owner
                                # 人気別統計ブレンドで確率・印を再計算
                                try:
                                    from src.calculator.popularity_blend import (
                                        blend_probabilities_dict,
                                        load_popularity_stats,
                                        reassign_marks_dict,
                                    )
                                    _pop_stats = load_popularity_stats()
                                    if _pop_stats:
                                        _horses = race.get("horses", [])
                                        _is_jra = race.get("is_jra", True)
                                        blend_probabilities_dict(
                                            _horses, race.get("venue", ""),
                                            _is_jra, len(_horses), _pop_stats,
                                        )
                                        if not _is_marks_frozen(race):
                                            reassign_marks_dict(_horses)
                                            # HTMLの印も同期
                                            if date:
                                                _dk = date.replace("-", "")
                                                _vn = venue or race.get("venue", "")
                                                _rn = race_no or race.get("race_no", 0)
                                                if _update_html_marks(_dk, _vn, _rn, _horses):
                                                    logger.info("HTML印を同期: %s %s%sR", _dk, _vn, _rn)
                                            # AI印見解・馬個別見解も再生成
                                            try:
                                                from src.calculator.calibration import (
                                                    generate_horse_comment,
                                                    generate_horse_diagnosis,
                                                    generate_mark_comment_rich,
                                                )
                                                _all_comps = [h.get("composite", 0) for h in _horses]
                                                _rc = {
                                                    "field_count": race.get("field_count", 0),
                                                    "straight_m": race.get("straight_m", 0),
                                                    "slope_type": race.get("slope_type", ""),
                                                    "surface": race.get("surface", ""),
                                                    "pace_predicted": race.get("pace_predicted", "MM"),
                                                    "leading_horses": race.get("leading_horses", []),
                                                    "front_horses": race.get("front_horses", []),
                                                    "mid_horses": race.get("mid_horses", []),
                                                    "rear_horses": race.get("rear_horses", []),
                                                    "estimated_front_3f": race.get("estimated_front_3f"),
                                                    "all_composites": _all_comps,
                                                }
                                                _mark_full = {"◉", "◎", "○", "▲"}
                                                for _hd in _horses:
                                                    _m = _hd.get("mark", "-")
                                                    _lvl = "full" if _m in _mark_full else ("normal" if _m in ("△", "★", "☆") else "short")
                                                    _hd["horse_comment"] = generate_horse_comment(_hd, _rc, _lvl)
                                                    _hd["horse_diagnosis"] = generate_horse_diagnosis(_hd, _rc)
                                                _sorted_h = sorted(_horses, key=lambda x: x.get("composite", 0), reverse=True)
                                                race["mark_comment_rich"] = generate_mark_comment_rich(_sorted_h, _rc)
                                                logger.info("AI印見解を再生成: %s", race_id)
                                            except Exception as _ce:
                                                logger.warning("AI印見解再生成に失敗: %s", _ce)
                                            logger.info("オッズ更新後の確率・印を再計算: %s", race_id)
                                        else:
                                            logger.info("オッズ更新（印固定中）: %s", race_id)
                                except Exception as _e:
                                    logger.warning("確率再計算に失敗: %s", _e)

                                # 三連複チケットのオッズを実オッズで更新
                                if sanrenpuku_odds_map:
                                    for t in race.get("tickets", []):
                                        if t.get("type") != "三連複":
                                            continue
                                        combo = t.get("combo", [])
                                        if len(combo) == 3:
                                            key = tuple(sorted(int(x) for x in combo))
                                            if key in sanrenpuku_odds_map:
                                                actual = sanrenpuku_odds_map[key]
                                                t["odds"] = round(actual, 1)
                                                prob = t.get("prob", 0)
                                                if prob > 0:
                                                    t["ev"] = round(prob * actual * 100, 1)
                                    logger.info("三連複チケットを実オッズで更新: %s (%d組中)", race_id, len(sanrenpuku_odds_map))

                                break
                        with open(pred_file, "w", encoding="utf-8") as f:
                            json.dump(pred, f, ensure_ascii=False, indent=2)
                    except Exception as e:
                        logger.warning("pred.json update failed: %s", e)

            # レース一覧キャッシュをクリア（印・確率変更を即反映）
            if date:
                _predictions_cache.pop(date, None)

            return jsonify(ok=True, odds=odds_map, weights=weight_map)
        except Exception as e:
            logger.error("race_odds failed: %s", e, exc_info=True)
            return jsonify(ok=False, error=str(e))

    @app.route("/api/home_info")
    def api_home_info():
        date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
        now = time.time()
        cached = _home_info_cache.get(date)
        if cached:
            ttl = cached[2] if len(cached) > 2 else _WEATHER_CACHE_TTL
            if (now - cached[0]) < ttl:
                return jsonify(**cached[1])
        venues = _get_todays_venues(date)
        weather = {}
        for v in venues:
            coords = VENUE_COORDS.get(v["code"])
            if coords:
                weather[v["name"]] = _fetch_weather(coords[0], coords[1])
        result = {"date": date, "venues": venues, "weather": weather}
        # JRA場のみの場合はキャッシュTTLを短く（NAR場が後から取得可能になる場合に対応）
        _jra_codes = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
        has_nar = any(v["code"] not in _jra_codes for v in venues)
        ttl = _WEATHER_CACHE_TTL if has_nar else 300  # NAR含む:30分, JRAのみ:5分
        _home_info_cache[date] = (now, result, ttl)
        return jsonify(**result)

    @app.route("/api/share_url")
    def api_share_url():
        date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
        date_key = date.replace("-", "")
        # 配布用HTMLを優先し、なければ旧_share.htmlにフォールバック
        # output/ ディレクトリの実ファイルをスキャンして一致するものを探す
        found_fname = None
        if os.path.isdir(OUTPUT_DIR):
            for f in os.listdir(OUTPUT_DIR):
                if f.startswith(date_key) and (f.endswith("_配布用.html") or f.endswith("_share.html")):
                    # 配布用を優先
                    if f.endswith("_配布用.html"):
                        found_fname = f
                        break
                    elif found_fname is None:
                        found_fname = f
        if found_fname:
            fname = found_fname
            fpath = os.path.join(OUTPUT_DIR, fname)
        else:
            fname = f"{date_key}_配布用.html"
            fpath = os.path.join(OUTPUT_DIR, fname)
        exists = os.path.isfile(fpath)
        size_kb = os.path.getsize(fpath) // 1024 if exists else 0
        port = request.host.split(":")[-1] if ":" in request.host else "5051"
        url = f"http://127.0.0.1:{port}/output/{fname}"
        return jsonify(exists=exists, url=url, filename=fname, size_kb=size_kb, date=date)

    @app.route("/api/state")
    def api_state():
        st = {}
        if os.path.exists(COURSE_DB_COLLECTOR_STATE_PATH):
            try:
                with open(COURSE_DB_COLLECTOR_STATE_PATH, "r", encoding="utf-8") as f:
                    st = json.load(f)
            except Exception:
                logger.debug("api/state JSON read failed", exc_info=True)
        return jsonify(st)

    @app.route("/api/start", methods=["POST"])
    def api_start():
        if not _is_admin(request):
            return jsonify(ok=False, error="この操作は管理者のみ実行できます"), 403
        global _collector_state
        if _collector_state["running"]:
            return jsonify(ok=False, error="既に実行中です")
        data = request.get_json() or {}
        mode = data.get("mode", "full")
        start_date = data.get("start_date", default_start)
        end_date = data.get("end_date", today)
        _collector_state["running"] = True
        _collector_state["error"] = None
        _collector_state["status"] = "starting"
        _collector_state["day_index"] = _collector_state["total_days"] = _collector_state[
            "total_runs"
        ] = 0
        _collector_state["current_date"] = ""
        _collector_state["elapsed_sec"] = 0
        _collector_state["start_time"] = time.time()

        def _run():
            global _collector_state
            try:
                from src.scraper.course_db_collector import collect_course_db_from_results
                from src.scraper.netkeiba import NetkeibaClient, RaceListScraper

                def prog(day_i, total, runs, cur_date, st):
                    _collector_state["day_index"] = day_i
                    _collector_state["total_days"] = total
                    _collector_state["total_runs"] = runs
                    _collector_state["current_date"] = cur_date
                    _collector_state["status"] = st
                    _collector_state["elapsed_sec"] = int(
                        time.time() - _collector_state["start_time"]
                    )

                client = NetkeibaClient(no_cache=True)
                race_list = RaceListScraper(client)
                collect_course_db_from_results(
                    client,
                    race_list,
                    start_date,
                    end_date,
                    COURSE_DB_PRELOAD_PATH,
                    state_path=COURSE_DB_COLLECTOR_STATE_PATH,
                    mode=mode,
                    progress_callback=prog,
                )
            except Exception as e:
                logger.warning("collector run failed: %s", e, exc_info=True)
                _collector_state["error"] = str(e)
            finally:
                _collector_state["running"] = False

        threading.Thread(target=_run, daemon=True).start()
        return jsonify(ok=True)

    @app.route("/api/status")
    def api_status():
        return jsonify(_collector_state)

    @app.route("/api/analyze", methods=["POST"])
    def api_analyze():
        global _analyzer_state
        if _analyzer_state["running"]:
            return jsonify(ok=False, error="既に実行中です")
        data = request.get_json() or {}
        date = data.get("date", datetime.now().strftime("%Y-%m-%d"))
        # 非adminは本日のみ許可
        if not _is_admin(request):
            date = datetime.now().strftime("%Y-%m-%d")
        selected_venues_raw = data.get("venues", [])  # ["船橋","大井",...] or ["43","44",...]
        # 会場名 → 場コードに変換（フロントエンドからは名前が送られる）
        from data.masters.venue_master import VENUE_NAME_TO_CODE
        selected_venues = [
            VENUE_NAME_TO_CODE.get(v, v) for v in selected_venues_raw
        ]
        _analyzer_state["running"] = True
        _analyzer_state["error"] = None
        _analyzer_state["done"] = False
        _analyzer_state["progress"] = "開始..."
        _analyzer_state["current_race"] = ""
        _analyzer_state["done_races"] = 0
        _analyzer_state["total_races"] = 0
        _analyzer_state["elapsed_sec"] = 0
        _analyzer_state["start_time"] = time.time()

        def _run():
            global _analyzer_state
            try:
                out_dir = OUTPUT_DIR
                cmd = [
                    sys.executable,
                    os.path.join(PROJECT_ROOT, "main.py"),
                    "--analyze_date",
                    date,
                    "--output",
                    out_dir,
                    "--no_open",
                ]
                if selected_venues:
                    cmd.extend(["--venues", ",".join(selected_venues)])
                cmd.extend(["--workers", "3"])  # 並列フェッチ（3倍速）
                cmd.append("--quiet")  # 冗長なログを抑制（進捗パース精度向上）
                _analyzer_state["progress"] = f"初期化中..."
                import re as _re
                # Windows の cp932 文字化けを防ぐため PYTHONUTF8=1 を設定
                _env = os.environ.copy()
                _env["PYTHONUTF8"] = "1"
                _env["PYTHONIOENCODING"] = "utf-8"
                _cflags = 0
                if sys.platform == "win32":
                    _cflags = subprocess.CREATE_NO_WINDOW
                global _analyzer_proc
                proc = subprocess.Popen(
                    cmd,
                    cwd=PROJECT_ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=_env,
                    creationflags=_cflags,
                )
                _analyzer_proc = proc
                full_out = []
                # タイムスタンプ付きログ行: "[02/27/26 19:35:27] INFO  ..."
                _re_log   = _re.compile(r"^\[\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\]")
                # 進捗行: "   (1/36 3%) 202642022701 ...  浦和1R OK"
                _re_prog  = _re.compile(r"\((\d+)/(\d+)\s+\d+%\)")
                # 完了: "浦和1R OK" / スキップ: "[スキップ]"
                _re_ok    = _re.compile(r"([^\s]+\d+R)\s+OK")
                _re_skip  = _re.compile(r"\[スキップ")
                # フェーズ行: "[1/3]" "[2/3]" "[3/3]"
                _re_phase = _re.compile(r"^\[(\d+/\d+)\](.+)")

                for line in iter(proc.stdout.readline, ""):
                    full_out.append(line)
                    # \r を除去してクリーンな文字列に
                    s = line.replace("\r", "").strip()
                    if not s:
                        continue
                    _analyzer_state["elapsed_sec"] = int(
                        time.time() - _analyzer_state["start_time"]
                    )
                    # タイムスタンプ付きログ行はスキップ
                    if _re_log.match(s):
                        continue
                    # 先頭スペース＋INFOはログ行
                    if s.startswith("INFO") or s.startswith("WARNING"):
                        continue
                    # フェーズ行: [1/3] レース一覧取得中...
                    m_phase = _re_phase.match(s)
                    if m_phase:
                        _analyzer_state["progress"] = s
                        continue
                    # レース進捗行: (1/36 3%) 202642022701 ... 浦和1R OK
                    m_prog = _re_prog.search(s)
                    if m_prog:
                        n     = int(m_prog.group(1))
                        total = int(m_prog.group(2))
                        _analyzer_state["total_races"] = total
                        m_ok = _re_ok.search(s)
                        if m_ok:
                            # 完了
                            _analyzer_state["done_races"] = n
                            label = m_ok.group(1)
                            _analyzer_state["current_race"] = f"[完了] {label}"
                            _analyzer_state["progress"] = f"[完了] {label} ({n}/{total}レース)"
                        elif _re_skip.search(s):
                            # スキップ
                            _analyzer_state["done_races"] = n
                            _analyzer_state["progress"] = f"[スキップ] ({n}/{total}レース)"
                        else:
                            # 取得中（まだ完了行が来ていない途中）
                            _analyzer_state["done_races"] = n - 1
                            _analyzer_state["progress"] = f"[分析中] ({n}/{total}レース)..."
                        continue
                proc.wait()
                _analyzer_state["elapsed_sec"] = int(time.time() - _analyzer_state["start_time"])
                _analyzer_state["progress"] = (
                    "[完了] 分析完了" if proc.returncode == 0 else f"[エラー] 終了 (コード:{proc.returncode})"
                )
                if proc.returncode != 0:
                    out = "".join(full_out)
                    _analyzer_state["error"] = out[-600:] if len(out) > 600 else out
            except Exception as e:
                logger.warning("analyzer run failed: %s", e, exc_info=True)
                _analyzer_state["error"] = str(e)
            finally:
                _analyzer_state["running"] = False
                _analyzer_state["done"] = True

        threading.Thread(target=_run, daemon=True).start()
        return jsonify(ok=True)

    @app.route("/api/analyze_status")
    def api_analyze_status():
        return jsonify(_analyzer_state)

    @app.route("/api/analyze_cancel", methods=["POST"])
    def api_analyze_cancel():
        global _analyzer_state, _analyzer_proc
        if not _analyzer_state.get("running"):
            return jsonify(ok=False, error="実行中ではありません")
        try:
            if _analyzer_proc and _analyzer_proc.poll() is None:
                _analyzer_proc.terminate()
                try:
                    _analyzer_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    _analyzer_proc.kill()
            _analyzer_state["running"] = False
            _analyzer_state["done"] = True
            _analyzer_state["error"] = "ユーザーにより中断されました"
            _analyzer_state["progress"] = "[中断] ユーザー操作"
            _analyzer_proc = None
            return jsonify(ok=True)
        except Exception as e:
            return jsonify(ok=False, error=str(e))

    # ── オッズ取得共通ロジック（手動/自動共通）──
    def _run_odds_update(date_key, source="manual"):
        global _odds_state, _odds_cancel
        import time as _tm
        pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
        _odds_state = {"running": True, "done": False, "error": None, "updated_at": None,
                       "count": 0, "total": 0, "current": 0, "current_race": "",
                       "started_at": _tm.time(), "source": source}
        _odds_cancel = False
        try:
            official = _get_official_odds_scraper()

            nk_scraper = None
            def _get_nk_scraper():
                nonlocal nk_scraper
                if nk_scraper is None:
                    from src.scraper.netkeiba import NetkeibaClient, OddsScraper
                    client = _get_auth_client() or NetkeibaClient(no_cache=True)
                    nk_scraper = OddsScraper(client)
                return nk_scraper

            with open(pred_file, "r", encoding="utf-8") as pf:
                pred = json.load(pf)

            races = pred.get("races", [])
            total_races = len(races)
            _odds_state["total"] = total_races

            live_odds: dict = {}
            pred_modified = False
            count = 0
            weight_fetch_failed = False
            for race_idx, race in enumerate(races):
                if _odds_cancel:
                    _odds_state.update(running=False, done=True,
                                       error="ユーザーにより中断されました", count=count)
                    if pred_modified:
                        try:
                            with open(pred_file, "w", encoding="utf-8") as wf:
                                json.dump(pred, wf, ensure_ascii=False, indent=2)
                        except Exception:
                            pass
                    return
                race_id = race.get("race_id")
                if not race_id:
                    continue
                venue_name = race.get("venue", "")
                race_no_val = race.get("race_no", "")
                _odds_state["current"] = race_idx + 1
                _odds_state["current_race"] = f"{venue_name}{race_no_val}R"
                _odds_state["count"] = count
                try:
                    result = {}
                    _odds_source = ""
                    # 1) 公式サイト（JRA/NAR）を優先
                    if official:
                        try:
                            result = official.get_tansho(race_id)
                            if result:
                                _odds_source = "official"
                                logger.info("公式オッズ取得: %s (%d頭)", race_id, len(result))
                        except Exception as oe:
                            logger.debug("公式オッズ失敗 %s: %s", race_id, oe)
                    # 2) フォールバック: ネット競馬（公式取得失敗時のみ）
                    if not result:
                        try:
                            result = _get_nk_scraper().get_tansho(race_id)
                            if result:
                                _odds_source = "netkeiba"
                                logger.info("netkeiba オッズ取得: %s (%d頭)", race_id, len(result))
                        except Exception:
                            pass

                    if result:
                        live_odds[race_id] = {
                            str(horse_no): [odds, rank]
                            for horse_no, (odds, rank) in result.items()
                        }
                        for h in race.get("horses", []):
                            hno = str(h.get("horse_no", ""))
                            if hno in live_odds[race_id]:
                                h["odds"] = live_odds[race_id][hno][0]
                                h["popularity"] = live_odds[race_id][hno][1]
                                _recalc_divergence(h)
                                pred_modified = True
                        # 実出走馬の人気を再計算（取消馬を除いたフィールド内で順位付け）
                        _active_horses = [
                            h for h in race.get("horses", [])
                            if h.get("odds") is not None and h.get("odds", 0) > 0
                        ]
                        _active_horses.sort(key=lambda h: h.get("odds", 9999))
                        for _rank, _h in enumerate(_active_horses, 1):
                            _h["popularity"] = _rank
                        count += 1
                except Exception as e:
                    logger.warning("odds fetch failed race_id=%s: %s", race_id, e)

                # 三連複オッズ取得（netkeiba → 公式サイトフォールバック）
                try:
                    _san_map = {}
                    try:
                        _san_map = _get_nk_scraper().get_sanrenpuku_odds(race_id)
                    except Exception:
                        pass
                    if not _san_map and official:
                        try:
                            _san_map = official.get_sanrenpuku_odds(race_id)
                        except Exception:
                            pass
                    if _san_map:
                        for t in race.get("tickets", []):
                            if t.get("type") != "三連複":
                                continue
                            combo = t.get("combo", [])
                            if len(combo) == 3:
                                key = tuple(sorted(int(x) for x in combo))
                                if key in _san_map:
                                    t["odds"] = round(_san_map[key], 1)
                                    prob = t.get("prob", 0)
                                    if prob > 0:
                                        t["ev"] = round(prob * _san_map[key] * 100, 1)
                                    pred_modified = True
                        logger.info("三連複オッズ一括更新: %s (%d組)", race_id, len(_san_map))
                except Exception as _se:
                    logger.debug("三連複オッズ一括取得失敗 %s: %s", race_id, _se)

                # 馬体重・馬主を取得（公式サイト優先 → netkeiba フォールバック）
                wt_fetched = False
                if official:
                    try:
                        wt_data = official.get_weights(race_id)
                        if wt_data:
                            for h in race.get("horses", []):
                                hno = h.get("horse_no")
                                if hno and hno in wt_data:
                                    info = wt_data[hno]
                                    h["horse_weight"] = info["weight"]
                                    h["weight_change"] = info["weight_change"]
                                    h["weight_confirmed"] = True
                                    owner = info.get("owner", "")
                                    if owner and not h.get("owner"):
                                        h["owner"] = owner
                                    pred_modified = True
                            wt_fetched = True
                    except Exception as e:
                        logger.debug("公式馬体重失敗 %s: %s", race_id, e)
                if not wt_fetched and not weight_fetch_failed:
                    try:
                        weights = _get_nk_scraper().get_weights(race_id)
                        if weights:
                            for h in race.get("horses", []):
                                hno = h.get("horse_no")
                                if hno and hno in weights:
                                    w, wc = weights[hno]
                                    h["horse_weight"] = w
                                    h["weight_change"] = wc
                                    pred_modified = True
                    except Exception as e:
                        if "403" in str(e) or "Forbidden" in str(e):
                            weight_fetch_failed = True
                            logger.info("馬体重取得スキップ（netkeiba 403ブロック）")
                        else:
                            logger.debug("weight fetch skipped race_id=%s: %s", race_id, e)

                # ばんえい: 馬場水分量を更新 + AI見解を再生成
                if race.get("is_banei") and official:
                    try:
                        moisture = official.get_banei_moisture(race_id)
                        if moisture is not None:
                            old_wc = race.get("water_content")
                            if old_wc != moisture:
                                race["water_content"] = moisture
                                pred_modified = True
                                logger.info("ばんえい水分量更新: %s → %.1f%%", race_id, moisture)
                    except Exception as _me:
                        logger.debug("ばんえい水分量取得失敗 %s: %s", race_id, _me)
                    # 水分量・馬体重が揃ったらAI見解を再生成
                    try:
                        from src.calculator.calibration import generate_banei_comment_dict
                        new_comment = generate_banei_comment_dict(race)
                        if new_comment and new_comment != race.get("pace_comment"):
                            race["pace_comment"] = new_comment
                            pred_modified = True
                            logger.info("ばんえいAI見解再生成: %s", race_id)
                    except Exception as _bce:
                        logger.debug("ばんえいAI見解再生成失敗 %s: %s", race_id, _bce)

            out_path = os.path.join(OUTPUT_DIR, f"{date_key}_live_odds.json")
            with open(out_path, "w", encoding="utf-8") as of:
                json.dump(live_odds, of, ensure_ascii=False)

            # JRA レース結果CNAME を取得して保存（毎回更新）
            if official:
                for race in races:
                    race_id = race.get("race_id")
                    if race_id:
                        try:
                            rc = official.get_result_cname(race_id)
                            if rc and rc != race.get("result_cname"):
                                race["result_cname"] = rc
                                pred_modified = True
                        except Exception:
                            pass

            # 全レースの確率・印を人気別統計ブレンドで再計算
            try:
                from src.calculator.popularity_blend import (
                    blend_probabilities_dict,
                    load_popularity_stats,
                    reassign_marks_dict,
                )
                _pop_stats = load_popularity_stats()
                if _pop_stats:
                    _frozen_count = 0
                    for race in races:
                        _horses = race.get("horses", [])
                        if any(h.get("popularity") for h in _horses):
                            blend_probabilities_dict(
                                _horses, race.get("venue", ""),
                                race.get("is_jra", True), len(_horses), _pop_stats,
                            )
                            if not _is_marks_frozen(race):
                                reassign_marks_dict(_horses)
                            else:
                                _frozen_count += 1
                    if _frozen_count:
                        logger.info("一括オッズ更新: 確率再計算完了（印固定: %dR）", _frozen_count)
                    else:
                        logger.info("一括オッズ更新後の確率・印を再計算完了")
            except Exception as _e:
                logger.warning("一括確率再計算に失敗: %s", _e)

            # pred.json にオッズ・馬体重 + タイムスタンプを書き戻し
            from datetime import datetime as _dt
            ts = _dt.now().strftime("%Y-%m-%dT%H:%M:%S")
            pred["odds_updated_at"] = ts
            if pred_modified or True:  # タイムスタンプは常に書き込み
                try:
                    with open(pred_file, "w", encoding="utf-8") as wf:
                        json.dump(pred, wf, ensure_ascii=False, indent=2)
                except Exception as e:
                    logger.warning("pred.json write-back failed: %s", e)

            _odds_state.update(running=False, done=True, count=count,
                               current=total_races, current_race="",
                               updated_at=_dt.now().strftime("%H:%M:%S"))
            # キャッシュクリア
            _predictions_cache.pop(date_key[:4] + "-" + date_key[4:6] + "-" + date_key[6:8], None)
        except Exception as e:
            logger.error("odds_update failed: %s", e, exc_info=True)
            _odds_state.update(running=False, done=True, error=str(e))

    # ── オッズ自動取得スケジューラー ──
    def _odds_target_date():
        """スケジュール時刻に応じて対象レース日を決定（19:00-23:59→翌日/0:00-17:59→当日）"""
        now = datetime.now()
        if now.hour >= 19:
            return (now + timedelta(days=1)).strftime("%Y%m%d")
        return now.strftime("%Y%m%d")

    def _start_odds_scheduler():
        global _odds_scheduler_running
        if _odds_scheduler_running:
            return
        _odds_scheduler_running = True

        def _scheduler_loop():
            global _odds_scheduler_running, _odds_last_auto_fetch
            import time as _st
            while _odds_scheduler_running:
                try:
                    now = datetime.now()
                    next_times = []
                    for h in _ODDS_SCHEDULE_HOURS:
                        t = now.replace(hour=h, minute=0, second=0, microsecond=0)
                        if t <= now:
                            t += timedelta(days=1)
                        next_times.append(t)
                    next_run = min(next_times)
                    wait_sec = (next_run - now).total_seconds()
                    logger.info("[odds-scheduler] 次回取得: %s (%d秒後)", next_run.strftime("%H:%M"), int(wait_sec))

                    # 60秒ごとにチェック（停止可能にする）
                    while wait_sec > 0 and _odds_scheduler_running:
                        _st.sleep(min(60, wait_sec))
                        wait_sec = (next_run - datetime.now()).total_seconds()

                    if not _odds_scheduler_running:
                        break

                    date_key = _odds_target_date()
                    pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
                    if not os.path.isfile(pred_file):
                        logger.info("[odds-scheduler] %s の予想データなし → スキップ", date_key)
                        continue

                    if _odds_state.get("running"):
                        logger.info("[odds-scheduler] 手動更新実行中 → スキップ")
                        continue

                    logger.info("[odds-scheduler] 自動取得開始: %s", date_key)
                    _run_odds_update(date_key, source="auto")
                    _odds_last_auto_fetch = datetime.now()
                except Exception as e:
                    logger.error("[odds-scheduler] 例外発生: %s", e, exc_info=True)
                    _st.sleep(60)

        import threading as _th
        _th.Thread(target=_scheduler_loop, daemon=True, name="odds-scheduler").start()
        logger.info("[odds-scheduler] スケジューラー起動 (時刻: %s)", _ODDS_SCHEDULE_HOURS)

    # スケジューラー起動
    _start_odds_scheduler()

    # ── 予想自動生成スケジューラー ──
    def _run_auto_predict(date_str):
        """自動予想生成（subprocess で main.py --official を呼び出す）"""
        global _analyzer_state, _analyzer_proc
        _analyzer_state = {
            "running": True, "done": False, "error": None,
            "progress": f"[自動] {date_str} 予想生成中...",
            "current_race": "", "done_races": 0,
            "total_races": 0, "elapsed_sec": 0,
            "start_time": time.time(),
        }
        try:
            _env = os.environ.copy()
            _env["PYTHONUTF8"] = "1"
            _env["PYTHONIOENCODING"] = "utf-8"
            cmd = [
                sys.executable,
                os.path.join(PROJECT_ROOT, "main.py"),
                "--analyze_date", date_str,
                "--official",
                "--output", OUTPUT_DIR,
                "--no_open",
                "--workers", "3",
                "--quiet",
                "--no_html",
            ]
            _cflags = 0
            if sys.platform == "win32":
                _cflags = subprocess.CREATE_NO_WINDOW
            proc = subprocess.Popen(
                cmd, cwd=PROJECT_ROOT,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace",
                env=_env, creationflags=_cflags,
            )
            _analyzer_proc = proc

            import re as _re
            full_out = []
            for line in proc.stdout:
                full_out.append(line)
                _analyzer_state["elapsed_sec"] = int(
                    time.time() - _analyzer_state["start_time"]
                )
                m = _re.search(r"\[完了\]\s+(.+?)\s+\((\d+)/(\d+)", line)
                if m:
                    label = m.group(1)
                    n, total = int(m.group(2)), int(m.group(3))
                    _analyzer_state["done_races"] = n
                    _analyzer_state["total_races"] = total
                    _analyzer_state["current_race"] = f"[完了] {label}"
                    _analyzer_state["progress"] = f"[自動] {label} ({n}/{total}レース)"
                else:
                    m2 = _re.search(r"\[スキップ\].*\((\d+)/(\d+)", line)
                    if m2:
                        _analyzer_state["done_races"] = int(m2.group(1))
                    else:
                        m3 = _re.search(r"\[分析中\].*\((\d+)/(\d+)", line)
                        if m3:
                            _analyzer_state["done_races"] = int(m3.group(1)) - 1
                            _analyzer_state["total_races"] = int(m3.group(2))
                            _analyzer_state["progress"] = f"[自動] 分析中... ({int(m3.group(1))}/{int(m3.group(2))}レース)"
            proc.wait()

            _analyzer_state["elapsed_sec"] = int(
                time.time() - _analyzer_state["start_time"]
            )
            if proc.returncode != 0:
                out = "".join(full_out)
                _analyzer_state["error"] = out[-600:] if len(out) > 600 else out
                logger.error("[predict-scheduler] 自動予想生成失敗 (code=%d)", proc.returncode)
            else:
                _analyzer_state["progress"] = f"[自動完了] {date_str}"
                logger.info("[predict-scheduler] 自動予想生成完了: %s", date_str)
                _predictions_cache.pop(date_str, None)
        except Exception as e:
            logger.error("[predict-scheduler] 自動予想生成エラー: %s", e, exc_info=True)
            _analyzer_state["error"] = str(e)
        finally:
            _analyzer_state["running"] = False
            _analyzer_state["done"] = True
            _analyzer_proc = None

    def _start_predict_scheduler():
        global _predict_scheduler_running
        if _predict_scheduler_running:
            return
        _predict_scheduler_running = True

        def _predict_scheduler_loop():
            global _predict_scheduler_running, _predict_last_auto_run
            import time as _st
            while _predict_scheduler_running:
                try:
                    now = datetime.now()
                    target = now.replace(
                        hour=_PREDICT_SCHEDULE_HOUR, minute=0, second=0, microsecond=0
                    )
                    if target <= now:
                        target += timedelta(days=1)
                    wait_sec = (target - now).total_seconds()
                    logger.info(
                        "[predict-scheduler] 次回実行: %s (%d秒後)",
                        target.strftime("%m/%d %H:%M"), int(wait_sec),
                    )

                    while wait_sec > 0 and _predict_scheduler_running:
                        _st.sleep(min(60, wait_sec))
                        wait_sec = (target - datetime.now()).total_seconds()

                    if not _predict_scheduler_running:
                        break

                    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
                    date_key = tomorrow.replace("-", "")
                    pred_file = os.path.join(
                        PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json"
                    )
                    if os.path.isfile(pred_file):
                        logger.info(
                            "[predict-scheduler] %s の予想データは既に存在 → スキップ",
                            tomorrow,
                        )
                        continue

                    if _analyzer_state.get("running"):
                        logger.info("[predict-scheduler] 手動分析実行中 → スキップ")
                        continue

                    logger.info("[predict-scheduler] 自動予想生成開始: %s", tomorrow)
                    _run_auto_predict(tomorrow)
                    _predict_last_auto_run = datetime.now()
                except Exception as e:
                    logger.error("[predict-scheduler] 例外発生: %s", e, exc_info=True)
                    _st.sleep(60)

        import threading as _th
        _th.Thread(
            target=_predict_scheduler_loop, daemon=True, name="predict-scheduler"
        ).start()
        logger.info(
            "[predict-scheduler] スケジューラー起動 (毎日 %d:00 → 翌日予想)",
            _PREDICT_SCHEDULE_HOUR,
        )

    _start_predict_scheduler()

    @app.route("/api/predict_schedule_status")
    def api_predict_schedule_status():
        return jsonify(
            scheduler_running=_predict_scheduler_running,
            schedule_hour=_PREDICT_SCHEDULE_HOUR,
            last_auto_run=_predict_last_auto_run.strftime("%Y-%m-%d %H:%M")
            if _predict_last_auto_run
            else None,
        )

    # ── 結果照合+DB更新 自動スケジューラー ──
    def _run_auto_results(date_str):
        """自動結果照合+DB更新（当日分の結果を取得し照合、その後DB更新）"""
        global _results_state, _db_update_state
        logger.info("[results-scheduler] 自動結果照合開始: %s", date_str)

        # --- Phase 1: 結果取得+照合 ---
        _results_state = {
            "running": True, "done": False, "cancel": False,
            "progress": f"[自動] {date_str} の結果を取得中...",
            "error": None, "total": 1, "completed": 0,
            "start_time": time.time(), "current_date": date_str,
            "log": [],
        }
        try:
            from src.results_tracker import compare_and_aggregate, fetch_actual_results
            from src.scraper.netkeiba import NetkeibaClient
            client = NetkeibaClient(no_cache=True)

            _results_state["progress"] = f"[自動] {date_str} の着順取得中..."
            fetch_actual_results(date_str, client)

            _results_state["progress"] = f"[自動] {date_str} を照合中..."
            result = compare_and_aggregate(date_str)

            _results_state["completed"] = 1
            _results_state["log"].append(f"✓ {date_str} 結果照合完了")
            if result:
                roi = result.get("roi", 0)
                _results_state["log"].append(
                    f"  回収率: {roi:.1f}%  的中: {result.get('hit_tickets', 0)}/{result.get('total_tickets', 0)}"
                )
            logger.info("[results-scheduler] 結果照合完了: %s", date_str)
        except Exception as e:
            logger.error("[results-scheduler] 結果照合エラー: %s", e, exc_info=True)
            _results_state["error"] = str(e)
            _results_state["log"].append(f"✗ 結果照合エラー: {e}")
        finally:
            _results_state["running"] = False
            _results_state["done"] = True
            _results_state["end_time"] = time.time()

        # --- Phase 2: DB更新 ---
        if _results_state.get("error"):
            logger.warning("[results-scheduler] 結果照合でエラー発生 → DB更新スキップ")
            return

        if _db_update_state.get("running"):
            logger.info("[results-scheduler] DB更新が既に実行中 → スキップ")
            return

        logger.info("[results-scheduler] 自動DB更新開始")
        _db_update_state = {
            "running": True, "done": False, "cancel": False,
            "progress": "[自動] DB更新中...", "error": None,
            "start_time": time.time(), "step": 0, "total_steps": 3,
            "log": [],
        }
        try:
            _db_update_state["step"] = 1
            _db_update_state["progress"] = "[自動] [1/3] コースDB更新中..."
            _db_update_state["log"].append("[自動] コースDB更新開始")
            from src.scraper.course_db_collector import collect_course_db_from_results
            from src.scraper.netkeiba import NetkeibaClient, RaceListScraper
            client2 = NetkeibaClient()
            rls = RaceListScraper(client2)
            collect_course_db_from_results(
                client2, rls, date_str, date_str,
                COURSE_DB_PRELOAD_PATH,
                state_path=COURSE_DB_COLLECTOR_STATE_PATH,
            )
            _db_update_state["log"].append("✓ コースDB更新完了")

            _db_update_state["step"] = 2
            _db_update_state["progress"] = "[自動] [2/3] レース戦績DB更新中..."
            _db_update_state["log"].append("[自動] race_log更新開始")
            try:
                from src.database import populate_race_log_from_predictions
                new_rows = populate_race_log_from_predictions()
                _db_update_state["log"].append(f"✓ race_log更新完了 (新規{new_rows:,}件)")
            except Exception as e:
                logger.warning("[results-scheduler] race_log更新失敗: %s", e, exc_info=True)
                _db_update_state["log"].append(f"⚠ race_log更新失敗: {e}")

            _db_update_state["step"] = 3
            _db_update_state["progress"] = "[自動] [3/3] 騎手・調教師キャッシュ再構築中..."
            _db_update_state["log"].append("[自動] キャッシュ再構築開始")
            _personnel_stats_cache.clear()
            _db_update_state["log"].append("✓ キャッシュ再構築完了")

            _db_update_state["progress"] = "[自動完了] DB更新完了"
            logger.info("[results-scheduler] 自動DB更新完了")
        except Exception as e:
            logger.error("[results-scheduler] DB更新エラー: %s", e, exc_info=True)
            _db_update_state["error"] = str(e)
            _db_update_state["log"].append(f"✗ DB更新エラー: {e}")
        finally:
            _db_update_state["running"] = False
            _db_update_state["done"] = True
            _db_update_state["end_time"] = time.time()

    def _start_results_scheduler():
        global _results_scheduler_running
        if _results_scheduler_running:
            return
        _results_scheduler_running = True

        def _results_scheduler_loop():
            global _results_scheduler_running, _results_last_auto_run
            import time as _st
            while _results_scheduler_running:
                try:
                    now = datetime.now()
                    target = now.replace(
                        hour=_RESULTS_SCHEDULE_HOUR, minute=0, second=0, microsecond=0
                    )
                    if target <= now:
                        target += timedelta(days=1)
                    wait_sec = (target - now).total_seconds()
                    logger.info(
                        "[results-scheduler] 次回実行: %s (%d秒後)",
                        target.strftime("%m/%d %H:%M"), int(wait_sec),
                    )

                    while wait_sec > 0 and _results_scheduler_running:
                        _st.sleep(min(60, wait_sec))
                        wait_sec = (target - datetime.now()).total_seconds()

                    if not _results_scheduler_running:
                        break

                    # 当日の結果を照合（レース終了後なので当日分）
                    today = datetime.now().strftime("%Y-%m-%d")

                    # 予想JSONが存在しない日はスキップ
                    from src.results_tracker import load_prediction
                    if not load_prediction(today):
                        logger.info(
                            "[results-scheduler] %s の予想データなし → スキップ", today
                        )
                        continue

                    # 結果照合が実行中ならスキップ
                    if _results_state.get("running"):
                        logger.info("[results-scheduler] 手動結果照合実行中 → スキップ")
                        continue

                    # 分析が実行中ならスキップ
                    if _analyzer_state.get("running"):
                        logger.info("[results-scheduler] 分析実行中 → スキップ")
                        continue

                    logger.info("[results-scheduler] 自動結果照合+DB更新開始: %s", today)
                    _run_auto_results(today)
                    _results_last_auto_run = datetime.now()
                except Exception as e:
                    logger.error("[results-scheduler] 例外発生: %s", e, exc_info=True)
                    _st.sleep(60)

        import threading as _th
        _th.Thread(
            target=_results_scheduler_loop, daemon=True, name="results-scheduler"
        ).start()
        logger.info(
            "[results-scheduler] スケジューラー起動 (毎日 %d:00 → 結果照合+DB更新)",
            _RESULTS_SCHEDULE_HOUR,
        )

    _start_results_scheduler()

    @app.route("/api/results_schedule_status")
    def api_results_schedule_status():
        return jsonify(
            scheduler_running=_results_scheduler_running,
            schedule_hour=_RESULTS_SCHEDULE_HOUR,
            last_auto_run=_results_last_auto_run.strftime("%Y-%m-%d %H:%M")
            if _results_last_auto_run
            else None,
        )

    @app.route("/api/odds_update", methods=["POST"])
    def api_odds_update():
        global _odds_state
        if _odds_state["running"]:
            return jsonify(ok=False, error="既に更新中です")

        data = request.get_json(force=True, silent=True) or {}
        date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
        if not _is_admin(request):
            date_str = datetime.now().strftime("%Y-%m-%d")
        date_key = date_str.replace("-", "")

        pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
        if not os.path.isfile(pred_file):
            return jsonify(ok=False, error=f"予想データが見つかりません: {date_key}")

        import threading
        threading.Thread(target=_run_odds_update, args=(date_key, "manual"), daemon=True).start()
        return jsonify(ok=True)

    @app.route("/api/odds_update_status")
    def api_odds_update_status():
        return jsonify(_odds_state)

    @app.route("/api/odds_update_cancel", methods=["POST"])
    def api_odds_update_cancel():
        global _odds_cancel
        if not _odds_state.get("running"):
            return jsonify(ok=False, error="実行中ではありません")
        _odds_cancel = True
        return jsonify(ok=True)

    @app.route("/api/odds_schedule_status")
    def api_odds_schedule_status():
        return jsonify(
            scheduler_running=_odds_scheduler_running,
            last_auto_fetch=_odds_last_auto_fetch.strftime("%H:%M") if _odds_last_auto_fetch else None,
            schedule=_ODDS_SCHEDULE_HOURS,
        )

    @app.route("/api/odds/unfetched_dates")
    def api_odds_unfetched_dates():
        """予想済みだがオッズ未取得の日付一覧を返す（2024-01-01〜昨日）"""
        try:
            from src.results_tracker import list_prediction_dates
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            pred_dates = [d for d in list_prediction_dates()
                          if "2024-01-01" <= d <= yesterday]
            unfetched = []
            for d in pred_dates:
                key = d.replace("-", "")
                odds_file = os.path.join(OUTPUT_DIR, f"{key}_live_odds.json")
                if not os.path.exists(odds_file):
                    unfetched.append(d)
            return jsonify(dates=unfetched)
        except Exception as e:
            logger.warning("odds unfetched dates failed: %s", e, exc_info=True)
            return jsonify(dates=[], error=str(e))

    @app.route("/api/predictions/unfetched_dates")
    def api_predictions_unfetched_dates():
        """開催日だがまだ予想未作成の日付一覧を返す（2024-01-01〜昨日）"""
        try:
            from src.results_tracker import list_prediction_dates
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            pred_dates_set = set(list_prediction_dates())
            # output/ の HTML ファイルから開催日を推測
            race_dates = set()
            if os.path.isdir(OUTPUT_DIR):
                for name in os.listdir(OUTPUT_DIR):
                    if name.endswith(".html") and len(name) >= 8:
                        dk = name[:8]
                        if dk.isdigit():
                            d = f"{dk[:4]}-{dk[4:6]}-{dk[6:8]}"
                            if "2024-01-01" <= d <= yesterday:
                                race_dates.add(d)
            # predictions ディレクトリからも日付を取得
            pred_dir = os.path.join(PROJECT_ROOT, "data", "predictions")
            if os.path.isdir(pred_dir):
                for name in os.listdir(pred_dir):
                    if name.endswith("_pred.json") and len(name) >= 8:
                        dk = name[:8]
                        if dk.isdigit():
                            d = f"{dk[:4]}-{dk[4:6]}-{dk[6:8]}"
                            if "2024-01-01" <= d <= yesterday:
                                race_dates.add(d)
            # 予想済み日付を除外
            unfetched = sorted([d for d in race_dates if d not in pred_dates_set], reverse=True)
            return jsonify(dates=unfetched)
        except Exception as e:
            logger.warning("predictions unfetched dates failed: %s", e, exc_info=True)
            return jsonify(dates=[], error=str(e))

    @app.route("/api/results/dates")
    def api_results_dates():
        """予想済み日付一覧 + 日次統計を返す"""
        try:
            from src.results_tracker import list_prediction_dates, aggregate_all

            dates = list_prediction_dates()
            # 日次統計を取得（軽量フィールドのみ）
            agg = aggregate_all(year_filter="all")
            by_date = agg.get("by_date", [])
            daily_stats = {}
            for r in by_date:
                d = r.get("date", "")
                if not d:
                    continue
                ht = r.get("honmei_total", 0)
                hw = r.get("honmei_win", 0)
                hp2 = r.get("honmei_place2", 0)
                hp3 = r.get("honmei_placed", 0)
                stake = r.get("honmei_tansho_stake", ht * 100)
                ret = r.get("honmei_tansho_ret", 0)
                daily_stats[d] = {
                    "races": r.get("total_races", 0),
                    "profit": ret - stake,
                    "roi": r.get("honmei_tansho_roi", 0),
                    "win": hw,
                    "place2": hp2,
                    "placed": hp3,
                    "total": ht,
                    "rate": r.get("honmei_rate", 0),
                }
            return jsonify(dates=dates, daily_stats=daily_stats)
        except Exception as e:
            logger.warning("prediction dates list failed: %s", e, exc_info=True)
            return jsonify(dates=[], daily_stats={}, error=str(e))

    @app.route("/api/results/unmatched_dates")
    def api_results_unmatched_dates():
        """予想済みだが着順取得済みでない日付一覧を返す（2024-01-01〜昨日）"""
        try:
            from src.results_tracker import list_prediction_dates
            from config.settings import RESULTS_DIR

            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            pred_dates = [d for d in list_prediction_dates()
                          if "2024-01-01" <= d <= yesterday]
            unmatched = []
            for d in pred_dates:
                key = d.replace("-", "")
                result_file = os.path.join(RESULTS_DIR, f"{key}_results.json")
                if not os.path.exists(result_file):
                    unmatched.append(d)
            return jsonify(dates=unmatched)
        except Exception as e:
            logger.warning("unmatched dates list failed: %s", e, exc_info=True)
            return jsonify(dates=[], error=str(e))

    @app.route("/api/results/summary")
    def api_results_summary():
        """通算成績を返す（year=all/2025/2026 等）"""
        try:
            from src.results_tracker import aggregate_all

            year = request.args.get("year", "all")
            return jsonify(aggregate_all(year_filter=year))
        except Exception as e:
            logger.warning("results summary failed: %s", e, exc_info=True)
            return jsonify(error=str(e))

    @app.route("/api/results/detailed")
    def api_results_detailed():
        """詳細集計（競馬場別・コース別・距離区分別・高額配当TOP10）"""
        try:
            from src.results_tracker import aggregate_detailed

            year = request.args.get("year", "all")
            return jsonify(aggregate_detailed(year_filter=year))
        except Exception as e:
            logger.warning("results detailed failed: %s", e, exc_info=True)
            return jsonify(error=str(e))

    @app.route("/api/results/trend")
    def api_results_trend():
        """累積回収率推移・月別収支データ（Chart.js用）"""
        try:
            from src.results_tracker import aggregate_all

            year = request.args.get("year", "all")
            agg = aggregate_all(year_filter=year)
            by_date = agg.get("by_date", [])
            if not by_date:
                return jsonify(labels=[], ticket_roi_cum=[], honmei_tansho_roi_cum=[],
                               monthly_labels=[], monthly_profit=[])

            # 日付昇順にソート
            by_date_sorted = sorted(by_date, key=lambda r: r.get("date", ""))

            labels = []
            ticket_roi_cum = []
            honmei_roi_cum = []
            cum_stake = 0
            cum_ret = 0
            cum_h_stake = 0
            cum_h_ret = 0

            monthly_profit_map: dict = {}

            for r in by_date_sorted:
                d = r.get("date", "")
                if not d:
                    continue
                labels.append(d)
                cum_stake += r.get("total_stake", 0)
                cum_ret   += r.get("total_return", 0)
                cum_h_stake += r.get("honmei_tansho_stake", r.get("honmei_total", 0) * 100)
                cum_h_ret   += r.get("honmei_tansho_ret", 0)
                roi_c  = round(cum_ret   / cum_stake   * 100, 1) if cum_stake   > 0 else 0.0
                h_roi_c = round(cum_h_ret / cum_h_stake * 100, 1) if cum_h_stake > 0 else 0.0
                ticket_roi_cum.append(roi_c)
                honmei_roi_cum.append(h_roi_c)

                # 月別収支 (YYYY-MM) — ◉◎単勝ベース
                month_key = d[:7]
                profit_day = r.get("honmei_tansho_ret", 0) - r.get("honmei_tansho_stake", r.get("honmei_total", 0) * 100)
                monthly_profit_map[month_key] = monthly_profit_map.get(month_key, 0) + profit_day

            monthly_sorted = sorted(monthly_profit_map.items())
            monthly_labels = [m for m, _ in monthly_sorted]
            monthly_profit = [v for _, v in monthly_sorted]

            return jsonify(
                labels=labels,
                ticket_roi_cum=ticket_roi_cum,
                honmei_tansho_roi_cum=honmei_roi_cum,
                monthly_labels=monthly_labels,
                monthly_profit=monthly_profit,
            )
        except Exception as e:
            logger.warning("results trend failed: %s", e, exc_info=True)
            return jsonify(error=str(e))

    @app.route("/api/generate_simple_html", methods=["POST"])
    def api_generate_simple_html():
        """指定日の配布用HTML（印・買い目のみ）を生成"""
        data = request.get_json() or {}
        date = data.get("date", "")
        if not date:
            return jsonify(ok=False, error="日付が指定されていません")
        try:
            from src.results_tracker import generate_simple_html
            from config.settings import OUTPUT_DIR

            fpath = generate_simple_html(date, OUTPUT_DIR)
            if fpath is None:
                return jsonify(ok=False, error=f"{date} の予想データがありません")
            return jsonify(ok=True, filename=os.path.basename(fpath), path=fpath)
        except Exception as e:
            logger.warning("generate_simple_html failed: %s", e, exc_info=True)
            return jsonify(ok=False, error=str(e))

    @app.route("/api/results/fetch", methods=["POST"])
    def api_results_fetch():
        """指定日の着順をnetkeiba から取得して照合"""
        if not _is_admin(request):
            return jsonify(ok=False, error="この操作は管理者のみ実行できます"), 403
        data = request.get_json() or {}
        date = data.get("date", "")
        if not date:
            return jsonify(ok=False, error="日付が指定されていません")
        try:
            from src.results_tracker import compare_and_aggregate, fetch_actual_results
            from src.scraper.netkeiba import NetkeibaClient

            client = NetkeibaClient(no_cache=True)
            fetch_actual_results(date, client)
            result = compare_and_aggregate(date)
            return jsonify(ok=True, result=result)
        except Exception as e:
            logger.warning("results fetch failed: %s", e, exc_info=True)
            return jsonify(ok=False, error=str(e))

    # ── 結果取得バッチ（中断対応） ──
    @app.route("/api/results/fetch_batch", methods=["POST"])
    def api_results_fetch_batch():
        """複数日付の結果取得をバックグラウンドで実行（中断対応）"""
        if not _is_admin(request):
            return jsonify(ok=False, error="この操作は管理者のみ実行できます"), 403
        global _results_state
        if _results_state.get("running"):
            return jsonify(ok=False, error="既に実行中です")
        data = request.get_json() or {}
        dates = data.get("dates", [])
        if not dates:
            return jsonify(ok=False, error="日付が指定されていません")

        _results_state = {"running": True, "done": False, "cancel": False,
                          "progress": f"0/{len(dates)}日", "error": None,
                          "total": len(dates), "completed": 0,
                          "start_time": time.time(), "current_date": "",
                          "log": []}

        def _run():
            global _results_state
            try:
                from src.results_tracker import compare_and_aggregate, fetch_actual_results
                from src.scraper.netkeiba import NetkeibaClient
                client = NetkeibaClient(no_cache=True)
                ok = 0
                for i, dt in enumerate(dates):
                    if _results_state.get("cancel"):
                        _results_state["error"] = f"中断しました（{ok}/{len(dates)}日完了）"
                        break
                    _results_state["current_date"] = dt
                    _results_state["progress"] = f"[{i+1}/{len(dates)}] {dt} の結果を取得中…"
                    fetch_actual_results(dt, client)
                    if _results_state.get("cancel"):
                        _results_state["error"] = f"中断しました（{ok}/{len(dates)}日完了）"
                        break
                    _results_state["progress"] = f"[{i+1}/{len(dates)}] {dt} を照合中…"
                    compare_and_aggregate(dt)
                    ok += 1
                    _results_state["completed"] = ok
                    _results_state["progress"] = f"{ok}/{len(dates)}日完了"
                    _results_state["log"].append(f"✓ {dt}")
                    if len(_results_state["log"]) > 50:
                        _results_state["log"] = _results_state["log"][-50:]
            except Exception as e:
                _results_state["error"] = str(e)
                _results_state["log"].append(f"✗ エラー: {e}")
            finally:
                _results_state["running"] = False
                _results_state["done"] = True
                _results_state["end_time"] = time.time()

        threading.Thread(target=_run, daemon=True).start()
        return jsonify(ok=True, total=len(dates))

    @app.route("/api/results/fetch_status")
    def api_results_fetch_status():
        out = dict(_results_state)
        st = _results_state.get("start_time")
        if st and _results_state.get("running"):
            out["elapsed_sec"] = round(time.time() - st)
        elif st:
            out["elapsed_sec"] = round((_results_state.get("end_time") or time.time()) - st)
        return jsonify(out)

    @app.route("/api/results/fetch_cancel", methods=["POST"])
    def api_results_fetch_cancel():
        global _results_state
        if not _results_state.get("running"):
            return jsonify(ok=False, error="実行中ではありません")
        _results_state["cancel"] = True
        return jsonify(ok=True)

    # ── DB更新（中断対応） ──
    @app.route("/api/db/update", methods=["POST"])
    def api_db_update():
        """騎手・調教師・コースDBを再構築"""
        if not _is_admin(request):
            return jsonify(ok=False, error="この操作は管理者のみ実行できます"), 403
        global _db_update_state
        if _db_update_state.get("running"):
            return jsonify(ok=False, error="既に実行中です")
        data = request.get_json() or {}
        date = data.get("date", datetime.now().strftime("%Y-%m-%d"))
        start_date = data.get("start_date", "")
        end_date = data.get("end_date", "")

        _db_update_state = {"running": True, "done": False, "cancel": False,
                            "progress": "開始中...", "error": None,
                            "start_time": time.time(), "step": 0, "total_steps": 3,
                            "log": []}

        def _run():
            global _db_update_state
            try:
                _db_update_state["step"] = 1
                _db_update_state["progress"] = "[1/3] コースDB更新中..."
                _db_update_state["log"].append("コースDB更新開始")
                from src.scraper.course_db_collector import collect_course_db_from_results
                from src.scraper.netkeiba import NetkeibaClient, RaceListScraper
                client = NetkeibaClient()
                rls = RaceListScraper(client)
                sd = start_date or date
                ed = end_date or date
                if _db_update_state.get("cancel"):
                    _db_update_state["error"] = "中断しました"
                    return
                collect_course_db_from_results(
                    client, rls, sd, ed,
                    COURSE_DB_PRELOAD_PATH,
                    state_path=COURSE_DB_COLLECTOR_STATE_PATH,
                )
                _db_update_state["log"].append("✓ コースDB更新完了")

                if _db_update_state.get("cancel"):
                    _db_update_state["error"] = "中断しました"
                    return

                _db_update_state["step"] = 2
                _db_update_state["progress"] = "[2/3] レース戦績DB更新中..."
                _db_update_state["log"].append("レース戦績DB(race_log)更新開始")
                try:
                    from src.database import populate_race_log_from_predictions
                    new_rows = populate_race_log_from_predictions()
                    _db_update_state["log"].append(f"✓ race_log更新完了 (新規{new_rows:,}件)")
                except Exception as e:
                    logger.warning("race_log update failed: %s", e, exc_info=True)
                    _db_update_state["log"].append(f"⚠ race_log更新失敗: {e}")

                if _db_update_state.get("cancel"):
                    _db_update_state["error"] = "中断しました"
                    return

                _db_update_state["step"] = 3
                _db_update_state["progress"] = "[3/3] 騎手・調教師キャッシュ再構築中..."
                _db_update_state["log"].append("騎手・調教師キャッシュ再構築開始")
                # 騎手・調教師キャッシュをクリア（次回アクセス時に再計算）
                try:
                    _personnel_stats_cache.clear()
                except Exception:
                    pass  # キャッシュ未初期化時は無視
                _db_update_state["log"].append("✓ キャッシュ再構築完了")

                _db_update_state["progress"] = "完了"
            except Exception as e:
                logger.warning("db update failed: %s", e, exc_info=True)
                _db_update_state["error"] = str(e)
                _db_update_state["log"].append(f"✗ エラー: {e}")
            finally:
                _db_update_state["running"] = False
                _db_update_state["done"] = True
                _db_update_state["end_time"] = time.time()

        threading.Thread(target=_run, daemon=True).start()
        return jsonify(ok=True)

    @app.route("/api/db/update_status")
    def api_db_update_status():
        out = dict(_db_update_state)
        st = _db_update_state.get("start_time")
        if st and _db_update_state.get("running"):
            out["elapsed_sec"] = round(time.time() - st)
        elif st:
            out["elapsed_sec"] = round((_db_update_state.get("end_time") or time.time()) - st)
        return jsonify(out)

    @app.route("/api/db/update_cancel", methods=["POST"])
    def api_db_update_cancel():
        global _db_update_state
        if not _db_update_state.get("running"):
            return jsonify(ok=False, error="実行中ではありません")
        _db_update_state["cancel"] = True
        return jsonify(ok=True)

    # ナビバー（全output/ファイルに注入） ─ネット競馬風：場タブ＋レース番号タブ
    _NAV_CSS = """<style>
.d-nav-wrap{position:sticky;top:0;z-index:9999;
  font-family:"Hiragino Sans","Yu Gothic UI",sans-serif;
  box-shadow:0 2px 6px rgba(0,0,0,.15)}
/* 上段：ロゴ＋HOME */
.d-nav-top{display:flex;align-items:center;gap:10px;padding:7px 14px;
  background:#0d2b5e;color:#fff}
.d-nav-logo{font-size:18px;font-weight:900;text-decoration:none;letter-spacing:-0.5px}
.d-nav-logo .d-ai{color:#e63946}.d-nav-logo .keiba{color:#4fc3f7}
.d-nav-home{color:#a0c4e8;font-size:12px;text-decoration:none;padding:3px 10px;
  border:1px solid #2a4a7a;border-radius:4px;white-space:nowrap}
.d-nav-home:hover{background:#1a3a6a;color:#fff}
/* 中段：場タブ（ネット競馬風：白背景＋枠線＋下線アクティブ） */
.d-nav-venues{display:flex;background:#f5f5f5;border-bottom:2px solid #ccc;
  overflow-x:auto;scrollbar-width:none;padding:0}
.d-nav-venues::-webkit-scrollbar{display:none}
.d-vnav-btn{color:#333;font-size:14px;font-weight:700;text-decoration:none;
  padding:10px 20px;border:none;border-right:1px solid #ddd;
  background:transparent;cursor:pointer;white-space:nowrap;transition:.15s;
  flex:1;text-align:center;min-width:0;position:relative}
.d-vnav-btn:first-child{border-left:1px solid #ddd}
.d-vnav-btn:hover{color:#0d2b5e;background:#e8ecf3}
.d-vnav-btn.active{color:#0d2b5e;background:#fff;
  border-bottom:3px solid #0d2b5e;margin-bottom:-2px;font-weight:900}
/* 下段：レース番号タブ（□ボックス囲み） */
.d-nav-races{display:flex;align-items:center;background:#fff;border-bottom:2px solid #ccc;
  overflow-x:auto;scrollbar-width:none;padding:4px 6px;gap:3px}
.d-nav-races::-webkit-scrollbar{display:none}
.d-rnav-btn{color:#374151;font-size:13px;font-weight:700;text-decoration:none;
  padding:7px 0;border:2px solid #ccc;border-radius:4px;
  background:#fff;cursor:pointer;white-space:nowrap;transition:.15s;
  flex:1;text-align:center;min-width:0}
.d-rnav-btn:hover{color:#0d2b5e;background:#eef2fa;border-color:#0d2b5e}
.d-rnav-btn.active{color:#fff;background:#0d2b5e;border-color:#0d2b5e}
/* 場リンク */
.d-nav-venue-link{color:#555;font-size:11px;text-decoration:none;white-space:nowrap;
  margin-left:auto;padding:0 10px;display:flex;align-items:center}
.d-nav-venue-link:hover{color:#0d2b5e;text-decoration:underline}
</style>"""

    def _build_nav_bar(filename: str) -> str:
        """ネット競馬風ナビバー（場タブ＋レース番号タブ）を生成"""
        import re as _re

        m = _re.match(r"^(\d{8})_(.+?)(\d+)R\.html$", filename)
        if not m:
            # 個別レースページ以外（全レース等）はシンプルなナビのみ
            return (
                _NAV_CSS
                + """<div class="d-nav-wrap">
<div class="d-nav-top">
  <a href="http://127.0.0.1:5051/dash" class="d-nav-logo"><span class="d-ai">D-AI</span><span class="keiba">keiba</span></a>
  <a href="http://127.0.0.1:5051/dash" class="d-nav-home">← HOME</a>
</div></div>"""
            )

        date_key = m.group(1)
        cur_venue = m.group(2)
        cur_race = int(m.group(3))

        # 同日の全レースファイルを収集
        all_files = sorted(
            [
                f
                for f in os.listdir(OUTPUT_DIR)
                if _re.match(r"^\d{8}_.+\d+R\.html$", f)
                and f.startswith(date_key + "_")
                and "全レース" not in f
                and "share" not in f
            ]
        )

        # 場ごとにグループ化（None・地方XX等の未登録場は除外）
        venues_dict = {}  # venue -> [(race_no, filename)]
        for f in all_files:
            mm = _re.match(r"^\d{8}_(.+?)(\d+)R\.html$", f)
            if mm:
                v, rno = mm.group(1), int(mm.group(2))
                if v == "None" or v.startswith("地方"):
                    continue
                venues_dict.setdefault(v, []).append((rno, f))

        venue_order = sorted(venues_dict.keys(), key=lambda v: _VENUE_PRIO_MAP.get(v, 999))

        # 場タブ生成（レース番号の小さい順にソートし先頭を取得）
        for v in venues_dict:
            venues_dict[v].sort(key=lambda x: x[0])
        venue_tabs = ""
        for v in venue_order:
            active = " active" if v == cur_venue else ""
            first_race_file = venues_dict[v][0][1]
            venue_tabs += f'<a href="/output/{first_race_file}" class="d-vnav-btn{active}">{v}</a>'

        # レース番号タブ（現在の場）
        race_tabs = ""
        for rno, fname in sorted(venues_dict.get(cur_venue, []), key=lambda x: x[0]):
            active = " active" if rno == cur_race else ""
            race_tabs += f'<a href="/output/{fname}" class="d-rnav-btn{active}">{rno}R</a>'

        # date_key は YYYYMMDD → YYYY-MM-DD に変換
        date_fmt = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"
        return (
            _NAV_CSS
            + f"""<div class="d-nav-wrap">
<div class="d-nav-top">
  <a href="http://127.0.0.1:5051/dash" class="d-nav-logo"><span class="d-ai">D-AI</span><span class="keiba">keiba</span></a>
  <a href="http://127.0.0.1:5051/dash" class="d-nav-home">← HOME</a>
  <button id="d-nav-odds-btn" onclick="dNavRefreshOdds('{date_fmt}')" style="margin-left:auto;padding:4px 12px;background:#0369a1;color:#fff;border:none;border-radius:6px;font-size:12px;cursor:pointer;font-weight:600">🔄 オッズ更新</button>
  <span id="d-nav-odds-status" style="font-size:11px;color:#64748b;margin-left:6px"></span>
</div>
<div class="d-nav-venues">{venue_tabs}</div>
<div class="d-nav-races">{race_tabs}</div>
</div>
<script>
function dNavRefreshOdds(date){{
  const btn=document.getElementById('d-nav-odds-btn');
  const st=document.getElementById('d-nav-odds-status');
  if(btn.disabled)return;
  btn.disabled=true;btn.textContent='更新中...';st.textContent='取得中...';
  fetch('/api/odds_update',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{date:date}})}})
  .then(r=>r.json()).then(j=>{{
    if(!j.ok){{st.textContent='エラー: '+(j.error||'不明');btn.disabled=false;btn.textContent='🔄 オッズ更新';return;}}
    const poll=setInterval(()=>{{
      fetch('/api/odds_update_status').then(r=>r.json()).then(s=>{{
        if(s.done||!s.running){{
          clearInterval(poll);
          btn.disabled=false;btn.textContent='🔄 オッズ更新';
          if(s.error){{st.textContent='エラー: '+s.error;return;}}
          st.textContent='✓ '+(s.count||0)+'R更新完了 '+( s.updated_at||'');
          setTimeout(()=>location.reload(),1200);
        }}else{{
          st.textContent='取得中...';
        }}
      }}).catch(()=>{{clearInterval(poll);btn.disabled=false;btn.textContent='🔄 オッズ更新';}});
    }},1500);
  }}).catch(e=>{{st.textContent='通信エラー';btn.disabled=false;btn.textContent='🔄 オッズ更新';}});
}}
</script>"""
        )

    import re as _re_fm
    _RE_COL_MARK = _re_fm.compile(
        r'class="m-([^"]+)"\s+style="margin-right:4px">[^<]*<span class="uma[^"]*">(\d+)</span>'
    )
    _RE_PLAIN_COMBO = _re_fm.compile(
        r'<span class="ftkt-combo">(\d+(?:\s*-\s*\d+)+)</span>'
    )

    def _patch_formation_combo_marks(html: str) -> str:
        """フォーメーション買い目のプレーン番号コンボをマーク付き表示に変換"""
        # 列表示から馬番→マークのマッピングを構築
        mark_map = {}
        for m in _RE_COL_MARK.finditer(html):
            mark, horse_no = m.group(1), m.group(2)
            if horse_no not in mark_map:
                mark_map[horse_no] = mark
        if not mark_map:
            return html

        def _replace_combo(m):
            nums = [n.strip() for n in m.group(1).split("-")]
            parts = []
            for n in nums:
                mk = mark_map.get(n, "－")
                parts.append(
                    f'<span class="m-{mk}">{mk}</span><span class="uma">{n}</span>'
                )
            return '<span class="ftkt-combo">' + " - ".join(parts) + "</span>"

        return _RE_PLAIN_COMBO.sub(_replace_combo, html)

    @app.route("/output/<path:filename>")
    def serve_output(filename):
        if ".." in filename or "/" in filename.replace("\\", "/"):
            return "Not Found", 404
        path = os.path.abspath(os.path.join(OUTPUT_DIR, filename))
        if not path.startswith(os.path.abspath(OUTPUT_DIR)) or not os.path.isfile(path):
            return "Not Found", 404

        # 配布用・全レースまとめはそのまま返す（巨大ファイルにナビ注入しない）
        if "_配布用.html" in filename or "_share.html" in filename or "全レース" in filename:
            return Response(
                open(path, "r", encoding="utf-8", errors="replace").read(),
                mimetype="text/html; charset=utf-8",
            )

        with open(path, "r", encoding="utf-8", errors="replace") as f:
            html = f.read()

        # 古いナビバー（formatter が焼き込んだ site-nav）を除去
        old_nav_start = html.find("<style>")
        if old_nav_start >= 0 and ".site-nav{" in html[old_nav_start : old_nav_start + 500]:
            nav_end = html.find("</nav>", old_nav_start)
            if nav_end >= 0:
                html = html[:old_nav_start] + html[nav_end + 6 :]

        # 古い CSS を最新版に差し替え（再生成不要で即反映）
        _OLD_CSS_MARKER = ".hds-row1{display:flex;align-items:center;gap:6px 8px;flex-wrap:wrap}"
        _NEW_CSS_PATCH = (
            ".hds-row1{display:flex;align-items:center;gap:3px 5px;flex-wrap:wrap}"
        )
        if _OLD_CSS_MARKER in html:
            html = html.replace(_OLD_CSS_MARKER, _NEW_CSS_PATCH)
            html = html.replace(
                ".hds-row2{display:flex;align-items:center;gap:4px 10px;flex-wrap:wrap;padding-left:4px;margin-top:2px}",
                ".hds-row2{display:flex;align-items:center;gap:3px 8px;flex-wrap:wrap;padding-left:4px;margin-top:3px}",
            )
        # 旧 nowrap パッチが適用済みの HTML も wrap に戻す
        html = html.replace(
            ".hds-row1{display:flex;align-items:center;gap:3px 5px;flex-wrap:nowrap;overflow-x:auto;scrollbar-width:none;white-space:nowrap}",
            ".hds-row1{display:flex;align-items:center;gap:3px 5px;flex-wrap:wrap}",
        )
        html = html.replace(
            ".hds-row2{display:flex;align-items:center;gap:3px 8px;flex-wrap:nowrap;padding-left:4px;margin-top:3px;white-space:nowrap}",
            ".hds-row2{display:flex;align-items:center;gap:3px 8px;flex-wrap:wrap;padding-left:4px;margin-top:3px}",
        )
        # グレードバッジのサイズを拡大
        html = html.replace(
            ".hds-grade-item{display:inline-flex;align-items:center;gap:1px;font-size:11px}",
            ".hds-grade-item{display:inline-flex;align-items:center;gap:2px;font-size:13px}",
        )
        html = html.replace(
            ".hds-grade-label{font-size:9px;color:var(--muted)}",
            ".hds-grade-label{font-size:11px;color:var(--muted);font-weight:600}",
        )
        html = html.replace(
            ".hds-grades{display:flex;gap:5px;align-items:center}",
            ".hds-grades{display:flex;gap:8px;align-items:center}",
        )

        # 勝率・連対率・複勝率カラーパッチ（旧HTMLのCSSデフォルト色を除去）
        html = html.replace(
            '.hds-wr-win{font-size:12px;font-weight:700;color:#1e40af}',
            '.hds-wr-win{font-size:12px;font-weight:700}',
        )
        # 旧ティール色を除去（再生成で順位色が入る）
        html = html.replace(
            'class="hds-wr-win" style="color:#0f766e"',
            'class="hds-wr-win"',
        )

        # 券種ラベル色分け + CSS色パッチ（既存HTMLに即反映）
        _OLD_FTKT_TYPE = ".ftkt-type{font-weight:700;min-width:42px;color:var(--navy);font-size:13px}"
        _NEW_FTKT_TYPE = (
            ".ftkt-type{font-weight:700;min-width:42px;font-size:13px;color:#fff;padding:2px 8px;border-radius:4px;text-align:center}"
            "\n.ftkt-type-tansho{background:#16a34a}"
            "\n.ftkt-type-umaren{background:#1a6fa8}"
            "\n.ftkt-type-sanren{background:#c0392b}"
        )
        if _OLD_FTKT_TYPE in html:
            html = html.replace(_OLD_FTKT_TYPE, _NEW_FTKT_TYPE)
        # 旧 ftkt-type クラスに券種別サブクラスを付与
        html = html.replace(
            '<span class="ftkt-type">馬連</span>',
            '<span class="ftkt-type ftkt-type-umaren">馬連</span>',
        )
        html = html.replace(
            '<span class="ftkt-type">三連複</span>',
            '<span class="ftkt-type ftkt-type-sanren">三連複</span>',
        )
        html = html.replace(
            '<span class="ftkt-type">単勝</span>',
            '<span class="ftkt-type ftkt-type-tansho">単勝</span>',
        )
        # 自信度バッジCSS色パッチ（SS=緑, S=青, A=赤, B/C=黒, D/E=灰）
        html = html.replace(
            ".b-SS{background:#6a0dad;color:#fff}",
            ".b-SS{background:#16a34a;color:#fff}",
        )
        html = html.replace(
            ".b-S{background:var(--navy);color:#fff}",
            ".b-S{background:#1a6fa8;color:#fff}",
        )
        html = html.replace(
            ".b-A2{background:var(--blue);color:#fff}",
            ".b-A2{background:#c0392b;color:#fff}",
        )
        html = html.replace(
            ".b-B2{background:#5dade2;color:#fff}",
            ".b-B2{background:#333;color:#fff}",
        )
        html = html.replace(
            ".b-C2{background:var(--muted);color:#fff}",
            ".b-C2{background:#333;color:#fff}",
        )
        # .b-E クラス追加（既存HTMLにない場合）
        if ".b-E{" not in html and ".b-D{background:" in html:
            html = html.replace(
                ".b-D{background:#aaa;color:#fff}",
                ".b-D{background:#aaa;color:#fff}.b-E{background:#aaa;color:#fff}",
            )
        # レース結果リンクを3ボタン化（旧フォーマット→新3ボタン、全箇所置換）
        import re as _re3
        _old_link_pat = _re3.compile(
            r'<a href="(https://(?:race|nar)\.netkeiba\.com)/race/result\.html\?race_id=(\d+)"'
            r'[^>]*>📋 レース結果・払戻</a>'
        )
        _bs = "display:inline-block;font-size:12px;font-weight:600;color:#fff;text-decoration:none;border-radius:4px;padding:3px 12px;margin-right:6px"
        def _replace_link(m):
            _lb = m.group(1)
            _lid = m.group(2)
            return (
                f'<a href="{_lb}/odds/index.html?race_id={_lid}" target="_blank" rel="noopener" style="{_bs};background:#16a34a">オッズ取得</a>'
                f'<a href="{_lb}/race/result.html?race_id={_lid}" target="_blank" rel="noopener" style="{_bs};background:#1a6fa8">レース結果</a>'
                f'<a href="{_lb}/race/movie.html?race_id={_lid}" target="_blank" rel="noopener" style="{_bs};background:#c0392b">レース映像</a>'
            )
        html = _old_link_pat.sub(_replace_link, html)

        # ナビバー注入
        if 'class="d-nav-wrap"' not in html:
            nav_html = _build_nav_bar(filename)
            if "<body>" in html:
                html = html.replace("<body>", "<body>" + nav_html, 1)
            elif "<body " in html:
                idx = html.find("<body ")
                end = html.find(">", idx) + 1
                html = html[:end] + nav_html + html[end:]

        # レース結果リンク注入（既存HTMLに未埋め込みの場合）
        if "race/result.html?race_id=" not in html:
            fn_m = re.match(r"^(\d{8})_(.+?)(\d+)R\.html$", filename)
            if fn_m:
                _dk, _vn, _rn = fn_m.group(1), fn_m.group(2), int(fn_m.group(3))
                _pred_file = os.path.join(
                    PROJECT_ROOT, "data", "predictions", f"{_dk}_pred.json"
                )
                _race_id = None
                _is_jra = True
                if os.path.isfile(_pred_file):
                    try:
                        with open(_pred_file, "r", encoding="utf-8") as _pf:
                            _pred = json.load(_pf)
                        for _r in _pred.get("races", []):
                            if _r.get("venue") == _vn and _r.get("race_no") == _rn:
                                _race_id = _r.get("race_id")
                                _is_jra = _r.get("is_jra", True)
                                break
                    except Exception:
                        pass
                if _race_id:
                    _base = "https://race.netkeiba.com" if _is_jra else "https://nar.netkeiba.com"
                    _btn = "display:inline-block;font-size:12px;font-weight:600;color:#fff;text-decoration:none;border-radius:4px;padding:3px 12px;margin-right:6px"
                    _rlink = (
                        f'  <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">'
                        f'<a href="{_base}/odds/index.html?race_id={_race_id}" target="_blank" rel="noopener" style="{_btn};background:#16a34a">オッズ取得</a>'
                        f'<a href="{_base}/race/result.html?race_id={_race_id}" target="_blank" rel="noopener" style="{_btn};background:#1a6fa8">レース結果</a>'
                        f'<a href="{_base}/race/movie.html?race_id={_race_id}" target="_blank" rel="noopener" style="{_btn};background:#c0392b">レース映像</a>'
                        f"</div>\n"
                    )
                    _inject_tgt = '\n</div>\n<div class="card">\n<div class="section-title">■ レース概要</div>'
                    if _inject_tgt in html:
                        html = html.replace(
                            _inject_tgt,
                            f"\n{_rlink}</div>\n<div class=\"card\">\n<div class=\"section-title\">■ レース概要</div>",
                            1,
                        )

        # ライブオッズ注入（オッズ更新済みの場合）
        fn_m2 = re.match(r"^(\d{8})_(.+?)(\d+)R\.html$", filename)
        if fn_m2:
            _dk2 = fn_m2.group(1)
            _live_odds_path = os.path.join(OUTPUT_DIR, f"{_dk2}_live_odds.json")
            if os.path.isfile(_live_odds_path):
                try:
                    with open(_live_odds_path, "r", encoding="utf-8") as _lf:
                        _all_live = json.load(_lf)
                    # このレースの race_id を特定
                    _live_race_id = None
                    _vn2 = fn_m2.group(2)
                    _rn2 = int(fn_m2.group(3))
                    _pred_file2 = os.path.join(PROJECT_ROOT, "data", "predictions", f"{_dk2}_pred.json")
                    if os.path.isfile(_pred_file2):
                        try:
                            with open(_pred_file2, "r", encoding="utf-8") as _pf2:
                                _pred2 = json.load(_pf2)
                            for _r2 in _pred2.get("races", []):
                                if _r2.get("venue") == _vn2 and _r2.get("race_no") == _rn2:
                                    _live_race_id = _r2.get("race_id")
                                    break
                        except Exception:
                            pass
                    _race_live = _all_live.get(_live_race_id, {}) if _live_race_id else {}
                    if _race_live:
                        import json as _json
                        _mtime = os.path.getmtime(_live_odds_path)
                        from datetime import datetime as _dt2
                        _updated = _dt2.fromtimestamp(_mtime).strftime("%H:%M")
                        _live_js = f"""<script>
(function(){{
  var liveOdds={_json.dumps(_race_live, ensure_ascii=False)};
  var updatedAt="{_updated}";

  function mkHtml(odds, rank){{
    return '<strong style="color:#0369a1;font-size:inherit">'
      + odds.toFixed(1) + '倍</strong>'
      + '<span style="color:#64748b;font-size:11px;margin-left:3px">(' + rank + '人気)</span>';
  }}

  // ① data-live-odds 属性（新HTMLファイル向け）
  document.querySelectorAll('[data-live-odds]').forEach(function(el){{
    var hn = el.getAttribute('data-live-odds');
    var lo = liveOdds[hn];
    if(!lo) return;
    el.innerHTML = mkHtml(lo[0], lo[1]);
  }});

  // ② 全馬一覧テーブル (旧HTMLフォールバック: span.uma in td → td列7)
  document.querySelectorAll('table:not(.pred-table) tbody tr').forEach(function(tr){{
    var umaSpan = tr.querySelector('td span.uma');
    if(!umaSpan) return;
    var hn = umaSpan.textContent.replace(/\\D+/g,'');
    var lo = liveOdds[hn];
    if(!lo) return;
    var cells = tr.querySelectorAll('td');
    if(cells.length >= 7 && !cells[6].hasAttribute('data-live-odds')){{
      cells[6].innerHTML = mkHtml(lo[0], lo[1]);
    }}
  }});

  // ③ 全頭評価サマリー hds-row1（旧HTMLフォールバック）
  document.querySelectorAll('.hds-row1').forEach(function(row1){{
    var uma = row1.querySelector('span.uma');
    if(!uma) return;
    var hn = uma.textContent.replace(/\\D+/g,'');
    var lo = liveOdds[hn];
    if(!lo) return;
    // [想定]XX.X倍 を含むspan（hds-grades外）
    var found = null;
    Array.from(row1.querySelectorAll('span')).forEach(function(s){{
      if(s.hasAttribute('data-live-odds')) return; // ①で処理済み
      if(/[0-9]+\\.[0-9]+倍/.test(s.textContent) && !s.closest('.hds-grades')){{
        found = s;
      }}
    }});
    if(found) found.innerHTML = mkHtml(lo[0], lo[1]);
  }});

  // ④ 印セクション（旧HTMLフォールバック: span.uma in strong → sibling span）
  document.querySelectorAll('span.uma').forEach(function(uma){{
    if(uma.closest('[data-live-odds]') || uma.closest('.hds-row1') || uma.closest('td')) return;
    var strongEl = uma.parentElement;
    if(!strongEl || strongEl.tagName !== 'STRONG') return;
    var parentDiv = strongEl.parentElement;
    if(!parentDiv) return;
    var hn = uma.textContent.replace(/\\D+/g,'');
    var lo = liveOdds[hn];
    if(!lo) return;
    var oddsSpan = parentDiv.querySelector('span:not(strong span)');
    if(oddsSpan && !oddsSpan.hasAttribute('data-live-odds')){{
      oddsSpan.innerHTML = mkHtml(lo[0], lo[1]);
    }}
  }});

  // ⑤ pred-table 実オッズ列（7列目）
  document.querySelectorAll('table.pred-table tbody tr').forEach(function(tr){{
    var cells = tr.querySelectorAll('td');
    if(cells.length < 7) return;
    var hn = cells[0].textContent.replace(/\\D+/g,'');
    var lo = liveOdds[hn];
    if(!lo) return;
    cells[6].innerHTML = '<strong style="color:#0369a1">' + lo[0].toFixed(1) + '</strong>'
      + '<small style="color:#64748b;margin-left:3px">(' + lo[1] + '人気)</small>';
  }});

  // ⑥ ページ最上部にタイムスタンプバナーを挿入
  var anchor = document.querySelector('.d-nav-wrap');
  if(anchor && anchor.nextElementSibling){{
    var banner = document.createElement('div');
    banner.style.cssText = 'background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px;'
      + 'padding:5px 14px;font-size:12px;color:#1e40af;margin:6px 0 4px;';
    banner.innerHTML = '🔄 <strong>ライブオッズ反映済み</strong>　更新: ' + updatedAt
      + '　<span style="color:#64748b;font-size:11px">（青色太字＝最新オッズ）</span>';
    anchor.parentNode.insertBefore(banner, anchor.nextSibling);
  }}
}})();
</script>"""
                        html = html.replace("</body>", _live_js + "\n</body>", 1)
                except Exception:
                    pass

        # 勝率/連対率/複勝率の順位色を動的適用（古いHTML + 新HTMLの両方に対応）
        # 1位=緑, 2位=青, 3位=赤, 平均以上=黒, 平均以下=灰
        _rate_color_js = """<script>
(function(){
  // 順位色を計算・適用（ユニーク値でランキング）
  function applyColors(arr){
    if(arr.length === 0) return;
    var uniq = [];
    var seen = {};
    arr.forEach(function(x){
      var k = Math.round(x.val * 100);
      if(!seen[k]){seen[k]=true; uniq.push(x.val);}
    });
    uniq.sort(function(a,b){return b-a});
    var avg = arr.reduce(function(s,x){return s+x.val},0) / arr.length;
    arr.forEach(function(item){
      var c;
      if(uniq.length >= 1 && item.val >= uniq[0] - 0.001) c = '#16a34a';
      else if(uniq.length >= 2 && item.val >= uniq[1] - 0.001) c = '#1a6fa8';
      else if(uniq.length >= 3 && item.val >= uniq[2] - 0.001) c = '#c0392b';
      else if(item.val >= avg) c = '#333';
      else c = '#aaa';
      item.el.style.color = c;
    });
  }
  // --- hds-row2 カード（Level 3）---
  var cg = {win:[], p2:[], p3:[]};
  document.querySelectorAll('.hds-row2').forEach(function(row){
    var labels = row.querySelectorAll('.hds-wr-label');
    var vals = row.querySelectorAll('.hds-wr-win');
    labels.forEach(function(lbl, i){
      if(i >= vals.length) return;
      var v = parseFloat(vals[i].textContent);
      if(isNaN(v)) return;
      var key = lbl.textContent.trim() === '勝' ? 'win' : lbl.textContent.trim() === '連' ? 'p2' : 'p3';
      cg[key].push({el: vals[i], val: v});
    });
  });
  applyColors(cg.win);
  applyColors(cg.p2);
  applyColors(cg.p3);
  // --- テーブル（Level 6 確率表 / Level 2 全馬一覧）---
  document.querySelectorAll('table thead tr').forEach(function(tr){
    var ths = tr.querySelectorAll('th');
    var colMap = {};
    ths.forEach(function(th, i){
      var t = th.textContent.trim();
      if(t === '勝率') colMap.win = i;
      if(t === '連対率') colMap.p2 = i;
      if(t === '複勝率') colMap.p3 = i;
    });
    if(colMap.win === undefined) return;
    var tbody = tr.closest('table').querySelector('tbody');
    if(!tbody) return;
    var tg = {win:[], p2:[], p3:[]};
    tbody.querySelectorAll('tr').forEach(function(row){
      var cells = row.querySelectorAll('td');
      ['win','p2','p3'].forEach(function(key){
        if(colMap[key] !== undefined && colMap[key] < cells.length){
          var v = parseFloat(cells[colMap[key]].textContent);
          if(!isNaN(v)) tg[key].push({el: cells[colMap[key]], val: v});
        }
      });
    });
    applyColors(tg.win);
    applyColors(tg.p2);
    applyColors(tg.p3);
  });
})();
</script>"""
        html = html.replace("</body>", _rate_color_js + "\n</body>", 1)

        # フォーメーション買い目のプレーン番号をマーク付き表示に変換
        html = _patch_formation_combo_marks(html)

        return Response(html, mimetype="text/html; charset=utf-8")

    @app.route("/api/predicted_odds")
    def api_predicted_odds():
        """予想オッズAPIエンドポイント: 指定日の予想JSONから予想オッズを返す"""
        date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
        pred_dir = os.path.join(PROJECT_ROOT, "data", "predictions")
        pred_file = os.path.join(pred_dir, f"{date.replace('-', '')}.json")

        if not os.path.exists(pred_file):
            return jsonify(ok=False, error="予想データなし", date=date)

        try:
            with open(pred_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            return jsonify(ok=False, error=str(e))

        result = {}
        for venue, races in data.items():
            if not isinstance(races, dict):
                continue
            venue_data = {}
            for race_no, race_data in races.items():
                if not isinstance(race_data, dict):
                    continue
                horses = race_data.get("horses", [])
                odds_data = []
                for h in horses:
                    odds_data.append({
                        "horse_no": h.get("horse_no", 0),
                        "horse_name": h.get("horse_name", ""),
                        "mark": h.get("mark", ""),
                        "win_prob": round(h.get("win_prob", 0) * 100, 1),
                        "top2_prob": round(h.get("place2_prob", 0) * 100, 1),
                        "top3_prob": round(h.get("place3_prob", 0) * 100, 1),
                        "predicted_odds": h.get("predicted_tansho_odds"),
                        "actual_odds": h.get("odds"),
                        "divergence": h.get("odds_divergence"),
                        "signal": h.get("divergence_signal", ""),
                    })
                venue_data[race_no] = {
                    "race_name": race_data.get("race_name", ""),
                    "horses": sorted(odds_data, key=lambda x: x.get("predicted_odds") or 999),
                    "value_bets": race_data.get("value_bets", []),
                }
            if venue_data:
                result[venue] = venue_data

        return jsonify(ok=True, date=date, venues=result)

    @app.route("/api/ev_map")
    def api_ev_map():
        """期待値マップ: 指定日の全レース全馬券の期待値を一覧で返す"""
        date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
        pred_dir = os.path.join(PROJECT_ROOT, "data", "predictions")
        pred_file = os.path.join(pred_dir, f"{date.replace('-', '')}.json")

        if not os.path.exists(pred_file):
            return jsonify(ok=False, error="予想データなし")

        try:
            with open(pred_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            return jsonify(ok=False, error=str(e))

        all_value_bets = []
        for venue, races in data.items():
            if not isinstance(races, dict):
                continue
            for race_no, race_data in races.items():
                if not isinstance(race_data, dict):
                    continue
                vbs = race_data.get("value_bets", [])
                for vb in vbs:
                    vb["venue"] = venue
                    vb["race_no"] = race_no
                    all_value_bets.append(vb)

        all_value_bets.sort(key=lambda x: -x.get("ev", 0))
        return jsonify(ok=True, date=date, value_bets=all_value_bets[:100])

    # ============================================================
    # ④ データベース API
    # ============================================================

    @app.route("/api/db/personnel")
    def api_db_personnel():
        """騎手・調教師マスタを返す（DB優先、JSONフォールバック）"""
        person_type = request.args.get("type", "jockey")  # jockey or trainer
        search = request.args.get("q", "").strip()
        try:
            from src.database import get_personnel_all
            data = get_personnel_all()
        except Exception:
            # JSON フォールバック
            try:
                with open(COURSE_DB_PRELOAD_PATH.replace("course_db_preload.json", "personnel_db.json"),
                          "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                return jsonify(error="personnel データ取得失敗")

        if person_type == "jockey":
            persons = data.get("jockeys", {})
        else:
            persons = data.get("trainers", {})

        # 検索フィルタ
        result = []
        for pid, pdata in persons.items():
            name = pdata.get("jockey_name") or pdata.get("trainer_name") or ""
            if search and search not in name and search not in pid:
                continue
            result.append({"id": pid, "name": name, "data": pdata})

        result.sort(key=lambda x: x["name"])
        return jsonify(type=person_type, total=len(result), persons=result[:200])

    @app.route("/api/db/course")
    def api_db_course():
        """コース DB を返す（venue/surface/distance フィルタ付き）"""
        venue_filter = request.args.get("venue", "")
        surface_filter = request.args.get("surface", "")
        dist_filter = request.args.get("dist", "")
        try:
            from src.database import get_course_db
            course_db = get_course_db()
        except Exception:
            try:
                with open(COURSE_DB_PRELOAD_PATH, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                course_db = raw.get("course_db", {})
            except Exception:
                return jsonify(error="course_db データ取得失敗")

        # race_log に実績のあるコースのみに絞り込む
        try:
            from src.database import DATABASE_PATH as _db_path
            import sqlite3 as _sql3
            _cconn = _sql3.connect(_db_path)
            _active = set()
            for row in _cconn.execute(
                "SELECT DISTINCT venue_code, surface, distance FROM race_log "
                "WHERE venue_code IS NOT NULL AND surface IS NOT NULL AND distance > 0"
            ):
                _active.add(f"{row[0]}_{row[1]}_{row[2]}")
            _cconn.close()
        except Exception:
            _active = None  # フォールバック: 全件返す

        result = {}
        for key, records in course_db.items():
            # key format: "{venue_code}_{surface}_{distance}"
            parts = key.split("_")
            if len(parts) < 3:
                continue
            vc, surf, dist = parts[0], parts[1], parts[2]
            # distance=0以下は無効エントリとして除外
            try:
                dist_int = int(dist)
            except (ValueError, TypeError):
                continue
            if dist_int <= 0:
                continue
            # race_log に実績がないコースは除外
            if _active is not None and key not in _active:
                continue
            if venue_filter and vc != venue_filter:
                continue
            if surface_filter and surf != surface_filter:
                continue
            if dist_filter and dist != dist_filter:
                continue
            result[key] = records

        return jsonify(total_keys=len(result), keys=sorted(result.keys()))

    @app.route("/api/db/course_stats")
    def api_db_course_stats():
        """コース別詳細統計（レコード・クラス平均・ペース・脚質・枠順・TOP騎手/調教師）"""
        key = request.args.get("key", "")
        if not key:
            return jsonify(error="key required")
        try:
            from src.database import get_course_db
            db = get_course_db(keys=[key])
        except Exception:
            try:
                with open(COURSE_DB_PRELOAD_PATH, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                db = {key: raw.get("course_db", {}).get(key, [])}
            except Exception:
                return jsonify(error="course_db 取得失敗")

        records = db.get(key, [])
        if not records:
            return jsonify(error="データなし", key=key)

        parts = key.split("_")
        venue_code = parts[0] if parts else ""
        _JRA_CODES_SET = {"01","02","03","04","05","06","07","08","09","10"}
        is_jra = venue_code in _JRA_CODES_SET

        # ── 時間フォーマット helper ──
        def _fmt_time(sec):
            if sec is None:
                return "—"
            m = int(sec // 60)
            s = sec % 60
            return f"{m}:{s:04.1f}"

        # ── 4角位置 → 脚質 ──
        def _pos_to_style(pos4c, field_count):
            if not pos4c or not field_count or field_count < 4:
                return ""
            r = pos4c / field_count
            if pos4c == 1:
                return "逃げ"
            elif r <= 0.40:
                return "先行"
            elif r <= 0.65:
                return "差し"
            else:
                return "追込"

        # ── クラス正規化 ──
        def _normalize_class(class_name, grade):
            g = (grade or "").strip()
            c = (class_name or "").strip()
            combined = g + " " + c
            if any(x in combined for x in ["GⅠ","GⅡ","GⅢ","GI","GII","GIII","重賞"]):
                return "重賞"
            if not is_jra:
                for cls in ["A1","A2","B1","B2","B3","C1","C2","C3"]:
                    if cls in combined:
                        return cls
                if "未" in combined:
                    return "未格付"
                return "その他(NAR)"
            else:
                if any(x in combined for x in ["新馬","未勝利"]):
                    return "新馬・未勝利"
                if any(x in combined for x in ["1勝","500万"]):
                    return "1勝クラス"
                if any(x in combined for x in ["2勝","1000万"]):
                    return "2勝クラス"
                if any(x in combined for x in ["3勝","1500万"]):
                    return "3勝クラス"
                if g == "OP":
                    return "OPクラス"
                if any(x in combined for x in ["オープン","OP","リステッド","L級"]):
                    return "OPクラス"
                return "その他(JRA)"

        # ── レコードタイム ──
        rec_entries = [(r["finish_time_sec"], r) for r in records if r.get("finish_time_sec")]
        record_data = None
        if rec_entries:
            min_sec, rec_r = min(rec_entries, key=lambda x: x[0])
            record_data = {
                "time_sec": round(min_sec, 1),
                "time_str": _fmt_time(min_sec),
                "grade":    rec_r.get("grade", ""),
                "date":     rec_r.get("race_date", ""),
                "class_name": rec_r.get("class_name", ""),
            }

        # ── クラス別平均走破タイム（良馬場基準）──
        from collections import defaultdict
        _COND_MAP_EARLY = {"良": "良", "稍": "稍重", "稍重": "稍重", "重": "重", "不": "不良", "不良": "不良"}
        class_times: dict = defaultdict(list)
        for r in records:
            fp = r.get("finish_pos")
            cond_lbl = _COND_MAP_EARLY.get(r.get("condition", ""), "")
            if r.get("finish_time_sec") and isinstance(fp, int) and 1 <= fp <= 3 and cond_lbl == "良":
                cls = _normalize_class(r.get("class_name",""), r.get("grade",""))
                class_times[cls].append(r["finish_time_sec"])
        if is_jra:
            class_order = ["新馬・未勝利","1勝クラス","2勝クラス","3勝クラス","OPクラス","重賞","その他(JRA)"]
        else:
            class_order = ["未格付","C3","C2","C1","B3","B2","B1","A2","A1","重賞"]
        class_avg = {}
        for cls in class_order:
            ts = class_times.get(cls, [])
            if ts:
                avg = sum(ts) / len(ts)
                class_avg[cls] = {"avg_sec": round(avg, 1), "avg_str": _fmt_time(avg), "n": len(ts)}
        for cls, ts in class_times.items():
            if cls not in class_avg and ts:
                avg = sum(ts) / len(ts)
                class_avg[cls] = {"avg_sec": round(avg, 1), "avg_str": _fmt_time(avg), "n": len(ts)}

        # ── ペース（前半3F・上がり3F 平均）──
        f3_list = [r["first_3f_sec"] for r in records if r.get("first_3f_sec")]
        l3_list = [r["last_3f_sec"]  for r in records if r.get("last_3f_sec")]
        pace_avg = {
            "first_3f": round(sum(f3_list)/len(f3_list), 2) if f3_list else None,
            "last_3f":  round(sum(l3_list)/len(l3_list), 2) if l3_list else None,
        }
        # 前半3F 速い比率でペース区分
        if f3_list and l3_list:
            avg_f3 = pace_avg["first_3f"]
            avg_l3 = pace_avg["last_3f"]
            ratio = avg_f3 / avg_l3 if avg_l3 else 1.0
            if ratio >= 1.12:
                pace_type = "HH（超ハイ）"
            elif ratio >= 1.06:
                pace_type = "HM（ハイ）"
            elif ratio >= 1.0:
                pace_type = "MM（ミドル）"
            elif ratio >= 0.94:
                pace_type = "MS（スロー気味）"
            else:
                pace_type = "SS（スロー）"
            pace_avg["pace_type"] = pace_type
        else:
            pace_avg["pace_type"] = "—"

        # ── クラス別 前半3F・上がり3F 平均 ──
        class_f3: dict = defaultdict(list)
        class_l3: dict = defaultdict(list)
        for r in records:
            cls = _normalize_class(r.get("class_name", ""), r.get("grade", ""))
            if r.get("first_3f_sec"):
                class_f3[cls].append(r["first_3f_sec"])
            if r.get("last_3f_sec"):
                class_l3[cls].append(r["last_3f_sec"])
        class_pace = {}
        all_cls = set(list(class_f3.keys()) + list(class_l3.keys()))
        for cls in all_cls:
            entry = {}
            fs = class_f3.get(cls, [])
            ls = class_l3.get(cls, [])
            if fs:
                entry["first_3f"] = round(sum(fs) / len(fs), 2)
            if ls:
                entry["last_3f"] = round(sum(ls) / len(ls), 2)
            entry["n"] = max(len(fs), len(ls))
            class_pace[cls] = entry
        pace_avg["by_class"] = class_pace

        # ── 枠番別成績（枠単位: 1レースにつき1枠=1カウント）──
        # レース識別キー: (race_date, class_name, field_count) ごとに枠番を集計
        # 同枠に複数頭いても1カウント。勝ち/連対/複勝は枠内の誰かが達成すればOK
        from collections import defaultdict as _dd
        # (race_key, gate) 単位で集計。勝率等は枠単位、ROIは実頭数ベース
        _gate_race: dict = {}  # (race_key, gate) -> {"win": bool, "place2": bool, "place3": bool, "odds": float, "horses": int}
        for r in records:
            gn = r.get("gate_no")
            if not gn:
                continue
            rk_ = (r.get("race_date",""), r.get("class_name",""), r.get("field_count", 0))
            grk_ = (rk_, int(gn))
            fp = r.get("finish_pos")
            if grk_ not in _gate_race:
                _gate_race[grk_] = {"win": False, "place2": False, "place3": False, "odds": 0.0, "horses": 0}
            entry = _gate_race[grk_]
            entry["horses"] += 1  # 実頭数カウント（ROI分母用）
            if fp == 1:
                entry["win"] = True
                entry["odds"] += float(r.get("win_odds") or 0)
            if isinstance(fp, int) and fp <= 2:
                entry["place2"] = True
            if isinstance(fp, int) and fp <= 3:
                entry["place3"] = True

        gate_agg: dict = defaultdict(lambda: {"runs": 0, "win": 0, "place2": 0, "place3": 0, "odds_sum": 0.0, "total_horses": 0})
        for (rk_, gate), v in _gate_race.items():
            g_str = str(gate)
            gate_agg[g_str]["runs"] += 1
            gate_agg[g_str]["total_horses"] += v["horses"]
            if v["win"]:
                gate_agg[g_str]["win"] += 1
                gate_agg[g_str]["odds_sum"] += v["odds"]
            if v["place2"]:
                gate_agg[g_str]["place2"] += 1
            if v["place3"]:
                gate_agg[g_str]["place3"] += 1

        def _gate_stat(v):
            t = v["runs"]  # 枠単位のレース数（勝率等の分母）
            th = v["total_horses"]  # 実頭数（ROIの分母）
            w = v["win"]
            return {
                "runs":         t,
                "win":          w,
                "place2":       v["place2"],
                "place3":       v["place3"],
                "win_rate":     round(w / t * 100, 1) if t else 0.0,
                "place2_rate":  round(v["place2"] / t * 100, 1) if t else 0.0,
                "place3_rate":  round(v["place3"] / t * 100, 1) if t else 0.0,
                "roi":          round(v["odds_sum"] * 100 / th, 1) if th else 0.0,
            }
        gate_bias = {
            g: _gate_stat(v)
            for g, v in sorted(gate_agg.items(), key=lambda x: int(x[0]))
            if v["runs"] >= 3
        }

        # ── 脚質別成績（position_4c ベース）──
        # レース毎の最小position_4cを逃げ判定に使う（データ欠損対応）
        _race_min_p4c: dict = {}
        for r in records:
            rk_ = (r.get("race_date",""), r.get("class_name",""), r.get("field_count", 0))
            p4c_ = r.get("position_4c")
            if p4c_:
                _race_min_p4c[rk_] = min(_race_min_p4c.get(rk_, 999), p4c_)

        def _pos_to_style_v2(pos4c, field_count, race_key):
            if not pos4c or not field_count or field_count < 4:
                return ""
            min_p4c = _race_min_p4c.get(race_key, 1)
            if pos4c == min_p4c:
                return "逃げ"
            r = pos4c / field_count
            if r <= 0.22:
                return "先行"
            elif r <= 0.38:
                return "好位"
            elif r <= 0.55:
                return "中団"
            elif r <= 0.72:
                return "差し"
            else:
                return "追込"

        style_stats: dict = defaultdict(lambda: {"total": 0, "win": 0, "place2": 0, "place3": 0, "odds_sum": 0.0})
        for r in records:
            rk_ = (r.get("race_date",""), r.get("class_name",""), r.get("field_count", 0))
            style = _pos_to_style_v2(r.get("position_4c"), r.get("field_count"), rk_)
            if style:
                fp = r.get("finish_pos")
                style_stats[style]["total"] += 1
                if fp == 1:
                    style_stats[style]["win"] += 1
                    style_stats[style]["odds_sum"] += float(r.get("win_odds") or 0)
                if isinstance(fp, int) and fp <= 2:
                    style_stats[style]["place2"] += 1
                if isinstance(fp, int) and fp <= 3:
                    style_stats[style]["place3"] += 1
        running_style_stats = {}
        for style in ["逃げ","先行","好位","中団","差し","追込"]:
            st = style_stats.get(style, {"total":0,"win":0,"place2":0,"place3":0,"odds_sum":0.0})
            t = st["total"]
            running_style_stats[style] = {
                "total":        t,
                "win":          st["win"],
                "place2":       st["place2"],
                "place3":       st["place3"],
                "win_rate":     round(st["win"]    / t * 100, 1) if t else 0.0,
                "place2_rate":  round(st["place2"] / t * 100, 1) if t else 0.0,
                "place3_rate":  round(st["place3"] / t * 100, 1) if t else 0.0,
                "roi":          round(st["odds_sum"] * 100 / t, 1) if t else 0.0,
            }

        # ── 過去1年のTOP5 騎手・調教師（データなければ全期間）──
        one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        recent = [r for r in records if (r.get("race_date","") or "") >= one_year_ago]
        top_period = "過去1年"
        if not recent:
            recent = records  # 全期間にフォールバック
            top_period = "全期間"
        jockey_wins: dict = defaultdict(lambda: {"name":"","total":0,"win":0,"place2":0,"place3":0,"odds_sum":0.0})
        trainer_wins: dict = defaultdict(lambda: {"name":"","total":0,"win":0,"place2":0,"place3":0,"odds_sum":0.0})
        for r in recent:
            fp = r.get("finish_pos")
            jid = r.get("jockey_id","")
            jname = r.get("jockey","")
            if jid:
                jockey_wins[jid]["name"] = jname
                jockey_wins[jid]["total"] += 1
                if fp == 1:
                    jockey_wins[jid]["win"] += 1
                    jockey_wins[jid]["odds_sum"] += float(r.get("win_odds") or 0)
                if isinstance(fp, int) and fp <= 2:
                    jockey_wins[jid]["place2"] += 1
                if isinstance(fp, int) and fp <= 3:
                    jockey_wins[jid]["place3"] += 1
            tid = r.get("trainer_id","")
            tname = r.get("trainer","")
            if tid:
                trainer_wins[tid]["name"] = tname
                trainer_wins[tid]["total"] += 1
                if fp == 1:
                    trainer_wins[tid]["win"] += 1
                    trainer_wins[tid]["odds_sum"] += float(r.get("win_odds") or 0)
                if isinstance(fp, int) and fp <= 2:
                    trainer_wins[tid]["place2"] += 1
                if isinstance(fp, int) and fp <= 3:
                    trainer_wins[tid]["place3"] += 1
        # personnel DB から調教師・騎手名を補完
        try:
            import re as _re2
            _hw_pat = _re2.compile(r'^\d+\s*\([+-]?\d+\)$')
            from src.database import get_personnel_all
            _pers = get_personnel_all()
            _trainer_names = {
                tid: (d.get("trainer_name") or d.get("name") or "")
                for tid, d in _pers.get("trainers", {}).items()
            }
            _jockey_names = {
                jid: (d.get("jockey_name") or d.get("name") or "")
                for jid, d in _pers.get("jockeys", {}).items()
            }
        except Exception:
            _trainer_names = {}
            _jockey_names = {}
            _hw_pat = None

        def _safe_name(pid, raw_name, name_map):
            """personnel_db優先・馬体重パターンはIDに置換"""
            n = name_map.get(pid) or raw_name or pid
            if _hw_pat and _hw_pat.match((n or "").strip()):
                n = pid
            return n or pid

        def _build_person_entry(k, v, name_map, is_jockey):
            t = v["total"]
            return {
                "id":          k,
                "name":        _safe_name(k, v["name"], name_map),
                "wins":        v["win"],
                "place2":      v["place2"],
                "place3":      v["place3"],
                "total":       t,
                "win_rate":    round(v["win"]    / t * 100, 1) if t else 0.0,
                "place2_rate": round(v["place2"] / t * 100, 1) if t else 0.0,
                "place3_rate": round(v["place3"] / t * 100, 1) if t else 0.0,
                "roi":         round(v["odds_sum"] * 100 / t, 1) if t else 0.0,
            }
        top_jockeys = sorted(
            [_build_person_entry(k, v, _jockey_names, True)  for k, v in jockey_wins.items()],
            key=lambda x: -x["wins"]
        )[:5]
        top_trainers = sorted(
            [_build_person_entry(k, v, _trainer_names, False) for k, v in trainer_wins.items()],
            key=lambda x: -x["wins"]
        )[:5]

        # ── 旧互換フィールド（ペース傾向テキスト）──
        win_entries = [r for r in records if r.get("finish_pos")==1 and r.get("position_4c") and r.get("field_count",0)>=4]
        total_w = len(win_entries)
        senkou = sum(1 for r in win_entries if r["position_4c"]/r["field_count"] <= 0.35)
        oikomi = sum(1 for r in win_entries if r["position_4c"]/r["field_count"] >= 0.65)
        if total_w:
            if senkou/total_w >= 0.50:
                pace_tendency = "先行有利"
            elif oikomi/total_w >= 0.35:
                pace_tendency = "差し・追込有利"
            else:
                pace_tendency = "平均的"
        else:
            pace_tendency = "—"

        # 1〜3着馬のみで平均走破タイムを算出
        top3_times = [r["finish_time_sec"] for r in records
                      if r.get("finish_time_sec") and isinstance(r.get("finish_pos"), int) and 1 <= r["finish_pos"] <= 3]
        avg_t = round(sum(top3_times)/len(top3_times), 1) if top3_times else None
        min_t = round(min(top3_times), 1) if top3_times else None

        # レース数（ユニーク日付×レース名で概算）
        race_set = set()
        for r in records:
            race_set.add((r.get("race_date",""), r.get("class_name",""), r.get("field_count",0)))
        race_count = len(race_set)

        # ── ① データ取得期間 ──
        dates = [r.get("race_date", "") for r in records if r.get("race_date")]
        period = {"min_date": min(dates), "max_date": max(dates)} if dates else None

        # ── ④ コースの特徴テキスト自動生成 ──
        course_description = None
        try:
            _cm_path = os.path.join(os.path.dirname(__file__), "..", "data", "masters", "course_master_draft.json")
            if os.path.exists(_cm_path):
                with open(_cm_path, "r", encoding="utf-8") as _cmf:
                    _cm_all = json.load(_cmf)
                for _cm in _cm_all:
                    if _cm.get("course_id") == key:
                        parts = []
                        d_ = _cm.get("direction", "")
                        if d_:
                            parts.append(f"{d_}回り")
                        io_ = _cm.get("inside_outside", "")
                        if io_ and io_ != "なし":
                            parts.append(f"{io_}コース")
                        sm_ = _cm.get("straight_m")
                        if sm_:
                            parts.append(f"直線{sm_}m")
                        cc_ = _cm.get("corner_count", 0)
                        if cc_:
                            parts.append(f"コーナー{cc_}回")
                        ct_ = _cm.get("corner_type", "")
                        if ct_:
                            parts.append(ct_)
                        fc_ = _cm.get("first_corner", "")
                        if fc_ and fc_ != "なし":
                            parts.append(f"初角まで{fc_}")
                        sl_ = _cm.get("slope_type", "")
                        if sl_:
                            parts.append(sl_)
                        if parts:
                            course_description = "。".join(parts) + "。"
                        break
        except Exception:
            pass

        # ── ③⑦ クラス別にペース情報を統合 ──
        for cls in list(class_avg.keys()):
            cp = class_pace.get(cls, {})
            class_avg[cls]["first_3f"] = cp.get("first_3f")
            class_avg[cls]["last_3f"] = cp.get("last_3f")
            f3v = cp.get("first_3f")
            l3v = cp.get("last_3f")
            if f3v and l3v and l3v > 0:
                ratio_ = f3v / l3v
                if ratio_ >= 1.12:
                    class_avg[cls]["pace_type"] = "HH"
                elif ratio_ >= 1.06:
                    class_avg[cls]["pace_type"] = "HM"
                elif ratio_ >= 1.0:
                    class_avg[cls]["pace_type"] = "MM"
                elif ratio_ >= 0.94:
                    class_avg[cls]["pace_type"] = "MS"
                else:
                    class_avg[cls]["pace_type"] = "SS"
            else:
                class_avg[cls]["pace_type"] = None

        # ── 馬場状態別タイム差 ──
        # conditionは1文字格納の場合がある（良/稍/重/不）→ 表示名にマッピング
        _COND_MAP = {"良": "良", "稍": "稍重", "稍重": "稍重", "重": "重", "不": "不良", "不良": "不良"}
        _COND_ORDER = ["良", "稍重", "重", "不良"]
        condition_times: dict = defaultdict(list)
        for r in records:
            fp = r.get("finish_pos")
            if r.get("finish_time_sec") and isinstance(fp, int) and 1 <= fp <= 3:
                cond_raw = r.get("condition", "")
                cond_label = _COND_MAP.get(cond_raw, cond_raw)
                condition_times[cond_label].append(r["finish_time_sec"])
        good_ts = condition_times.get("良", [])
        good_avg = sum(good_ts) / len(good_ts) if good_ts else None
        condition_diff = {}
        for cond in _COND_ORDER:
            ts = condition_times.get(cond, [])
            if ts:
                avg = sum(ts) / len(ts)
                diff = round(avg - good_avg, 2) if good_avg else None
                condition_diff[cond] = {"n": len(ts), "avg_sec": round(avg, 2), "avg_str": _fmt_time(avg), "diff": diff}

        # ── 季節別タイム差（良馬場のみ）──
        season_times: dict = defaultdict(list)
        for r in records:
            fp = r.get("finish_pos")
            cond_raw = r.get("condition", "")
            cond_label = _COND_MAP.get(cond_raw, cond_raw)
            if r.get("finish_time_sec") and isinstance(fp, int) and 1 <= fp <= 3 and cond_label == "良":
                d_ = r.get("race_date", "")
                if d_ and len(d_) >= 7:
                    m_ = int(d_[5:7])
                    if m_ in (3, 4, 5):
                        sn_ = "春"
                    elif m_ in (6, 7, 8):
                        sn_ = "夏"
                    elif m_ in (9, 10, 11):
                        sn_ = "秋"
                    else:
                        sn_ = "冬"
                    season_times[sn_].append(r["finish_time_sec"])
        season_diff = {}
        for sn_ in ["春", "夏", "秋", "冬"]:
            ts = season_times.get(sn_, [])
            if ts:
                avg = sum(ts) / len(ts)
                diff = round(avg - good_avg, 2) if good_avg else None
                season_diff[sn_] = {"n": len(ts), "avg_sec": round(avg, 2), "avg_str": _fmt_time(avg), "diff": diff}

        return jsonify(
            key=key,
            count=len(records),
            race_count=race_count,
            is_jra=is_jra,
            period=period,
            course_description=course_description,
            # 馬場状態別・季節別タイム差
            condition_diff=condition_diff,
            season_diff=season_diff,
            # レコード
            record=record_data,
            # クラス別平均（ペース統合済み）
            class_avg=class_avg,
            # ペース
            pace_avg=pace_avg,
            # 脚質別
            running_style=running_style_stats,
            # 枠番別
            gate_bias=gate_bias,
            # TOP 騎手/調教師
            top_jockeys=top_jockeys,
            top_trainers=top_trainers,
            top_period=top_period,
            # 旧互換
            avg_time_sec=avg_t,
            min_time_sec=min_t,
            pace_tendency=pace_tendency,
            senkou_pct=round(senkou/total_w*100,1) if total_w else 0,
            oikomi_pct=round(oikomi/total_w*100,1) if total_w else 0,
        )

    # ── 騎手/調教師 成績集計（predictions × race_results）──
    _personnel_stats_cache: dict = {}

    @app.route("/api/db/personnel_agg")
    def api_db_personnel_agg():
        """
        predictions × race_results から集計した騎手/調教師/種牡馬/母父の成績を返す。
        ?type=jockey|trainer|sire|bms  &q=名前検索  &sort=total|win_rate|place3_rate  &limit=100
        詳細モード: ?type=jockey&id=05638  → by_venue 付き単一人物
        フィルタ: ?jra_nar=JRA|NAR&surface=芝|ダート&smile=SS|S|M|I|L|E
        """
        person_type = request.args.get("type", "jockey")
        search      = request.args.get("q", "").strip()
        pid_detail  = request.args.get("id", "").strip()
        sort_key    = request.args.get("sort", "total")
        limit       = min(int(request.args.get("limit", 200)), 500)
        # フィルタパラメータ
        jra_nar_filter = request.args.get("jra_nar", "").strip()  # "JRA","NAR",""=全体
        surface_filter = request.args.get("surface", "").strip()  # "芝","ダート",""=全体
        smile_filter   = request.args.get("smile", "").strip()    # "SS","S","M","I","L","E",""=全体
        venue_filter   = request.args.get("venue", "").strip()    # "01"〜"55" 等の場コード,""=全体
        year_filter    = request.args.get("year", "").strip()     # "2024","2025","2026",""=全体

        try:
            from src.database import compute_personnel_stats_from_race_log
            # キャッシュ再計算が必要な場合はフラグでクリア
            nocache = request.args.get("nocache", "0") == "1"
            if nocache:
                _personnel_stats_cache.clear()
                # ディスクキャッシュも削除
                import glob as _glob_p
                from config.settings import DATABASE_PATH as _dbp
                for _cf in _glob_p.glob(os.path.join(os.path.dirname(_dbp), "cache", "personnel_stats", "*.json")):
                    try:
                        os.remove(_cf)
                    except Exception:
                        pass
            cache_key = f"_year_{year_filter}" if year_filter else ""
            if cache_key not in _personnel_stats_cache or not _personnel_stats_cache.get(cache_key):
                stats = compute_personnel_stats_from_race_log(year_filter=year_filter or None)
                if cache_key:
                    _personnel_stats_cache[cache_key] = stats
                else:
                    _personnel_stats_cache.update(stats)
            all_stats = (_personnel_stats_cache.get(cache_key) or _personnel_stats_cache).get(person_type, {})
        except Exception as e:
            return jsonify(error=str(e))

        def _pick_stat(st: dict) -> dict:
            """フィルタ設定に応じて正しい stat を返す"""
            # 場コード指定 → by_venue から直接取得（最優先）
            if venue_filter:
                return st.get("by_venue", {}).get(venue_filter, {})
            # jra/nar フィルタ
            prefix = jra_nar_filter.lower()  # "jra","nar",""
            # 馬場フィルタ
            if surface_filter and smile_filter:
                key = surface_filter + smile_filter
                if prefix == "jra":
                    return st.get("jra_by_smile", {}).get(key, {})
                elif prefix == "nar":
                    return st.get("nar_by_smile", {}).get(key, {})
                else:
                    return st.get("by_smile", {}).get(key, {})
            elif surface_filter:
                if prefix == "jra":
                    return st.get("jra_by_surface", {}).get(surface_filter, {})
                elif prefix == "nar":
                    return st.get("nar_by_surface", {}).get(surface_filter, {})
                else:
                    return st.get("by_surface", {}).get(surface_filter, {})
            elif smile_filter:
                # SMILE指定あり・馬場指定なし → 芝+ダート合算は困難なため全馬場SMILE
                jra_key = smile_filter  # by_smile は "芝S", "ダートM" 等でキーになっているので
                # 同じSMILEの全馬場合算
                _SMILE_SURFACES = frozenset(["芝", "ダート", "障害"])
                def _sum_smile(smile_map: dict, sk: str) -> dict:
                    merged = {"total": 0, "win": 0, "place2": 0, "place3": 0}
                    for k, v in smile_map.items():
                        # "芝S" → prefix="芝", suffix="S" → suffix完全一致かつprefixが馬場名
                        if k.endswith(sk) and k[:-len(sk)] in _SMILE_SURFACES:
                            for f in ("total", "win", "place2", "place3"):
                                merged[f] += v.get(f, 0)
                    t = merged["total"]
                    merged["win_rate"] = round(merged["win"]/t*100, 1) if t else 0
                    merged["place2_rate"] = round(merged["place2"]/t*100, 1) if t else 0
                    merged["place3_rate"] = round(merged["place3"]/t*100, 1) if t else 0
                    return merged
                if prefix == "jra":
                    return _sum_smile(st.get("jra_by_smile", {}), smile_filter)
                elif prefix == "nar":
                    return _sum_smile(st.get("nar_by_smile", {}), smile_filter)
                else:
                    return _sum_smile(st.get("by_smile", {}), smile_filter)
            elif prefix == "jra":
                return st.get("jra", {})
            elif prefix == "nar":
                return st.get("nar", {})
            else:
                return st  # 全体

        # 単一人物の詳細
        if pid_detail:
            st = all_stats.get(pid_detail)
            if not st:
                return jsonify(error=f"{pid_detail} not found")
            # 期間情報を追加
            _p_detail = {}
            if cache_key and cache_key in _personnel_stats_cache:
                _p_detail = _personnel_stats_cache[cache_key].get("_period", {})
            if not _p_detail:
                _p_detail = _personnel_stats_cache.get("_period", {})
            if _p_detail.get("min"):
                st["period_str"] = f"{_p_detail['min']}〜{_p_detail['max']}"
            return jsonify(**st)

        # 一覧
        persons = []
        for pid, st in all_stats.items():
            if search and search not in st["name"] and search not in pid:
                continue
            fst = _pick_stat(st)  # フィルタ済みstat
            if not fst or fst.get("total", 0) == 0:
                continue  # フィルタ結果が空の人は除外
            entry = {"id": pid, "name": st["name"], "location": st.get("location", "")}
            entry.update({
                "total":       fst.get("total", 0),
                "win":         fst.get("win", 0),
                "place2":      fst.get("place2", 0),
                "place3":      fst.get("place3", 0),
                "win_rate":    fst.get("win_rate", 0),
                "place2_rate": fst.get("place2_rate", 0),
                "place3_rate": fst.get("place3_rate", 0),
                "roi":         st.get("roi"),  # 全体ROIのみ（フィルタ別ROIは未実装）
            })
            persons.append(entry)

        # ソート
        if sort_key in ("win_rate", "place2_rate", "place3_rate"):
            persons.sort(key=lambda x: x.get(sort_key, 0), reverse=True)
        elif sort_key == "roi":
            persons.sort(key=lambda x: (x.get("roi") or 0), reverse=True)
        elif sort_key == "fukusho_roi":
            persons.sort(key=lambda x: (x.get("fukusho_roi") or 0), reverse=True)
        elif sort_key in ("win", "wins"):
            persons.sort(key=lambda x: x.get("win", 0), reverse=True)
        else:
            persons.sort(key=lambda x: x.get("total", 0), reverse=True)

        # 集計期間（race_log ベース）— 年フィルタ時はキャッシュ内の年別dictから取得
        _p = {}
        if cache_key and cache_key in _personnel_stats_cache:
            _p = _personnel_stats_cache[cache_key].get("_period", {})
        if not _p:
            _p = _personnel_stats_cache.get("_period", {})
        if _p.get("min"):
            rc = _p.get("race_count", 0)
            period_str = f"{_p['min']}〜{_p['max']}（{rc:,}レース・着外含む全馬集計）"
        else:
            period_str = "不明"

        return jsonify(
            type=person_type,
            total=len(persons),
            persons=persons[:limit],
            period=period_str,
        )

    @app.route("/api/home/high_confidence")
    def api_home_high_confidence():
        """指定日の自信度A以上のレースを返す"""
        date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
        try:
            from src.results_tracker import load_prediction
            pred = load_prediction(date)
            if not pred:
                return jsonify(date=date, races=[])
            high_conf = []
            for race in pred.get("races", []):
                conf = race.get("confidence", "B")
                if conf in ("SS", "S+", "S", "A+", "A"):
                    honmei = next(
                        (h for h in race.get("horses", []) if h.get("mark") in ("◉", "◎")),
                        None,
                    )
                    high_conf.append({
                        "venue": race.get("venue", ""),
                        "race_no": race.get("race_no", 0),
                        "race_name": race.get("race_name", ""),
                        "confidence": conf,
                        "surface": race.get("surface", ""),
                        "distance": race.get("distance", 0),
                        "honmei_name": honmei.get("horse_name", "") if honmei else "",
                        "honmei_mark": honmei.get("mark", "") if honmei else "",
                    })
            return jsonify(date=date, races=high_conf)
        except Exception as e:
            logger.warning("high_confidence failed: %s", e)
            return jsonify(date=date, races=[], error=str(e))

    @app.route("/api/results/unmatched_dates_db")
    def api_results_unmatched_dates_db():
        """予想済みだが結果未取得の日付一覧（DB対応版、2024-01-01〜昨日）"""
        try:
            from src.results_tracker import list_prediction_dates
            from src.database import results_exist
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            pred_dates = [d for d in list_prediction_dates()
                          if "2024-01-01" <= d <= yesterday]
            unmatched = [d for d in pred_dates if not results_exist(d)]
            return jsonify(dates=unmatched)
        except Exception as e:
            return jsonify(dates=[], error=str(e))

    # ── 特徴量重要度 API ──────────────────────────────────────────
    _FEAT_LABEL = {
        # コース基本
        "surface": ("芝/ダート", "レースの走路種別"),
        "distance": ("距離", "レース距離(m)"),
        "condition": ("馬場状態", "良・稍重・重・不良"),
        "field_count": ("出走頭数", "そのレースの出走頭数"),
        "is_jra": ("JRA/地方", "中央か地方か"),
        "grade_code": ("クラス", "新馬〜G1のクラス"),
        "venue_code": ("競馬場", "開催競馬場"),
        "month": ("開催月", "レースの月"),
        # コース構造
        "venue_straight_m": ("直線距離", "最終直線の長さ"),
        "venue_slope": ("坂", "コースの坂の有無・傾斜"),
        "venue_first_corner": ("1角までの距離", "スタートから最初のコーナー"),
        "venue_corner_type": ("コーナー形状", "コーナーの種類"),
        "venue_direction": ("回り方向", "右回り/左回り"),
        # 馬個体
        "gate_no": ("枠番", "ゲートの枠番号"),
        "horse_no": ("馬番", "馬番号"),
        "sex_code": ("性別", "牡・牝・セン"),
        "age": ("馬齢", "馬の年齢"),
        "weight_kg": ("斤量", "背負う重量(kg)"),
        "horse_weight": ("馬体重", "馬の体重"),
        "weight_change": ("体重増減", "前走からの体重変化"),
        "horse_win_rate": ("馬勝率", "通算勝率"),
        "horse_place_rate": ("馬複勝率", "通算複勝率"),
        "horse_runs": ("馬出走数", "通算出走回数"),
        "horse_avg_finish": ("馬平均着順", "通算平均着順"),
        "horse_last_finish": ("前走着順", "前走の着順"),
        "horse_days_since": ("レース間隔", "前走からの日数"),
        # 騎手
        "jockey_win_rate": ("騎手勝率", "騎手の通算勝率"),
        "jockey_place_rate": ("騎手複勝率", "騎手の通算複勝率"),
        "jockey_runs": ("騎手騎乗数", "騎手の通算騎乗数"),
        "jockey_win_rate_90d": ("騎手勝率90日", "直近90日の勝率"),
        "jockey_place_rate_90d": ("騎手複勝率90日", "直近90日の複勝率"),
        "jockey_venue_wr": ("騎手×場勝率", "その競馬場での勝率"),
        "jockey_surface_wr": ("騎手×芝ダ勝率", "芝/ダートでの勝率"),
        "jockey_dist_wr": ("騎手×距離勝率", "距離帯での勝率"),
        "is_jockey_change": ("騎手乗替", "前走から騎手が変わったか"),
        "kishu_pattern_code": ("乗替パターン", "乗替の種類コード"),
        "jockey_place_rank_in_race": ("騎手力順位", "レース内での騎手力ランク"),
        "jockey_place_zscore_in_race": ("騎手力Zスコア", "レース内での騎手力偏差"),
        "jockey_wp_ratio": ("騎手勝/複比", "勝率と複勝率の比率"),
        # 調教師
        "trainer_win_rate": ("調教師勝率", "調教師の通算勝率"),
        "trainer_place_rate": ("調教師複勝率", "調教師の通算複勝率"),
        "trainer_runs": ("調教師出走数", "調教師の通算出走数"),
        "trainer_win_rate_90d": ("調教師勝率90日", "直近90日の勝率"),
        "trainer_place_rate_90d": ("調教師複勝率90日", "直近90日の複勝率"),
        "trainer_venue_wr": ("調教師×場勝率", "その競馬場での勝率"),
        "trainer_surface_wr": ("調教師×芝ダ勝率", "芝/ダートでの勝率"),
        "trainer_dist_wr": ("調教師×距離勝率", "距離帯での勝率"),
        "jt_combo_wr": ("騎手×調教師勝率", "コンビの通算勝率"),
        "jt_combo_runs": ("騎手×調教師回数", "コンビの騎乗回数"),
        "trainer_place_rank_in_race": ("調教師力順位", "レース内での調教師力ランク"),
        "trainer_wp_ratio": ("調教師勝/複比", "勝率と複勝率の比率"),
        # コース類似度
        "venue_sim_place_rate": ("類似場複勝率", "コース類似度加重の複勝率"),
        "venue_sim_win_rate": ("類似場勝率", "コース類似度加重の勝率"),
        "venue_sim_avg_finish": ("類似場平均着順", "コース類似度加重の平均着順"),
        "venue_sim_runs": ("類似場出走数", "コース類似が高い場での出走数"),
        "venue_sim_n_venues": ("類似場数", "類似するコース数"),
        "same_dir_place_rate": ("同回り複勝率", "同じ回り方向での複勝率"),
        "same_dir_runs": ("同回り出走数", "同じ回り方向での出走数"),
        "venue_sim_rank_in_race": ("類似場順位", "レース内での類似場成績ランク"),
        # 血統
        "sire_win_rate": ("父馬勝率", "父の産駒通算勝率"),
        "sire_place_rate": ("父馬複勝率", "父の産駒通算複勝率"),
        "bms_win_rate": ("母父勝率", "母父の産駒通算勝率"),
        "bms_place_rate": ("母父複勝率", "母父の産駒通算複勝率"),
        "sire_surf_wr": ("父×芝ダ勝率", "父の産駒の芝/ダート勝率"),
        "sire_smile_wr": ("父×距離勝率", "父の産駒の距離帯勝率"),
        "bms_surf_wr": ("母父×芝ダ勝率", "母父の産駒の芝/ダート勝率"),
        # 能力トレンド
        "trend_position_slope": ("着順トレンド", "直近着順の上昇/下降傾向"),
        "trend_deviation_slope": ("偏差値トレンド", "偏差値の上昇/下降傾向"),
        "dev_run1": ("前走偏差値", "前走の能力偏差値"),
        "dev_run2": ("前々走偏差値", "前々走の能力偏差値"),
        "chakusa_index_avg3": ("着差指数", "直近3走の着差指数平均"),
        # 展開・脚質
        "horse_running_style": ("脚質", "逃げ/先行/好位/中団/差し/追込"),
        "horse_condition_match": ("条件適性", "コース条件との適性度"),
        "ml_pos_est": ("ML位置取り予測", "機械学習による位置取り推定"),
        "ml_l3f_est": ("ML上がり予測", "機械学習による上がり3F推定"),
        "speed_sec_per_m_est": ("スピード指数", "走破タイムの距離補正値"),
        "is_long_break": ("長期休養明け", "半年以上の休養明けか"),
        # コース分析
        "gate_venue_wr": ("枠×場勝率", "枠番と競馬場の組合せ勝率"),
        "style_surface_wr": ("脚質×芝ダ勝率", "脚質と芝/ダートの組合せ勝率"),
        "gate_style_wr": ("枠×脚質勝率", "枠番と脚質の組合せ勝率"),
        # 前走オッズ・クラス
        "prev_odds_1": ("前走オッズ", "前走の単勝オッズ"),
        "prev_odds_2": ("前々走オッズ", "前々走の単勝オッズ"),
        "class_change": ("クラス変動", "前走からのクラス変化"),
        "prev_grade_code": ("前走クラス", "前走のクラスコード"),
        # レース内順位
        "horse_form_rank_in_race": ("調子順位", "レース内での近走調子ランク"),
        "horse_place_rank_in_race": ("馬力順位", "レース内での通算成績ランク"),
        "horse_form_zscore_in_race": ("調子Zスコア", "レース内での近走調子偏差"),
        "relative_weight_kg": ("相対斤量", "出走馬の中での相対斤量"),
        # ML-1 追加
        "odds_log_drift": ("オッズ変動", "オッズの対数変動幅"),
        "jt_combo_wr_30d": ("騎手×調教師勝率30日", "直近30日のコンビ勝率"),
        "jt_combo_place_rate_30d": ("騎手×調教師複勝率30日", "直近30日のコンビ複勝率"),
        "weight_kg_trend_3run": ("斤量トレンド", "直近3走の斤量変化傾向"),
    }
    _feature_imp_cache: list = []

    # ──────────────────────────────────────────────
    # 競馬場研究 API
    # ──────────────────────────────────────────────

    @app.route("/api/venue/profile")
    def api_venue_profile():
        """競馬場プロファイル（一覧 or 個別詳細）"""
        from data.masters.venue_similarity import get_all_profiles, get_similar_venues
        from data.masters.course_master import ALL_COURSES
        from config.settings import get_composite_weights
        from data.masters.venue_master import VENUE_CODE_TO_NAME

        code = request.args.get("code", "").strip()
        profiles = get_all_profiles()

        if not code:
            # 全場一覧
            items = []
            for vname, p in sorted(profiles.items(), key=lambda x: x[1].venue_code):
                n_courses = sum(1 for c in ALL_COURSES if c.venue_code == p.venue_code)
                items.append({
                    "venue": vname,
                    "venue_code": p.venue_code,
                    "is_jra": p.is_jra,
                    "has_turf": p.has_turf,
                    "has_dirt": p.has_dirt,
                    "direction": p.direction,
                    "avg_straight_m": round(p.avg_straight_m, 1),
                    "max_straight_m": p.max_straight_m,
                    "slope_type": p.slope_type,
                    "first_corner_score": round(p.first_corner_score, 2),
                    "corner_type_dominant": p.corner_type_dominant,
                    "n_courses": n_courses,
                })
            return jsonify({"venues": items})

        # 個別詳細
        venue_name = VENUE_CODE_TO_NAME.get(code)
        if not venue_name or venue_name not in profiles:
            return jsonify(error=f"不明な場コード: {code}"), 404

        p = profiles[venue_name]
        similar = get_similar_venues(venue_name, n=5)
        weights = get_composite_weights(venue_name)
        courses = [c for c in ALL_COURSES if c.venue_code == code]
        course_list = []
        for c in sorted(courses, key=lambda x: (x.surface, x.distance)):
            course_list.append({
                "course_id": f"{c.venue_code}_{c.surface}_{c.distance}",
                "surface": c.surface,
                "distance": c.distance,
                "direction": c.direction,
                "straight_m": c.straight_m,
                "corner_count": c.corner_count,
                "corner_type": c.corner_type,
                "first_corner": c.first_corner,
                "slope_type": c.slope_type,
                "inside_outside": c.inside_outside,
            })

        sim_list = []
        for sv, score in similar:
            sp = profiles.get(sv)
            sim_list.append({
                "venue": sv,
                "venue_code": sp.venue_code if sp else "",
                "similarity": round(score, 3),
            })

        return jsonify({
            "venue": venue_name,
            "venue_code": code,
            "is_jra": p.is_jra,
            "has_turf": p.has_turf,
            "has_dirt": p.has_dirt,
            "direction": p.direction,
            "profile": {
                "avg_straight_m": round(p.avg_straight_m, 1),
                "max_straight_m": p.max_straight_m,
                "slope_type": p.slope_type,
                "first_corner_score": round(p.first_corner_score, 2),
                "corner_type_dominant": p.corner_type_dominant,
            },
            "composite_weights": {k: round(v, 3) for k, v in weights.items()},
            "similar_venues": sim_list,
            "courses": course_list,
            "n_courses": len(course_list),
        })

    @app.route("/api/venue/bias")
    def api_venue_bias():
        """競馬場バイアス・傾向データ（course_dbベース）"""
        from src.database import get_course_db, get_course_last3f_sigma
        from collections import defaultdict

        code = request.args.get("code", "").strip()
        if not code:
            return jsonify(error="code パラメータ必須"), 400

        # course_db から当該venueの全コースデータを取得
        try:
            all_db = get_course_db()
        except Exception:
            all_db = {}
        venue_keys = [k for k in all_db if k.startswith(f"{code}_")]

        # 全レコードをコース別にまとめつつ、venue全体も集計
        all_records: list = []
        per_course_records: dict = {}
        for ckey in sorted(venue_keys):
            recs = all_db[ckey]
            all_records.extend(recs)
            per_course_records[ckey] = recs

        def _aggregate_gate_bias(records):
            """枠番別成績を集計（course_statsと同じロジック）"""
            _gate_race = {}
            for r in records:
                gn = r.get("gate_no")
                if not gn:
                    continue
                rk_ = (r.get("race_date", ""), r.get("class_name", ""), r.get("field_count", 0))
                grk_ = (rk_, int(gn))
                fp = r.get("finish_pos")
                if grk_ not in _gate_race:
                    _gate_race[grk_] = {"win": False, "place2": False, "place3": False, "odds": 0.0, "horses": 0}
                entry = _gate_race[grk_]
                entry["horses"] += 1
                if fp == 1:
                    entry["win"] = True
                    entry["odds"] += float(r.get("win_odds") or 0)
                if isinstance(fp, int) and fp <= 2:
                    entry["place2"] = True
                if isinstance(fp, int) and fp <= 3:
                    entry["place3"] = True

            gate_agg = defaultdict(lambda: {"runs": 0, "win": 0, "place2": 0, "place3": 0, "odds_sum": 0.0, "total_horses": 0})
            for (_rk, gate), v in _gate_race.items():
                g_str = str(gate)
                gate_agg[g_str]["runs"] += 1
                gate_agg[g_str]["total_horses"] += v["horses"]
                if v["win"]:
                    gate_agg[g_str]["win"] += 1
                    gate_agg[g_str]["odds_sum"] += v["odds"]
                if v["place2"]:
                    gate_agg[g_str]["place2"] += 1
                if v["place3"]:
                    gate_agg[g_str]["place3"] += 1

            result = {}
            for g, v in sorted(gate_agg.items(), key=lambda x: int(x[0])):
                if v["runs"] < 3:
                    continue
                t = v["runs"]
                th = v["total_horses"]
                result[g] = {
                    "runs": t, "win": v["win"], "place2": v["place2"], "place3": v["place3"],
                    "win_rate": round(v["win"] / t * 100, 1) if t else 0.0,
                    "place2_rate": round(v["place2"] / t * 100, 1) if t else 0.0,
                    "place3_rate": round(v["place3"] / t * 100, 1) if t else 0.0,
                    "roi": round(v["odds_sum"] * 100 / th, 1) if th else 0.0,
                }
            return result

        def _aggregate_running_style(records):
            """脚質別成績を集計（course_statsと同じロジック）"""
            _race_min_p4c = {}
            for r in records:
                rk_ = (r.get("race_date", ""), r.get("class_name", ""), r.get("field_count", 0))
                p4c_ = r.get("position_4c")
                if p4c_:
                    _race_min_p4c[rk_] = min(_race_min_p4c.get(rk_, 999), p4c_)

            def _pos_to_style(pos4c, field_count, race_key):
                if not pos4c or not field_count or field_count < 4:
                    return ""
                min_p4c = _race_min_p4c.get(race_key, 1)
                if pos4c == min_p4c:
                    return "逃げ"
                r = pos4c / field_count
                if r <= 0.22:
                    return "先行"
                elif r <= 0.38:
                    return "好位"
                elif r <= 0.55:
                    return "中団"
                elif r <= 0.72:
                    return "差し"
                else:
                    return "追込"

            style_stats = defaultdict(lambda: {"total": 0, "win": 0, "place2": 0, "place3": 0, "odds_sum": 0.0})
            for r in records:
                rk_ = (r.get("race_date", ""), r.get("class_name", ""), r.get("field_count", 0))
                style = _pos_to_style(r.get("position_4c"), r.get("field_count"), rk_)
                if style:
                    fp = r.get("finish_pos")
                    style_stats[style]["total"] += 1
                    if fp == 1:
                        style_stats[style]["win"] += 1
                        style_stats[style]["odds_sum"] += float(r.get("win_odds") or 0)
                    if isinstance(fp, int) and fp <= 2:
                        style_stats[style]["place2"] += 1
                    if isinstance(fp, int) and fp <= 3:
                        style_stats[style]["place3"] += 1

            result = {}
            for style in ["逃げ", "先行", "好位", "中団", "差し", "追込"]:
                st = style_stats.get(style, {"total": 0, "win": 0, "place2": 0, "place3": 0, "odds_sum": 0.0})
                t = st["total"]
                result[style] = {
                    "total": t, "win": st["win"], "place2": st["place2"], "place3": st["place3"],
                    "win_rate": round(st["win"] / t * 100, 1) if t else 0.0,
                    "place2_rate": round(st["place2"] / t * 100, 1) if t else 0.0,
                    "place3_rate": round(st["place3"] / t * 100, 1) if t else 0.0,
                    "roi": round(st["odds_sum"] * 100 / t, 1) if t else 0.0,
                }
            return result

        # venue全体の集計
        gate_bias_all = _aggregate_gate_bias(all_records)
        running_style_all = _aggregate_running_style(all_records)

        # コース別の集計
        per_course = {}
        for ckey, recs in per_course_records.items():
            parts = ckey.split("_")
            if len(parts) >= 3:
                surf, dist = parts[1], parts[2]
            else:
                surf, dist = "", ""
            per_course[ckey] = {
                "surface": surf,
                "distance": int(dist) if dist.isdigit() else 0,
                "count": len(recs),
                "gate_bias": _aggregate_gate_bias(recs),
                "running_style": _aggregate_running_style(recs),
            }

        # 上がり3F（既存のcourse_dbベース関数を使用）
        all_l3f = get_course_last3f_sigma()
        last3f: dict = {}
        for (vc, surf, dist), stats in all_l3f.items():
            if vc == code:
                ckey = f"{vc}_{surf}_{dist}"
                last3f[ckey] = {
                    "surface": surf,
                    "distance": dist,
                    "mean": round(stats.get("mean", 0), 2),
                    "sigma": round(stats.get("sigma", 0), 2),
                    "cnt": stats.get("cnt", 0),
                }

        # 人気別成績（win_oddsからレース内の人気順を算出）
        def _aggregate_popularity(records):
            """人気別成績を集計（win_oddsの低い順＝人気順）"""
            # レース毎にグループ化
            race_groups = defaultdict(list)
            for r in records:
                rk_ = (r.get("race_date", ""), r.get("class_name", ""), r.get("field_count", 0))
                odds = r.get("win_odds")
                if odds and r.get("finish_pos"):
                    race_groups[rk_].append(r)
            # レース毎にオッズ昇順でソートし人気順を付与
            pop_stats = defaultdict(lambda: {"total": 0, "win": 0, "place2": 0, "place3": 0, "odds_sum": 0.0})
            for rk_, horses in race_groups.items():
                horses_sorted = sorted(horses, key=lambda x: float(x.get("win_odds") or 9999))
                for rank, h in enumerate(horses_sorted, 1):
                    pop_key = str(rank) if rank <= 12 else "13+"
                    fp = h.get("finish_pos")
                    pop_stats[pop_key]["total"] += 1
                    if fp == 1:
                        pop_stats[pop_key]["win"] += 1
                        pop_stats[pop_key]["odds_sum"] += float(h.get("win_odds") or 0)
                    if isinstance(fp, int) and fp <= 2:
                        pop_stats[pop_key]["place2"] += 1
                    if isinstance(fp, int) and fp <= 3:
                        pop_stats[pop_key]["place3"] += 1
            result = {}
            for pop_key in sorted(pop_stats.keys(), key=lambda x: (0, int(x)) if x.isdigit() else (1, 0)):
                st = pop_stats[pop_key]
                t = st["total"]
                result[pop_key] = {
                    "total": t, "win": st["win"], "place2": st["place2"], "place3": st["place3"],
                    "win_rate": round(st["win"] / t * 100, 1) if t else 0.0,
                    "place2_rate": round(st["place2"] / t * 100, 1) if t else 0.0,
                    "place3_rate": round(st["place3"] / t * 100, 1) if t else 0.0,
                    "roi": round(st["odds_sum"] * 100 / t, 1) if t else 0.0,
                }
            return result

        popularity_all = _aggregate_popularity(all_records)

        return jsonify({
            "venue_code": code,
            "gate_bias": gate_bias_all,
            "running_style": running_style_all,
            "popularity": popularity_all,
            "per_course": per_course,
            "last3f": last3f,
        })

    @app.route("/api/feature_importance")
    def api_feature_importance():
        """LGBMPredictor 全サブモデル平均特徴量重要度を返す（キャッシュあり）"""
        if _feature_imp_cache:
            return jsonify(_feature_imp_cache)
        try:
            import lightgbm as lgb
            from collections import defaultdict
            from src.ml.lgbm_model import FEATURE_COLUMNS, SHAP_FEATURE_GROUPS

            model_dir = os.path.join(PROJECT_ROOT, "data", "models")
            feat_gain: defaultdict = defaultdict(list)
            for fname in sorted(os.listdir(model_dir)):
                if fname.endswith(".txt") and "lgbm_place" in fname and "ranker" not in fname:
                    try:
                        m = lgb.Booster(model_file=os.path.join(model_dir, fname))
                        for name, g in zip(m.feature_name(),
                                           m.feature_importance(importance_type="gain")):
                            feat_gain[name].append(float(g))
                    except Exception:
                        pass

            if not feat_gain:
                return jsonify([])

            avg = {k: sum(v) / len(v) for k, v in feat_gain.items()}
            total = sum(avg.values()) or 1.0

            feat_cat: dict = {}
            for cat, feats in SHAP_FEATURE_GROUPS.items():
                for f in feats:
                    feat_cat[f] = cat
            result = []
            for name in FEATURE_COLUMNS:
                g = avg.get(name, 0.0)
                lbl, dsc = _FEAT_LABEL.get(name, (name, ""))
                result.append({
                    "name": name,
                    "label": lbl,
                    "desc": dsc,
                    "cat": feat_cat.get(name, "適性"),
                    "gain": round(g, 1),
                    "pct": round(g / total * 100, 2),
                })
            result.sort(key=lambda x: -x["gain"])
            for i, r in enumerate(result):
                r["rank"] = i + 1

            _feature_imp_cache[:] = result
            return jsonify(result)
        except Exception as e:
            return jsonify(error=str(e))

    return app


def run_server(port: int = None, open_browser: bool = False):
    app = create_app()
    host = SERVER_HOST
    port = port or SERVER_PORT

    local_url = f"http://127.0.0.1:{port}/"
    logger.info("\n[D-AI競馬予想] 統合ダッシュボード")
    logger.info(f"   ローカル: {local_url}")
    if host == "0.0.0.0":
        logger.info(f"   LAN:    http://<このPCのIP>:{port}/")
    if AUTH_ENABLED:
        logger.info(f"   認証: 有効 (ユーザー: {AUTH_USERNAME})")
    logger.info("")

    def _open():
        import time
        import webbrowser

        time.sleep(1.5)
        try:
            webbrowser.open(local_url)
        except Exception:
            logger.debug("browser open failed", exc_info=True)

    if open_browser:
        threading.Thread(target=_open, daemon=True).start()

    from werkzeug.serving import run_simple
    run_simple(host, port, app, threaded=True, use_reloader=False)


if __name__ == "__main__":
    try:
        _port = int(os.environ.get("PORT", os.environ.get("KEIBA_PORT", SERVER_PORT)))
        run_server(port=_port)
    except Exception as _fatal:
        logger.critical("ダッシュボード異常終了: %s", _fatal, exc_info=True)
