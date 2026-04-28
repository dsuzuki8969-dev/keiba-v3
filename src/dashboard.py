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

from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory

from src.log import get_logger
from src.utils.atomic_json import atomic_write_json

logger = get_logger(__name__)

try:
    from config.settings import (
        AUTH_ENABLED,
        AUTH_PASSWORD,
        AUTH_USERNAME,
        COURSE_DB_COLLECTOR_STATE_PATH,
        COURSE_DB_PRELOAD_PATH,
        OUTPUT_DIR,
        SERVER_HOST,
        SERVER_PORT,
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
_ODDS_SCHEDULE_HOURS = [5, 8, 11, 14, 17, 20]
_odds_scheduler_running = False
_odds_last_auto_fetch = None   # datetime of last auto-fetch
# ── 予想自動生成スケジューラー ──
_PREDICT_SCHEDULE_HOUR = 17   # 前日17:00に翌日の予想を生成
_predict_scheduler_running = False
_predict_last_auto_run = None  # datetime of last auto-run
# ── 結果照合+DB更新 自動スケジューラー ──
_RESULTS_SCHEDULE_HOUR = 23   # 当日23:00に結果照合+DB更新
_results_scheduler_running = False
_results_last_auto_run = None  # datetime of last auto-run
_results_state = {"running": False, "done": False, "cancel": False, "progress": "", "error": None}
_db_update_state = {"running": False, "done": False, "cancel": False, "progress": "", "error": None}

# ── キャッシュ: 日付→(timestamp, データ) ──
_predictions_cache: dict = {}
_home_info_cache: dict = {}
_CACHE_TTL = 1800  # 秒（予想データ: 30分、ただしpred.json更新時は自動無効化）
_WEATHER_CACHE_TTL = 1800  # 秒（天気データ: 30分）

# ── 案A (2026-04-23): Home API 自動フェッチ用 state ──
# /api/home/today_stats 呼び出し時に、発走+10分経過かつ results.json 未登録のレースを
# バックグラウンドで自動フェッチする。以下はそのための重複防止・クールダウン state。
_auto_fetch_lock = threading.Lock()
_auto_fetch_busy_dates: set = set()         # 現在 fetch 処理中の日付
_auto_fetch_cooldown: dict = {}             # race_id → last_attempt_timestamp
_AUTO_FETCH_COOLDOWN_SEC = 30               # T-017 (2026-04-27): 60→30秒。フロント polling 2分と相性改善・5R遅延解消
_AUTO_FETCH_MAX_PER_CALL = 50               # T-001 (2026-04-25): 5R→50R。70R/日規模に追従、netkeiba 1.5s × 50R ≒ 75秒で完了
_AUTO_FETCH_COOLDOWN_MAX = 1000             # cooldown_max も拡張（race_id 蓄積防止用、cooldown_sec×2 の余裕）

# ── T-017 (2026-04-27): 手動強制更新 /api/force_refresh_today 用 state ──
_FORCE_REFRESH_LOCK = threading.Lock()      # 連打防止用排他ロック
_force_refresh_ip_rate: dict = {}           # IP → last_request_timestamp（簡易レートリミット）
_FORCE_REFRESH_RATE_LIMIT_SEC = 5           # 同一 IP 5秒以内の再リクエストは 429 返す
_FORCE_REFRESH_MAX_PER_CALL = 100           # force=True 時の1回あたり最大処理数


def _cleanup_cooldown_if_needed() -> None:
    """_auto_fetch_cooldown が肥大化したら期限切れエントリをパージ。

    常時稼働 Flask プロセスで race_id が蓄積し続けるのを防ぐ。
    2倍の cooldown 期間を過ぎたエントリは削除（再試行判定に不要）。
    """
    if len(_auto_fetch_cooldown) < _AUTO_FETCH_COOLDOWN_MAX:
        return
    now = time.time()
    expired = [k for k, v in list(_auto_fetch_cooldown.items())
               if now - v > _AUTO_FETCH_COOLDOWN_SEC * 2]
    for k in expired:
        _auto_fetch_cooldown.pop(k, None)

# 競馬場コード → 緯度・経度（天気API用）
VENUE_COORDS = {
    "01": (43.06, 141.35),  # 札幌
    "02": (41.77, 140.73),  # 函館
    "03": (37.75, 140.47),  # 福島
    "04": (37.92, 139.04),  # 新潟
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
    "52": (42.93, 143.20),  # 帯広（ばんえい）SPAT4 互換コード
    "65": (42.93, 143.20),  # 帯広（ばんえい）netkeiba race_id 準拠コード（venue_master "帯広":"65"）— T-018
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
        from data.masters.venue_master import get_venue_code_from_race_id, get_venue_name
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
                from src.scraper.netkeiba import NAR_URL
                year = date_str[:4]
                mmdd = date_str[5:7] + date_str[8:10]
                # nar.netkeiba.comで1R目を試行（キャッシュ or ネットワーク）
                probe_id = f"{year}65{mmdd}01"
                probe_soup = client.get(
                    f"{NAR_URL}/race/shutuba.html",
                    params={"race_id": probe_id}
                )
                horse_links = probe_soup.select("a[href*='/horse/']") if probe_soup else []
                if len(horse_links) >= 3:
                    # 出走馬が確認できた場合のみ開催とみなす
                    banei_ids = [f"{year}65{mmdd}{rno:02d}" for rno in range(1, 13)]
                    for rid in banei_ids:
                        if rid not in existing:
                            ids.append(rid)
                            existing.add(rid)
                    logger.info("ばんえいプローブ補完: 12R")
            except Exception as e:
                logger.debug("ばんえい補完失敗: %s", e)

        # 岩手安全策（水沢・盛岡がまだ含まれていない場合のみ発動）
        for iwate_vc, iwate_name in [("36", "水沢"), ("35", "盛岡")]:
            if not any(rid[4:6] == iwate_vc for rid in ids):
                try:
                    from src.scraper.netkeiba import NAR_URL as _NAR_URL
                    _year = date_str[:4]
                    _mmdd = date_str[5:7] + date_str[8:10]
                    probe_id = f"{_year}{iwate_vc}{_mmdd}01"
                    probe_soup = client.get(
                        f"{_NAR_URL}/race/shutuba.html",
                        params={"race_id": probe_id}
                    )
                    horse_links = probe_soup.select("a[href*='/horse/']") if probe_soup else []
                    if len(horse_links) >= 3:
                        iwate_ids = [f"{_year}{iwate_vc}{_mmdd}{rno:02d}" for rno in range(1, 13)]
                        for rid in iwate_ids:
                            if rid not in existing:
                                ids.append(rid)
                                existing.add(rid)
                        logger.info("岩手プローブ補完(%s): 12R", iwate_name)
                except Exception as e:
                    logger.debug("岩手補完失敗(%s): %s", iwate_name, e)

        seen_names = set()
        result = []
        for rid in ids:
            vc = get_venue_code_from_race_id(rid)
            if not vc:
                continue
            name = get_venue_name(vc)
            if not name or name in seen_names:
                continue
            seen_names.add(name)
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


def _race_belongs_to_date(race_id: str, date_key: str) -> bool:
    """race_idが指定日(YYYYMMDD)に属するか判定。

    NAR: race_id[6:10] == MMDD（race_idにカレンダー日付が直接埋め込まれる）
    JRA: race_id[6:10] は回次+日（カレンダー日付ではない）ため判定不可 → True を返す
    """
    if len(race_id) < 10 or len(date_key) < 8:
        return True  # 判定不能 → 許可
    _JRA_VENUE_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
    venue_code = race_id[4:6]
    if venue_code in _JRA_VENUE_CODES:
        return True  # JRA はrace_idから日付判定できないので許可
    # NAR: race_id[6:10] がMMDD
    mmdd = date_key[4:8]  # YYYYMMDD → MMDD
    return race_id[6:10] == mmdd


def _scan_today_predictions(date_str: str) -> dict:
    """指定日の個別レースHTML (YYYYMMDD_場名XR.html) をスキャンして会場別に整理"""
    date_key = date_str.replace("-", "")
    races: dict = {}
    if not os.path.isdir(OUTPUT_DIR):
        return {"races": races, "order": []}

    # pred JSONから馬券自信度 + 馬データ + チケットデータを取得
    _pred_conf = {}     # {(venue, race_no): confidence}
    _pred_race_id = {}  # T-039: {(venue, race_no): race_id}
    _pred_horses = {}   # {(venue, race_no): [horse_dict, ...]}
    _pred_tickets = {}  # {(venue, race_no): [ticket_dict, ...]}
    _pred_is_jra = {}   # {(venue, race_no): bool}
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
                # 日付フィルタ: 他日のレースが混在している場合はスキップ
                _rid = pr.get("race_id", "")
                if _rid and not _race_belongs_to_date(_rid, date_key):
                    continue
                key = (venue, rno)
                _pred_conf[key] = pr.get("confidence", "")
                _pred_race_id[key] = _rid  # T-039: race_id を保存
                _horses = pr.get("horses", [])
                # 取消馬処理 + 印再割り当て（個別ページと同じロジック）
                # マスター指示 2026-04-23: 実オッズ未取得だが ML 予測値を持つ馬を
                #   取消扱いにしていたバグを修正。predicted_tansho_odds か win_prob が
                #   有効な馬は取消とみなさない（=部分オッズ取得の穴埋め失敗にすぎない）。
                _has_any_odds = any(h.get("odds") is not None for h in _horses)
                if _has_any_odds:
                    for _hd in _horses:
                        _has_ml = (_hd.get("predicted_tansho_odds") is not None
                                    or (_hd.get("win_prob") or 0) > 0)
                        # T-045: is_scratched=True が明示済みの場合は _has_ml 保護を上書き
                        # （前日取消馬: training_recordsに「出走取消」テキストがあり
                        #   engine.py 側で既に is_scratched=True が設定済みのケース）
                        _explicitly_scratched = _hd.get("is_scratched") is True
                        if _explicitly_scratched or (
                            _hd.get("odds") is None and _hd.get("popularity") is None
                            and not _hd.get("is_scratched") and not _has_ml
                        ):
                            _hd["is_scratched"] = True
                            _hd["win_prob"] = 0.0
                            _hd["place2_prob"] = 0.0
                            _hd["place3_prob"] = 0.0
                            _hd["mark"] = ""
                            _hd["predicted_corners"] = ""
                            _hd["running_style"] = ""
                        # 取消解除: オッズが復帰した馬はis_scratchedを解除
                        elif _hd.get("is_scratched") and _hd.get("odds") is not None and _hd.get("popularity") is not None:
                            _hd["is_scratched"] = False
                            # win_prob=0 のまま残っていると 0.0% 表示になるため、中間値から復元
                            try:
                                from src.calculator.popularity_blend import restore_win_prob_if_zero
                                restore_win_prob_if_zero(_hd, field_count=len(_horses))
                            except Exception:
                                pass
                # 印はpred.json（formatter.py assign_marks）の値をそのまま使用
                # （リアルタイムオッズ更新時は1412/1657行の reassign_marks_dict で再割り振り）
                # 取消馬のみ印をクリア（既にis_scratched処理で対応済み）
                _pred_horses[key] = _horses
                _pred_tickets[key] = pr.get("tickets", [])
                _pred_is_jra[key] = pr.get("is_jra", False)
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
                "race_id": _pred_race_id.get((venue, race_no), ""),  # T-039: 的中バッジ用
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

    # pred JSON から不足レースを補完（HTMLがない会場・レースを追加）
    if True:
        pred_json_path = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
        if os.path.isfile(pred_json_path):
            try:
                with open(pred_json_path, "r", encoding="utf-8") as pf:
                    pred_data = json.load(pf)
                # HTML既読レースを記録（重複防止）
                _existing = set()
                for _v, _rlist in races.items():
                    for _r in _rlist:
                        _existing.add((_v, _r["race_no"]))
                for pr in pred_data.get("races", []):
                    venue = pr.get("venue", "")
                    if not venue or venue == "None" or venue.startswith("地方"):
                        continue
                    # 日付フィルタ: 他日のレースが混在している場合はスキップ
                    _rid = pr.get("race_id", "")
                    if _rid and not _race_belongs_to_date(_rid, date_key):
                        continue
                    race_no = pr.get("race_no", 0)
                    if (venue, race_no) in _existing:
                        continue  # HTML版が既にあるのでスキップ
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

    # ── 厳選穴馬: 回帰ベース妙味スコアで評価 ──
    from config import settings as _s
    ana_horses: list = []
    for venue_name in list(races.keys()):
        for r in races[venue_name]:
            rno = r["race_no"]
            horses = _pred_horses.get((venue_name, rno), [])
            is_jra = _pred_is_jra.get((venue_name, rno), False)

            for h in horses:
                mk = h.get("mark", "")
                odds_val = h.get("odds", 0) or 0
                ts = h.get("tokusen_score", 0) or 0
                ana_sc = h.get("ana_score", 0) or 0
                comp = h.get("composite", 0) or 0
                course = h.get("course_total", 0) or 0
                p3 = h.get("place3_prob", 0) or 0

                # ☆印は無条件、それ以外は10倍以上の非本命馬
                is_star = mk == "☆"
                is_ana = mk not in ("◉", "◎", "○", "▲", "△", "×") and odds_val >= 10.0

                if not (is_star or is_ana):
                    continue

                # 回帰ベース妙味スコア
                miryoku = round(
                    _s.MIRYOKU_W_TOKUSEN * ts
                    + _s.MIRYOKU_W_COMPOSITE * (comp - 45) / 10
                    + _s.MIRYOKU_W_COURSE * (course - 45) / 10
                    + _s.MIRYOKU_W_ANA * ana_sc / 5
                    + _s.MIRYOKU_W_PLACE3 * p3 * 10
                    + _s.MIRYOKU_W_JRA * (1 if is_jra else 0),
                    2)

                # 星評価判定（SS/S→★★★、A→★★、B/C→★、D/E→除外）
                if miryoku >= _s.MIRYOKU_GRADE_S:
                    star_rating = 3  # ★★★
                elif miryoku >= _s.MIRYOKU_GRADE_A:
                    star_rating = 2  # ★★
                elif miryoku >= _s.MIRYOKU_GRADE_C:
                    star_rating = 1  # ★
                else:
                    continue  # D/E は表示対象外

                ana_horses.append({
                    "venue": venue_name,
                    "race_no": rno,
                    "race_name": r.get("name", f"{rno}R"),
                    "post_time": r.get("post_time", ""),
                    "horse_no": h.get("horse_no", 0),
                    "horse_name": h.get("horse_name", ""),
                    "mark": mk,
                    "odds": odds_val,
                    "popularity": h.get("popularity", 0),
                    "composite": round(comp, 1),
                    "place3_prob": round(p3 * 100, 1),
                    "miryoku": miryoku,
                    "star_rating": star_rating,
                    "is_star": is_star,
                })
    # 妙味スコア降順でソート
    ana_horses.sort(key=lambda x: -x["miryoku"])

    return {"races": races, "order": order, "ana_horses": ana_horses}


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


# レガシーHTMLテンプレートは削除済み（React SPAに移行完了）
# 旧 BASE_HTML は 2026-04-03 に除去（3,330行のデッドコード）



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
    """localhost からのアクセスなら admin とみなす。

    v6.1.19 セキュリティ強化: cloudflared 経由の外部アクセスも 127.0.0.1 として
    到達するため、単純な remote_addr チェックだけだと外部クライアントが admin 扱い
    になってしまう。以下いずれかの Cloudflare ヘッダがあれば「トンネル経由」と
    判定し admin ではないとする。

    - CF-Connecting-IP: Cloudflare が付与する実クライアント IP
    - CF-Ray:           Cloudflare リクエスト識別子
    - X-Forwarded-For:  上流プロキシ/tunnel 経由の目印

    これにより `dash.d-aikeiba.com` 経由のリクエストは admin にならず、
    ローカル PC 上の Chrome など `http://127.0.0.1:5051` に直接アクセスした
    場合のみ admin として扱われる。
    """
    # トンネル経由のヘッダがあれば admin ではない（外部クライアント）
    for header in ("CF-Connecting-IP", "CF-Ray", "X-Forwarded-For"):
        if req.headers.get(header):
            return False
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


def _get_pending_fetch_stats(date: str, races: list) -> tuple:
    """results.json を読み、発走済み・未取り込みレースの統計を返す。
    今日stats と /api/health の両方から呼ばれる共通ロジック（M-1 DRY 解消）。

    Returns: (finished_rids: set, pending_count: int, pending_age_max_min: int)
    """
    date_key = date.replace("-", "")
    res_fp = os.path.join(PROJECT_ROOT, "data", "results", f"{date_key}_results.json")
    finished_rids: set = set()
    if os.path.isfile(res_fp):
        try:
            with open(res_fp, "r", encoding="utf-8") as _rf:
                _rd = json.load(_rf)
            finished_rids = {
                rid for rid, entry in _rd.items()
                if isinstance(entry, dict) and entry.get("order")
            }
        except Exception:
            pass
    now_dt = datetime.now()
    pending_minutes = []
    for r in races:
        rid = str(r.get("race_id", ""))
        if not rid or rid in finished_rids:
            continue
        pt = r.get("post_time", "") or ""
        if not pt:
            continue
        try:
            post_dt = datetime.strptime(f"{date} {pt}", "%Y-%m-%d %H:%M")
        except Exception:
            continue
        if now_dt < post_dt:
            continue
        pending_minutes.append(int((now_dt - post_dt).total_seconds() // 60))
    return (
        finished_rids,
        len(pending_minutes),
        max(pending_minutes) if pending_minutes else 0,
    )


def _collect_sanrentan_tickets(race: dict) -> list:
    """pred.json から三連単チケット集合を取得 (T-039 / LIVE STATS 共通)。

    探索範囲:
        race["tickets"] + race["formation_tickets"]
        + race["tickets_by_mode"]["fixed"|"accuracy"|"balanced"|"recovery"]

    Returns:
        type == "三連単" のチケット dict の list (順不同)
    """
    all_tix = list(race.get("tickets", []) or [])
    all_tix += list(race.get("formation_tickets", []) or [])
    tbm = race.get("tickets_by_mode", {}) or {}
    for mk in ("fixed", "accuracy", "balanced", "recovery"):
        all_tix += list(tbm.get(mk, []) or [])
    return [t for t in all_tix if t.get("type") == "三連単"]


def _check_sanrentan_hit(
    sanrentan_tix: list,
    top3_ordered: list,
):
    """1-2-3 着 combo と完全一致するチケットがあるか判定。

    Returns:
        True  : combo の上位 3 つが top3_ordered と完全一致するチケットあり
        False : チケットはあるが一致なし
        None  : チケット空 (= 三連単対象外レース) または top3_ordered 不完全
    """
    if not sanrentan_tix:
        return None
    if len(top3_ordered) < 3:
        return None
    for t in sanrentan_tix:
        combo = t.get("combo", [])
        if combo and len(combo) >= 3 and [int(x) for x in combo[:3]] == top3_ordered:
            return True
    return False


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

    # --- React SPA (メインUI) ---
    _react_build_dir = os.path.join(PROJECT_ROOT, "frontend", "dist")

    @app.route("/")
    def index():
        """React SPAをルートで配信（キャッシュ無効化）"""
        resp = send_from_directory(_react_build_dir, "index.html")
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

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

    # --- コース見取り図配信 ---
    _course_images_dir = os.path.join(PROJECT_ROOT, "data", "course_images")

    @app.route("/course_images/<path:filename>")
    def serve_course_image(filename):
        return send_from_directory(_course_images_dir, filename)

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
        """指定日の生成済み個別レースHTMLを会場別に返す（30分キャッシュ、pred.json更新時は自動無効化）"""
        date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
        nocache = request.args.get("nocache", "0") == "1"
        now = time.time()
        cached = _predictions_cache.get(date)
        # pred.json の更新時刻をチェック（外部から生成された場合のキャッシュ無効化）
        if cached and not nocache:
            date_key = date.replace("-", "")
            _pf = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
            try:
                _mt = os.path.getmtime(_pf)
                if _mt > cached[0]:
                    cached = None  # pred.json がキャッシュより新しい → 再読込
            except OSError:
                pass
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
                       total=total, odds_updated_at=odds_ts,
                       ana_horses=result.get("ana_horses", []))

    def _apply_paraphrase_cache(race: dict) -> int:
        """
        race.horses[].training_records[0].stable_comment_bullets を
        DB cache の paraphrase で上書きする。
        pred.json が他プロセスで上書きされても paraphrase 表示が維持される。
        Returns: 更新した training_records 数
        """
        import hashlib as _hashlib
        updated = 0
        try:
            from src.database import get_db as _get_db
            conn = _get_db()
        except Exception:
            return 0
        for horse in race.get("horses", []):
            tr_recs = horse.get("training_records") or []
            if not tr_recs:
                continue
            rec = tr_recs[0]
            stable = rec.get("stable_comment") or ""
            if not stable:
                continue
            # 句点・改行で分割（local_llm_paraphrase.py の split_bullets と同等）
            parts: list[str] = []
            for p in stable.replace("。", "。\n").split("\n"):
                p = p.strip().rstrip("。").strip()
                if p:
                    parts.append(p)
            if not parts:
                continue
            new_bullets: list[str] = []
            any_paraphrased = False
            for b in parts:
                h = _hashlib.sha256(b.encode("utf-8")).hexdigest()
                try:
                    row = conn.execute(
                        "SELECT paraphrased FROM stable_comment_paraphrase_cache WHERE input_hash=?",
                        (h,)
                    ).fetchone()
                except Exception:
                    row = None
                if row and row[0]:
                    new_bullets.append(row[0])
                    any_paraphrased = True
                else:
                    new_bullets.append(b)
            if any_paraphrased:
                rec["stable_comment_bullets"] = new_bullets
                updated += 1
        return updated

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
                    # パラフレーズ cache を適用（pred.json 上書き対策）
                    _apply_paraphrase_cache(race)

                    # odds があるのに乖離率未計算の馬を補完
                    patched = False
                    for h in race.get("horses", []):
                        if h.get("odds") and h.get("predicted_tansho_odds") and h.get("odds_divergence") is None:
                            _recalc_divergence(h)
                            patched = True
                    # 馬個別見解・印見解・買い目は廃止（2026-03 以降）

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
                    # 出走取消馬の処理（オッズ確定後に検出可能）
                    _race_horses = race.get("horses", [])
                    _has_any_odds = any(h.get("odds") is not None for h in _race_horses)
                    if _has_any_odds:
                        _scratched_changed = False
                        for _hd in _race_horses:
                            # マスター指示 2026-04-23: ML予測がある馬は「取消」扱いしない
                            # （部分オッズ取得失敗で odds=None になった馬を取消と誤判定するバグ対策）
                            _has_ml = (_hd.get("predicted_tansho_odds") is not None
                                        or (_hd.get("win_prob") or 0) > 0)
                            # T-045: is_scratched=True が明示済みの場合は _has_ml 保護を上書き
                            # （前日取消馬: engine/scraper 側で既に is_scratched=True が
                            #   設定済みのケース。_has_ml があっても取消を優先する）
                            _explicitly_scratched = _hd.get("is_scratched") is True
                            if _explicitly_scratched or (
                                _hd.get("odds") is None and _hd.get("popularity") is None
                                and not _hd.get("is_scratched") and not _has_ml
                            ):
                                _hd["is_scratched"] = True
                                _hd["win_prob"] = 0.0
                                _hd["place2_prob"] = 0.0
                                _hd["place3_prob"] = 0.0
                                _hd["mark"] = ""
                                _hd["predicted_corners"] = ""
                                _hd["running_style"] = ""
                                # 評価データ（ability_total等）は保持
                                _scratched_changed = True
                            # 取消解除: オッズが復帰した馬はis_scratchedを解除
                            elif _hd.get("is_scratched") and _hd.get("odds") is not None and _hd.get("popularity") is not None:
                                _hd["is_scratched"] = False
                                # win_prob=0 のまま残っていると 0.0% 表示になるため、中間値から復元
                                try:
                                    from src.calculator.popularity_blend import restore_win_prob_if_zero
                                    restore_win_prob_if_zero(_hd, field_count=len(_race_horses))
                                except Exception:
                                    pass
                                _scratched_changed = True
                        if _scratched_changed:
                            # 確率再配分
                            _active = [h for h in _race_horses if not h.get("is_scratched")]
                            for _pk, _ts in [("win_prob", 1.0), ("place2_prob", 2.0), ("place3_prob", 3.0)]:
                                _asum = sum(h.get(_pk, 0) for h in _active)
                                if _asum > 0:
                                    for h in _active:
                                        h[_pk] = round(min(1.0, h[_pk] / _asum * _ts), 4)

                    # pred.jsonのcomposite・確率を正として印のみ再割り振り（composite不変）
                    if _race_horses and any(h.get("win_prob") for h in _race_horses):
                        try:
                            from src.calculator.popularity_blend import reassign_marks_dict
                            if not _is_marks_frozen(race):
                                # 取消馬を除外して印再割り振り
                                _active_for_marks = [h for h in _race_horses if not h.get("is_scratched")]
                                reassign_marks_dict(_active_for_marks, is_jra=race.get("is_jra", True))
                            else:
                                logger.debug("印固定中（発走%d分前以内）: %s", MARK_FREEZE_MINUTES, race.get("race_id"))
                        except Exception:
                            pass

                    # 買い目指南 Phase 1: formation_tickets/formation_columns を復活
                    # 以前は下記3行で強制的に空にしていたが、◎◉心中・無印列制約を
                    # 導入したのでエンジン出力（pred.json）をそのまま UI に流す。
                    # race["formation_tickets"] = []  # 廃止
                    # race["formation_columns"] = {}  # 廃止
                    # race["tickets"] = []            # 廃止

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

                    # EV再計算: 常にwin_prob × odds で最新化
                    for _hd in race.get("horses", []):
                        _wp = _hd.get("win_prob") or 0
                        _o = _hd.get("odds") or _hd.get("predicted_tansho_odds") or 0
                        _hd["ev"] = round(_wp * _o, 3) if _wp > 0 and _o > 0 else None

                    if patched:
                        try:
                            # 原子書き込み + プロセス間ロック（並行書き込みで JSON が破損するのを防止）
                            atomic_write_json(pred_file, data)
                        except Exception as _e:
                            logger.warning("pred.json 印パッチ書き戻し失敗: %s", _e)
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

    @app.route("/api/horse_history/<horse_id>")
    def api_horse_history(horse_id: str):
        """馬1頭の過去成績（race_log から直近N走）+ run_dev を返す。
        馬指数グラフ（HorseHistoryChart）用。
        """
        try:
            limit = max(1, min(int(request.args.get("limit", 12)), 30))
        except (ValueError, TypeError):
            limit = 12
        if not horse_id or len(horse_id) > 32:
            return jsonify({"ok": False, "error": "invalid horse_id"}), 400
        try:
            from src.database import get_db
            from config.settings import _CALIB_VC_TO_NAME as _VC_MAP
            conn = get_db()
            rows = conn.execute(
                """SELECT race_date, race_id, venue_code, surface, distance,
                          grade, race_name, field_count, finish_pos, jockey_name,
                          win_odds, finish_time_sec, last_3f_sec,
                          run_dev, race_level_dev, horse_name
                   FROM race_log
                   WHERE horse_id = ? AND finish_pos < 90
                   ORDER BY race_date DESC
                   LIMIT ?""",
                (horse_id, limit),
            ).fetchall()
            if not rows:
                return jsonify({"ok": True, "horse_id": horse_id, "horse_name": "", "runs": []})
            runs = []
            for r in rows:
                vc = str(r["venue_code"] or "").zfill(2)
                venue_name = _VC_MAP.get(vc, r["venue_code"] or "")
                runs.append({
                    "race_date": r["race_date"] or "",
                    "race_id": r["race_id"] or "",
                    "venue": venue_name,
                    "surface": r["surface"] or "",
                    "distance": r["distance"] or 0,
                    "grade": r["grade"] or "",
                    "race_name": r["race_name"] or "",
                    "field_count": r["field_count"] or 0,
                    "finish_pos": r["finish_pos"] or 0,
                    "jockey_name": r["jockey_name"] or "",
                    "win_odds": r["win_odds"],
                    "finish_time_sec": r["finish_time_sec"],
                    "last_3f_sec": r["last_3f_sec"],
                    "run_dev": r["run_dev"],
                    "race_level_dev": r["race_level_dev"],
                })
            # 古い順に並び替えて返す（グラフX軸が左→右で時系列順）
            runs.reverse()
            return jsonify({
                "ok": True,
                "horse_id": horse_id,
                "horse_name": rows[0]["horse_name"] or "",
                "runs": runs,
            })
        except Exception as e:
            logger.exception("api_horse_history failed: %s", e)
            return jsonify({"ok": False, "error": "internal error"}), 500

    @app.route("/api/race_odds", methods=["POST"])
    def api_race_odds():
        """単一レースのオッズを取得して返す"""
        data = request.get_json(force=True, silent=True) or {}
        race_id = data.get("race_id", "")
        date = data.get("date", "")
        venue = data.get("venue", "")
        race_no = data.get("race_no", 0)
        # マスター指示 2026-04-23: auto=true の場合は fire-and-forget モード
        # （フロント側が race 詳細タブを開いた瞬間に裏で叩く用途。クールダウン付き。）
        _auto_mode = bool(data.get("auto"))
        if not race_id:
            return jsonify(ok=False, error="race_id が必要です")
        # auto モード: cooldown チェック → 別スレッドで直接スクレイプ → 即レスポンス
        # マスター指示 2026-04-23 (python-reviewer 指摘):
        # Flask test_client はスレッドローカル依存で別スレッドから呼ぶと危険
        # → 直接スクレイプ関数を呼ぶ fire-and-forget に変更
        if _auto_mode:
            now_ts = time.time()
            _cd_key = f"odds:{race_id}"
            with _auto_fetch_lock:
                last_attempt = _auto_fetch_cooldown.get(_cd_key, 0)
                if now_ts - last_attempt < _AUTO_FETCH_COOLDOWN_SEC:
                    return jsonify(ok=True, auto=True, skipped="cooldown",
                                   remaining=int(_AUTO_FETCH_COOLDOWN_SEC - (now_ts - last_attempt)))
                _auto_fetch_cooldown[_cd_key] = now_ts
            # マスター指示 2026-04-23 (修正版): 単に get_tansho だけでは
            # pred.json のチケットオッズが更新されない（実オッズ反映されない）。
            # 実装済の同期 POST (/api/race_odds) を localhost HTTP で自己呼出しし、
            # 完全な更新パイプラインを走らせる。
            def _bg_worker(payload: dict):
                try:
                    import requests as _req
                    _p = dict(payload)
                    _p.pop("auto", None)   # 無限ループ防止
                    port = os.environ.get("KEIBA_PORT", "5051")
                    resp = _req.post(
                        f"http://127.0.0.1:{port}/api/race_odds",
                        json=_p, timeout=60,
                    )
                    if resp.status_code == 200:
                        logger.info("auto-odds bg fetch 完了: %s (%dB)", _p.get("race_id"), len(resp.content))
                    else:
                        logger.warning("auto-odds bg fetch HTTP %d: %s", resp.status_code, _p.get("race_id"))
                except Exception as _e:
                    logger.warning("auto odds fetch bg worker error: %s", _e)
            threading.Thread(target=_bg_worker, args=(dict(data),), daemon=True).start()
            return jsonify(ok=True, auto=True, started=True)
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
                from src.scraper.netkeiba import NetkeibaClient
                from src.scraper.netkeiba import OddsScraper as _OS
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
                    from src.scraper.netkeiba import NetkeibaClient
                    from src.scraper.netkeiba import OddsScraper as _OS
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
                                            reassign_marks_dict(_horses, is_jra=_is_jra)
                                            # HTMLの印も同期
                                            if date:
                                                _dk = date.replace("-", "")
                                                _vn = venue or race.get("venue", "")
                                                _rn = race_no or race.get("race_no", 0)
                                                if _update_html_marks(_dk, _vn, _rn, _horses):
                                                    logger.info("HTML印を同期: %s %s%sR", _dk, _vn, _rn)
                                            logger.info("オッズ更新後の確率・印を再計算: %s", race_id)
                                        else:
                                            logger.info("オッズ更新（印固定中）: %s", race_id)
                                except Exception as _e:
                                    logger.warning("確率再計算に失敗: %s", _e)

                                # 三連複チケットのオッズを実オッズで更新
                                # マスター指摘 2026-04-23: tickets_by_mode も走査対象に追加
                                if sanrenpuku_odds_map:
                                    _san_collections = [race.get("tickets", [])]
                                    _tbm2 = race.get("tickets_by_mode") or {}
                                    for _mk in ("fixed", "accuracy", "balanced", "recovery"):
                                        _tl = _tbm2.get(_mk)
                                        if isinstance(_tl, list):
                                            _san_collections.append(_tl)
                                    for _ts in _san_collections:
                                        for t in _ts:
                                            if t.get("type") != "三連複":
                                                continue
                                            combo = t.get("combo", [])
                                            if len(combo) == 3:
                                                key = tuple(sorted(int(x) for x in combo))
                                                if key in sanrenpuku_odds_map:
                                                    actual = sanrenpuku_odds_map[key]
                                                    t["odds"] = round(actual, 1)
                                                    t["odds_source"] = "real"
                                                    prob = t.get("prob", 0)
                                                    if prob > 0:
                                                        t["ev"] = round(prob * actual * 100, 1)
                                    logger.info("三連複チケットを実オッズで更新: %s (%d組中)", race_id, len(sanrenpuku_odds_map))

                                # ── マスター指示 2026-04-22 / 2026-04-23:
                                # 馬連・馬単・ワイド・三連単 も公式実オッズで更新
                                # （netkeiba に同等メソッドが無いため公式のみ。
                                #  公式失敗時は WARNING でログ出力し、マスターに気付かせる） ──
                                _umaren_odds = {}
                                _umatan_odds = {}
                                _wide_odds = {}
                                _sanrentan_odds = {}
                                try:
                                    _off2 = _get_official_odds_scraper()
                                    if _off2:
                                        try:
                                            _umaren_odds = _off2.get_umaren_odds(race_id) or {}
                                            if not _umaren_odds:
                                                logger.warning(
                                                    "馬連実オッズ 公式取得 0組: %s（フォールバック対象）",
                                                    race_id,
                                                )
                                        except Exception as _e:
                                            logger.warning("馬連実オッズ公式失敗: %s (%s)", race_id, _e)
                                        try:
                                            _umatan_odds = _off2.get_umatan_odds(race_id) or {}
                                            if not _umatan_odds:
                                                logger.warning(
                                                    "馬単実オッズ 公式取得 0組: %s（フォールバック対象）",
                                                    race_id,
                                                )
                                        except Exception as _e:
                                            logger.warning("馬単実オッズ公式失敗: %s (%s)", race_id, _e)
                                        try:
                                            _wide_odds = _off2.get_wide_odds(race_id) or {} if hasattr(_off2, "get_wide_odds") else {}
                                            if not _wide_odds:
                                                logger.info(
                                                    "ワイド実オッズ 未取得: %s（メソッド未実装の可能性）",
                                                    race_id,
                                                )
                                        except Exception as _e:
                                            logger.warning("ワイド実オッズ公式失敗: %s (%s)", race_id, _e)
                                        try:
                                            _sanrentan_odds = _off2.get_sanrentan_odds(race_id) or {}
                                            if not _sanrentan_odds:
                                                logger.warning(
                                                    "三連単実オッズ 公式取得 0組: %s（フォールバック対象）",
                                                    race_id,
                                                )
                                        except Exception as _e:
                                            logger.warning("三連単実オッズ公式失敗: %s (%s)", race_id, _e)
                                    else:
                                        logger.warning(
                                            "公式スクレイパ取得失敗 → 全券種実オッズ更新不可: %s",
                                            race_id,
                                        )
                                except Exception as _e:
                                    logger.warning("実オッズ一括取得失敗: %s (%s)", race_id, _e)

                                # 実 horse.odds を辞書化（単勝再計算用）
                                _horse_odds_map = {
                                    int(h.get("horse_no", 0)): (h.get("odds") or 0)
                                    for h in race.get("horses", [])
                                    if h.get("horse_no") is not None
                                }

                                # 全チケットを走査してオッズ更新
                                # マスター指摘 2026-04-23: tickets_by_mode.{fixed,accuracy,balanced,recovery} も
                                # 更新対象に含める（旧実装では race.tickets / formation_tickets のみで tickets_by_mode を漏らしていた）
                                _updated_counts = {"単勝": 0, "馬連": 0, "馬単": 0, "ワイド": 0, "三連単": 0}
                                _ticket_collections = [
                                    race.get("tickets", []),
                                    race.get("formation_tickets", []),
                                ]
                                _tbm = race.get("tickets_by_mode") or {}
                                for _mode_key in ("fixed", "accuracy", "balanced", "recovery"):
                                    _mode_tickets = _tbm.get(_mode_key)
                                    if isinstance(_mode_tickets, list):
                                        _ticket_collections.append(_mode_tickets)
                                for _ts in _ticket_collections:
                                    for t in _ts:
                                        _tt = t.get("type", "")
                                        _combo = t.get("combo", [])
                                        _new_odds = None
                                        _src = None
                                        if _tt == "単勝" and len(_combo) == 1:
                                            _o = _horse_odds_map.get(int(_combo[0]))
                                            if _o and _o > 0:
                                                _new_odds = float(_o)
                                                _src = "real"
                                        elif _tt == "馬連" and len(_combo) == 2:
                                            _k = tuple(sorted(int(x) for x in _combo))
                                            if _k in _umaren_odds:
                                                _new_odds = float(_umaren_odds[_k])
                                                _src = "real"
                                        elif _tt == "馬単" and len(_combo) == 2:
                                            _k = (int(_combo[0]), int(_combo[1]))
                                            if _k in _umatan_odds:
                                                _new_odds = float(_umatan_odds[_k])
                                                _src = "real"
                                        elif _tt == "ワイド" and len(_combo) == 2:
                                            _k = tuple(sorted(int(x) for x in _combo))
                                            if _k in _wide_odds:
                                                _new_odds = float(_wide_odds[_k])
                                                _src = "real"
                                        elif _tt == "三連単" and len(_combo) == 3:
                                            _k = tuple(int(x) for x in _combo)  # 順序固定
                                            if _k in _sanrentan_odds:
                                                _new_odds = float(_sanrentan_odds[_k])
                                                _src = "real"
                                        if _new_odds is not None and _new_odds > 0:
                                            t["odds"] = round(_new_odds, 1)
                                            t["odds_source"] = _src
                                            _prob = t.get("prob", 0)
                                            if _prob > 0:
                                                t["ev"] = round(_prob * _new_odds * 100, 1)
                                            _updated_counts[_tt] = _updated_counts.get(_tt, 0) + 1
                                if any(_updated_counts.values()):
                                    logger.info("チケット実オッズ更新: %s %s", race_id, _updated_counts)

                                break
                        # 個別レース更新でも pred レベル odds_updated_at を更新
                        # （UI 右上「最終オッズ HH:MM」表示がボタン押下のたびに更新されるようにする）
                        pred["odds_updated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                        # 原子書き込み + プロセス間ロック（オッズ更新の並行書き込みでも JSON が壊れないように）
                        atomic_write_json(pred_file, pred)
                    except Exception as e:
                        logger.warning("pred.json update failed: %s", e)

            # ---- 発走後なら単一レース結果も取得（着順・払戻） ----
            # 判定: 現在時刻 >= post_time + 10分（発走直後はまだ結果出ない）
            result_fetched = False
            result_entry = None
            try:
                _post_time = ""
                _is_jra_for_result = True
                if date:
                    _date_key_tmp = date.replace("-", "")
                    _pf = os.path.join(PROJECT_ROOT, "data", "predictions", f"{_date_key_tmp}_pred.json")
                    if os.path.isfile(_pf):
                        with open(_pf, "r", encoding="utf-8") as _rf:
                            _pj = json.load(_rf)
                        for _r in _pj.get("races", []):
                            if _r.get("race_id") == race_id:
                                _post_time = _r.get("post_time", "") or ""
                                _is_jra_for_result = _r.get("is_jra", True)
                                break
                _is_post = False
                if _post_time and date:
                    try:
                        _dt_str = f"{date} {_post_time}"  # "YYYY-MM-DD HH:MM"
                        _post_dt = datetime.strptime(_dt_str, "%Y-%m-%d %H:%M")
                        # 発走+10分後以降なら結果取得を試行
                        if datetime.now() >= _post_dt + timedelta(minutes=10):
                            _is_post = True
                    except Exception:
                        pass

                if _is_post:
                    from src.results_tracker import fetch_single_race_result
                    from src.scraper.netkeiba import NetkeibaClient as _NC
                    _rc_client = _get_auth_client() or _NC(no_cache=True)
                    # 公式スクレイパ準備
                    _off_for_result = _get_official_odds_scraper()
                    result_entry = fetch_single_race_result(
                        date, race_id, _rc_client,
                        official_scraper=_off_for_result,
                    )
                    if result_entry and result_entry.get("order"):
                        result_fetched = True
                        logger.info("データ更新: 発走後レース結果を取得 %s (%d着順)",
                                    race_id, len(result_entry["order"]))
            except Exception as _e:
                logger.warning("発走後レース結果取得でエラー: %s", _e, exc_info=True)

            # レース一覧キャッシュをクリア（印・確率変更を即反映）
            if date:
                _predictions_cache.pop(date, None)

            return jsonify(
                ok=True,
                odds=odds_map,
                weights=weight_map,
                result_fetched=result_fetched,
                result_order_count=len(result_entry.get("order", [])) if result_entry else 0,
            )
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
        _analyzer_state["postprocessing"] = False

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
                _analyzer_state["progress"] = "初期化中..."
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
                        # [3/3] 統合HTML出力中 → 後処理フェーズを通知
                        if "統合HTML" in s or "出力中" in s:
                            _analyzer_state["postprocessing"] = True
                        continue
                    # [完了] 行 → 後処理終了
                    if s.startswith("[完了]"):
                        _analyzer_state["progress"] = s
                        _analyzer_state["postprocessing"] = False
                        continue
                    # 統合HTML進捗行: "       統合HTML: 5/34"
                    if "統合HTML:" in s:
                        _analyzer_state["progress"] = s.strip()
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
    def _run_odds_update(date_key, source="manual", include_top10_odds: bool = False):
        """オッズ一括更新（コアロジックは scheduler_tasks.py に委譲）"""
        global _odds_state, _odds_cancel
        import time as _tm

        from src.scheduler_tasks import run_odds_update as _core_odds_update

        _odds_state = {"running": True, "done": False, "error": None, "updated_at": None,
                       "count": 0, "total": 0, "current": 0, "current_race": "",
                       "started_at": _tm.time(), "source": source,
                       "include_top10_odds": bool(include_top10_odds)}
        _odds_cancel = False

        cancel_event = threading.Event()

        def _progress(current, total, label):
            if _odds_cancel:
                cancel_event.set()
            _odds_state["current"] = current
            _odds_state["total"] = total
            _odds_state["current_race"] = label
            _odds_state["count"] = current

        try:
            count = _core_odds_update(date_key, cancel_event=cancel_event,
                                      progress_callback=_progress,
                                      include_top10_odds=include_top10_odds)
            from datetime import datetime as _dt
            _odds_state.update(running=False, done=True, count=count or 0,
                               current_race="",
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
            "start_time": time.time(), "postprocessing": False,
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

            # 公式スクレイパーを優先的に使用（netkeiba不安定対策）
            _official_scraper = None
            try:
                from src.scraper.official_odds import OfficialOddsScraper
                _official_scraper = OfficialOddsScraper()
            except Exception:
                logger.debug("公式スクレイパー初期化失敗、netkeibaフォールバック")

            _results_state["progress"] = f"[自動] {date_str} の着順取得中..."
            fetch_actual_results(date_str, client, official_scraper=_official_scraper)

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
            "start_time": time.time(), "step": 0, "total_steps": 4,
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
            _db_update_state["progress"] = "[自動] [3/4] 騎手・調教師キャッシュ再構築中..."
            _db_update_state["log"].append("[自動] キャッシュ再構築開始")
            _personnel_stats_cache.clear()
            _db_update_state["log"].append("✓ キャッシュ再構築完了")

            _db_update_state["step"] = 4
            _db_update_state["progress"] = "[自動] [4/4] MLモデル更新中..."
            _db_update_state["log"].append("[自動] MLモデル更新開始")
            try:
                # First1Cモデルのキャッシュクリア（次回analyze時に再ロード）
                import src.engine as _eng
                _eng._CACHE_1C_PREDICTOR = None
                _eng._CACHE_1C_LOADED = False
                _db_update_state["log"].append("✓ MLモデルキャッシュクリア完了")
            except Exception as e:
                logger.warning("[results-scheduler] MLモデル更新失敗: %s", e)
                _db_update_state["log"].append(f"⚠ MLモデル更新失敗: {e}")

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
        include_top10 = bool(data.get("include_top10_odds", False))
        if not _is_admin(request):
            date_str = datetime.now().strftime("%Y-%m-%d")
        date_key = date_str.replace("-", "")

        pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
        if not os.path.isfile(pred_file):
            return jsonify(ok=False, error=f"予想データが見つかりません: {date_key}")

        import threading
        threading.Thread(
            target=_run_odds_update,
            kwargs={"date_key": date_key, "source": "manual",
                    "include_top10_odds": include_top10},
            daemon=True,
        ).start()
        return jsonify(ok=True, include_top10_odds=include_top10)

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

    def _auto_fetch_single_race(date: str, race_id: str) -> None:
        """単一レースの結果を発走+10分経過後に裏で取得するヘルパー。

        マスター指示 2026-04-23: レース結果タブ / オッズタブを開いた時、
        既に発走+10分経過しているのに results.json に未登録なら自動取得。
        fire-and-forget、クールダウン付き（_auto_fetch_lock で TOCTOU 保護）。
        """
        # クールダウン（TOCTOU 保護のため Lock 内で check-then-set）
        now_ts = time.time()
        with _auto_fetch_lock:
            last_attempt = _auto_fetch_cooldown.get(race_id, 0)
            if now_ts - last_attempt < _AUTO_FETCH_COOLDOWN_SEC:
                return
            _auto_fetch_cooldown[race_id] = now_ts

        # pred.json から post_time を取得
        try:
            from src.results_tracker import load_prediction, fetch_single_race_result
            pred = load_prediction(date)
            if not pred:
                return
            target_race = None
            for r in pred.get("races", []):
                if str(r.get("race_id", "")) == race_id:
                    target_race = r
                    break
            if not target_race:
                return
            post_time = target_race.get("post_time", "") or ""
            if not post_time:
                return
            try:
                post_dt = datetime.strptime(f"{date} {post_time}", "%Y-%m-%d %H:%M")
            except Exception:
                return
            if datetime.now() < post_dt + timedelta(minutes=10):
                return  # 発走+10分まだ
            # 既に取得済みチェック
            date_key = date.replace("-", "")
            rfp = os.path.join(PROJECT_ROOT, "data", "results", f"{date_key}_results.json")
            if os.path.isfile(rfp):
                try:
                    with open(rfp, "r", encoding="utf-8") as f:
                        _rdata = json.load(f)
                    _entry = _rdata.get(race_id) or {}
                    if _entry.get("order"):
                        return  # 取得済み
                except Exception as _e:
                    logger.debug("auto-fetch 既取得判定で読込失敗 %s: %s", rfp, _e)
            # 取得実行
            from src.scraper.netkeiba import NetkeibaClient
            client = NetkeibaClient(no_cache=True)
            official = _get_official_odds_scraper()
            logger.info("auto-fetch(single): %s %s 開始", date, race_id)
            entry = fetch_single_race_result(date, race_id, client, official_scraper=official)
            if entry and entry.get("order"):
                logger.info("auto-fetch(single): %s %s 取得成功", date, race_id)
                # 集計キャッシュ無効化（全体集計が古くならないように）
                try:
                    from src.results_tracker import invalidate_aggregate_cache
                    invalidate_aggregate_cache()
                except Exception:
                    pass
        except Exception as e:
            logger.warning("auto-fetch(single) 失敗 %s/%s: %s", date, race_id, e)

    @app.route("/api/results/race")
    def api_results_race():
        """個別レースの結果（着順・払戻）を返す"""
        date_str = request.args.get("date", "")
        race_id = request.args.get("race_id", "")
        if not date_str or not race_id:
            return jsonify(ok=False, error="date and race_id required")
        # 入力バリデーション（パストラバーサル防御）
        _date_norm = date_str.replace("-", "")
        if not re.fullmatch(r"\d{8}", _date_norm):
            return jsonify(ok=False, error="invalid date format")
        if not re.fullmatch(r"[A-Za-z0-9_]{6,20}", race_id):
            return jsonify(ok=False, error="invalid race_id format")

        # マスター指示 2026-04-23: 発走+10分経過かつ未取得なら裏で自動 fetch
        # （レース結果タブを開いた瞬間に取得、次回 polling で反映）
        _iso_date = f"{_date_norm[:4]}-{_date_norm[4:6]}-{_date_norm[6:]}"
        if _iso_date == datetime.now().strftime("%Y-%m-%d"):
            threading.Thread(
                target=_auto_fetch_single_race,
                args=(_iso_date, race_id),
                daemon=True,
            ).start()

        try:
            import pathlib
            results_path = pathlib.Path("data/results") / f"{_date_norm}_results.json"
            if not results_path.exists():
                return jsonify(ok=True, found=False)
            import json as _json
            with open(results_path, encoding="utf-8") as f:
                all_results = _json.load(f)
            race_result = all_results.get(race_id)
            if not race_result:
                return jsonify(ok=True, found=False)
            # 予想データから馬名・印等を補完
            pred_path = pathlib.Path("data/predictions") / f"{_date_norm}_pred.json"
            horse_map = {}
            if pred_path.exists():
                with open(pred_path, encoding="utf-8") as f:
                    pred_data = _json.load(f)
                for r in pred_data.get("races", []):
                    if r.get("race_id") == race_id:
                        for h in r.get("horses", []):
                            horse_map[h.get("horse_no")] = {
                                "horse_name": h.get("horse_name", ""),
                                "jockey": h.get("jockey", ""),
                                "mark": h.get("mark", ""),
                                "predicted_rank": h.get("predicted_rank"),
                                "win_prob": h.get("win_prob"),
                                "gate_no": h.get("gate_no"),
                                # 総合指数（UI表示用、印の後に数値）
                                "composite": h.get("composite"),
                                # 人気がpred側にあれば初期値として使う（race_log優先）
                                "popularity": h.get("popularity"),
                            }
                        break
            # race_log から通過順・走破タイム・後3F・着差・人気・オッズを補完（results.jsonに不足分）
            racelog_map = {}
            try:
                import sqlite3 as _sql
                from config.settings import DATABASE_PATH as _DBP
                with _sql.connect(_DBP) as _c:
                    _c.row_factory = _sql.Row
                    for _r in _c.execute(
                        "SELECT horse_no, positions_corners, finish_time_sec, last_3f_sec, "
                        "margin_ahead, margin_behind, position_4c, popularity, win_odds "
                        "FROM race_log WHERE race_id=?",
                        (race_id,)
                    ).fetchall():
                        racelog_map[_r["horse_no"]] = dict(_r)
            except Exception:
                logger.debug("race_log 補完失敗 race_id=%s", race_id, exc_info=True)

            # 結果取得スクレイパーのバグ対策（2026-04-22 JRA/NAR 両方で判明）:
            # JRA _parse_jra_result_order / NAR result parser の右→左スキャンが
            # 「単勝オッズ」列を飛ばして「人気」列を odds に代入してしまう場合がある。
            # → 8割以上 odds==popularity なら全 odds を None 化して信頼しない。
            # → 払戻金（単勝）から着順1位の実オッズを逆算して補完する。
            _bug_detected = False
            if racelog_map:
                _same_count = sum(
                    1 for _rl in racelog_map.values()
                    if _rl.get("win_odds") is not None
                    and _rl.get("popularity") is not None
                    and float(_rl["win_odds"]) == float(_rl["popularity"])
                )
                if _same_count >= len(racelog_map) * 0.8:
                    _bug_detected = True
                    for _rl in racelog_map.values():
                        _rl["win_odds"] = None

            # 払戻金（単勝）から 1位馬の実オッズを逆算
            _winner_odds_from_payout = None
            if _bug_detected:
                try:
                    _payouts = race_result.get("payouts", {}) or {}
                    _tansho = _payouts.get("単勝")
                    if isinstance(_tansho, dict):
                        _tansho_list = [_tansho]
                    elif isinstance(_tansho, list):
                        _tansho_list = _tansho
                    else:
                        _tansho_list = []
                    if _tansho_list:
                        _p = _tansho_list[0]
                        _win_combo = str(_p.get("combo", ""))
                        _win_payout = int(_p.get("payout", 0) or 0)
                        # 100円 → payout/100 倍
                        if _win_payout > 0 and _win_combo.isdigit():
                            _winner_odds_from_payout = (
                                int(_win_combo),
                                round(_win_payout / 100.0, 1),
                            )
                except Exception:
                    logger.debug("単勝払戻 → オッズ逆算失敗 race_id=%s", race_id, exc_info=True)

            # 着順データに馬名・通過順等をマージ
            order = []
            for o in race_result.get("order", []):
                entry = dict(o)
                hno = entry.get("horse_no")
                if hno in horse_map:
                    entry.update(horse_map[hno])
                # race_log から不足分を補完（既存値があれば優先 / corners は短い場合は上書き）
                rl = racelog_map.get(hno)
                if rl:
                    # corners: results.json に 1要素しかない場合がある → race_log の完全版で上書き
                    _existing_corners = entry.get("corners") or []
                    if len(_existing_corners) <= 1:
                        try:
                            import json as _j
                            _pc = rl.get("positions_corners") or ""
                            if _pc and _pc.startswith("["):
                                _parsed = _j.loads(_pc)
                                if isinstance(_parsed, list) and len(_parsed) > len(_existing_corners):
                                    entry["corners"] = _parsed
                            elif rl.get("position_4c") and not _existing_corners:
                                entry["corners"] = [rl["position_4c"]]
                        except Exception:
                            pass
                    if entry.get("last_3f") is None and rl.get("last_3f_sec"):
                        entry["last_3f"] = rl["last_3f_sec"]
                    if not entry.get("time") and rl.get("finish_time_sec"):
                        _sec = rl["finish_time_sec"]
                        m = int(_sec // 60)
                        s = _sec - m * 60
                        entry["time"] = f"{m}:{s:04.1f}" if m > 0 else f"{s:.1f}"
                    # 人気・単勝オッズ・着差 (race_log に持っていれば使う)
                    if entry.get("popularity") in (None, 0) and rl.get("popularity"):
                        entry["popularity"] = rl["popularity"]
                    # オッズバグ（popularity 値が odds に入っていた）→ 全て None に
                    # ただし 1位馬は払戻金から逆算した実オッズを入れる
                    if _bug_detected:
                        if (_winner_odds_from_payout is not None
                                and entry.get("horse_no") == _winner_odds_from_payout[0]):
                            entry["odds"] = _winner_odds_from_payout[1]
                        else:
                            entry["odds"] = None
                    elif entry.get("odds") in (None, 0) and rl.get("win_odds") is not None:
                        entry["odds"] = rl["win_odds"]
                    if not entry.get("margin") and rl.get("margin_ahead") is not None:
                        _ma = rl["margin_ahead"]
                        # 1着は 0.0 → "—" 表示、それ以外は "+X.X" 形式
                        if _ma == 0:
                            entry["margin"] = ""
                        else:
                            entry["margin"] = f"+{_ma:.1f}" if _ma > 0 else f"{_ma:.1f}"
                # results.json に time_sec / win_odds キーが直接ある場合のフォールバック
                # （スクレイパー新形式: race_log 未格納の当日レースでも表示できるようにする）
                if not entry.get("time") and entry.get("time_sec") is not None:
                    _sec = entry["time_sec"]
                    m = int(_sec // 60)
                    s = _sec - m * 60
                    entry["time"] = f"{m}:{s:04.1f}" if m > 0 else f"{s:.1f}"
                if entry.get("odds") in (None, 0) and entry.get("win_odds") is not None:
                    entry["odds"] = entry["win_odds"]
                # 総合指数（composite）を horse_map 経由で補完 / horse_map 側を拡張
                order.append(entry)
            return jsonify(
                ok=True, found=True, order=order,
                payouts=race_result.get("payouts", {}),
                # 応急パッチで odds を埋めた場合、データが未完全であることを通知
                data_incomplete=bool(_bug_detected),
            )
        except Exception as e:
            logger.warning("race result fetch failed: %s", e, exc_info=True)
            return jsonify(ok=False, error=str(e))

    @app.route("/api/results/dates")
    def api_results_dates():
        """予想済み日付一覧 + 日次統計を返す"""
        try:
            from src.results_tracker import aggregate_all, list_prediction_dates

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
            from config.settings import RESULTS_DIR
            from src.results_tracker import list_prediction_dates

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

    @app.route("/api/results/invalidate_cache", methods=["POST"])
    def api_results_invalidate_cache():
        """集計キャッシュを全クリアする（pred.json修正後に呼び出し）

        v6.1.18 security fix: 管理者のみ許可。cloudflared で外部公開されており、
        第三者が無差別に POST すると 234 秒級の fallback が連発して DoS と同等になる。
        """
        # reviewer HIGH #3 対応: 認証ガードを追加
        if not _is_admin(request):
            return jsonify(error="管理者のみ実行可能"), 403
        try:
            from src.results_tracker import invalidate_aggregate_cache
            invalidate_aggregate_cache()
            # 事前生成サマリキャッシュ（data/cache/results/）もクリア
            cache_dir = os.path.join(PROJECT_ROOT, "data", "cache", "results")
            removed = 0
            if os.path.isdir(cache_dir):
                for f in os.listdir(cache_dir):
                    if f.endswith(".json"):
                        try:
                            os.remove(os.path.join(cache_dir, f))
                            removed += 1
                        except OSError:
                            pass
            return jsonify(status="ok",
                           message=f"集計キャッシュクリア完了 (results_cache削除={removed}件)")
        except Exception as e:
            return jsonify(error=str(e)), 500

    # ============================================================
    # 成績ページ用 事前計算キャッシュ (scripts/build_results_cache.py)
    #   - data/cache/results/{kind}_{year}.json を読むだけなら <50ms
    #   - 古い / 未存在の場合は裏で lazy 再生成（_results_cache_lock で二重実行防止）
    # ============================================================
    _RESULTS_CACHE_DIR = os.path.join(PROJECT_ROOT, "data", "cache", "results")
    _RESULTS_CACHE_MAX_AGE_SEC = 6 * 3600  # 6時間以上古ければ lazy 再生成
    _results_cache_lock = threading.Lock()
    _results_cache_building: dict = {}  # year → True: 生成中フラグ
    _results_cache_stats = {"hits": 0, "misses": 0, "stale": 0, "lazy_builds": 0}

    # v6.1.19 reviewer MEDIUM 対応: stats カウンタの原子性保証
    def _inc_cache_stat(key: str) -> None:
        """results cache 統計のロック付きインクリメント。

        CPython GIL 下では `dict[key] += 1` はほぼ原子的だが、
        read-modify-write を明示的に保護して TOCTOU/ロストアップデートを防ぐ。
        """
        with _results_cache_lock:
            _results_cache_stats[key] = _results_cache_stats.get(key, 0) + 1

    # reviewer HIGH #2 対応: year パラメータは許可リスト検証
    # Windows \ 経由の path traversal を防ぐ。"all" または 4桁数字のみ許可。
    _VALID_YEAR_RE = re.compile(r"^(all|\d{4})$")

    def _validate_year(year: str | None) -> str:
        """year パラメータを許可リストで検証。不正なら 'all' にフォールバック。"""
        if year and _VALID_YEAR_RE.fullmatch(year):
            return year
        if year:
            logger.warning("不正な year パラメータ: %r → 'all' にフォールバック", year)
        return "all"

    def _results_cache_path(kind: str, year: str) -> str:
        # year は既に _validate_year 済みで、slash / backslash は含まれない保証あり
        return os.path.join(_RESULTS_CACHE_DIR, f"{kind}_{year}.json")

    def _results_cache_load(kind: str, year: str) -> tuple[dict | None, bool]:
        """キャッシュ JSON を読み込む。(data, is_stale) を返す。

        - ファイルなし → (None, True)
        - 古い（_RESULTS_CACHE_MAX_AGE_SEC 超）→ (data, True)
        - 正常 → (data, False)
        """
        p = _results_cache_path(kind, year)
        if not os.path.exists(p):
            return None, True
        try:
            mt = os.path.getmtime(p)
            is_stale = (time.time() - mt) > _RESULTS_CACHE_MAX_AGE_SEC
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data, is_stale
        except Exception as e:
            logger.debug("results cache 読み込み失敗 %s/%s: %s", kind, year, e)
            return None, True

    def _results_cache_build_bg(year: str) -> None:
        """バックグラウンドで特定 year のキャッシュを再生成する。

        - 多重実行は _results_cache_building で抑止
        - 失敗しても握りつぶし（フォールバックは呼び出し元が持つ）
        """
        with _results_cache_lock:
            if _results_cache_building.get(year):
                return
            _results_cache_building[year] = True

        def _run():
            try:
                _inc_cache_stat("lazy_builds")
                from scripts.build_results_cache import build_year_cache
                logger.info("results cache lazy build start: year=%s", year)
                t0 = time.time()
                r = build_year_cache(year, force=True)
                logger.info("results cache lazy build done: year=%s ok=%s elapsed=%.1fs",
                            year, r.get("ok"), time.time() - t0)
            except Exception as e:
                logger.warning("results cache lazy build failed year=%s: %s", year, e, exc_info=True)
            finally:
                with _results_cache_lock:
                    _results_cache_building.pop(year, None)

        threading.Thread(target=_run, daemon=True).start()

    def _serve_results_cache(kind: str, year: str, fallback):
        """キャッシュを返すヘルパー。

        - hit かつ新鮮 → キャッシュを返す
        - hit かつ stale → キャッシュを返しつつ裏で lazy 再生成
        - miss → fallback() を呼んで計算結果を返す + 裏で lazy 生成
        """
        data, stale = _results_cache_load(kind, year)
        if data is not None:
            if stale:
                _inc_cache_stat("stale")
                _results_cache_build_bg(year)
            else:
                _inc_cache_stat("hits")
            return jsonify(data)
        # miss → fallback（重い）
        _inc_cache_stat("misses")
        _results_cache_build_bg(year)
        return fallback()

    # 公開（/api/health から参照）
    app.config["_RESULTS_CACHE_STATS"] = _results_cache_stats

    @app.route("/api/results/summary")
    def api_results_summary():
        """通算成績を返す（year=all/2025/2026 等）。by_dateは巨大なため除外"""
        year = _validate_year(request.args.get("year", "all"))

        def _fallback():
            try:
                from src.results_tracker import aggregate_all

                result = aggregate_all(year_filter=year)
                # by_dateは2MB超→summaryには不要。trendで必要な列だけ別途返す
                return jsonify({k: v for k, v in result.items() if k != "by_date"})
            except Exception as e:
                logger.warning("results summary failed: %s", e, exc_info=True)
                return jsonify(error=str(e))

        return _serve_results_cache("summary", year, _fallback)

    @app.route("/api/results/sanrentan_summary")
    def api_results_sanrentan_summary():
        """三連単フォーメーション戦略（Phase 3）の成績を返す。
        year=all/2024/2025/2026 の単位でキャッシュ（30分）。
        """
        year = _validate_year(request.args.get("year", "all"))
        force = request.args.get("force", "") in ("1", "true")

        def _fallback():
            try:
                from src.analytics.sanrentan_summary import get_sanrentan_summary

                result = get_sanrentan_summary(year_filter=year, force=force)
                return jsonify(result)
            except Exception as e:
                logger.warning("results sanrentan_summary failed: %s", e, exc_info=True)
                return jsonify(error=str(e))

        # force 指定時はキャッシュを無視してフルパスで再計算
        if force:
            return _fallback()
        return _serve_results_cache("sanrentan_summary", year, _fallback)

    @app.route("/api/results/detailed")
    def api_results_detailed():
        """詳細集計（競馬場別・コース別・距離区分別・高額配当TOP10）"""
        year = _validate_year(request.args.get("year", "all"))

        def _fallback():
            try:
                from src.results_tracker import aggregate_detailed

                result = aggregate_detailed(year_filter=year)
                # by_venueの各会場からフロントで未使用の巨大フィールドを除去（1.4MB→200KB）
                _trim_keys = {"by_surface", "by_dist_zone"}
                for cat_key in ("all", "jra", "nar"):
                    cat_data = result.get(cat_key)
                    if not isinstance(cat_data, dict):
                        continue
                    by_venue = cat_data.get("by_venue")
                    if isinstance(by_venue, dict):
                        for venue_data in by_venue.values():
                            if isinstance(venue_data, dict):
                                for tk in _trim_keys:
                                    venue_data.pop(tk, None)
                return jsonify(result)
            except Exception as e:
                logger.warning("results detailed failed: %s", e, exc_info=True)
                return jsonify(error=str(e))

        return _serve_results_cache("detailed", year, _fallback)

    @app.route("/api/results/trend")
    def api_results_trend():
        """累積回収率推移・月別収支データ（Chart.js用）"""
        year = _validate_year(request.args.get("year", "all"))

        def _fallback():
            try:
                from src.results_tracker import aggregate_all

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

        return _serve_results_cache("trend", year, _fallback)

    @app.route("/api/generate_simple_html", methods=["POST"])
    def api_generate_simple_html():
        """指定日の配布用HTML（印・買い目のみ）を生成"""
        data = request.get_json() or {}
        date = data.get("date", "")
        if not date:
            return jsonify(ok=False, error="日付が指定されていません")
        try:
            from config.settings import OUTPUT_DIR
            from src.results_tracker import generate_simple_html

            fpath = generate_simple_html(date, OUTPUT_DIR)
            if fpath is None:
                return jsonify(ok=False, error=f"{date} の予想データがありません")
            return jsonify(ok=True, filename=os.path.basename(fpath), path=fpath)
        except Exception as e:
            logger.warning("generate_simple_html failed: %s", e, exc_info=True)
            return jsonify(ok=False, error=str(e))

    @app.route("/api/results/fetch", methods=["POST"])
    def api_results_fetch():
        """指定日の着順を取得して照合（公式優先→netkeiba フォールバック）"""
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
            # 公式スクレイパーを優先使用
            _off = None
            try:
                from src.scraper.official_odds import OfficialOddsScraper
                _off = OfficialOddsScraper()
            except Exception:
                pass
            fetch_actual_results(date, client, official_scraper=_off)
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
                # 公式スクレイパーを優先使用
                _off = None
                try:
                    from src.scraper.official_odds import OfficialOddsScraper
                    _off = OfficialOddsScraper()
                except Exception:
                    pass
                ok = 0
                for i, dt in enumerate(dates):
                    if _results_state.get("cancel"):
                        _results_state["error"] = f"中断しました（{ok}/{len(dates)}日完了）"
                        break
                    _results_state["current_date"] = dt
                    _results_state["progress"] = f"[{i+1}/{len(dates)}] {dt} の結果を取得中…"
                    fetch_actual_results(dt, client, official_scraper=_off)
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
            """DB更新（コアロジックは scheduler_tasks.py に委譲）"""
            global _db_update_state
            try:
                from src.scheduler_tasks import run_db_update as _core_db_update
                sd = start_date or date
                ed = end_date or date

                def _progress(step, total, msg):
                    if _db_update_state.get("cancel"):
                        raise InterruptedError("中断しました")
                    _db_update_state["step"] = step
                    _db_update_state["progress"] = f"[{step}/{total}] {msg}"

                logs = _core_db_update(sd, progress_callback=_progress)
                _db_update_state["log"].extend(logs)

                # 騎手・調教師キャッシュもクリア
                try:
                    _personnel_stats_cache.clear()
                except Exception:
                    pass

                _db_update_state["progress"] = "完了"
            except InterruptedError:
                _db_update_state["error"] = "中断しました"
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
                _pred = None
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
                # pred.jsonからresult_cname/shutuba_cnameも取得（既に読み込み済みの_predを再利用）
                _result_cname = None
                _shutuba_cname = None
                if _pred:
                    for _r2 in _pred.get("races", []):
                        if _r2.get("venue") == _vn and _r2.get("race_no") == _rn:
                            _result_cname = _r2.get("result_cname")
                            _shutuba_cname = _r2.get("shutuba_cname")
                            break
                if _race_id:
                    _btn = "display:inline-block;font-size:12px;font-weight:600;color:#fff;text-decoration:none;border-radius:4px;padding:3px 12px;margin-right:6px"
                    # JRA公式 vs NAR公式 vs netkeibaフォールバック
                    if _is_jra:
                        # JRA: 公式サイトリンク
                        _result_url = f"https://www.jra.go.jp/JRADB/accessS.html?CNAME={_result_cname}" if _result_cname else "https://www.jra.go.jp/keiba/thisweek/seiseki/"
                        _shutuba_url = f"https://www.jra.go.jp/JRADB/accessD.html?CNAME={_shutuba_cname}" if _shutuba_cname else "https://www.jra.go.jp/keiba/thisweek/syutsuba/"
                        _odds_url = "https://www.jra.go.jp/keiba/thisweek/odds/"
                        _movie_url = "https://www.jra.go.jp/keiba/thisweek/movie/"
                    else:
                        # NAR: 公式サイトリンク
                        _nar_base = "https://nar.netkeiba.com"
                        _result_url = f"{_nar_base}/race/result.html?race_id={_race_id}"
                        _shutuba_url = f"{_nar_base}/race/shutuba.html?race_id={_race_id}"
                        _odds_url = f"{_nar_base}/odds/index.html?race_id={_race_id}"
                        _movie_url = f"{_nar_base}/race/movie.html?race_id={_race_id}"
                    _rlink = (
                        f'  <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">'
                        f'<a href="{_odds_url}" target="_blank" rel="noopener" style="{_btn};background:#16a34a">オッズ</a>'
                        f'<a href="{_shutuba_url}" target="_blank" rel="noopener" style="{_btn};background:#2563eb">出馬表</a>'
                        f'<a href="{_result_url}" target="_blank" rel="noopener" style="{_btn};background:#1a6fa8">レース結果</a>'
                        f'<a href="{_movie_url}" target="_blank" rel="noopener" style="{_btn};background:#c0392b">レース映像</a>'
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

        # 勝率/連対率/複勝率の色はcomposite順位連動でサーバーサイド適用済み
        # （JS上書き不要）

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
            import sqlite3 as _sql3

            from src.database import DATABASE_PATH as _db_path
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
            # ペース区分: 新3段階 (H/M/S)
            if ratio >= 1.06:
                pace_type = "H（ハイ）"
            elif ratio >= 0.94:
                pace_type = "M（ミドル）"
            else:
                pace_type = "S（スロー）"
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
            if r <= 0.35:
                return "先行"
            elif r <= 0.70:
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
        for style in ["逃げ","先行","差し","追込"]:
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
                # ペース区分: 新3段階 (H/M/S)
                if ratio_ >= 1.06:
                    class_avg[cls]["pace_type"] = "H"
                elif ratio_ >= 0.94:
                    class_avg[cls]["pace_type"] = "M"
                else:
                    class_avg[cls]["pace_type"] = "S"
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
    # 位置取りキャッシュ（騎手/調教師の前行き率・4角率・軌跡データ）
    _position_cache: dict = {}  # {"jockey": {...}, "trainer": {...}}

    # v6.1.18: 起動時に personnel キャッシュをバックグラウンドでウォームアップ
    # 初回アクセス時の 22 秒待機を回避（2 回目以降は 5ms）
    def _warmup_personnel_cache():
        """dashboard 起動後、all + 各年の personnel stats を裏でロードする"""
        try:
            from src.database import compute_personnel_stats_from_race_log
            import time as _tw
            years_to_warm = [None, "2026", "2025"]  # 重要な年度のみ先読み
            for yf in years_to_warm:
                try:
                    t0 = _tw.time()
                    stats = compute_personnel_stats_from_race_log(year_filter=yf)
                    cache_key = f"_year_{yf}" if yf else ""
                    if cache_key:
                        _personnel_stats_cache[cache_key] = stats
                    else:
                        _personnel_stats_cache.update(stats)
                    yl = yf or "all"
                    logger.info(
                        "personnel キャッシュ ウォームアップ完了 year=%s (%.1fs)",
                        yl, _tw.time() - t0,
                    )
                except Exception as e:
                    logger.warning("personnel ウォームアップ失敗 year=%s: %s", yf, e)
        except Exception as e:
            logger.warning("personnel ウォームアップ全体失敗: %s", e)

    # 起動 5 秒後にバックグラウンドでウォームアップ（ダッシュボード応答を優先）
    def _delayed_warmup():
        import time as _tw
        _tw.sleep(5)
        _warmup_personnel_cache()

    threading.Thread(target=_delayed_warmup, daemon=True).start()

    def _load_position_cache() -> dict:
        """ML位置取りキャッシュ（騎手/調教師）を読み込み"""
        if _position_cache:
            return _position_cache
        import json as _json_pc
        _ml_dir = os.path.join(os.path.dirname(__file__), "..", "data", "ml")
        for kind, fname in [("jockey", "position_jockey_cache.json"), ("trainer", "position_trainer_cache.json")]:
            fpath = os.path.join(_ml_dir, fname)
            if os.path.exists(fpath):
                with open(fpath, "r", encoding="utf-8") as _f:
                    _position_cache[kind] = _json_pc.load(_f)
        return _position_cache

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
            # v6.1.19 bugfix: year="" のとき cache_key="" だと
            # `"" not in _personnel_stats_cache` が常に True となり
            # 毎回 compute が走って 350ms かかっていた。
            # person_type（"jockey"等）がトップレベル key で既に存在するかを
            # 判定基準にすることで、warmup 済みキャッシュを正しく再利用する。
            cache_key = f"_year_{year_filter}" if year_filter else ""
            if year_filter:
                need_compute = cache_key not in _personnel_stats_cache or not _personnel_stats_cache.get(cache_key)
            else:
                # year="" (全期間) → トップレベルに person_type 系キーがあれば warm
                need_compute = person_type not in _personnel_stats_cache
            if need_compute:
                stats = compute_personnel_stats_from_race_log(year_filter=year_filter or None)
                if cache_key:
                    _personnel_stats_cache[cache_key] = stats
                else:
                    _personnel_stats_cache.update(stats)
            if cache_key:
                all_stats = _personnel_stats_cache.get(cache_key, {}).get(person_type, {})
            else:
                all_stats = _personnel_stats_cache.get(person_type, {})
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
            # 位置取りデータを付加（詳細モード）
            if person_type in ("jockey", "trainer"):
                _pc = _load_position_cache().get(person_type, {})
                if _pc:
                    st["position_stats"] = {
                        "nige_rate": _pc.get("nige_rate", {}).get(pid_detail),
                        "mae_iki_rate": _pc.get("mae_iki_rate", {}).get(pid_detail),
                        "ds_mae_iki_rate": _pc.get("ds_mae_iki_rate", {}).get(pid_detail),
                        "pos_4c_nige_rate": _pc.get("4c_nige_rate", {}).get(pid_detail),
                        "pos_4c_mae_iki_rate": _pc.get("4c_mae_iki_rate", {}).get(pid_detail),
                        "pos_4c_ds_mae_iki_rate": _pc.get("4c_ds_mae_iki_rate", {}).get(pid_detail),
                        "pos_delta": _pc.get("pos_delta", {}).get(pid_detail),
                        "hold_rate": _pc.get("hold_rate", {}).get(pid_detail),
                        "ds_pos_delta": _pc.get("ds_pos_delta", {}).get(pid_detail),
                    }
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
            # 脚質率（逃げ率/前行き率/マクリ率）— by_running_style から算出
            _brs = st.get("by_running_style", {})
            _rs_total = sum(_brs.get(s, {}).get("total", 0) for s in ("逃げ", "先行", "差し", "追込"))
            if _rs_total > 0:
                _nige = _brs.get("逃げ", {}).get("total", 0)
                _senkou = _brs.get("先行", {}).get("total", 0)
                _sashi = _brs.get("差し", {}).get("total", 0)
                _oikomi = _brs.get("追込", {}).get("total", 0)
                entry["nige_rate"] = round(_nige / _rs_total * 100, 1)
                entry["maeiki_rate"] = round((_nige + _senkou) / _rs_total * 100, 1)
                entry["makuri_rate"] = round((_sashi + _oikomi) / _rs_total * 100, 1)
            else:
                entry["nige_rate"] = 0
                entry["maeiki_rate"] = 0
                entry["makuri_rate"] = 0
            persons.append(entry)

        # ソート
        if sort_key in ("win_rate", "place2_rate", "place3_rate",
                        "nige_rate", "maeiki_rate", "makuri_rate"):
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

    @app.route("/api/db/personnel_agg_course")
    def api_db_personnel_agg_course():
        """
        当該コース条件（会場×馬場×距離レンジ）で race_log を直接集計した
        騎手/調教師/種牡馬/母父の成績を返す。
        ?type=jockey|trainer|sire|bms
        &venue=03 &surface=ダート &distance=1300
        距離は ±200m レンジで集計（例: 1300m → 1100〜1500m）。
        """
        person_type = request.args.get("type", "jockey").strip()
        venue       = request.args.get("venue", "").strip()
        surface     = request.args.get("surface", "").strip()
        distance    = request.args.get("distance", "").strip()
        try:
            dist = int(distance) if distance else 0
        except ValueError:
            dist = 0

        if not venue or not surface or dist <= 0:
            return jsonify(
                type=person_type, total=0, persons=[],
                period="",
                error="venue/surface/distance 必須",
            )

        # 距離レンジ ±200m（最低200m）
        d_min = max(200, dist - 200)
        d_max = dist + 200

        # 集計対象カラム選択
        if person_type == "jockey":
            id_col   = "jockey_id"
            name_col = "jockey_name"
            where_id = "jockey_id != '' AND jockey_id NOT IN ('001','002','003')"
        elif person_type == "trainer":
            id_col   = "CASE WHEN trainer_id != '' THEN trainer_id ELSE trainer_name END"
            name_col = "trainer_name"
            where_id = "(trainer_id != '' OR trainer_name != '')"
        elif person_type == "sire":
            id_col   = "sire_name"
            name_col = "sire_name"
            where_id = "sire_name IS NOT NULL AND sire_name != ''"
        elif person_type == "bms":
            id_col   = "bms_name"
            name_col = "bms_name"
            where_id = "bms_name IS NOT NULL AND bms_name != ''"
        else:
            return jsonify(
                type=person_type, total=0, persons=[],
                error=f"unknown type: {person_type}",
            )

        try:
            conn = db_instance.get_conn() if hasattr(db_instance, "get_conn") else None
        except Exception:
            conn = None
        if conn is None:
            from src.database import get_db as _get_db
            conn = _get_db()

        sql = f"""
            SELECT {id_col} AS pid, {name_col} AS name,
                   COUNT(*)                                        AS total,
                   SUM(CASE WHEN finish_pos=1 THEN 1 ELSE 0 END)  AS win,
                   SUM(CASE WHEN finish_pos<=2 THEN 1 ELSE 0 END) AS place2,
                   SUM(CASE WHEN finish_pos<=3 THEN 1 ELSE 0 END) AS place3,
                   SUM(CASE WHEN finish_pos=1 THEN COALESCE(win_odds,0) ELSE 0 END) AS win_odds_sum
            FROM race_log
            WHERE {where_id}
              AND venue_code = ?
              AND surface = ?
              AND distance BETWEEN ? AND ?
            GROUP BY {id_col}, {name_col}
        """
        try:
            rows = conn.execute(sql, (venue, surface, d_min, d_max)).fetchall()
        except Exception as e:
            return jsonify(error=str(e))

        # 期間
        period_row = conn.execute(
            """
            SELECT MIN(race_date) AS mn, MAX(race_date) AS mx, COUNT(DISTINCT race_id) AS rc
            FROM race_log
            WHERE venue_code = ? AND surface = ? AND distance BETWEEN ? AND ?
            """,
            (venue, surface, d_min, d_max),
        ).fetchone()
        min_d = period_row["mn"] if period_row and period_row["mn"] else ""
        max_d = period_row["mx"] if period_row and period_row["mx"] else ""
        race_count = period_row["rc"] if period_row and period_row["rc"] else 0

        # 同一人物集約（sire/bms は name キー、jockey/trainer は pid キー）
        agg: dict = {}
        for r in rows:
            pid = r["pid"] or ""
            if not pid:
                continue
            if pid not in agg:
                agg[pid] = {
                    "id": pid,
                    "name": r["name"] or pid,
                    "total": 0, "win": 0, "place2": 0, "place3": 0,
                    "win_odds_sum": 0.0,
                }
            a = agg[pid]
            a["total"]        += int(r["total"] or 0)
            a["win"]          += int(r["win"] or 0)
            a["place2"]       += int(r["place2"] or 0)
            a["place3"]       += int(r["place3"] or 0)
            a["win_odds_sum"] += float(r["win_odds_sum"] or 0.0)

        persons = []
        for pid, a in agg.items():
            t = a["total"]
            if t <= 0:
                continue
            roi = round(a["win_odds_sum"] / t * 100, 1) if t else 0.0
            persons.append({
                "id": pid,
                "name": a["name"],
                "location": "",
                "total": t,
                "win": a["win"],
                "place2": a["place2"],
                "place3": a["place3"],
                "win_rate":    round(a["win"]    / t * 100, 1),
                "place2_rate": round(a["place2"] / t * 100, 1),
                "place3_rate": round(a["place3"] / t * 100, 1),
                "roi": roi,
            })

        # 総出走降順
        persons.sort(key=lambda x: x["total"], reverse=True)

        period_str = (
            f"{min_d}〜{max_d}（{race_count:,}レース・{venue} {surface} {d_min}〜{d_max}m）"
            if min_d else f"{venue} {surface} {d_min}〜{d_max}m（データなし）"
        )

        return jsonify(
            type=person_type,
            venue=venue,
            surface=surface,
            distance_range=[d_min, d_max],
            total=len(persons),
            persons=persons,
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

    def _count_pending_races(date: str, force: bool = False) -> int:
        """発走済み (or +10分経過) かつ結果未取得のレース数を返す。

        T-017 (2026-04-27): pending_before/pending_after の重複計算を共通化。
        T-020 (2026-04-27): force 引数追加。
          - force=False (default): 発走+10分経過後のみカウント (自動 fetch 用)
          - force=True: 発走済み全 race をカウント (手動更新ボタン用)
            → ホーム LIVE STATS の pending_fetch (発走直後カウント) と一致させる
        """
        try:
            from src.results_tracker import load_prediction as _lp_cp
            pred = _lp_cp(date)
            if not pred:
                return 0
            date_key = date.replace("-", "")
            rfp = os.path.join(PROJECT_ROOT, "data", "results", f"{date_key}_results.json")
            existing_rids: set = set()
            if os.path.isfile(rfp):
                try:
                    with open(rfp, "r", encoding="utf-8") as _f:
                        _rd = json.load(_f)
                    existing_rids = {
                        rid for rid, entry in _rd.items()
                        if isinstance(entry, dict) and entry.get("order")
                    }
                except Exception:
                    pass
            now_dt = datetime.now()
            count = 0
            # force=True: 発走後即カウント / force=False: 発走+10分経過後のみ
            threshold = timedelta(minutes=0) if force else timedelta(minutes=10)
            for _race in pred.get("races", []):
                _rid = str(_race.get("race_id", ""))
                if not _rid or _rid in existing_rids:
                    continue
                _pt = _race.get("post_time", "") or ""
                if not _pt:
                    continue
                try:
                    _pdt = datetime.strptime(f"{date} {_pt}", "%Y-%m-%d %H:%M")
                except Exception:
                    continue
                if now_dt >= _pdt + threshold:
                    count += 1
            return count
        except Exception:
            return 0

    def _auto_fetch_post_races(date: str, force: bool = False) -> dict:
        """当日の発走+10分経過かつ未取得レースを バックグラウンドで順次 fetch。

        マスター指示 2026-04-23 (案 A):
          - Home 画面が 2分ごとに polling → この関数を fire-and-forget で起動
          - 発走+10分経過 & results.json 未登録のレースを最大 5R 取得
          - クールダウン 5分（同 race_id の多重試行防止）
          - netkeiba レートリミット 1.5秒 を NetkeibaClient 内で尊守

        T-017 (2026-04-27): force=True 追加:
          - force=True の場合、race_id 単位のクールダウンチェックを bypass
          - force=True の場合、最大処理数を _FORCE_REFRESH_MAX_PER_CALL (100) に緩和
          - 戻り値: fetched/aggregated/skipped/errors の統計辞書
        """
        stats: dict = {"fetched": 0, "skipped": 0, "errors": 0}
        # 多重起動防止（force でも排他は維持する）
        with _auto_fetch_lock:
            if date in _auto_fetch_busy_dates:
                stats["skipped"] = -1  # busy を示す特殊値（呼び出し側で判断）
                return stats
            _auto_fetch_busy_dates.add(date)
        # T-001 reviewer HIGH 対応: cooldown dict のメモリリーク対策（拡張済み 1000 件で
        # 期限切れエントリを削除）。50R 上限拡張により race_id 蓄積速度が増したため必須。
        _cleanup_cooldown_if_needed()
        try:
            # 当日のみ対象（過去日は nightly batch に任せる）
            if date != datetime.now().strftime("%Y-%m-%d"):
                return stats
            from src.results_tracker import load_prediction
            pred = load_prediction(date)
            if not pred:
                return stats
            date_key = date.replace("-", "")
            rfp = os.path.join(PROJECT_ROOT, "data", "results", f"{date_key}_results.json")
            existing_rids: set = set()
            if os.path.isfile(rfp):
                try:
                    with open(rfp, "r", encoding="utf-8") as f:
                        _rdata = json.load(f)
                    existing_rids = {
                        rid for rid, entry in _rdata.items()
                        if isinstance(entry, dict) and entry.get("order")
                    }
                except Exception:
                    existing_rids = set()

            now = datetime.now()
            now_ts = time.time()
            targets: list = []
            # force=True 時は上限を _FORCE_REFRESH_MAX_PER_CALL に緩和
            max_per_call = _FORCE_REFRESH_MAX_PER_CALL if force else _AUTO_FETCH_MAX_PER_CALL
            for race in pred.get("races", []):
                rid = str(race.get("race_id", ""))
                if not rid or rid in existing_rids:
                    continue
                post_time = race.get("post_time", "") or ""
                if not post_time:
                    continue
                try:
                    post_dt = datetime.strptime(f"{date} {post_time}", "%Y-%m-%d %H:%M")
                except Exception:
                    continue
                # 発走+10分経過していること（force=True なら発走後即対象、ただし結果未掲載なら取得失敗）
                # T-020 (2026-04-27): 手動更新ボタンは「即座に取れるもの全部」の意図のため
                # force=True で 10 分閾値解除。netkeiba 未掲載なら errors+= で報告
                if not force and now < post_dt + timedelta(minutes=10):
                    continue
                if force and now < post_dt:
                    # 発走前 race は force でも対象外
                    continue
                # クールダウン: force=True の場合は bypass
                if not force:
                    last_attempt = _auto_fetch_cooldown.get(rid, 0)
                    if now_ts - last_attempt < _AUTO_FETCH_COOLDOWN_SEC:
                        stats["skipped"] += 1
                        continue
                targets.append(rid)
                if len(targets) >= max_per_call:
                    break

            if not targets:
                return stats

            # 早発の post_time が古い順（= 古い race から）を優先
            logger.info("auto-fetch: %s 対象 %d レース開始 (force=%s)", date, len(targets), force)

            from src.results_tracker import fetch_single_race_result
            from src.scraper.netkeiba import NetkeibaClient
            client = NetkeibaClient(no_cache=True)
            official = _get_official_odds_scraper()

            for rid in targets:
                # クールダウンを先に記録（失敗時も再試行を抑制）。Lock で TOCTOU 保護
                with _auto_fetch_lock:
                    _auto_fetch_cooldown[rid] = time.time()
                try:
                    entry = fetch_single_race_result(
                        date, rid, client, official_scraper=official
                    )
                    if entry and entry.get("order"):
                        stats["fetched"] += 1
                        logger.info("auto-fetch: %s 取得成功", rid)
                    else:
                        stats["skipped"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    logger.warning("auto-fetch: %s 失敗 %s", rid, e)

            if stats["fetched"] > 0:
                # 集計キャッシュを無効化（次回 today_stats 呼び出しで再計算）
                try:
                    from src.results_tracker import invalidate_aggregate_cache
                    invalidate_aggregate_cache()
                except Exception:
                    pass
                logger.info(
                    "auto-fetch: %s 完了 fetched=%d skipped=%d errors=%d",
                    date, stats["fetched"], stats["skipped"], stats["errors"],
                )
            return stats
        except Exception as e:
            logger.warning("auto-fetch 全体失敗: %s", e, exc_info=True)
            stats["errors"] += 1
            return stats
        finally:
            with _auto_fetch_lock:
                _auto_fetch_busy_dates.discard(date)

    @app.route("/api/force_refresh_today", methods=["POST"])
    def api_force_refresh_today():
        """成績の手動強制更新 endpoint。

        T-017 (2026-04-27):
          - 手動ボタンから呼び出し、未取得レースを即時 fetch + 集計再計算
          - 連打防止: _FORCE_REFRESH_LOCK で排他（busy 時 409）
          - レートリミット: 同一 IP 5秒以内の再リクエストは 429
          - date body パラメータ未指定時は本日 JST を使用

        2026-04-28 マスター指示「ここは誰でも更新できるようにして」:
          - admin 制限除去 (Cloudflare 経由の閲覧者からも実行可)
          - DoS 対策は既存の _FORCE_REFRESH_LOCK (排他) + IP 5秒レートリミットで十分
        """
        t_start = time.time()

        # ── レートリミット: 同一 IP 5秒以内の再リクエストは 429 ──
        client_ip = request.remote_addr or "unknown"
        now_ts = time.time()
        with _auto_fetch_lock:
            last_ip_req = _force_refresh_ip_rate.get(client_ip, 0)
            if now_ts - last_ip_req < _FORCE_REFRESH_RATE_LIMIT_SEC:
                remaining = int(_FORCE_REFRESH_RATE_LIMIT_SEC - (now_ts - last_ip_req))
                return jsonify(
                    status="error",
                    message=f"リクエスト頻度が高すぎます。{remaining}秒後に再試行してください",
                    code="RATE_LIMITED",
                    retry_after=remaining,
                ), 429
            _force_refresh_ip_rate[client_ip] = now_ts

        # ── body の date 取得・バリデーション ──
        body = request.get_json(force=True, silent=True) or {}
        date = body.get("date") or datetime.now().strftime("%Y-%m-%d")
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            return jsonify(status="error", message="date は YYYY-MM-DD 形式で指定してください", code="INVALID_DATE"), 400

        # ── 連打防止: _FORCE_REFRESH_LOCK で排他 ──
        if not _FORCE_REFRESH_LOCK.acquire(blocking=False):
            return jsonify(status="error", message="他の更新処理が実行中です。しばらく待ってから再試行してください", code="BUSY"), 409

        logger.info("force_refresh_today: date=%s start", date)
        try:
            from src.results_tracker import invalidate_aggregate_cache, compare_and_aggregate

            # pending 数（処理前）を計算（共通ヘルパー使用、force=True で 10 分閾値解除）
            # T-020 (2026-04-27): 手動更新ボタンは「発走済み全 race を即取得したい」意図のため、
            # 発走+10分待たずに発走後即カウント（ホーム LIVE STATS の pending_fetch と一致）
            pending_before = _count_pending_races(date, force=True)

            # ── 強制 fetch 実行 ──
            fetch_stats = _auto_fetch_post_races(date, force=True)

            # ── 集計キャッシュ無効化 + 強制再集計 ──
            aggregated = 0
            try:
                invalidate_aggregate_cache()
                agg = compare_and_aggregate(date, _skip_disk_cache=True)
                if agg:
                    aggregated = fetch_stats.get("fetched", 0)
            except Exception:
                logger.exception("force_refresh_today: 集計失敗 date=%s", date)
                # 集計エラーは fetch の成果は失わない

            # pending 数（処理後）を再計算（共通ヘルパー使用、同じく force=True）
            try:
                pending_after = _count_pending_races(date, force=True)
            except Exception:
                pending_after = max(0, pending_before - fetch_stats.get("fetched", 0))

            elapsed_ms = int((time.time() - t_start) * 1000)
            logger.info(
                "force_refresh_today: date=%s done fetched=%d aggregated=%d elapsed=%dms",
                date, fetch_stats.get("fetched", 0), aggregated, elapsed_ms,
            )
            return jsonify(
                status="ok",
                date=date,
                fetched=fetch_stats.get("fetched", 0),
                aggregated=aggregated,
                skipped=max(0, fetch_stats.get("skipped", 0)),
                errors=fetch_stats.get("errors", 0),
                pending_before=pending_before,
                pending_after=pending_after,
                elapsed_ms=elapsed_ms,
            )
        except Exception:
            logger.exception("force_refresh_today failed date=%s", date)
            elapsed_ms = int((time.time() - t_start) * 1000)
            return jsonify(status="error", message="internal error", code="INTERNAL", elapsed_ms=elapsed_ms), 500
        finally:
            _FORCE_REFRESH_LOCK.release()

    @app.route("/api/home/today_stats")
    def api_home_today_stats():
        """本日（指定日）の ◉◎単勝 リアルタイム成績 + 三連単F 集計を返す。

        マスター指示 2026-04-22:
          - ◉◎結果 X-X-X-X（勝率 / 連対率 / 単回収率）
          - 三連単F: 予想R / 的中R / 投資 / 回収 / 回収率
          - 各レースの 10分後自動更新

        マスター指示 2026-04-23 (案 A):
          - 本 API 呼び出し時、発走+10分経過かつ未取得レースを
            バックグラウンド Thread で最大5件自動 fetch（fire-and-forget）
          - 応答は即座に返す（次回 polling で新データ反映）
        """
        date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))

        # ── 案 A: 発走+10分経過の未取得レースを裏で fetch（非同期） ──
        threading.Thread(
            target=_auto_fetch_post_races, args=(date,), daemon=True,
        ).start()

        try:
            from src.results_tracker import compare_and_aggregate, load_prediction
            # T-001 reviewer MEDIUM 対応: pred を一度だけ load して使い回す（disk I/O 削減）
            pred_cached = load_prediction(date)
            # まず当日の集計（キャッシュ有効）
            agg = compare_and_aggregate(date)
            if not agg:
                # 予想はあるが結果がまだ → 枠を返す
                if not pred_cached:
                    return jsonify(date=date, found=False)
                return jsonify(
                    date=date, found=True, results_pending=True,
                    total_races=len(pred_cached.get("races", [])),
                    honmei={"total": 0, "win": 0, "place2": 0, "place3": 0,
                            "win_rate": 0, "place2_rate": 0, "place_rate": 0,
                            "tansho_stake": 0, "tansho_ret": 0, "tansho_roi": 0},
                    sanrentan={"played": 0, "hit": 0, "stake": 0, "payback": 0, "roi_pct": 0},
                )

            # ◉◎単勝ベース（honmei_* フィールドを直接利用）
            h_total = agg.get("honmei_total", 0)
            h_win = agg.get("honmei_win", 0)
            h_p2 = agg.get("honmei_place2", 0)
            h_p3 = agg.get("honmei_placed", 0)
            h_stake = agg.get("honmei_tansho_stake", 0)
            h_ret = agg.get("honmei_tansho_ret", 0)
            honmei = {
                "total": h_total,
                "win": h_win,
                "place2": h_p2 - h_win,  # 2着のみ
                "place3": h_p3 - h_p2,   # 3着のみ
                "out": h_total - h_p3,   # 着外
                "win_count": h_win,
                "place2_count": h_p2,
                "place3_count": h_p3,
                "win_rate":    round(h_win / h_total * 100, 1) if h_total else 0.0,
                "place2_rate": round(h_p2  / h_total * 100, 1) if h_total else 0.0,
                "place_rate":  round(h_p3  / h_total * 100, 1) if h_total else 0.0,
                "tansho_stake": h_stake,
                "tansho_ret":   h_ret,
                "tansho_roi":   round(h_ret / h_stake * 100, 1) if h_stake > 0 else 0.0,
            }

            # 三連単F集計 — pred.json 永続値ベース (T-039 ロジックに統一)
            sanrentan = {"played": 0, "hit": 0, "stake": 0, "payback": 0, "roi_pct": 0.0}
            try:
                from scripts.monthly_backtest import get_payout
                from config.settings import RESULTS_DIR as _RDIR
                import os as _os
                pred = pred_cached  # T-001 reviewer MEDIUM: 上部 load の結果を再利用
                if pred:
                    date_key = date.replace("-", "")
                    res_fp = _os.path.join(_RDIR, f"{date_key}_results.json")
                    if _os.path.isfile(res_fp):
                        with open(res_fp, "r", encoding="utf-8") as _rf:
                            results = json.load(_rf)
                        for r in pred.get("races", []):
                            rid = str(r.get("race_id", ""))
                            rdata = results.get(rid)
                            if not rdata:
                                continue
                            payouts = rdata.get("payouts", {})
                            if "三連単" not in payouts and "sanrentan" not in payouts:
                                continue

                            # 1-2-3 着の馬番を確定 (T-039 と同じロジック)
                            order = rdata.get("order") or []
                            if len(order) < 3:
                                continue
                            finish_map = {int(o["horse_no"]): int(o["finish"]) for o in order}
                            top3_ordered = [
                                h for h, f in sorted(finish_map.items(), key=lambda x: x[1])
                                if f <= 3
                            ]
                            if len(top3_ordered) < 3:
                                continue

                            # 共通ヘルパーで三連単チケット集合を取得
                            sanrentan_tix = _collect_sanrentan_tickets(r)
                            if not sanrentan_tix:
                                continue  # 三連単対象外レース (永続値に三連単無し = 予想時点 SS/C/D 等で skip 済)

                            # ここまで来れば played にカウント (真値 40R 系列)
                            sanrentan["played"] += 1
                            race_hit = _check_sanrentan_hit(sanrentan_tix, top3_ordered) is True
                            for t in sanrentan_tix:
                                stake = int(t.get("stake", 0) or 0)
                                if stake <= 0:
                                    continue
                                sanrentan["stake"] += stake
                                if race_hit:
                                    # 当該チケットが的中チケットか個別判定
                                    combo = t.get("combo", [])
                                    if combo and len(combo) >= 3 and [int(x) for x in combo[:3]] == top3_ordered:
                                        pp = get_payout(payouts, t)
                                        sanrentan["payback"] += pp * (stake // 100)
                            if race_hit:
                                sanrentan["hit"] += 1
                        sanrentan["roi_pct"] = (
                            round(sanrentan["payback"] / sanrentan["stake"] * 100, 1)
                            if sanrentan["stake"] > 0 else 0.0
                        )
                        sanrentan["hit_rate_pct"] = (
                            round(sanrentan["hit"] / sanrentan["played"] * 100, 1)
                            if sanrentan["played"] > 0 else 0.0
                        )
                        sanrentan["balance"] = sanrentan["payback"] - sanrentan["stake"]
            except Exception as _e:
                logger.debug("today_stats sanrentan 計算失敗: %s", _e)

            # T-001 (2026-04-25): UI 3 段表記用のメタ情報を計算
            # total_races (pred.json 全数) / finished_races (results.json 取り込み済み)
            # / eligible_for_sanrentan (S/A/B のレース数) / pending_fetch / pending_age_max_min
            meta = {
                "total_races": 0,
                "finished_races": 0,
                "eligible_for_sanrentan": 0,
                "pending_fetch": 0,
                "pending_age_max_min": 0,
            }
            try:
                from src.calculator.betting import SANRENTAN_SKIP_CONFIDENCES
                pred2 = pred_cached  # T-001 reviewer MEDIUM: 上部 load の結果を再利用
                if pred2:
                    races2 = pred2.get("races", [])
                    meta["total_races"] = len(races2)
                    meta["eligible_for_sanrentan"] = sum(
                        1 for r in races2
                        if r.get("confidence", "B") not in SANRENTAN_SKIP_CONFIDENCES
                    )
                    # results.json 取り込み済み件数・未取り込み統計（M-1 共通ヘルパー）
                    finished_rids, _pf, _pa = _get_pending_fetch_stats(date, races2)
                    meta["finished_races"] = len(finished_rids)
                    meta["pending_fetch"] = _pf
                    meta["pending_age_max_min"] = _pa
            except Exception as _me:
                logger.debug("today_stats meta 計算失敗: %s", _me)

            return jsonify(
                date=date, found=True,
                total_races=meta["total_races"] or agg.get("total_races", 0),
                finished_races=meta["finished_races"],
                eligible_for_sanrentan=meta["eligible_for_sanrentan"],
                pending_fetch=meta["pending_fetch"],
                pending_age_max_min=meta["pending_age_max_min"],
                honmei=honmei,
                sanrentan=sanrentan,
                last_updated=datetime.now().strftime("%H:%M"),
            )
        except Exception as e:
            logger.warning("today_stats failed: %s", e, exc_info=True)
            return jsonify(date=date, error=str(e), found=False)

    @app.route("/api/results/unmatched_dates_db")
    def api_results_unmatched_dates_db():
        """予想済みだが結果未取得の日付一覧（DB対応版、2024-01-01〜昨日）"""
        try:
            from src.database import results_exist
            from src.results_tracker import list_prediction_dates
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
        "dev_run1_adj": ("前走偏差値", "前走の能力偏差値（グレード補正）"),
        "dev_run2_adj": ("前々走偏差値", "前々走の能力偏差値（グレード補正）"),
        "chakusa_index_avg3": ("着差指数", "直近3走の着差指数平均"),
        # 展開・脚質
        "horse_running_style": ("脚質", "逃げ/先行/差し/追込"),
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
        # コース構造
        "first_corner_m": ("初角実距離", "スタートから最初のコーナーまでの実距離"),
        # 騎手×条件別（複合条件）
        "jockey_surf_dist_wr": ("騎手×芝ダ×距離勝率", "芝/ダートと距離帯の組合せ勝率"),
        "jockey_surf_dist_pr": ("騎手×芝ダ×距離複勝率", "芝/ダートと距離帯の組合せ複勝率"),
        "jockey_sim_venue_wr": ("騎手×類似場勝率", "類似度加重の競馬場における勝率"),
        "jockey_sim_venue_pr": ("騎手×類似場複勝率", "類似度加重の競馬場における複勝率"),
        "jockey_sim_venue_dist_wr": ("騎手×類似場×距離勝率", "類似場・距離の組合せ勝率"),
        "jockey_sim_venue_dist_pr": ("騎手×類似場×距離複勝率", "類似場・距離の組合せ複勝率"),
        # 調教師×条件別（複合条件）
        "trainer_surf_dist_wr": ("調教師×芝ダ×距離勝率", "芝/ダートと距離帯の組合せ勝率"),
        "trainer_surf_dist_pr": ("調教師×芝ダ×距離複勝率", "芝/ダートと距離帯の組合せ複勝率"),
        "trainer_sim_venue_wr": ("調教師×類似場勝率", "類似度加重の競馬場における勝率"),
        "trainer_sim_venue_pr": ("調教師×類似場複勝率", "類似度加重の競馬場における複勝率"),
        "trainer_sim_venue_dist_wr": ("調教師×類似場×距離勝率", "類似場・距離の組合せ勝率"),
        "trainer_sim_venue_dist_pr": ("調教師×類似場×距離複勝率", "類似場・距離の組合せ複勝率"),
        # コーナー別位置変化
        "avg_pos_change_3to4c": ("3→4角前進量", "3角→4角の位置前進量の平均"),
        "pos_change_3to4c_last": ("前走3→4角前進量", "前走の3角→4角位置前進量"),
        "avg_pos_change_1to4c": ("1→4角総移動量", "1角→4角の総移動量の平均"),
        "front_hold_rate": ("先頭維持率", "1角先頭30%時に4角でも維持できた率"),
        # 距離ロス・コーナーロス
        "past_avg_outer_ratio": ("平均外回り度", "直近5走の全コーナー平均相対位置"),
        "past_outer_ratio_last": ("前走外回り度", "前走の全コーナー平均相対位置"),
        "past_corner_loss_sec_avg": ("コーナーロス秒平均", "直近5走の推定コーナーロス秒平均"),
        "past_corner_loss_sec_last": ("前走コーナーロス秒", "前走の推定コーナーロス秒"),
        "past_pos_spread": ("コーナー間位置変動幅", "直近5走のコーナー間位置変動幅平均"),
        # 着差指数再設計
        "margin_norm_last": ("頭数補正着差(前走)", "前走の頭数補正着差"),
        "margin_norm_avg3": ("頭数補正着差(直近3走)", "直近3走の頭数補正着差平均"),
        # タイム指数（走破タイム補正）
        "speed_index_last": ("タイム指数(前走)", "前走のタイム指数"),
        "speed_index_avg3": ("タイム指数(直近3走)", "直近3走タイム指数平均"),
        "speed_index_best3": ("タイム指数ベスト3", "直近3走タイム指数最高値"),
        # ペース適性
        "place_rate_fast_pace": ("ハイペース複勝率", "ハイペース時の複勝率"),
        "place_rate_slow_pace": ("スローペース複勝率", "スローペース時の複勝率"),
        "pace_pref_score": ("ペース得意度", "ハイペース複勝率−スローペース複勝率"),
        "pace_count_fast": ("ハイペース出走数", "ハイペース出走数"),
        "pace_count_slow": ("スローペース出走数", "スローペース出走数"),
        "pace_norm_last": ("前走ペース指標", "前走のレースペース指標"),
        "pace_norm_avg3": ("ペース指標(直近3走)", "直近3走のペース指標平均"),
        # 展開予測（フィールド脚質構成）
        "front_runner_count_in_race": ("逃げ先行馬数", "フィールド内の逃げ・先行馬数"),
        "pace_pressure_index": ("ペース圧力指数", "逃げ・先行馬比率"),
        "style_pace_affinity": ("脚質展開相性", "脚質×展開相性スコア"),
        # ニック理論・血統
        "sire_x_bms_place_rate": ("父×母父複勝率", "父×母父の組み合わせ複勝率"),
        "sire_bms_wr": ("父×母父勝率", "父と母父の組み合わせ勝率"),
        # 血統context_pr（統合版）
        "sire_context_pr": ("父条件適性", "父の現レース条件に最適な複勝率"),
        "bms_context_pr": ("母父条件適性", "母父の現レース条件に最適な複勝率"),
        # 馬context_pr（統合版）
        "horse_context_pr": ("馬条件適性", "馬の現条件に最適な複勝率"),
        # グレード補正版タイム指数
        "speed_index_adj_6m": ("タイム指数補正(半年)", "グレード補正済み半年タイム指数平均"),
        "speed_index_adj_best3": ("タイム指数補正ベスト", "グレード補正済み直近3走ベスト"),
        # 父馬×条件別
        "sire_surf_dist_wr": ("父×芝ダ×距離勝率", "父の産駒の芝/ダート×距離帯勝率"),
        "sire_surf_dist_pr": ("父×芝ダ×距離複勝率", "父の産駒の芝/ダート×距離帯複勝率"),
        "sire_sim_venue_wr": ("父×類似場勝率", "父の産駒の類似度加重勝率"),
        "sire_sim_venue_pr": ("父×類似場複勝率", "父の産駒の類似度加重複勝率"),
        "sire_sim_venue_dist_wr": ("父×類似場×距離勝率", "父の産駒の類似場・距離勝率"),
        "sire_sim_venue_dist_pr": ("父×類似場×距離複勝率", "父の産駒の類似場・距離複勝率"),
        # 母父×条件別
        "bms_surf_dist_wr": ("母父×芝ダ×距離勝率", "母父の産駒の芝/ダート×距離帯勝率"),
        "bms_surf_dist_pr": ("母父×芝ダ×距離複勝率", "母父の産駒の芝/ダート×距離帯複勝率"),
        "bms_sim_venue_wr": ("母父×類似場勝率", "母父の産駒の類似度加重勝率"),
        "bms_sim_venue_pr": ("母父×類似場複勝率", "母父の産駒の類似度加重複勝率"),
        "bms_sim_venue_dist_wr": ("母父×類似場×距離勝率", "母父の産駒の類似場・距離勝率"),
        "bms_sim_venue_dist_pr": ("母父×類似場×距離複勝率", "母父の産駒の類似場・距離複勝率"),
        # Phase 10B: 展開特徴量追加
        "field_pace_variance": ("フィールド脚質分散", "フィールド内脚質の均等vsペース偏り"),
        "early_position_est": ("序盤位置取り推定", "枠番×脚質からの序盤位置取り推定"),
        "last3f_pace_diff": ("上がり差分", "上がり3F推定−位置推定の差分"),
        "pace_horse_match": ("馬ペース相性", "馬のペース選好×予想ペースの一致度"),
        # Phase 10B: 血統特徴量追加
        "sire_credibility": ("父馬信頼度", "父馬の産駒成績信頼度"),
        "bms_credibility": ("母父信頼度", "母父の産駒成績信頼度"),
        "sire_surface_pref": ("父馬芝ダ適性差", "父馬の芝PR−ダートPRの差"),
        "bms_surface_pref": ("母父芝ダ適性差", "母父の芝PR−ダートPRの差"),
        "sire_dist_pref": ("父馬距離適性差", "父馬の短距離PR−長距離PRの差"),
        "sire_recent_trend": ("父馬産駒トレンド", "父馬の直近産駒成績トレンド"),
        # Phase 10B: 調教師特徴量追加
        "trainer_class_trend": ("調教師クラス推移", "直近20走のクラスレベル推移"),
        "trainer_rest_wr": ("調教師休養明け複勝率", "休養明け馬の複勝率"),
        # Phase 11: タイム指数マルチウィンドウ
        "speed_index_avg_1y": ("タイム指数(1年平均)", "過去1年のタイム指数平均"),
        "speed_index_best_1y": ("タイム指数(1年ベスト)", "過去1年のタイム指数ベスト"),
        "speed_index_avg_6m": ("タイム指数(半年平均)", "過去半年のタイム指数平均"),
        "speed_index_trend": ("タイム指数トレンド", "過去1年のタイム指数傾き"),
        # Phase 11: 馬の条件別複勝率
        "horse_pr_2y": ("馬複勝率(2年)", "過去2年の複勝率"),
        "horse_venue_pr": ("馬×場複勝率", "当競馬場での複勝率"),
        "horse_dist_pr": ("馬×距離複勝率", "当距離帯の複勝率"),
        "horse_smile_pr": ("馬×SMILE複勝率", "当SMILE区分の複勝率"),
        "horse_style_pr": ("馬×脚質複勝率", "脚質帯での複勝率"),
        "horse_gate_pr": ("馬×枠番複勝率", "枠番帯の複勝率"),
        "horse_jockey_pr": ("馬×騎手複勝率", "当騎手との複勝率"),
        # Phase 11: 騎手の条件別複勝率
        "jockey_pr_2y": ("騎手複勝率(2年)", "過去2年の複勝率"),
        "jockey_venue_pr": ("騎手×場複勝率", "当競馬場の複勝率"),
        "jockey_dist_pr": ("騎手×距離複勝率", "当距離帯の複勝率"),
        "jockey_smile_pr": ("騎手×SMILE複勝率", "当SMILE区分の複勝率"),
        "jockey_cond_pr": ("騎手×馬場状態複勝率", "当馬場状態の複勝率"),
        # Phase 11: 調教師の条件別複勝率
        "trainer_pr_2y": ("調教師複勝率(2年)", "過去2年の複勝率"),
        "trainer_venue_pr": ("調教師×場複勝率", "当競馬場の複勝率"),
        "trainer_dist_pr": ("調教師×距離複勝率", "当距離帯の複勝率"),
        "trainer_smile_pr": ("調教師×SMILE複勝率", "当SMILE区分の複勝率"),
        "trainer_cond_pr": ("調教師×馬場状態複勝率", "当馬場状態の複勝率"),
        # Phase 11: 父の条件別複勝率
        "sire_smile_pr": ("父×SMILE複勝率", "SMILE区分の産駒複勝率"),
        "sire_cond_pr": ("父×馬場状態複勝率", "馬場状態別の産駒複勝率"),
        "sire_venue_pr": ("父×場複勝率", "当競馬場の産駒複勝率"),
        # Phase 11: 母父の条件別複勝率
        "bms_smile_pr": ("母父×SMILE複勝率", "SMILE区分の産駒複勝率"),
        "bms_cond_pr": ("母父×馬場状態複勝率", "馬場状態別の産駒複勝率"),
        "bms_venue_pr": ("母父×場複勝率", "当競馬場の産駒複勝率"),
        "bms_dist_pr": ("母父×距離複勝率", "SMILE区分複勝率"),
        # Phase 12: 条件別複勝率追加
        "horse_cond_pr": ("馬×馬場状態複勝率", "馬の馬場状態別複勝率"),
        "jockey_pace_pr": ("騎手×ペース複勝率", "騎手のペース別複勝率"),
        "jockey_style_pr": ("騎手×脚質複勝率", "騎手の脚質別複勝率"),
        "jockey_gate_pr": ("騎手×枠番複勝率", "騎手の枠番帯別複勝率"),
        "jockey_horse_pr": ("騎手×馬複勝率", "騎手の騎乗馬別複勝率"),
        "trainer_pace_pr": ("調教師×ペース複勝率", "調教師のペース別複勝率"),
        "trainer_style_pr": ("調教師×脚質複勝率", "調教師の脚質別複勝率"),
        "trainer_gate_pr": ("調教師×枠番複勝率", "調教師の枠番帯別複勝率"),
        "trainer_horse_pr": ("調教師×馬複勝率", "調教師の騎乗馬別複勝率"),
        "sire_pace_pr": ("父×ペース複勝率", "父のペース別産駒複勝率"),
        "sire_style_pr": ("父×脚質複勝率", "父の脚質別産駒複勝率"),
        "sire_gate_pr": ("父×枠番複勝率", "父の枠番帯別産駒複勝率"),
        "sire_jockey_pr": ("父×騎手複勝率", "父×騎手の産駒複勝率"),
        "sire_trainer_pr": ("父×調教師複勝率", "父×調教師の産駒複勝率"),
        "bms_pace_pr": ("母父×ペース複勝率", "母父のペース別産駒複勝率"),
        "bms_style_pr": ("母父×脚質複勝率", "母父の脚質別産駒複勝率"),
        "bms_gate_pr": ("母父×枠番複勝率", "母父の枠番帯別産駒複勝率"),
        "bms_jockey_pr": ("母父×騎手複勝率", "母父×騎手の産駒複勝率"),
        "bms_trainer_pr": ("母父×調教師複勝率", "母父×調教師の産駒複勝率"),
        # 前走オッズ
        "prev_odds_1": ("前走オッズ", "前走の単勝オッズ"),
        "prev_odds_2": ("前々走オッズ", "前々走の単勝オッズ"),
        # ---- 調教特徴量 [24本] ----
        # A: タイム系（自馬比較）
        "train_final_4f": ("追切4Fタイム", "最終追い切りの4Fタイム(秒)"),
        "train_final_3f_self_best_ratio": ("追切3F自馬ベスト比", "今回3Fの自馬過去ベストとの比率"),
        "train_final_3f_trend": ("追切3Fトレンド", "直近3走+今回の3Fタイム傾き(負=改善)"),
        "train_final_3f_rank_in_race": ("追切3F順位", "レース出走馬内での追切3F順位"),
        "train_final_3f_dev": ("追切3F偏差値", "レース出走馬内での追切3F偏差値"),
        "train_final_1f_dev": ("追切1F偏差値", "レース出走馬内での追切1F偏差値"),
        "train_final_1f_trend": ("追切1Fトレンド", "直近3走+今回の1Fタイム傾き(負=改善)"),
        # B: ラスト加速・余力系
        "train_first1f_pace": ("追切入り1Fペース", "追切の入り1F区間タイム(4F-3F)"),
        # C: 強さ効率系
        "train_intensity_max": ("追切最大強さ", "全追い切り中の最大強度(0.5-3.0)"),
        "train_3f_per_intensity": ("追切3F効率", "3Fタイム÷強さ(低い=強度の割に速い)"),
        "train_efficiency_self_diff": ("追切効率自馬差", "3F効率の自馬過去平均との差"),
        "train_narinori_3f": ("馬なり時3F", "馬なり追切時の3Fタイム(余力指標)"),
        # D: 厩舎基準偏差系
        "train_3f_trainer_dev": ("厩舎3F偏差値", "厩舎×コース基準での3F偏差値"),
        "train_1f_trainer_dev": ("厩舎1F偏差値", "厩舎×コース基準での1F偏差値"),
        "train_trainer_intensity_diff": ("厩舎強さ差", "追切強さの厩舎平均との差"),
        # E: ボリューム・パターン系
        "train_volume_self_diff": ("追切本数自馬差", "追切本数の自馬過去平均との差"),
        "train_intensity_pattern": ("追切強さ推移", "追切の強さ推移パターン(0-3)"),
        "train_course_primary": ("追切主コース", "主に使用した追切コース種別"),
        # F: 併せ馬系
        "train_partner_margin": ("併せ馬着差", "最終併せ馬の着差(先着+/遅れ-秒)"),
        "train_partner_win_rate": ("併せ馬先着率", "全併せ馬での先着率"),
        # G: コメント・評価系
        "train_stable_mark": ("厩舎評価マーク", "厩舎コメント先頭の評価(◎3/○2/△1/無0)"),
        "train_comment_sentiment": ("追切コメント感情", "ポジティブ・ネガティブの正味スコア"),
        # H: 複合・状態推定系
        "train_state_score": ("総合状態スコア", "タイム差・効率・厩舎偏差値の加重合成"),
        "train_readiness_index": ("仕上がり指数", "本数・強さ推移・自馬比較の総合仕上がり度"),
    }
    _feature_imp_cache: list = []

    # ──────────────────────────────────────────────
    # 競馬場研究 API
    # ──────────────────────────────────────────────

    # 競馬場別幅員テーブル（芝/ダート別）
    _VENUE_WIDTH = {
        # JRA 10場
        "05": {"芝": "31-41m", "ダート": "25m"},     # 東京
        "06": {"芝": "20-32m", "ダート": "20-25m"},   # 中山
        "08": {"芝": "27-38m", "ダート": "25m"},      # 京都
        "09": {"芝": "24-29m", "ダート": "22-25m"},   # 阪神
        "07": {"芝": "25-30m", "ダート": "25m"},      # 中京
        "04": {"芝": "20-25m", "ダート": "20m"},      # 新潟
        "10": {"芝": "20-30m", "ダート": "20-24m"},   # 小倉
        "03": {"芝": "20-27m", "ダート": "20-25m"},   # 福島
        "02": {"芝": "20-29m", "ダート": "20m"},      # 函館
        "01": {"芝": "25-27m", "ダート": "20m"},      # 札幌
        # NAR 15場
        "30": {"ダート": "25-28m"},                    # 門別
        "65": {"ダート": "2m"},                        # 帯広（ばんえい）
        "35": {"芝": "25m", "ダート": "25m"},          # 盛岡
        "36": {"ダート": "20m"},                       # 水沢
        "42": {"ダート": "16-21.5m"},                  # 浦和
        "43": {"ダート": "20-25m"},                    # 船橋
        "44": {"ダート": "23.6-28.6m"},                # 大井
        "45": {"ダート": "25m"},                       # 川崎
        "46": {"ダート": "20m"},                       # 金沢
        "47": {"ダート": "20-25m"},                    # 笠松
        "48": {"ダート": "30m"},                       # 名古屋
        "50": {"ダート": "20-25m"},                    # 園田
        "51": {"ダート": "20-25m"},                    # 姫路
        "54": {"ダート": "22-27m"},                    # 高知
        "55": {"ダート": "20-25m"},                    # 佐賀
    }

    @app.route("/api/venue/profile")
    def api_venue_profile():
        """競馬場プロファイル（一覧 or 個別詳細）"""
        from config.settings import get_composite_weights
        from data.masters.course_master import ALL_COURSES
        from data.masters.venue_master import VENUE_CODE_TO_NAME
        from data.masters.venue_similarity import get_all_profiles, get_similar_venues

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
        venue_widths = _VENUE_WIDTH.get(code, {})
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
                "first_corner_m": c.first_corner_m,
                "slope_type": c.slope_type,
                "inside_outside": c.inside_outside,
                "width_m": venue_widths.get(c.surface, ""),
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
        from collections import defaultdict

        from src.database import get_course_db, get_course_last3f_sigma

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
                if r <= 0.35:
                    return "先行"
                elif r <= 0.70:
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
            for style in ["逃げ", "先行", "差し", "追込"]:
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
            from collections import defaultdict

            import lightgbm as lgb

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

    # ── ヘルスチェックエンドポイント ──────────────────────────
    _start_time = time.time()

    @app.route("/api/health")
    def api_health():
        """ヘルスチェック: uptime, メモリ使用量, DB接続状態, 当日取り込み遅延状況を返す。
        T-001 Phase 2 (B1) で pending_fetch / pending_age_max_min / match_results_today / auto_fetch_busy 追加。
        """
        mem_mb = None
        try:
            import psutil
            mem_mb = round(psutil.Process().memory_info().rss / 1024 / 1024, 1)
        except ImportError:
            pass
        db_ok = False
        try:
            from src.database import get_db as _get_db
            _get_db().execute("SELECT 1")
            db_ok = True
        except Exception:
            pass
        # 成績キャッシュ統計（build_results_cache.py 由来の hit/miss）
        results_cache = app.config.get("_RESULTS_CACHE_STATS", {}) or {}
        # manifest.json があれば last_built_at を露出
        results_cache_manifest = None
        try:
            mf = os.path.join(PROJECT_ROOT, "data", "cache", "results", "manifest.json")
            if os.path.exists(mf):
                with open(mf, "r", encoding="utf-8") as f:
                    mdata = json.load(f)
                results_cache_manifest = {
                    "generated_at": mdata.get("generated_at"),
                    "years": mdata.get("years", []),
                }
        except Exception:
            pass

        # T-001 Phase 2 (B1): 当日のリアルタイム性メトリクス
        today_str = datetime.now().strftime("%Y-%m-%d")
        today_metrics = {
            "date": today_str,
            "total_races": 0,
            "finished_races": 0,
            "pending_fetch": 0,
            "pending_age_max_min": 0,
            "match_results_today": 0,
            "auto_fetch_busy": today_str in _auto_fetch_busy_dates,
        }
        try:
            # pred.json から当日の総レース数と発走時刻
            pred_fp = os.path.join(PROJECT_ROOT, "data", "predictions",
                                    f"{today_str.replace('-', '')}_pred.json")
            races_today = []
            if os.path.isfile(pred_fp):
                with open(pred_fp, "r", encoding="utf-8") as _pf:
                    _pdata = json.load(_pf)
                races_today = _pdata.get("races", []) or []
                today_metrics["total_races"] = len(races_today)

            # results.json 取り込み済み件数・未取り込み統計（M-1 共通ヘルパー）
            _frids, _pf2, _pa2 = _get_pending_fetch_stats(today_str, races_today)
            today_metrics["finished_races"] = len(_frids)
            today_metrics["pending_fetch"] = _pf2
            today_metrics["pending_age_max_min"] = _pa2

            # match_results テーブル当日件数（get_db() でスレッドローカル接続を再利用）
            try:
                from src.database import get_db as _get_db
                _hconn = _get_db()
                _row = _hconn.execute(
                    "SELECT COUNT(*) FROM match_results WHERE date = ?", (today_str,)
                ).fetchone()
                today_metrics["match_results_today"] = int(_row[0]) if _row else 0
            except Exception:
                pass
        except Exception as _e:
            logger.debug("/api/health today_metrics 計算失敗: %s", _e)

        # スクレイパーレイアウト変更検知: 本日の警告件数を返す
        try:
            from src.scraper._layout_check import (
                get_layout_warning_count,
                get_layout_warning_details,
            )
            _layout_count = get_layout_warning_count()
            _layout_details = get_layout_warning_details()
        except Exception:
            _layout_count = 0
            _layout_details = {}

        return jsonify({
            "status": "ok",
            "uptime_sec": round(time.time() - _start_time),
            "memory_mb": mem_mb,
            "db_connected": db_ok,
            "pid": os.getpid(),
            "results_cache": {**results_cache, "manifest": results_cache_manifest},
            "today": today_metrics,
            "layout_warnings": _layout_count,
            "layout_warnings_detail": _layout_details,
        })

    # ── T-038 開催カレンダーマスタ API ──────────────────────────────────────
    @app.route("/api/kaisai_calendar")
    def api_kaisai_calendar():
        """T-038 開催カレンダーマスタを返す。
        data/masters/kaisai_calendar.json を読み込んでそのまま返す。
        """
        master_path = os.path.join(PROJECT_ROOT, "data", "masters", "kaisai_calendar.json")
        try:
            with open(master_path, "r", encoding="utf-8") as _f:
                data = json.load(_f)
            return jsonify(data)
        except FileNotFoundError:
            logger.warning("kaisai_calendar.json が見つかりません: %s", master_path)
            return jsonify({"error": "calendar_master_not_found"}), 500
        except Exception as _e:
            logger.error("/api/kaisai_calendar 読み込みエラー: %s", _e)
            return jsonify({"error": str(_e)}), 500

    # ── データ品質チェック API ────────────────────────────────────────────
    @app.route("/api/data_quality")
    def api_data_quality():
        """データ品質チェック最終結果を返す。
        daily_data_quality_check.py が生成した logs/data_quality_latest.json を読み込む。
        未実行の場合は status='not_run' を返す。
        フロントエンドの警告バナー表示に使用。
        """
        result_path = os.path.join(PROJECT_ROOT, "logs", "data_quality_latest.json")
        if not os.path.isfile(result_path):
            return jsonify({
                "status": "not_run",
                "checked_at": None,
                "has_violation": False,
                "message": "daily_data_quality_check.py 未実行",
                "race_log": [],
                "pred_json": [],
            })
        try:
            with open(result_path, "r", encoding="utf-8") as _f:
                data = json.load(_f)
            # 違反項目のみを抽出してフロントエンドに渡す
            violations = [
                r for r in (data.get("race_log") or []) + (data.get("pred_json") or [])
                if r.get("violated")
            ]
            return jsonify({
                "status": "ok",
                "checked_at": data.get("checked_at"),
                "target_date": data.get("target_date"),
                "has_violation": data.get("has_violation", False),
                "violations": violations,
                "race_log": data.get("race_log", []),
                "pred_json": data.get("pred_json", []),
            })
        except Exception as _e:
            logger.error("/api/data_quality 読み込みエラー: %s", _e)
            return jsonify({
                "status": "error",
                "has_violation": False,
                "message": str(_e),
            }), 500

    # ── T-039: レースカード的中バッジ用ヘルパー ─────────────────────────────
    def _build_race_card_results(date: str) -> dict:
        """
        T-039: 指定日の全レースについて、単勝◎的中 / 三連単F的中 を race_id 別に返す。

        処理概要:
          1. data/predictions/<date>_pred.json を読み込む
          2. data/results/<date>_results.json を読み込む（なければ DB fallback）
          3. 各レースの ◎◉ 馬が 1 着 → win_hit=True
          4. tickets / formation_tickets に「三連単」があり、1-2-3 着順序と combo が一致
             → sanrentan_hit=True
          5. 結果未取得のレースは win_hit/sanrentan_hit=None（フロントは非表示）

        返り値:
          {"date": "YYYY-MM-DD", "results": {"race_id": {"win_hit": bool|None, "sanrentan_hit": bool|None}}}
        """
        import os as _os
        pred_fp = _os.path.join(PROJECT_ROOT, "data", "predictions",
                                f"{date.replace('-', '')}_pred.json")
        if not _os.path.isfile(pred_fp):
            return {"date": date, "results": {}}

        with open(pred_fp, "r", encoding="utf-8") as _f:
            pred = json.load(_f)

        # 結果ファイルを読み込む（RESULTS_DIR → DB fallback）
        from config.settings import RESULTS_DIR
        actual: dict = {}
        res_fp = _os.path.join(RESULTS_DIR, f"{date.replace('-', '')}_results.json")
        if _os.path.isfile(res_fp):
            try:
                with open(res_fp, "r", encoding="utf-8") as _rf:
                    actual = json.load(_rf)
            except Exception as _re:
                logger.warning("race_card_results: results.json 読み込み失敗 %s", _re)

        if not actual:
            try:
                from src import database as _db2
                actual = _db2.load_results(date) or {}
            except Exception:
                pass

        race_results: dict = {}
        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id:
                continue

            result = actual.get(race_id)
            if not result:
                # 結果未取得 → win_hit/sanrentan_hit は None（フロントは非表示）
                race_results[race_id] = {"win_hit": None, "sanrentan_hit": None}
                continue

            order = result.get("order", [])
            if not order:
                # 着順データなし（中止等）→ None
                race_results[race_id] = {"win_hit": None, "sanrentan_hit": None}
                continue

            # 1-2-3 着の馬番（int）を確定
            finish_map = {int(r["horse_no"]): int(r["finish"]) for r in order}
            top3_ordered = [
                h for h, f in sorted(finish_map.items(), key=lambda x: x[1])
                if f <= 3
            ]

            # ── 単勝 ◎◉ 的中判定 ─────────────────────────────────────────
            win_hit: bool | None = None
            for h in race.get("horses", []):
                if h.get("mark") in ("◎", "◉"):
                    winner = top3_ordered[0] if top3_ordered else None
                    win_hit = (int(h["horse_no"]) == winner)
                    break

            # ── 三連単 チケット 的中判定 ──────────────────────────────────
            # 共通ヘルパーで三連単チケット集合を取得・判定 (LIVE STATS と single source of truth)
            sanrentan_tix = _collect_sanrentan_tickets(race)
            hit = _check_sanrentan_hit(sanrentan_tix, top3_ordered)
            # T-039 既存仕様: 「チケットあるが top3 < 3」 → None
            # 「チケットあるが combo 不一致」 → False
            # 「チケットなし」 → None
            if hit is True:
                sanrentan_hit = True
            elif sanrentan_tix and len(top3_ordered) >= 3:
                sanrentan_hit = False
            else:
                sanrentan_hit = None

            race_results[race_id] = {
                "win_hit": win_hit,
                "sanrentan_hit": sanrentan_hit,
            }

        return {"date": date, "results": race_results}

    @app.route("/api/race_card_results")
    def api_race_card_results():
        """T-039: レースカード的中バッジ用 endpoint。

        パラメータ:
          date: YYYY-MM-DD (必須)

        戻り値:
          {
            "date": "2026-04-28",
            "results": {
              "<race_id>": {
                "win_hit": true/false/null,       # 単勝 ◎ 的中 (null=結果未取得)
                "sanrentan_hit": true/false/null  # 三連単 F 的中 (null=未対象/結果未取得)
              }
            }
          }
        """
        date = request.args.get("date", "")
        if not date or not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            return jsonify(error="date は YYYY-MM-DD 形式で指定してください"), 400
        try:
            result = _build_race_card_results(date)
            return jsonify(result)
        except Exception as _e:
            logger.exception("race_card_results 失敗 date=%s", date)
            return jsonify(error=str(_e)), 500

    # ── A4: 発走+10分タイマー（イベント駆動フェッチ）──────────────────────
    def _schedule_post_race_timers(date: str) -> None:
        """当日の各レースに発走+10分タイマーをセット。Flask 起動時に一度呼ぶ。
        delay=0（既発走済み）はほぼ即時発火→起動時 catch-up として機能する。
        """
        pred_fp = os.path.join(PROJECT_ROOT, "data", "predictions",
                               f"{date.replace('-', '')}_pred.json")
        if not os.path.isfile(pred_fp):
            return
        try:
            with open(pred_fp, "r", encoding="utf-8") as _tf:
                _races_a4 = json.load(_tf).get("races", []) or []
        except Exception as _e4:
            logger.warning("A4 タイマーセット: pred.json 読み込み失敗 %s", _e4)
            return

        _now4 = datetime.now()
        _n4 = 0
        for _r4 in _races_a4:
            _pt4 = _r4.get("post_time", "") or ""
            if not _pt4:
                continue
            try:
                _fire4 = (
                    datetime.strptime(f"{date} {_pt4}", "%Y-%m-%d %H:%M")
                    + timedelta(minutes=10)
                )
            except Exception:
                continue
            _delay4 = max(0.0, (_fire4 - _now4).total_seconds())

            def _make_cb(d: str = date) -> None:
                def _cb() -> None:
                    threading.Thread(
                        target=_auto_fetch_post_races,
                        args=(d,),
                        daemon=True,
                        name=f"post_race_timer_{d}",
                    ).start()
                return _cb

            _t4 = threading.Timer(_delay4, _make_cb())
            _t4.daemon = True
            _t4.start()
            _n4 += 1

        logger.info("A4 発走+10分タイマー: %d 件セット（%s）", _n4, date)

    _schedule_post_race_timers(datetime.now().strftime("%Y-%m-%d"))

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


def _acquire_singleton_lock(port: int):
    """ダッシュボードの二重起動を防ぐ pidfile ロック。

    2026-04-19 の pred.json 破損事故（05:00 に dashboard が 2 プロセス稼働していて
    両方の内蔵 odds-scheduler が同時に pred.json を書き込み JSON 破損）の再発防止。

    - 取得失敗したら即 exit(0) し、新プロセスは Flask 起動しない
    - filelock はプロセス終了で自動解放（OS が fd を閉じる）
    - pidfile には自身の PID を書いておき、調査用に残す
    """
    from pathlib import Path
    from src.utils.atomic_json import _HAS_FILELOCK  # 存在チェック

    lock_dir = Path(PROJECT_ROOT) / "data" / "logs"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"dashboard.{port}.lock"

    if not _HAS_FILELOCK:
        logger.warning("filelock 未導入のため singleton ロックをスキップ")
        return None

    from filelock import FileLock, Timeout
    lock = FileLock(str(lock_path) + ".flock", timeout=0.1)
    try:
        lock.acquire()
    except Timeout:
        logger.critical(
            "ダッシュボードは既に別プロセスで起動中です (port=%d, lock=%s)。"
            "本プロセスは終了します。",
            port, lock_path,
        )
        # pidfile から既存プロセスの PID を読んでログに残す
        try:
            existing_pid = lock_path.read_text(encoding="utf-8").strip()
            logger.critical("既存プロセス PID: %s", existing_pid)
        except Exception:
            pass
        sys.exit(0)  # 重複起動は正常扱いで終了（bat の exit code を汚さない）

    # 自分の PID を書き込む
    try:
        lock_path.write_text(str(os.getpid()), encoding="utf-8")
    except Exception:
        pass
    logger.info("singleton ロック取得: pid=%d port=%d", os.getpid(), port)
    return lock  # 参照を保持してプロセス終了まで解放しない


if __name__ == "__main__":
    try:
        _port = int(os.environ.get("PORT", os.environ.get("KEIBA_PORT", SERVER_PORT)))
        # 二重起動防止: 別プロセスが既に起動していたら即終了
        _singleton = _acquire_singleton_lock(_port)
        run_server(port=_port)
    except SystemExit:
        raise
    except Exception as _fatal:
        logger.critical("ダッシュボード異常終了: %s", _fatal, exc_info=True)
