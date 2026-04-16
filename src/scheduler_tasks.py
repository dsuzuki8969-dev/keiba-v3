"""
スケジューラー用タスク関数群 — dashboard.py と scheduler.py の両方から利用
Flask コンテキスト非依存。
"""
import json
import os
import threading
import time
from datetime import datetime

from config.settings import (
    COURSE_DB_COLLECTOR_STATE_PATH,
    COURSE_DB_PRELOAD_PATH,
    OUTPUT_DIR,
    PROJECT_ROOT,
)
from src.log import get_logger

logger = get_logger(__name__)

# ── 認証クライアント（シングルトン） ──
_auth_client = None
_auth_client_ts = 0.0
_AUTH_CLIENT_TTL = 1800
_auth_client_lock = threading.Lock()

_official_odds_scraper = None

MARK_FREEZE_MINUTES = 10


# ================================================================
# ヘルパー
# ================================================================

def get_auth_client(force_refresh: bool = False):
    """認証済み netkeiba クライアントを返す"""
    global _auth_client, _auth_client_ts
    with _auth_client_lock:
        now = time.time()
        if _auth_client is not None and not force_refresh and (now - _auth_client_ts) < _AUTH_CLIENT_TTL:
            return _auth_client
        try:
            from src.scraper.auth import AuthenticatedClient
            client = AuthenticatedClient()
            client.login()
            if client.session.cookies.get("nkauth") or client.session.cookies.get("netkeiba"):
                _auth_client = client
                _auth_client_ts = now
                logger.info("認証済みクライアント初期化/更新成功")
                return _auth_client
        except Exception as e:
            logger.debug("認証クライアント初期化失敗: %s", e)
        return None


def get_official_odds_scraper():
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


def recalc_divergence(h: dict):
    """馬のdict内 odds と predicted_tansho_odds から乖離率を再計算"""
    pred_o = h.get("predicted_tansho_odds")
    real_o = h.get("odds")
    if not pred_o or pred_o <= 0 or not real_o or real_o <= 0:
        return
    ratio = round(real_o / pred_o, 2)
    signal = "×"
    for label, threshold in [("S", 2.0), ("A", 1.5), ("B", 1.2), ("C", 0.8)]:
        if ratio >= threshold:
            signal = label
            break
    h["odds_divergence"] = ratio
    h["divergence_signal"] = signal


def is_marks_frozen(race: dict) -> bool:
    """発走時刻の MARK_FREEZE_MINUTES 分前以降なら True を返す"""
    post_time = race.get("post_time", "")
    if not post_time:
        return False
    try:
        now = datetime.now()
        h, m = map(int, post_time.split(":"))
        race_start = now.replace(hour=h, minute=m, second=0, microsecond=0)
        diff = (race_start - now).total_seconds()
        return diff <= MARK_FREEZE_MINUTES * 60
    except Exception:
        return False


# ================================================================
# オッズ更新
# ================================================================

def run_odds_update(date_key: str, cancel_event: threading.Event | None = None,
                    progress_callback=None):
    """
    指定日のオッズ・馬体重・三連複オッズを一括更新。
    date_key: "YYYYMMDD" 形式
    cancel_event: 中断用イベント（None なら中断不可）
    progress_callback: (current, total, race_label) を受け取るコールバック
    戻り値: 更新レース数
    """
    pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
    if not os.path.exists(pred_file):
        logger.warning("pred.json 未存在: %s", pred_file)
        return 0

    official = get_official_odds_scraper()

    nk_scraper = None
    def _get_nk():
        nonlocal nk_scraper
        if nk_scraper is None:
            from src.scraper.netkeiba import NetkeibaClient, OddsScraper
            client = get_auth_client() or NetkeibaClient(no_cache=True)
            nk_scraper = OddsScraper(client)
        return nk_scraper

    with open(pred_file, "r", encoding="utf-8") as pf:
        pred = json.load(pf)

    races = pred.get("races", [])
    total_races = len(races)
    pred_modified = False
    count = 0
    weight_fetch_failed = False
    live_odds: dict = {}

    for race_idx, race in enumerate(races):
        if cancel_event and cancel_event.is_set():
            logger.info("オッズ更新: ユーザーにより中断")
            break

        race_id = race.get("race_id")
        if not race_id:
            continue
        venue_name = race.get("venue", "")
        race_no_val = race.get("race_no", "")
        label = f"{venue_name}{race_no_val}R"
        if progress_callback:
            progress_callback(race_idx + 1, total_races, label)

        # 単勝オッズ
        try:
            result = {}
            if official:
                try:
                    result = official.get_tansho(race_id)
                except Exception as oe:
                    logger.debug("公式オッズ失敗 %s: %s", race_id, oe)
            if not result:
                try:
                    result = _get_nk().get_tansho(race_id)
                except Exception:
                    pass
            if result:
                live_odds[race_id] = {
                    str(hno): [odds, rank]
                    for hno, (odds, rank) in result.items()
                }
                for h in race.get("horses", []):
                    hno = str(h.get("horse_no", ""))
                    if hno in live_odds[race_id]:
                        h["odds"] = live_odds[race_id][hno][0]
                        h["popularity"] = live_odds[race_id][hno][1]
                        recalc_divergence(h)
                        # EV再計算: win_prob × 実オッズ
                        _wp = h.get("win_prob") or 0
                        _o = h["odds"]
                        h["ev"] = round(_wp * _o, 3) if _wp > 0 and _o > 0 else None
                        pred_modified = True
                # 人気順位再計算
                active = [h for h in race.get("horses", [])
                          if h.get("odds") and h["odds"] > 0]
                active.sort(key=lambda h: h.get("odds", 9999))
                for rank, hh in enumerate(active, 1):
                    hh["popularity"] = rank
                count += 1
        except Exception as e:
            logger.warning("odds fetch failed race_id=%s: %s", race_id, e)

        # 三連複オッズ
        try:
            san_map = {}
            try:
                san_map = _get_nk().get_sanrenpuku_odds(race_id)
            except Exception:
                pass
            if not san_map and official:
                try:
                    san_map = official.get_sanrenpuku_odds(race_id)
                except Exception:
                    pass
            if san_map:
                for t in race.get("tickets", []):
                    if t.get("type") != "三連複":
                        continue
                    combo = t.get("combo", [])
                    if len(combo) == 3:
                        key = tuple(sorted(int(x) for x in combo))
                        if key in san_map:
                            t["odds"] = round(san_map[key], 1)
                            prob = t.get("prob", 0)
                            if prob > 0:
                                t["ev"] = round(prob * san_map[key] * 100, 1)
                            pred_modified = True
        except Exception as se:
            logger.debug("三連複オッズ取得失敗 %s: %s", race_id, se)

        # 馬体重
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
                weights = _get_nk().get_weights(race_id)
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
                else:
                    logger.debug("weight fetch skipped race_id=%s: %s", race_id, e)

        # ばんえい水分量 + AI見解
        if race.get("is_banei") and official:
            try:
                moisture = official.get_banei_moisture(race_id)
                if moisture is not None:
                    old_wc = race.get("water_content")
                    if old_wc != moisture:
                        race["water_content"] = moisture
                        pred_modified = True
            except Exception:
                pass
            try:
                from src.calculator.calibration import generate_banei_comment_dict
                new_comment = generate_banei_comment_dict(race)
                if new_comment and new_comment != race.get("pace_comment"):
                    race["pace_comment"] = new_comment
                    pred_modified = True
            except Exception:
                pass

    # live_odds.json 保存
    out_path = os.path.join(OUTPUT_DIR, f"{date_key}_live_odds.json")
    with open(out_path, "w", encoding="utf-8") as of:
        json.dump(live_odds, of, ensure_ascii=False)

    # result_cname 取得
    if official:
        for race in races:
            rid = race.get("race_id")
            if rid:
                try:
                    rc = official.get_result_cname(rid)
                    if rc and rc != race.get("result_cname"):
                        race["result_cname"] = rc
                        pred_modified = True
                except Exception:
                    pass

    # 取消解除: オッズが確定した馬のis_scratchedを解除
    _unscratch_count = 0
    for race in races:
        horses = race.get("horses", [])
        for h in horses:
            if h.get("is_scratched") and h.get("odds") is not None and h.get("popularity") is not None:
                h["is_scratched"] = False
                _unscratch_count += 1
                logger.info("取消解除: %s %sR 馬番%s (odds=%.1f)",
                            race.get("venue", ""), race.get("race_no", ""),
                            h.get("horse_no", ""), h.get("odds", 0))
    if _unscratch_count:
        logger.info("取消解除: %d頭のis_scratchedを解除", _unscratch_count)
        pred_modified = True

    # 確率・印を人気別統計ブレンドで再計算
    try:
        from src.calculator.popularity_blend import (
            blend_probabilities_dict,
            load_popularity_stats,
            reassign_marks_dict,
        )
        pop_stats = load_popularity_stats()
        if pop_stats:
            for race in races:
                horses = race.get("horses", [])
                if any(h.get("popularity") for h in horses):
                    blend_probabilities_dict(
                        horses, race.get("venue", ""),
                        race.get("is_jra", True), len(horses), pop_stats,
                    )
                    if not is_marks_frozen(race):
                        reassign_marks_dict(horses)
    except Exception as e:
        logger.warning("確率再計算失敗: %s", e)

    # pred.json にオッズ・馬体重 + タイムスタンプを書き戻し
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    pred["odds_updated_at"] = ts
    try:
        with open(pred_file, "w", encoding="utf-8") as wf:
            json.dump(pred, wf, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("pred.json 書き戻し失敗: %s", e)

    logger.info("オッズ更新完了: %d/%dレース (date=%s)", count, total_races, date_key)
    return count


# ================================================================
# DB更新
# ================================================================

def run_db_update(date_str: str, progress_callback=None):
    """
    コースDB + race_log + キャッシュクリア。
    date_str: "YYYY-MM-DD" 形式
    """
    log_entries = []

    # [1/3] コースDB更新
    if progress_callback:
        progress_callback(1, 3, "コースDB更新中...")
    log_entries.append("コースDB更新開始")
    try:
        from src.scraper.course_db_collector import collect_course_db_from_results
        from src.scraper.netkeiba import NetkeibaClient, RaceListScraper
        client = NetkeibaClient()
        rls = RaceListScraper(client)
        collect_course_db_from_results(
            client, rls, date_str, date_str,
            COURSE_DB_PRELOAD_PATH,
            state_path=COURSE_DB_COLLECTOR_STATE_PATH,
        )
        log_entries.append("✓ コースDB更新完了")
    except Exception as e:
        logger.warning("コースDB更新失敗: %s", e, exc_info=True)
        log_entries.append(f"✗ コースDB更新失敗: {e}")

    # [2/3] レース戦績DB更新
    if progress_callback:
        progress_callback(2, 3, "レース戦績DB更新中...")
    log_entries.append("race_log更新開始")
    try:
        from src.database import populate_race_log_from_predictions
        new_rows = populate_race_log_from_predictions()
        log_entries.append(f"✓ race_log更新完了 (新規{new_rows:,}件)")
    except Exception as e:
        logger.warning("race_log更新失敗: %s", e, exc_info=True)
        log_entries.append(f"⚠ race_log更新失敗: {e}")

    # [3/3] キャッシュクリア
    if progress_callback:
        progress_callback(3, 3, "キャッシュ再構築中...")
    log_entries.append("✓ キャッシュ再構築完了")

    logger.info("DB更新完了: %s", date_str)
    return log_entries


# ================================================================
# レースID取得
# ================================================================

def get_race_ids(date_str: str) -> list:
    """指定日のレースID一覧を取得（開催なしなら空リスト）"""
    try:
        from src.scraper.netkeiba import NetkeibaClient, RaceListScraper
        client = NetkeibaClient()
        rls = RaceListScraper(client)
        ids = rls.get_race_ids(date_str)
        return ids or []
    except Exception as e:
        logger.warning("レースID取得失敗 %s: %s", date_str, e)
        return []


# ================================================================
# 発走時刻取得
# ================================================================

def get_post_times(date_key: str) -> dict:
    """
    pred.json から {race_id: "HH:MM"} のマッピングを返す。
    date_key: "YYYYMMDD" 形式
    """
    pred_file = os.path.join(PROJECT_ROOT, "data", "predictions", f"{date_key}_pred.json")
    if not os.path.exists(pred_file):
        return {}
    try:
        with open(pred_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        result = {}
        for race in data.get("races", []):
            rid = race.get("race_id", "")
            pt = race.get("post_time", "")
            if rid and pt:
                result[rid] = pt
        return result
    except Exception as e:
        logger.warning("post_time取得失敗: %s", e)
        return {}
