"""
競馬解析マスターシステム v3.0 - 設定ファイル
"""

import os
import threading

# ============================================================
# パス（プロジェクトルート相対・全環境で動作）
# ============================================================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# ============================================================
# データベース
# ============================================================
DATABASE_PATH = os.path.join(DATA_DIR, "keiba.db")

# ============================================================
# サーバー設定（WEB公開対応）
# ============================================================
SERVER_HOST = os.environ.get("KEIBA_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("KEIBA_PORT", "5051"))
AUTH_ENABLED = os.environ.get("KEIBA_AUTH", "false").lower() == "true"
AUTH_USERNAME = os.environ.get("KEIBA_USER", "admin")
AUTH_PASSWORD = os.environ.get("KEIBA_PASS", "")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
KEIBABOOK_CACHE_DIR = os.path.join(CACHE_DIR, "keibabook")
CACHE_MAX_AGE_SEC = 24 * 3600  # キャッシュ有効期限（秒）。24時間経過で再取得
PERSONNEL_DB_PATH = os.path.join(DATA_DIR, "personnel_db.json")
COURSE_DB_PRELOAD_PATH = os.path.join(DATA_DIR, "course_db_preload.json")
COURSE_DB_COLLECTOR_STATE_PATH = os.path.join(DATA_DIR, "course_db_collector_state.json")
TRAINER_BASELINE_DB_PATH = os.path.join(DATA_DIR, "trainer_baseline_db.json")
BLOODLINE_DB_PATH = os.path.join(DATA_DIR, "bloodline_db.json")  # 父馬・母父馬×距離別成績
POPULARITY_RATES_PATH = os.path.join(DATA_DIR, "popularity_rates.json")  # 人気別実績統計テーブル
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
PREDICTIONS_DIR = os.path.join(DATA_DIR, "predictions")
RESULTS_DIR = os.path.join(DATA_DIR, "results")

# ============================================================
# LLM見解生成 (Anthropic Claude API)
# ============================================================
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ============================================================
# Phase設定
# ============================================================
PHASE = 1  # 1: netkeiba+競馬ブック / 2: JRA-VAN

# ============================================================
# データソース (Phase 1)
# ============================================================
DATASOURCE = {
    "jra": "netkeiba",
    "chiho": "netkeiba",  # 地方競馬
    "training": "keibaguide",  # 調教データ: 競馬ブック
}

# ============================================================
# 偏差値レンジ（設計書 第0章）
# ============================================================
DEVIATION = {
    "ability": {"min": 20, "max": 100},
    "pace": {"min": 20, "max": 100},
    "course": {"min": 20, "max": 100},
    "composite": {"min": 20, "max": 100},
}

# ============================================================
# 総合偏差値の重み (D-1 / I-3 B案)
# ============================================================
# 6因子 composite 重み（能力/展開/適性/騎手/調教師/血統 = 合計1.00）
# 調教は加算ではなく能力・展開への乗算ボーナスとして適用（models.py composite参照）
COMPOSITE_WEIGHTS = {
    "ability":   0.32,   # 能力偏差値
    "pace":      0.30,   # 展開偏差値
    "course":    0.06,   # コース適性
    "jockey":    0.13,   # 騎手偏差値
    "trainer":   0.14,   # 調教師偏差値
    "bloodline": 0.05,   # 血統偏差値
}

# 競馬場別6因子重み (ML特徴量重要度分析 2024-01〜2026-02 から算出)
# 調教は加算ではなく能力・展開への乗算ボーナスとして適用（models.py composite参照）
# venue_master.py の競馬場名をキー。
VENUE_COMPOSITE_WEIGHTS: dict[str, dict[str, float]] = {
    # --- JRA 10場 ---
    "東京": {"ability": 0.383, "pace": 0.249, "course": 0.051, "jockey": 0.122, "trainer": 0.142, "bloodline": 0.053},  # 自動較正 n=16,270
    "中山": {"ability": 0.34, "pace": 0.33, "course": 0.06, "jockey": 0.105, "trainer": 0.115, "bloodline": 0.05},  # 自動較正 n=16,500
    "阪神": {"ability": 0.346, "pace": 0.277, "course": 0.058, "jockey": 0.138, "trainer": 0.138, "bloodline": 0.043},  # 自動較正 n=9,457
    "京都": {"ability": 0.35, "pace": 0.292, "course": 0.05, "jockey": 0.133, "trainer": 0.117, "bloodline": 0.058},  # 自動較正 n=18,078
    "中京": {"ability": 0.335, "pace": 0.277, "course": 0.061, "jockey": 0.128, "trainer": 0.158, "bloodline": 0.041},  # 自動較正 n=10,140
    "小倉": {"ability": 0.282, "pace": 0.346, "course": 0.079, "jockey": 0.135, "trainer": 0.11, "bloodline": 0.048},  # 自動較正 n=9,233
    "新潟": {"ability": 0.324, "pace": 0.288, "course": 0.084, "jockey": 0.13, "trainer": 0.139, "bloodline": 0.035},  # 自動較正 n=8,655
    "福島": {"ability": 0.273, "pace": 0.368, "course": 0.052, "jockey": 0.124, "trainer": 0.143, "bloodline": 0.04},  # 自動較正 n=6,804
    "札幌": {"ability": 0.296, "pace": 0.285, "course": 0.069, "jockey": 0.155, "trainer": 0.171, "bloodline": 0.024},  # 自動較正 n=4,180
    "函館": {"ability": 0.265, "pace": 0.315, "course": 0.057, "jockey": 0.125, "trainer": 0.176, "bloodline": 0.062},  # 自動較正 n=3,541
    # --- NAR 15場 ---
    "大井": {"ability": 0.353, "pace": 0.387, "course": 0.052, "jockey": 0.113, "trainer": 0.058, "bloodline": 0.037},  # 自動較正 n=29,767
    "川崎": {"ability": 0.275, "pace": 0.429, "course": 0.095, "jockey": 0.085, "trainer": 0.077, "bloodline": 0.039},  # 自動較正 n=18,021
    "船橋": {"ability": 0.301, "pace": 0.354, "course": 0.073, "jockey": 0.114, "trainer": 0.104, "bloodline": 0.054},  # 自動較正 n=17,231
    "浦和": {"ability": 0.216, "pace": 0.569, "course": 0.065, "jockey": 0.073, "trainer": 0.054, "bloodline": 0.023},  # 自動較正 n=15,271
    "門別": {"ability": 0.32, "pace": 0.444, "course": 0.056, "jockey": 0.077, "trainer": 0.042, "bloodline": 0.061},  # 自動較正 n=19,779
    "盛岡": {"ability": 0.276, "pace": 0.464, "course": 0.07, "jockey": 0.08, "trainer": 0.064, "bloodline": 0.046},  # 自動較正 n=14,630
    "水沢": {"ability": 0.236, "pace": 0.547, "course": 0.07, "jockey": 0.061, "trainer": 0.047, "bloodline": 0.039},  # 自動較正 n=15,927
    "金沢": {"ability": 0.23, "pace": 0.599, "course": 0.054, "jockey": 0.042, "trainer": 0.038, "bloodline": 0.037},  # 自動較正 n=17,796
    "笠松": {"ability": 0.243, "pace": 0.521, "course": 0.064, "jockey": 0.082, "trainer": 0.048, "bloodline": 0.042},  # 自動較正 n=19,089
    "名古屋": {"ability": 0.243, "pace": 0.535, "course": 0.056, "jockey": 0.08, "trainer": 0.031, "bloodline": 0.055},  # 自動較正 n=34,077
    "園田": {"ability": 0.284, "pace": 0.517, "course": 0.059, "jockey": 0.056, "trainer": 0.033, "bloodline": 0.051},  # 自動較正 n=33,779
    "姫路": {"ability": 0.288, "pace": 0.472, "course": 0.051, "jockey": 0.077, "trainer": 0.084, "bloodline": 0.028},  # 自動較正 n=8,294
    "高知": {"ability": 0.289, "pace": 0.496, "course": 0.055, "jockey": 0.076, "trainer": 0.031, "bloodline": 0.053},  # 自動較正 n=25,804
    "佐賀": {"ability": 0.228, "pace": 0.578, "course": 0.052, "jockey": 0.067, "trainer": 0.029, "bloodline": 0.046},  # 自動較正 n=28,351
    # --- ばんえい ---
    "帯広": {"ability": 0.28, "pace": 0.27, "course": 0.05, "jockey": 0.15, "trainer": 0.15, "bloodline": 0.10},  # Phase5: ばんえい適性スコア導入に伴い重み修正
}

# ばんえい馬券購入フィルタ: 許可する最低自信度
# "S" → SS/Sのみ購入, "A" → SS/S/Aまで購入, "B" → 通常NARと同じ
BANEI_MIN_CONFIDENCE = "A"

# 後方互換: 旧COMPOSITE_PERSONNEL_WEIGHTS を参照するコード向け
# Phase 10Aで6因子化したため、補正項としての使用は廃止
COMPOSITE_PERSONNEL_WEIGHTS = {
    "jockey": 0.13,
    "trainer": 0.14,
    "bloodline": 0.05,
}


# 展開(pace)重みの上限。地方競馬場で展開偏差値が composite を支配する問題を防止。
# JRA最大=小倉40.2% なのでJRA側に影響なし。超過分は ability に再配分。
PACE_WEIGHT_CAP = 0.35

# ============================================================
# 展開因子改善フラグ（段階的導入・A/Bテスト対応）
# ============================================================
PACE_DYNAMIC_WEIGHT_ENABLED = True       # 改善1: 条件別ウェイト動的化
PACE_FIELD_STYLE_RELATIVE = True         # 改善2: コース脚質バイアス相対化
PACE_TRAJECTORY_FIX_ENABLED = True       # 改善3: 軌跡方向スコア復活
PACE_MERGE_LAST3F_INTO_BALANCE = True    # 改善4: ❶❷共線性解消
PACE_NAR_POSITION_FIX_ENABLED = True     # 改善5: NAR位置取り改善
PACE_LAST3F_RANGE_HALVED = True          # 改善6: ❶レンジ縮小(-12~+12 → -6~+6)
PACE_ESCAPE_SCORING_V2 = True            # 改善7: 逃げ馬判定精度向上

# 改善1: 条件別ペースウェイト動的調整の倍率
PACE_WEIGHT_TURF_LONG_LARGE = 1.15      # 芝 + 1800m以上 + 14頭以上
PACE_WEIGHT_TURF_LONG = 1.08            # 芝 + 1800m以上（頭数不問）
PACE_WEIGHT_DIRT_SHORT_SMALL = 0.75      # ダ + 1200m以下 + 10頭以下
PACE_WEIGHT_DIRT_SHORT = 0.85            # ダ + 1200m以下（頭数不問）
PACE_WEIGHT_LARGE_FIELD = 1.05          # 14頭以上（面不問）
PACE_WEIGHT_SMALL_FIELD = 0.90          # 10頭以下（面不問）


def get_composite_weights(venue_name: str = None, *,
                          surface: str = None,
                          field_size: int = None,
                          distance: int = None) -> dict[str, float]:
    """
    競馬場名＋レース条件に応じた重みを返す。

    優先順位:
      1. 較正済みファイル (data/models/venue_weights_calibrated.json) の venue_code ベース値
         → venue_code と venue_name の対応は _CALIB_VC_TO_NAME で解決
      2. settings.py の VENUE_COMPOSITE_WEIGHTS (venue_name ベース手動設定)
      3. デフォルト COMPOSITE_WEIGHTS

    較正ファイルがなければ従来通り settings.py の値を使用する。
    改善1 (PACE_DYNAMIC_WEIGHT_ENABLED) が有効な場合、面/距離/頭数で pace ウェイトを動的補正。
    pace 重みが PACE_WEIGHT_CAP を超える場合、超過分を ability に再配分する。
    """
    # 較正済み重みキャッシュ: {venue_name: {"ability":, "pace":, "course":}}
    # モジュール初回インポート時に一度だけ読み込む
    calib = _get_calibrated_weights()
    if venue_name and venue_name in calib:
        weights = dict(calib[venue_name])  # コピーして元データを汚さない
    elif venue_name and venue_name in VENUE_COMPOSITE_WEIGHTS:
        weights = dict(VENUE_COMPOSITE_WEIGHTS[venue_name])
    else:
        weights = dict(COMPOSITE_WEIGHTS)

    # 改善1: 条件別ペースウェイト動的調整
    if PACE_DYNAMIC_WEIGHT_ENABLED and "pace" in weights:
        _is_turf = surface == "芝" if surface else False
        _is_dirt = surface in ("ダート", "ダ") if surface else False
        _is_long = (distance or 0) >= 1800
        _is_sprint = (distance or 0) <= 1200
        _is_large = (field_size or 0) >= 14
        _is_small = 0 < (field_size or 0) <= 10

        # 複合条件（優先）→ 単独条件（フォールバック）
        if _is_turf and _is_long and _is_large:
            weights["pace"] *= PACE_WEIGHT_TURF_LONG_LARGE
        elif _is_dirt and _is_sprint and _is_small:
            weights["pace"] *= PACE_WEIGHT_DIRT_SHORT_SMALL
        elif _is_turf and _is_long:
            weights["pace"] *= PACE_WEIGHT_TURF_LONG
        elif _is_dirt and _is_sprint:
            weights["pace"] *= PACE_WEIGHT_DIRT_SHORT
        elif _is_large:
            weights["pace"] *= PACE_WEIGHT_LARGE_FIELD
        elif _is_small:
            weights["pace"] *= PACE_WEIGHT_SMALL_FIELD

        # 正規化: 合計1.0に戻す
        _total = sum(weights.values())
        if _total > 0:
            weights = {k: v / _total for k, v in weights.items()}

    # pace重みキャップ: 超過分をabilityに再配分
    if weights.get("pace", 0) > PACE_WEIGHT_CAP:
        excess = weights["pace"] - PACE_WEIGHT_CAP
        weights["pace"] = PACE_WEIGHT_CAP
        weights["ability"] = weights.get("ability", 0) + excess

    return weights


# 較正済みファイルのキャッシュ（None = 未ロード、{} = ロード済みだが空）
_CALIB_WEIGHTS_CACHE: "dict | None" = None
_CALIB_WEIGHTS_LOCK = threading.Lock()

# venue_code → 競馬場名 対応表（settings.py の VENUE_COMPOSITE_WEIGHTS キーに合わせる）
_CALIB_VC_TO_NAME: dict[str, str] = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
    "30": "門別", "35": "盛岡", "36": "水沢",
    "42": "浦和", "43": "船橋", "44": "大井", "45": "川崎",
    "46": "金沢", "47": "笠松", "48": "名古屋",
    "49": "園田", "50": "園田",
    "51": "姫路",
    "52": "帯広", "65": "帯広",
    "54": "高知", "55": "佐賀",
}


def _get_calibrated_weights() -> dict[str, dict]:
    """
    data/models/venue_weights_calibrated.json を一度だけ読み込んでキャッシュする。
    ファイルが存在しないか読み込み失敗の場合は空辞書を返す。
    スレッドセーフ: ダブルチェックロッキングで並列初期化を防止。
    """
    global _CALIB_WEIGHTS_CACHE
    if _CALIB_WEIGHTS_CACHE is not None:
        return _CALIB_WEIGHTS_CACHE

    with _CALIB_WEIGHTS_LOCK:
        # ダブルチェック: 他スレッドがLock待ち中にキャッシュ済みになった場合
        if _CALIB_WEIGHTS_CACHE is not None:
            return _CALIB_WEIGHTS_CACHE

        result: dict[str, dict] = {}
        calib_path = os.path.join(PROJECT_ROOT, "data", "models", "venue_weights_calibrated.json")
        if not os.path.exists(calib_path):
            _CALIB_WEIGHTS_CACHE = result
            return _CALIB_WEIGHTS_CACHE

        try:
            import json
            with open(calib_path, "r", encoding="utf-8") as f:
                raw: dict = json.load(f)
            for vc, weights in raw.items():
                name = _CALIB_VC_TO_NAME.get(str(vc).zfill(2), "")
                if not name:
                    continue
                # 必須キーが揃っている場合のみ採用
                if all(k in weights for k in ("ability", "pace", "course")):
                    # 較正ファイルが旧3因子形式の場合、6因子に拡張
                    w = {
                        "ability": float(weights["ability"]),
                        "pace":    float(weights["pace"]),
                        "course":  float(weights["course"]),
                    }
                    if "jockey" not in weights:
                        # 旧3因子を合計0.80にスケール、残り0.20をpersonnel
                        s = w["ability"] + w["pace"] + w["course"]
                        if s > 0:
                            scale = 0.80 / s
                            w["ability"] *= scale
                            w["pace"] *= scale
                            w["course"] *= scale
                        w["jockey"] = 0.10
                        w["trainer"] = 0.05
                        w["bloodline"] = 0.05
                    else:
                        w["jockey"] = float(weights["jockey"])
                        w["trainer"] = float(weights["trainer"])
                        w["bloodline"] = float(weights["bloodline"])
                    result[name] = w
        except Exception:
            # 読み込み失敗しても既存動作に影響しない
            result = {}

        _CALIB_WEIGHTS_CACHE = result  # 構築完了後に一括代入（atomic）

    return _CALIB_WEIGHTS_CACHE


def reload_calibrated_weights() -> int:
    """
    較正済み重みキャッシュをクリアして再読み込みする。
    ダッシュボードの「データ更新」ボタンなどから呼び出せる。
    Returns: 読み込んだ競馬場数
    """
    global _CALIB_WEIGHTS_CACHE
    with _CALIB_WEIGHTS_LOCK:
        _CALIB_WEIGHTS_CACHE = None
    return len(_get_calibrated_weights())

# ============================================================
# 加重平均偏差値の重み (C-2)
# 5走分: 直近→古い順
# ============================================================
WA_WEIGHTS = [0.35, 0.25, 0.20, 0.12, 0.08]  # 直近走重視（LightGBM特徴量設計と整合）

# Phase 4-1: 距離帯別WA重み
# 短距離: 直近走の好不調が予測力高い → 直近走重視
# 長距離: 過去実績の安定性が重要 → 均等寄り
WA_WEIGHTS_BY_DISTANCE = {
    "sprint":  [0.40, 0.25, 0.20, 0.10, 0.05],  # 1000-1400m: 直近重視
    "mile":    [0.35, 0.25, 0.20, 0.12, 0.08],  # 1500-1600m: 現行維持（最高的中率）
    "middle":  [0.30, 0.25, 0.22, 0.14, 0.09],  # 1700-2200m: 安定重視
    "long":    [0.28, 0.24, 0.22, 0.16, 0.10],  # 2300m+: 実績重視
}


def get_wa_weights(distance: int) -> list:
    """距離に応じたWA重みを返す"""
    if not PIPELINE_V2_ENABLED:
        return WA_WEIGHTS
    if distance <= 1400:
        return WA_WEIGHTS_BY_DISTANCE["sprint"]
    elif distance <= 1600:
        return WA_WEIGHTS_BY_DISTANCE["mile"]
    elif distance <= 2200:
        return WA_WEIGHTS_BY_DISTANCE["middle"]
    else:
        return WA_WEIGHTS_BY_DISTANCE["long"]

# ============================================================
# 距離係数・換算定数 (B-2)
# ============================================================
DISTANCE_BASE = 1600  # 距離係数の基準距離(m)
CONVERSION_CONSTANT = 3.5  # レガシー互換用（デフォルト値）

# 案1: 距離帯別変換定数（短距離ほど1秒の差が大きい）
# 実データ・Walk-Forward CV AUC最大化から設定
CONVERSION_CONSTANT_BY_DIST = {
    1000: 5.0,
    1150: 4.8,
    1200: 4.5,
    1400: 4.0,
    1600: 3.7,
    1800: 3.4,
    2000: 3.2,
    2200: 3.0,
    2400: 2.8,
    2600: 2.7,
    3000: 2.5,
    3200: 2.4,
    3600: 2.2,
}


def get_conversion_constant(distance: int) -> float:
    """距離に最も近い帯の変換定数を返す"""
    keys = sorted(CONVERSION_CONSTANT_BY_DIST.keys())
    best = keys[0]
    for k in keys:
        if k <= distance:
            best = k
        else:
            break
    return CONVERSION_CONSTANT_BY_DIST[best]

# ============================================================
# 斤量補正 (D-5)
# ============================================================
WEIGHT_CORRECTION_PER_KG = 0.15  # 1kgあたり0.15秒

# ============================================================
# C-3: α可変ルール
# ============================================================
ALPHA_DEFAULT = 0.5  # デフォルトα
ALPHA_DECLINE_PENALTY = 0.10  # E-1下降時のα減少量
ALPHA_DIVERGENCE_THRESHOLD = 5.0  # 乖離幅でα変動するしきい値

# ============================================================
# 展開偏差値ベーススコア (F-0)
# ============================================================
PACE_BASE = 50
PACE_AI_ADJUSTMENT_MAX = 18  # AI層±18pt（100スケール対応: 旧12→18）

# F-0 各コンポーネントの範囲（100スケール対応: 旧値×1.5）
PACE_COMPONENTS = {
    "末脚評価": (-12, 12),
    "位置取り×末脚バランス": (-12, 12),
    "枠順バイアス": (-8, 8),
    "コース脚質バイアス": (-8, 8),
    "騎手展開影響": (-6, 6),
}

# ============================================================
# コース適性偏差値 (G-0)
# ============================================================
COURSE_BASE = 50
COURSE_AI_ADJUSTMENT_MAX = 12  # AI層±12pt（100スケール対応: 旧8→12）

COURSE_COMPONENTS = {
    "コース実績": (-8, 8),
    "コース形状相性": (-5, 5),
    "騎手コース影響": (-5, 5),
}

# ============================================================
# E-3: 着差評価指数 加重(C-2反映時)
# ============================================================
CHAKUSA_INDEX_WEIGHT = 1.5

# ============================================================
# 枠順バイアス (G-2) 頭数別ゾーン数
# ============================================================
WAKU_ZONE_RULE = {
    10: 5,  # 10頭以上→5ゾーン
    8: 3,  # 8-9頭→3ゾーン
    0: 0,  # 7頭以下→バイアス0
}

# ============================================================
# 過去走参照範囲 (C-1)
# ============================================================
RACE_HISTORY_DAYS_DEFAULT = 365  # 通常: 1年以内
RACE_HISTORY_DAYS_休養明け = 730  # 長期休養明け: 2年
RACE_HISTORY_MAX_RUNS = 5  # 最大5走
RACE_HISTORY_休養DECAY = 0.5  # 長期休養明けの減衰係数

# ============================================================
# 穴馬・危険馬 閾値 (I-1, I-2)
# ============================================================
ANA_ODDS_THRESHOLD = 10.0  # 穴馬: 10倍以上
OANA_ODDS_THRESHOLD = 50.0  # 大穴: 50倍以上
ANA_NINKI_THRESHOLD = 5  # 穴馬: 5番人気以下
ANA_SCORE_A = 7  # 穴馬スコアA閾値（7pt以上→穴A）
ANA_SCORE_B = 5  # 穴馬スコアB閾値
KIKEN_NINKI_MAX = 3  # 危険馬: 1-3番人気
KIKEN_ODDS_MAX = 10.0  # 危険馬: 10倍未満
KIKEN_SCORE_A = 5  # 危険スコアA閾値
KIKEN_SCORE_B = 3  # 危険スコアB閾値
KIKEN_ML_GUARD_RANK = 2    # win_prob rank がこの値以下なら危険馬判定を早期除外
KIKEN_ML_ENDORSE_R3 = -1.5  # win_prob rank 3位の減点（ML endorsement）
KIKEN_ML_ENDORSE_MID = -0.5  # win_prob rank 上位半分の減点

# 特選穴馬
TOKUSEN_SCORE_THRESHOLD = 5.5   # v3: 3.0→5.5に引き上げ（断層ボーナス追加に伴い☆の質を維持）
TOKUSEN_ODDS_THRESHOLD = 15.0   # 対象: オッズ15倍以上
TOKUSEN_MAX_PER_RACE = 2        # 1レース最大2頭

# 特選危険馬
TOKUSEN_KIKEN_SCORE_THRESHOLD = 3.0  # 特選判定閾値（ML×composite二重否定通過後の追加スコア）
TOKUSEN_KIKEN_MAX_PER_RACE = 2      # 1レース最大2頭

# 特選危険馬 必須条件（JRA/NAR分離）
# v2改善(2026-04-13): 1番人気除外 + win_prob絶対値ベース + AND条件化 + comp下位25%
# v3改善(2026-04-13): NAR断層条件追加 + ML wp絶対値 + comp下位30% → NAR複勝率18.9%→11.4%
TOKUSEN_KIKEN_POP_MIN_JRA = 2         # JRA: 2番人気以上（1番人気は×対象外 — 複勝率69.5%を否定するのは無理筋）
TOKUSEN_KIKEN_POP_MIN_NAR = 1         # NAR: 1番人気も対象
TOKUSEN_KIKEN_POP_LIMIT_JRA = 3       # JRA: 3番人気まで
TOKUSEN_KIKEN_POP_LIMIT_NAR = 6       # NAR v3: 6番人気まで拡大（4-6人気+断層下+ML低が有効）
TOKUSEN_KIKEN_ODDS_LIMIT_JRA = 15.0   # JRA: 15倍未満
TOKUSEN_KIKEN_ODDS_LIMIT_NAR = 30.0   # NAR v3: 30倍未満に拡大（4-6人気をカバー）
# win_prob絶対値ベース: 人気別期待win_probの30%未満で×候補
# （例: 2番人気の期待wp=14.8% → 4.4%未満の馬だけが×候補）
TOKUSEN_KIKEN_WP_RATIO = 0.30         # 期待win_probに対する倍率（30%未満で通過）
TOKUSEN_KIKEN_EXPECTED_WP = {          # 人気別の平均win_prob（39,000R実績ベース）
    1: 0.234, 2: 0.148, 3: 0.121,
}
# JRA: AND条件に変更（OR条件は複勝率33%→ AND+wp条件で19%に改善）
TOKUSEN_KIKEN_ML_RANK_PCT_JRA = 0.45  # JRA: 参考値（win_probベースに移行したため補助的）
TOKUSEN_KIKEN_ML_RANK_PCT_NAR = 0.60  # NAR: 参考値（v3ではwp絶対値ベースに移行）
TOKUSEN_KIKEN_COMP_RANK_PCT_JRA = 0.25  # JRA: 下位25%（35%→25%に厳格化）
TOKUSEN_KIKEN_COMP_RANK_PCT_NAR = 0.30  # NAR v3: 下位30%（50%→30%に厳格化、断層条件で補完）

# ============================================================
# 妙味スコア（穴馬評価の回帰ベーススコア）
# 2026年実績23,271頭のロジスティック回帰で導出
# ============================================================
MIRYOKU_W_TOKUSEN   = 0.1492   # tokusen_score（最重要: 32.7%）
MIRYOKU_W_COMPOSITE = 0.1336   # (composite - 45) / 10（29.2%）
MIRYOKU_W_COURSE    = 0.0445   # (course_total - 45) / 10（9.7%）
MIRYOKU_W_ANA       = 0.0386   # ana_score / 5（8.5%）
MIRYOKU_W_PLACE3    = 0.0266   # place3_prob * 10（5.8%）
MIRYOKU_W_JRA       = -0.0643  # JRAペナルティ（14.1%）
MIRYOKU_BIAS        = -2.1620  # 切片

# 妙味グレード閾値（Top N%に基づく）
MIRYOKU_GRADE_SS = 0.92   # Top 1%  複勝49.6% ROI≈440%
MIRYOKU_GRADE_S  = 0.75   # Top 3%  複勝32.2% ROI≈357%
MIRYOKU_GRADE_A  = 0.58   # Top 7%  複勝26.3% ROI≈287%
MIRYOKU_GRADE_B  = 0.42   # Top 15% 複勝16.0% ROI≈216%
MIRYOKU_GRADE_C  = 0.22   # Top 30% 複勝 8.1% ROI≈108%
MIRYOKU_GRADE_D  = 0.02   # Top 60% 複勝11.7% ROI≈141%
# E = それ以下

# ============================================================
# TEKIPAN(◉)パラメータ（JRA/NAR分離）
# ============================================================
TEKIPAN_GAP_JRA = 7.0              # v4: 5.0→7.0（勝率60%/出現1%へ厳選化）
TEKIPAN_GAP_NAR = 5.0              # v4: 据置（NAR gap5で十分な精度）
TEKIPAN_WIN_PROB_JRA = 0.25        # v4: 0.30→0.25（gap7で絞るためwp緩和）
TEKIPAN_WIN_PROB_NAR = 0.35        # v4: 0.25→0.35（勝率72%達成の核心条件）
TEKIPAN_PLACE3_PROB_JRA = 0.70     # v4: 0.65→0.70（複勝率89%へ引上げ）
TEKIPAN_PLACE3_PROB_NAR = 0.75     # v4: 0.0→0.75（NAR◉精度の最大改善ポイント）
TEKIPAN_POP_MAX_JRA = 2            # v4新設: 1-2番人気限定（市場との合意確認）
TEKIPAN_POP_MAX_NAR = 2            # v4新設: 1-2番人気限定（勝率71.8%の核心条件）
# ◉EV下限: 43万馬2.3年分の分析に基づく
# 全期間◉: EV≥0.80で勝率40.6%/回収率138.6% (現状56.2%/118.5%, +20pt)
# 2025H2: EV≥0.80で42.1%/129.4% — 回収率改善しつつ件数維持
# v4: EVフィルター撤廃 — ◉は「予測精度」の印であり「馬券価値」の印ではない
# 低オッズ本命（市場も認める圧倒的1番人気）をEV不足で◉脱落させていた問題を解消
# 旧: EV≥0.80 → 勝率38%(JRA)/30%(NAR)  新: EVなし → 勝率60%(JRA)/72%(NAR)
TEKIPAN_MIN_EV_JRA = 0.0           # v4: 0.80→0.0（撤廃）
TEKIPAN_MIN_EV_NAR = 0.0           # v4: 0.80→0.0（撤廃）

# ============================================================
# 自信度パラメータ（JRA/NAR分離）
# ============================================================
# gap_norm 正規化除数（gap / N → 1.0上限）
CONFIDENCE_GAP_DIVISOR_JRA = 6.0   # JRA: gap_normノイズ低減（4→6）
CONFIDENCE_GAP_DIVISOR_NAR = 8.0   # NAR: gap平均2.82, 8ptで満点（現行維持）

# v5: SS硬性条件は廃止（win_prob/gapゲートに統合）
# 旧SS_GAP/SS_VALUE設定は削除済み

# v5: JRA/NAR別パーセンタイル閾値（目標構成比 SS5% S10% A15% B35% C25% D10%）
# v5変更: value_score（市場信号）除去後の6信号スコアでパーセンタイル再算出
# SS/Sゲートを人気→win_prob/gapに変更（市場フリー化）
CONFIDENCE_THRESHOLDS_JRA = {"SS": 0.7327, "S": 0.6085, "A": 0.4835, "B": 0.2407, "C": 0.0987}
CONFIDENCE_THRESHOLDS_NAR = {"SS": 0.809, "S": 0.7128, "A": 0.61, "B": 0.3501, "C": 0.1361}
# v5: win_prob/gapゲート（市場フリー — 自モデルの確信度で判定）
# 「両方厳格」設定: JRA SS W61.0%/P85.7%, NAR SS W77.2%/P94.3%
CONFIDENCE_WP_GATE_SS_JRA = 0.30   # JRA SS: モデル勝率30%以上
CONFIDENCE_WP_GATE_SS_NAR = 0.35   # NAR SS: モデル勝率35%以上
CONFIDENCE_GAP_GATE_SS_JRA = 6.0   # JRA SS: composite差6.0以上
CONFIDENCE_GAP_GATE_SS_NAR = 7.0   # NAR SS: composite差7.0以上
CONFIDENCE_WP_GATE_S_JRA = 0.22    # JRA S: モデル勝率22%以上
CONFIDENCE_WP_GATE_S_NAR = 0.28    # NAR S: モデル勝率28%以上
CONFIDENCE_GAP_GATE_S_JRA = 4.0    # JRA S: composite差4.0以上
CONFIDENCE_GAP_GATE_S_NAR = 5.0    # NAR S: composite差5.0以上

# ============================================================
# 買い目 期待値閾値 (5-2)
# ============================================================
EV_BUY_STRONG = 120  # ◎買い
EV_BUY_NORMAL = 100  # ○買い
BUY_MAX_TICKETS = 8  # 最大8点

# ============================================================
# 三連複フォーメーション設定 (5-4)
# ============================================================
MAX_FORMATION_TICKETS = 15  # EV上位N点に制限
MIN_FORMATION_EV = 80  # 最低EV%（これ未満は買い目から除外）

# ============================================================
# 自信度別賭け金デフォルト (5-3)
# ============================================================
STAKE_DEFAULT = {
    "SS": 12000,
    "S": 6000,
    "A": 3000,
    "B": 1500,
    "C": 500,
    "D": 0,
}

# ============================================================
# オッズ控除率 (5-1) - 券種別・JRA/地方別
# 払戻率 = 1 - 控除率。例: JRA単勝の払戻率80% = 控除率20%
# ============================================================
# 市場確率アンカリング: モデル推定と市場確率のブレンド比率（0=モデルのみ, 1=市場のみ）
MARKET_BLEND_RATIO = 0.10  # キャリブレーション修正: 旧0.20→0.10（市場ブレンド1回限りで十分）

# 最終確率のメリハリ拡大: べき乗変換（1.0=変更なし, >1.0で差を拡大）
# 正規化後に適用するため、合計値は保たれつつ上位がより突出する
PROB_SHARPNESS = 1.45  # キャリブレーション修正: 旧1.30→1.45（上流圧縮削減に伴い強化）

# 事後キャリブレーション（Isotonic Regression）
USE_POST_CALIBRATOR = True

PAYOUT_RATES = {
    "JRA": {
        "単勝": 0.800,
        "複勝": 0.800,
        "馬連": 0.775,
        "馬単": 0.750,
        "ワイド": 0.775,
        "三連複": 0.750,
        "三連単": 0.725,
    },
    "NAR": {
        "単勝": 0.750,
        "複勝": 0.750,
        "馬連": 0.750,
        "馬単": 0.725,
        "ワイド": 0.750,
        "三連複": 0.750,
        "三連単": 0.700,
    },
    # K-2: フラットキー（JRA/NAR別払戻率適用用）
    "jra_win":       0.800,
    "jra_umaren":    0.775,
    "jra_sanrenpuku": 0.750,
    "nar_win":       0.750,
    "nar_umaren":    0.725,
    "nar_sanrenpuku": 0.700,
}

# 補正係数 (5-1) 頭数別
CORRECTION_UMAREN = {16: 1.8, 12: 1.5, 8: 1.0}  # 馬連
CORRECTION_WIDE = {16: 2.2, 12: 1.7, 8: 1.2}  # ワイド

# ============================================================
# 予想オッズ・乖離検出 (5-5)
# ============================================================
DIVERGENCE_SIGNAL = {
    "S": 2.0,   # 実オッズ/予想オッズ ≧ 2.0 → 超妙味
    "A": 1.5,   # ≧ 1.5 → 妙味大
    "B": 1.2,   # ≧ 1.2 → 妙味あり
    "C": 0.8,   # ≧ 0.8 → 適正〜やや過剰
}
EV_THRESHOLD_BUY = 1.0       # 期待値 ≧ 1.0 で買い対象
EV_THRESHOLD_STRONG = 1.5    # 期待値 ≧ 1.5 で強い買い

# ============================================================
# 調教強度 σ閾値 (J-4)
# ============================================================
TRAINING_INTENSITY = {
    "猛時計": (2.0, float("inf")),
    "やや速い": (1.0, 2.0),
    "通常": (-1.0, 1.0),
    "やや軽め": (-2.0, -1.0),
    "軽め": (float("-inf"), -2.0),
}
TRAINING_EMOJI = {
    "猛時計": "⚡",
    "やや速い": "🔺",
    "通常": "→",
    "やや軽め": "🔻",
    "軽め": "⏸",
    # 競馬ブック表記（強度ラベル）
    "一杯": "⚡",
    "強め": "🔺",
    "馬なり": "→",
    "極軽め": "⏸",
}

# ============================================================
# Phase 12: 表示用偏差値ファクター重み
# 各カテゴリの compute_category_deviation() で使用
# 重みは「そのファクターがカテゴリ評価にどれだけ寄与するか」を示す
# ============================================================
JOCKEY_FACTOR_WEIGHTS = {
    "overall": 1.0,      # 全体複勝率
    "pr_2y": 0.9,        # 直近2年複勝率
    "venue": 0.8,        # 当場複勝率
    "sim_venue": 0.5,    # 類似場複勝率
    "distance": 0.7,     # 距離帯複勝率
    "smile": 0.6,        # SMILE区分複勝率
    "condition": 0.6,    # 馬場状態別複勝率
    "pace": 0.5,         # ペース別複勝率
    "style": 0.5,        # 脚質別複勝率
    "gate": 0.4,         # 枠番帯別複勝率
    "horse": 0.7,        # 騎乗馬別複勝率
}

TRAINER_FACTOR_WEIGHTS = {
    "overall": 1.0,
    "pr_2y": 0.9,
    "venue": 0.8,
    "sim_venue": 0.5,
    "distance": 0.7,
    "smile": 0.6,
    "condition": 0.6,
    "pace": 0.4,
    "style": 0.4,
    "gate": 0.3,
    "horse": 0.6,
}

SIRE_FACTOR_WEIGHTS = {
    "overall": 1.0,
    "smile": 0.8,      # SMILE区分（面×距離帯のプロキシ）
    "condition": 0.6,
    "venue": 0.5,
    "pace": 0.5,
    "style": 0.5,
    "gate": 0.4,
    "jockey": 0.4,
    "trainer": 0.3,
}

BMS_FACTOR_WEIGHTS = {
    "overall": 1.0,
    "smile": 0.8,
    "condition": 0.6,
    "venue": 0.5,
    "pace": 0.5,
    "style": 0.5,
    "gate": 0.4,
    "jockey": 0.4,
    "trainer": 0.3,
}

# 騎手/調教師の base_mean / base_sigma (rate_to_dev 用)
# ファクターは全て「複勝率」（3着以内率）ベース
# JRA: 平均複勝率 ≈ 19% (≈3/16頭), σ ≈ 6%
# NAR: 平均複勝率 ≈ 15% (≈3/12-14頭 + 実力差大), σ ≈ 7%
# 目標分布: SS=2.5%, S=7.5%, A=20%, B=40%, C=20%, D=7.5%, E=2.5% (mean≈52.5, σ_dev≈6.4)
# 条件付きファクターの上方バイアス（場/距離等は成功条件のサブセット）を考慮し base_mean を高めに設定
JOCKEY_BASE_PARAMS_JRA = {"mean": 0.19, "sigma": 0.15}   # 実測mean=0.189
JOCKEY_BASE_PARAMS_NAR = {"mean": 0.258, "sigma": 0.112}  # 実測(30走+372騎手): mean=0.258, σ=0.112
TRAINER_BASE_PARAMS_JRA = {"mean": 0.20, "sigma": 0.10}   # 実測mean=0.217
TRAINER_BASE_PARAMS_NAR = {"mean": 0.26, "sigma": 0.14}   # 実測mean=0.280
# 血統: σを縮小して分散拡大（個別σ→加重平均の0.72倍を考慮）
SIRE_BASE_PARAMS = {"mean": 0.25, "sigma": 0.065}
BMS_BASE_PARAMS = {"mean": 0.26, "sigma": 0.075}

# ============================================================
# 芝ダ転換適性 (Surface Switch Aptitude)
# ============================================================
# 異表面走の基本割引率（50基準で圧縮: 0.65 → dev55の馬は 48+(55-48)*0.65=52.55 に）
SURFACE_SWITCH_BASE_DISCOUNT = 0.65

# 各因子の重み（JRA 2023-2026 馬場転向初戦の三連率レンジから導出）
# 合計 = 1.0
SURFACE_SWITCH_FACTOR_WEIGHTS = {
    "position":     0.348,  # 脚質/位置取り（転向後に前に行けるか）
    "horse_weight": 0.141,  # 馬体重（パワー型はダート向き）
    "age":          0.134,  # 年齢（若い馬ほど一変しやすい）
    "sire":         0.134,  # 父馬の芝ダ適性
    "jockey":       0.105,  # 騎手の馬場別実績
    "gate":         0.083,  # 枠順（ダートは外枠有利）
    "bms":          0.055,  # 母父の芝ダ適性
}

# 転換ボーナス/ペナルティの最大値 (偏差値pt)
# 全因子がMAXに振れた場合の合計が収まるよう設定
SURFACE_SWITCH_MAX_BONUS = 6.0    # 血統◎ + 大型 + 若齢 + 先行 → 最大+6pt
SURFACE_SWITCH_MAX_PENALTY = -4.0  # 血統× + 軽量 + 高齢 + 追込 → 最大-4pt

# ============================================================
# Phase 2: ブレンドパイプライン情報損失削減パラメータ
# ============================================================
# ロールバックフラグ: Falseにすると旧パイプライン（固定85:15、固定alpha）に戻る
PIPELINE_V2_ENABLED = True

# 2-1: composite再推定比率（model_level依存）
# 高精度モデルほど再推定の影響を小さくし、ML予測の情報を保持する
REEST_RATIO_BY_LEVEL = {
    4: 0.03,   # 競馬場専用モデル: 再推定3%のみ（メリハリ改善: 旧0.05）
    3: 0.05,   # JRA馬場×SMILE: 再推定5%（旧0.08）
    2: 0.10,   # JRA全体/NAR: ML情報90%保持（旧0.15）
    1: 0.15,   # 馬場全体フォールバック（旧0.20）
    0: 0.20,   # globalモデル（旧0.25）
}
REEST_RATIO_DEFAULT = 0.10  # 旧パイプライン互換（メリハリ改善: 旧0.15）

# 人気統計ブレンド無効化フラグ
# True=MLの予測をそのまま使う（人気・オッズ統計による補正なし）
# 2026-04-01: 1番人気+10pt補正が印の人気偏重を招いていたため無効化
DISABLE_POPULARITY_BLEND = True

# 2-2: 人気統計ブレンドの動的alpha拡張パラメータ
# model_level >= 3 のとき ALPHA_MODEL_MAX を引き上げ
ALPHA_MODEL_MAX_HIGH = 0.99    # 高精度モデル時はほぼML依存（旧0.95）
ALPHA_MODEL_HIGH_THRESHOLD = 3  # この model_level 以上で ALPHA_MODEL_MAX_HIGH を使用
CONFIDENCE_GAP_V2 = 0.20       # 飽和防止: 旧0.15→0.20に拡大

# 5-0: Level 4モデル品質フィルター
# Lift(本命勝率/ランダム) 1.3x以下 = 競馬場専用モデルがほぼ機能していない
# → Level 2(NAR全体)にフォールバックさせる
VENUE_MODEL_SKIP = {"36", "30"}  # 水沢(1.3x), 門別(1.2x)

# ============================================================
# Phase 11: 順位ベース確率テーブル
# ============================================================
RANK_PROBABILITY_TABLE_PATH = os.path.join(DATA_DIR, "rank_probability_table.json")
USE_RANK_TABLE = True  # Falseで現行softmaxにフォールバック
RANK_TABLE_SHARPNESS = 1.0  # テーブル値シャープ化は無効（最終段PROB_SHARPNESSで統一）

# gap補正パラメータ
RANK_GAP_THRESHOLD_STRONG = 5.0   # これ以上で「一強」判定
RANK_GAP_MULT_MAX = 1.2           # 一強時の最大倍率補正（旧0.6→1.2: 対数補正に対応）
RANK_GAP_FLAT_FACTOR_MAX = 0.15   # 混戦時の均等化係数（メリハリ改善: 旧0.3→0.15）
