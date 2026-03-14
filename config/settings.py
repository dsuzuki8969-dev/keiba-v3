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
    "ability": {"min": 30, "max": 70},
    "pace": {"min": 30, "max": 70},
    "course": {"min": 30, "max": 70},
    "composite": {"min": 30, "max": 70},
}

# ============================================================
# 総合偏差値の重み (D-1 / I-3 B案)
# ============================================================
# 6因子 composite 重み（能力/展開/適性/騎手/調教師/血統 = 合計1.00）
COMPOSITE_WEIGHTS = {
    "ability":   0.40,   # 能力偏差値
    "pace":      0.25,   # 展開偏差値
    "course":    0.15,   # コース適性
    "jockey":    0.10,   # 騎手偏差値
    "trainer":   0.05,   # 調教師偏差値
    "bloodline": 0.05,   # 血統偏差値
}

# 競馬場別6因子重み (ML特徴量重要度分析 2024-01〜2026-02 から算出)
# 旧3因子(ability/pace/course)を合計0.80にスケーリングし、
# 騎手0.10/調教師0.05/血統0.05を加算。venue_master.py の競馬場名をキー。
VENUE_COMPOSITE_WEIGHTS: dict[str, dict[str, float]] = {
    # --- JRA 10場 ---（旧3因子を×0.80でスケール + personnel固定）
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
    # 帯広（ばんえい）は対象外のため除外
}

# 後方互換: 旧COMPOSITE_PERSONNEL_WEIGHTS を参照するコード向け
# Phase 10Aで6因子化したため、補正項としての使用は廃止
COMPOSITE_PERSONNEL_WEIGHTS = {
    "jockey": 0.10,
    "trainer": 0.05,
    "bloodline": 0.05,
}
COMPOSITE_PERSONNEL_MAX = 5.0  # 未使用（後方互換のため残す）


def get_composite_weights(venue_name: str = None) -> dict[str, float]:
    """
    競馬場名に応じた重みを返す。

    優先順位:
      1. 較正済みファイル (data/models/venue_weights_calibrated.json) の venue_code ベース値
         → venue_code と venue_name の対応は _CALIB_VC_TO_NAME で解決
      2. settings.py の VENUE_COMPOSITE_WEIGHTS (venue_name ベース手動設定)
      3. デフォルト COMPOSITE_WEIGHTS

    較正ファイルがなければ従来通り settings.py の値を使用する。
    """
    # 較正済み重みキャッシュ: {venue_name: {"ability":, "pace":, "course":}}
    # モジュール初回インポート時に一度だけ読み込む
    calib = _get_calibrated_weights()
    if venue_name and venue_name in calib:
        return calib[venue_name]
    if venue_name and venue_name in VENUE_COMPOSITE_WEIGHTS:
        return VENUE_COMPOSITE_WEIGHTS[venue_name]
    return COMPOSITE_WEIGHTS


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
    # "52": 帯広（ばんえい）は対象外
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
WA_WEIGHTS = [0.1524, 0.1125, 0.3481, 0.341, 0.046]  # Optuna最適値 (更新: 2026-03-04)

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
PACE_AI_ADJUSTMENT_MAX = 12  # AI層±12pt

# F-0 各コンポーネントの範囲
PACE_COMPONENTS = {
    "末脚評価": (-8, 8),
    "位置取り×末脚バランス": (-8, 8),
    "枠順バイアス": (-5, 5),
    "コース脚質バイアス": (-5, 5),
    "騎手展開影響": (-4, 4),
}

# ============================================================
# コース適性偏差値 (G-0)
# ============================================================
COURSE_BASE = 50
COURSE_AI_ADJUSTMENT_MAX = 8  # AI層±8pt

COURSE_COMPONENTS = {
    "コース実績": (-5, 5),
    "コース形状相性": (-3, 3),
    "騎手コース影響": (-3, 3),
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
TOKUSEN_SCORE_THRESHOLD = 7.0   # 特選判定閾値（16pt満点中）
TOKUSEN_ODDS_THRESHOLD = 15.0   # 対象: オッズ15倍以上
TOKUSEN_MAX_PER_RACE = 2        # 1レース最大2頭

# ============================================================
# 買い目 期待値閾値 (5-2)
# ============================================================
EV_BUY_STRONG = 120  # ◎買い
EV_BUY_NORMAL = 100  # ○買い
BUY_MAX_TICKETS = 8  # 最大8点

# ============================================================
# 自信度別賭け金デフォルト (5-3)
# ============================================================
STAKE_DEFAULT = {
    "SS": 12000,
    "S": 6000,
    "A": 3000,
    "B": 1500,
    "C": 500,  # 自信度Cでも最小限の賭け金で買い目を表示
    "D": 0,
}

# ============================================================
# オッズ控除率 (5-1) - 券種別・JRA/地方別
# 払戻率 = 1 - 控除率。例: JRA単勝の払戻率80% = 控除率20%
# ============================================================
ODDS_DEDUCTION = {
    "JRA": 0.80,  # JRA単勝
    "JRA_複": 0.725,  # JRA馬連/馬単/ワイド
}

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
