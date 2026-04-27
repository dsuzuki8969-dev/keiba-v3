"""
競馬解析マスターシステム v3.0 - 計算層コア
A章: 基準タイム算出
B章: 馬場補正・距離係数
C章: 過去走参照・加重平均偏差値
D章: 総合偏差値枠組み・各補正
E章: トレンド・着差プロファイル
"""

import statistics
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from config.settings import (
    ALPHA_DECLINE_PENALTY,
    ALPHA_DEFAULT,
    ALPHA_DIVERGENCE_THRESHOLD,
    CHAKUSA_INDEX_WEIGHT,
    DEVIATION,
    DISTANCE_BASE,
    RACE_HISTORY_DAYS_DEFAULT,
    RACE_HISTORY_DAYS_休養明け,
    RACE_HISTORY_MAX_RUNS,
)
from src.log import get_logger
from src.models import (
    AbilityDeviation,
    BakenType,
    ChakusaPattern,
    Horse,
    PastRun,
    Reliability,
    Trend,
)

logger = get_logger(__name__)


# ============================================================
# ばんえい専用: 斤量帯別基準タイム + クラス補正
# ============================================================

_BANEI_BASELINES_CACHE = None

def _load_banei_time_baselines() -> dict:
    """ばんえい基準タイムテーブルをロード（キャッシュ付き）"""
    global _BANEI_BASELINES_CACHE
    if _BANEI_BASELINES_CACHE is not None:
        return _BANEI_BASELINES_CACHE
    import json
    import os
    path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "models", "banei_time_baselines.json")
    try:
        with open(path, encoding="utf-8") as f:
            _BANEI_BASELINES_CACHE = json.load(f)
    except Exception:
        _BANEI_BASELINES_CACHE = {"weight_bands": {}, "default_baseline": 128.0}
    return _BANEI_BASELINES_CACHE


def _get_banei_baseline(weight_kg: float, baselines: dict) -> float:
    """斤量から基準タイムを取得"""
    default = baselines.get("default_baseline", 128.0)
    if not weight_kg or weight_kg <= 0:
        return default
    bands = baselines.get("weight_bands", {})
    for band_key, info in bands.items():
        parts = band_key.split("-")
        if len(parts) == 2:
            lo, hi = float(parts[0]), float(parts[1])
            if lo <= weight_kg < hi:
                return info.get("baseline", default)
    return default




# ============================================================
# A-1: 基準タイム算出
# ============================================================


class StandardTimeCalculator:
    """
    基準タイム = コース平均タイム(1-3着) - (スコア合計×距離係数)
    スコア6カテゴリ: ❶馬場❷クラス❸種別❹条件❺性別❻季節
    """

    # クラス補正スコアテーブル (相対値)
    # 実データ検証 (JRA良馬場1着タイム, 1勝=0基準, dist_coeff=1.143換算):
    #   新馬≈-1.3, 未勝利≈-0.9, 1勝=0, 2勝≈+0.4, OP≈+0.8
    #   G1/G2/G3はデータ不足のため現行値を維持
    CLASS_SCORE = {
        "G1": 6,
        "G2": 5,
        "G3": 4,
        "OP": 3,
        "L": 3,
        "3勝": 2,
        "1600万": 2,
        "2勝": 1,
        "1000万": 1,
        "1勝": 0,
        "500万": 0,
        "未勝利": -1,
        "新馬": -2,
        # NAR クラス (JRA相当値で推定) — フォールバック用。
        # 会場別の実データ値は VENUE_CLASS_SCORE を優先参照。
        "A1": 3,
        "A2": 2,
        "B1": 1.5,
        "B2": 1,
        "B3": 0.5,
        "C1": 0,
        "C2": -0.5,
        "C3": -1,
        "C4": -1.5,
        "D": -1.5,
        "重賞": 3,
        "交流重賞": 4,
        "特別": 1,
        "未格付": -1,
        "その他": -1,
        # 世代限定戦（フォールバック用。VENUE_CLASS_SCORE優先参照）
        "3歳": -1,
        "2歳": -1.5,
    }

    # ── NAR 会場×クラス別スコアテーブル ──
    # JRA/NAR両走の移籍馬12,774頭（race_log 70万件）から算出。
    # JRA 1勝クラス = 0.0 基準。サンプル50件以上・信頼性◎○のみ収録。
    # 参照: data/analysis/venue_class_level_v2.txt (古馬戦セクション)
    # キー: (会場名, クラス名) → スコア
    VENUE_CLASS_SCORE: Dict[tuple, float] = {
        # ── 大井 ──
        ("大井", "重賞"): 2.2,
        ("大井", "交流重賞"): 2.2,
        ("大井", "OP"): 2.1,
        ("大井", "A2"): 1.8,
        ("大井", "B1"): 1.0,
        ("大井", "B2"): 0.5,
        ("大井", "B3"): -0.1,
        ("大井", "C1"): -0.6,
        ("大井", "C2"): -0.9,
        ("大井", "C3"): -1.0,
        # ── 船橋 ──
        ("船橋", "重賞"): 2.5,
        ("船橋", "交流重賞"): 2.1,
        ("船橋", "OP"): 2.2,
        ("船橋", "A2"): 1.5,
        ("船橋", "B1"): 0.6,
        ("船橋", "B2"): 0.2,
        ("船橋", "B3"): -0.2,
        ("船橋", "C1"): -0.6,
        ("船橋", "C2"): -0.9,
        ("船橋", "C3"): -1.0,
        # ── 川崎 ──
        ("川崎", "重賞"): 2.2,
        ("川崎", "交流重賞"): 1.7,
        ("川崎", "OP"): 1.8,
        ("川崎", "A2"): 1.4,
        ("川崎", "B1"): 0.9,
        ("川崎", "B2"): 0.2,
        ("川崎", "B3"): -0.2,
        ("川崎", "C1"): -0.6,
        ("川崎", "C2"): -0.9,
        ("川崎", "C3"): -1.0,
        # ── 浦和 ──
        ("浦和", "重賞"): 2.2,
        ("浦和", "交流重賞"): 2.4,
        ("浦和", "OP"): 2.2,
        ("浦和", "A2"): 1.4,
        ("浦和", "B1"): 0.9,
        ("浦和", "B2"): 0.2,
        ("浦和", "B3"): -0.2,
        ("浦和", "C1"): -0.6,
        ("浦和", "C2"): -1.0,
        ("浦和", "C3"): -1.0,
        # ── 園田 ──
        ("園田", "重賞"): 1.0,
        ("園田", "交流重賞"): 1.9,
        ("園田", "A1"): 1.5,
        ("園田", "A2"): 0.6,
        ("園田", "B1"): 0.1,
        ("園田", "B2"): -0.2,
        ("園田", "C1"): -0.6,
        ("園田", "C2"): -0.9,
        ("園田", "C3"): -0.9,
        # ── 姫路 ──
        ("姫路", "重賞"): 1.9,
        ("姫路", "A1"): 1.3,
        ("姫路", "A2"): 0.7,
        ("姫路", "B1"): 0.2,
        ("姫路", "B2"): -0.2,
        ("姫路", "C1"): -0.6,
        ("姫路", "C2"): -0.8,
        ("姫路", "C3"): -1.0,
        # ── 名古屋 ──
        ("名古屋", "重賞"): 0.7,
        ("名古屋", "交流重賞"): 2.0,
        ("名古屋", "OP"): 0.8,
        ("名古屋", "A1"): 0.8,  # A2のデータから推定
        ("名古屋", "A2"): 0.1,
        ("名古屋", "B1"): -0.8,
        ("名古屋", "B2"): -0.6,
        ("名古屋", "B3"): -0.7,
        ("名古屋", "C1"): -1.0,
        ("名古屋", "C2"): -1.0,
        ("名古屋", "C3"): -1.0,
        ("名古屋", "C4"): -1.0,
        # ── 笠松 ──
        ("笠松", "重賞"): 1.0,
        ("笠松", "OP"): -0.3,
        ("笠松", "A2"): 0.4,
        ("笠松", "A5"): 0.4,  # A2相当
        ("笠松", "A6"): 0.4,  # A2相当
        ("笠松", "B1"): -0.8,
        ("笠松", "B2"): -0.5,
        ("笠松", "B3"): -0.6,
        ("笠松", "B4"): -0.6,
        ("笠松", "B5"): -0.6,
        ("笠松", "B6"): -0.6,
        ("笠松", "B7"): -0.6,
        ("笠松", "B8"): -0.6,
        ("笠松", "B9"): -0.6,
        ("笠松", "C1"): -1.0,
        ("笠松", "C2"): -0.9,
        ("笠松", "C3"): -0.9,
        ("笠松", "C4"): -0.9,
        ("笠松", "C5"): -0.9,
        ("笠松", "C6"): -0.9,
        ("笠松", "C7"): -0.9,
        ("笠松", "C8"): -0.9,
        ("笠松", "C9"): -0.9,
        # ── 金沢 ──
        ("金沢", "重賞"): 0.4,
        ("金沢", "交流重賞"): 2.0,
        ("金沢", "A1"): 0.9,
        ("金沢", "A2"): 0.2,
        ("金沢", "B1"): 0.0,
        ("金沢", "B2"): -0.3,
        ("金沢", "B3"): -1.1,
        ("金沢", "C1"): -0.5,
        ("金沢", "C2"): -0.7,
        # ── 門別 ──
        ("門別", "重賞"): 1.6,
        ("門別", "交流重賞"): 1.4,
        ("門別", "OP"): 1.8,
        ("門別", "A1"): 1.9,
        ("門別", "A2"): 0.5,
        ("門別", "B1"): -0.3,
        ("門別", "B2"): -0.4,
        ("門別", "B3"): -0.5,
        ("門別", "C1"): -0.8,
        ("門別", "C2"): -0.8,
        ("門別", "C3"): -0.9,
        ("門別", "C4"): -0.9,
        # ── 盛岡 ──
        ("盛岡", "重賞"): 1.2,
        ("盛岡", "交流重賞"): 2.1,
        ("盛岡", "OP"): 1.0,
        ("盛岡", "A2"): 1.0,
        ("盛岡", "B1"): -0.1,
        ("盛岡", "B2"): -0.5,
        ("盛岡", "C1"): -0.7,
        ("盛岡", "C2"): -0.9,
        # ── 水沢 ──
        ("水沢", "重賞"): 0.9,
        ("水沢", "OP"): 0.8,
        ("水沢", "A2"): 0.8,
        ("水沢", "B1"): -0.1,
        ("水沢", "B2"): -0.5,
        ("水沢", "C1"): -0.7,
        ("水沢", "C2"): -0.8,
        # ── 高知 ──
        ("高知", "重賞"): 1.0,
        ("高知", "交流重賞"): 2.0,
        ("高知", "A2"): 1.4,
        ("高知", "B2"): 1.0,
        ("高知", "C1"): 0.7,
        ("高知", "C2"): 0.3,
        ("高知", "C3"): -0.2,
        # ── 佐賀 ──
        ("佐賀", "重賞"): 1.0,
        ("佐賀", "交流重賞"): 2.1,
        ("佐賀", "OP"): 1.1,
        ("佐賀", "A1"): 1.5,
        ("佐賀", "A2"): 0.5,
        ("佐賀", "B2"): -0.2,
        ("佐賀", "C1"): -0.7,
        ("佐賀", "C2"): -0.9,
        # ── 3歳限定戦 ── (v2分析 3歳セクション ◎/○のみ)
        # race_name が "3歳N組", "3歳四" 等でC/B/Aクラス不明のレース
        ("大井", "3歳"): -0.8,
        ("船橋", "3歳"): -0.8,
        ("川崎", "3歳"): -0.9,
        ("浦和", "3歳"): -0.9,
        ("園田", "3歳"): -0.3,
        ("名古屋", "3歳"): -1.1,
        ("笠松", "3歳"): -1.0,
        ("金沢", "3歳"): -1.0,
        ("門別", "3歳"): -1.0,
        ("盛岡", "3歳"): -0.8,
        ("高知", "3歳"): -1.1,
        ("佐賀", "3歳"): -1.0,
        # ── 2歳限定戦 ── (v2分析 2歳セクション ○のみ)
        ("門別", "2歳"): -0.5,   # OP+3歳混合の平均、門別は2歳戦のメッカ
        ("名古屋", "2歳"): -1.1,
        ("佐賀", "2歳"): -0.3,
    }

    # 性別補正スコア
    SEX_SCORE = {
        "牡": 0.0,
        "セン": 0.0,
        "牝": -0.5,  # 牝馬限定戦は0.0
    }

    # 季節補正スコア (月別) — 実データ(JRA芝2000m良馬場)から算出
    # 正値=良馬場より好条件(速い月), 負値=遅い月
    # 旧値→実データ方向: 4月(+0.2→実-0.84s速い), 12月(0.0→実+0.60s遅い)
    SEASON_SCORE = {
        1: -0.2,  # 冬・やや遅い (+0.14s)
        2: -0.3,  # 冬・遅い    (+0.24s)
        3: -0.2,  # 早春・やや遅い (+0.21s)
        4: +0.8,  # 春ピーク・速い (-0.84s)
        5: +0.2,  # 春・やや速い  (-0.14s)
        6: +0.7,  # 初夏・速い    (-0.74s)
        7: -0.6,  # 盛夏・遅い   (+0.59s)
        8: -0.2,  # 夏・やや遅い (+0.18s)
        9: -0.1,  # 秋入り・ほぼ平均 (+0.10s)
        10: +0.6,  # 秋ピーク・速い (-0.64s)
        11: +0.1,  # 晩秋・ほぼ平均 (-0.07s)
        12: -0.6,  # 冬・遅い    (+0.60s)
    }

    # 競馬場別時計レベル補正 (秒/200m, 良馬場1着タイムの全場平均との差を正規化)
    # 正値=その競馬場は平均より遅い(補正で速く見せる必要なし→時計に加算して正規化)
    # 実データ: JRA芝2000m・ダート1400m良馬場勝ち馬平均タイムから算出
    VENUE_SPEED_TABLE: Dict[str, Dict[str, float]] = {
        # JRA 芝 (秒/200m, 全場平均との差, 正=遅い場)
        "JRA_芝": {
            "05": -0.073,  # 東京 (-0.73s/2000m)
            "02": -0.037,  # 函館 (-0.37s)
            "06": -0.012,  # 中山 (-0.12s)
            "09": -0.011,  # 阪神 (-0.11s)
            "08": -0.006,  # 京都 (-0.06s)
            "01": 0.0,  # 札幌  基準付近
            "10": +0.023,  # 小倉 (+0.23s)
            "07": +0.025,  # 中京 (+0.25s)
            "03": +0.042,  # 福島 (+0.42s)
            "04": +0.047,  # 新潟 (+0.47s)
        },
        # JRA ダート (秒/200m)
        "JRA_ダート": {
            "07": -0.039,  # 中京 (-0.27s/1400m)
            "09": -0.006,  # 阪神 (-0.04s)
            "05": +0.006,  # 東京 (+0.04s)
            "08": +0.023,  # 京都 (+0.16s)
        },
        # NAR: データ少のため暫定なし (将来拡張)
    }

    # 上がり3F基準値テーブル（JRA良馬場1着の実データ平均, 距離別）
    # 出典: analyze_all_facts.py §8
    LAST_3F_BASELINE: Dict[str, Dict[int, float]] = {
        "芝": {
            1000: 30.0,
            1200: 32.8,
            1400: 32.6,
            1500: 33.3,
            1600: 32.9,
            1800: 33.4,
            2000: 33.3,
            2200: 32.4,
            2400: 34.1,
            2500: 33.5,
            2600: 34.1,
        },
        "ダート": {
            1000: 33.7,
            1150: 33.6,
            1200: 33.5,
            1300: 32.4,
            1400: 32.8,
            1600: 33.9,
            1700: 33.3,
            1800: 32.9,
            1900: 33.8,
            2100: 33.3,
            2400: 32.9,
        },
    }

    def get_last3f_baseline(self, surface: str, distance: int) -> float:
        """上がり3F基準値を取得（最近傍距離で補完）"""
        tbl = self.LAST_3F_BASELINE.get(surface, {})
        if not tbl:
            return 33.5
        if distance in tbl:
            return tbl[distance]
        nearest = min(tbl.keys(), key=lambda d: abs(d - distance))
        return tbl[nearest]

    def __init__(self, course_db: Dict):
        """
        course_db: {course_id: [PastRun]} の形式
        過去の同コース1-3着走を蓄積したDB
        """
        self.course_db = course_db

    def calc_distance_coefficient(self, distance: int) -> float:
        """距離係数 = 1600÷距離 (B-2)"""
        if not distance or distance <= 0:
            return 1.0
        return DISTANCE_BASE / distance

    @staticmethod
    def _infer_grade_from_class_name(class_name: str) -> str:
        """class_name（レース名）からグレードを再推定する。
        「その他」やCLASS_SCOREにないグレードの場合に使用。
        """
        import re
        cn = class_name.strip()
        if not cn:
            return ""
        # C系 (C1 > C2 > C3 > C4)
        # C13, C15等の組番号に注意: "C1" substring matchで誤判定しないよう先にC4チェック
        if re.search(r"C[4-9]|Ｃ[４-９]|C\d{2}", cn):
            return "C3"  # C4以下はC3相当スコアで十分
        if "C1" in cn or "Ｃ１" in cn:
            return "C1"
        if "C2" in cn or "Ｃ２" in cn:
            return "C2"
        if re.search(r"C[3ーー\-]|Ｃ[３]|C級|Cクラス", cn) or cn == "C":
            return "C3"
        # B系
        if "B1" in cn or "Ｂ１" in cn:
            return "B1"
        if "B2" in cn or "Ｂ２" in cn:
            return "B2"
        if re.search(r"B[3-9ーー\-]|Ｂ[３-９]|B\d|B級|Bクラス", cn) or cn == "B":
            return "B3"
        # A系
        if "A1" in cn or "Ａ１" in cn:
            return "A1"
        if re.search(r"A[2-9ーー\-]|Ａ[２-９]|Aクラス", cn) or cn == "A":
            return "A2"
        # 重賞系
        if re.search(r"Jpn\s*[123]|JPN|交流", cn, re.IGNORECASE):
            return "交流重賞"
        if re.search(r"記念|賞|杯|盃|カップ|トロフィー|チャンピオン|重賞|大賞典|ダービー|オークス", cn):
            return "重賞"
        # OP
        if re.search(r"\bOP\b|オープン", cn):
            return "OP"
        if "特別" in cn:
            return "OP"
        # 新馬・未勝利
        if "新馬" in cn or "デビュー" in cn:
            return "新馬"
        if "未勝利" in cn or "未格付" in cn:
            return "未格付"
        # 世代限定戦（C/B/A系にマッチしなかった3歳/2歳レース）
        # "3歳以上" は古馬混合 → C/B/A系で既にマッチ済みなので除外
        if "3歳" in cn and "3歳以上" not in cn and "3歳上" not in cn:
            return "3歳"
        if "2歳" in cn:
            return "2歳"
        return ""

    def calc_score_total(self, run: PastRun) -> float:
        """スコア合計 (A-1 6カテゴリ)"""
        # ❶馬場 (略称 '稍'/'不' も正規化して対応)
        cond_score = {"良": 0.0, "稍重": -0.5, "重": -1.0, "不良": -1.5}.get(
            _norm_cond(run.condition), 0.0
        )
        # ❷クラス — 「その他」や未登録グレードはclass_nameから再推定
        grade = run.grade
        if grade not in self.CLASS_SCORE or grade == "その他":
            inferred = self._infer_grade_from_class_name(
                getattr(run, "class_name", "") or ""
            )
            if inferred and inferred in self.CLASS_SCORE:
                grade = inferred
        # 会場×クラス別スコア（NAR）を優先参照。未登録の場合はフォールバック。
        _venue = getattr(run, "venue", "") or ""
        _venue_key = (_venue, grade)
        if _venue_key in self.VENUE_CLASS_SCORE:
            class_score = self.VENUE_CLASS_SCORE[_venue_key]
        else:
            class_score = self.CLASS_SCORE.get(grade, -1)
        # ❸種別 (芝/ダート)
        surface_score = {"芝": 0.0, "ダート": -0.5}.get(run.surface, 0.0)
        # ❹条件 (頭数)
        cond_count = 0.3 if run.field_count >= 16 else (0.1 if run.field_count >= 12 else 0.0)
        # ❺性別 (PastRunには性別がないため0.0とする。HorseEvalから呼ぶ場合は別途渡す)
        sex_score = 0.0
        # ❻季節
        try:
            month = int(run.race_date[5:7])
            season_score = self.SEASON_SCORE.get(month, 0.0)
        except Exception:
            season_score = 0.0

        return cond_score + class_score + surface_score + cond_count + sex_score + season_score

    def calc_standard_time(
        self, course_id: str, grade: str, condition: str, distance: int
    ) -> Tuple[Optional[float], Reliability]:
        """
        基準タイムを算出。
        クラス分布が均質な場合（NAR等）はスコア外挿を抑制し、
        avg_time を直接使用して過大偏差値を防止する。
        Returns: (基準タイム秒, 信頼度)
        """
        runs = self.course_db.get(course_id, [])
        top3_runs = [r for r in runs if r.finish_pos <= 3]

        if len(top3_runs) >= 30:
            reliability = Reliability.A
        elif len(top3_runs) >= 10:
            reliability = Reliability.B
        elif len(top3_runs) >= 3:
            reliability = Reliability.C
        else:
            # A-2: データ不足 → 同コース全距離プール
            return self._fallback_standard_time(course_id, distance), Reliability.C

        dist_coeff = self.calc_distance_coefficient(distance)
        avg_time = statistics.mean([r.finish_time_sec for r in top3_runs])

        # JRA/NAR別の基準タイム算出方式
        # JRA: スコア外挿（クラス多様性があり、k値・補正テーブルが最適化済み）
        # NAR: avg_time直接使用（クラス分布偏りが大きく、スコア外挿で過大偏差値が発生）
        _JRA_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
        vc = course_id.split("_")[0] if course_id else ""

        if vc in _JRA_CODES:
            # JRA: 従来のスコア外挿
            avg_score = statistics.mean([self.calc_score_total(r) for r in top3_runs])
            standard_time = avg_time - (avg_score * dist_coeff)
        else:
            # NAR: avg_timeを基準タイムとして使用
            # スコア外挿はNARのクラス構造・馬場補正データ不足で不正確
            standard_time = avg_time

        return standard_time, reliability

    def _resolve_grade(self, run: PastRun) -> str:
        """PastRunのグレードを解決（「その他」の場合はclass_nameから再推定）"""
        grade = run.grade
        if grade not in self.CLASS_SCORE or grade == "その他":
            inferred = self._infer_grade_from_class_name(
                getattr(run, "class_name", "") or ""
            )
            if inferred and inferred in self.CLASS_SCORE:
                return inferred
        return grade

    def _fallback_standard_time(self, course_id: str, distance: int) -> Optional[float]:
        """A-2: データ不足代替。同系統コースの近距離帯データで距離加重補間"""
        venue = course_id.split("_")[0]
        surface = course_id.split("_")[1] if len(course_id.split("_")) > 1 else ""
        # 近距離帯(±400m)のデータのみ使用
        dist_lo = max(distance - 400, 0)
        dist_hi = distance + 400
        similar_runs = [
            r
            for cid, runs in self.course_db.items()
            for r in runs
            if cid.startswith(venue) and surface in cid and r.finish_pos <= 3
            and r.distance and dist_lo <= r.distance <= dist_hi
        ]
        if not similar_runs:
            return None
        valid_runs = [r for r in similar_runs if r.distance > 0]
        # サンプル3件未満は信頼性不足 → None（dev=50.0固定の方が異常値より安全）
        if len(valid_runs) < 3:
            return None
        # 距離加重: 対象距離に近いデータほど高い重みを付与
        # weight = 1 / (1 + |dist_diff| / 100) で距離差100mごとに重み半減
        total_weighted_spm = 0.0
        total_weight = 0.0
        for r in valid_runs:
            dist_diff = abs(r.distance - distance)
            w = 1.0 / (1.0 + dist_diff / 100.0)
            total_weighted_spm += (r.finish_time_sec / r.distance) * w
            total_weight += w
        if total_weight <= 0:
            return None
        weighted_avg_spm = total_weighted_spm / total_weight
        return weighted_avg_spm * distance


# ============================================================
# B-1: 馬場補正
# ============================================================

# 馬場状態の略称 → 正式名 正規化
_COND_NORMALIZE = {"稍": "稍重", "不": "不良", "良": "良", "重": "重"}


def _norm_cond(c: str) -> str:
    return _COND_NORMALIZE.get(c, c)


def _dist_band(distance: int, surface: str) -> str:
    """距離帯を返す (short/mile/mid/long)"""
    if surface == "芝":
        if distance <= 1400:
            return "short"
        if distance <= 1600:
            return "mile"
        if distance <= 2200:
            return "mid"
        return "long"
    else:  # ダート・その他
        if distance <= 1200:
            return "short"
        if distance <= 1600:
            return "mile"
        if distance <= 2000:
            return "mid"
        return "long"


# 実データ分析値 (秒/200m, 良馬場との差)
# 正値 = 良より遅い(芝)、負値 = 良より速い(ダート)
# 出典: course_db_preload.json の勝ち馬タイム統計 (analyze_condition_by_dist.py)
_EMPIRICAL_RATES: Dict[str, Dict[str, Dict[str, float]]] = {
    "JRA_芝": {
        "稍重": {"short": 0.141, "mile": 0.126, "mid": 0.162, "long": 0.152},
        "重": {"short": 0.191, "mile": 0.262, "mid": 0.256, "long": 0.173},
        "不良": {"short": 0.213, "mile": 0.434, "mid": 0.357, "long": 0.357},
    },
    "JRA_ダート": {
        "稍重": {"short": -0.058, "mile": -0.072, "mid": -0.039, "long": -0.039},
        "重": {"short": -0.171, "mile": -0.243, "mid": -0.169, "long": -0.133},
        "不良": {"short": -0.192, "mile": -0.229, "mid": -0.185, "long": -0.185},
    },
    "NAR_芝": {
        # サンプル少 → 保守的に小さい値
        "稍重": {"short": 0.0, "mile": -0.024, "mid": 0.065, "long": 0.065},
        "重": {"short": 0.0, "mile": 0.0, "mid": 0.0, "long": 0.0},
        "不良": {"short": 0.0, "mile": 0.0, "mid": 0.0, "long": 0.0},
    },
    "NAR_ダート": {
        "稍重": {"short": -0.004, "mile": -0.004, "mid": 0.029, "long": 0.029},
        "重": {"short": -0.049, "mile": -0.036, "mid": 0.019, "long": 0.019},
        "不良": {"short": -0.071, "mile": -0.150, "mid": -0.148, "long": -0.148},
    },
}


class TrackCorrector:
    """
    馬場補正：実データから算出した距離帯別・馬場状態別補正値を使用。

    返値の意味:
      正値 → タイム加算（良馬場より遅い条件を補正、芝の重・不良）
      負値 → タイム減算（良馬場より速い条件を補正、ダートの重・不良）

    過去走タイムに加算することで「良馬場相当タイム」へ正規化する。
    """

    def calc_empirical_correction(
        self,
        surface: str,
        condition: str,
        distance: int,
        is_jra: bool,
    ) -> float:
        """
        良馬場基準の時計補正(秒)を返す。
        correction = -(rate_per_200m) × distance / 200
        （ratが負=ダートは速い → correctionは正 → 時間を戻してフェアに比較）
        """
        cond = _norm_cond(condition)
        if cond == "良":
            return 0.0

        org = "JRA" if is_jra else "NAR"
        key = f"{org}_{surface}"
        band = _dist_band(distance, surface)

        rates = _EMPIRICAL_RATES.get(key, {}).get(cond, {})
        rate = rates.get(band, 0.0)
        # 正規化補正: rate は「条件馬場タイム - 良馬場タイム」を /200m で表したもの
        # → 良馬場に換算するには rate 分だけ逆方向に動かす
        return -rate * distance / 200.0

    # --- 後方互換のため旧APIも残す (内部では empirical を呼ぶ) ---
    def calc_jra_turf_correction(self, cv_value: Optional[float]) -> float:
        """旧API: cv_valueが取得できない環境ではfallback=0.0 (現在は empirical で代替)"""
        return 0.0  # 実際の補正は calc_empirical_correction で行う

    def calc_jra_dirt_correction(self, moisture: Optional[float]) -> float:
        """旧API: moistureが取得できない環境ではfallback=0.0"""
        return 0.0

    def calc_chiho_correction(self, condition: str) -> float:
        """旧API: 地方補正 (empirical で代替)"""
        return 0.0


# ============================================================
# B-2: 走破偏差値算出
# ============================================================


def calc_run_deviation(
    finish_time_corrected: float,
    standard_time: float,
    distance: int,
    venue_code: str = "",
) -> float:
    """
    走破偏差値 = 50 + (基準タイム - 走破タイム補正後) × 距離係数 × 換算定数
    (B-2: 案1 距離帯別CONVERSION_CONSTANT使用)

    venue_code: JRAコード（01-10）以外は NAR 用 k 値テーブルを使用する。
                省略または空文字列の場合は JRA 用テーブルにフォールバック（既存動作互換）。
    """
    from config.settings import get_conversion_constant
    if not distance or distance <= 0:
        return 50.0
    dist_coeff = DISTANCE_BASE / distance
    # venue_code を渡して JRA/NAR を自動判別
    _k = get_conversion_constant(distance, venue_code)
    dev = 50 + (standard_time - finish_time_corrected) * dist_coeff * _k
    return max(DEVIATION["ability"]["min"], min(DEVIATION["ability"]["max"], dev))


# ============================================================
# C-1: 過去走の参照範囲フィルタ
# ============================================================


def filter_past_runs(
    runs: List[PastRun],
    race_date: str,
    target_surface: str,
    is_long_break: bool = False,
) -> Tuple[List[PastRun], bool, bool]:
    """
    C-1: 1年以内×同系統×最大5走
    長期休養明け: 2年拡張+減衰適用
    同馬場走がゼロの場合、異馬場走をフォールバック返却（is_surface_switch=True）
    Returns: (filtered_runs, is_decayed, is_surface_switch)
    """
    try:
        ref_date = datetime.strptime(race_date, "%Y-%m-%d")
    except Exception:
        ref_date = datetime.now()

    days_limit = RACE_HISTORY_DAYS_休養明け if is_long_break else RACE_HISTORY_DAYS_DEFAULT

    # 日付・同系統フィルタ
    filtered = []
    cross_surface = []  # 異馬場走（フォールバック用）
    for r in sorted(runs, key=lambda x: x.race_date, reverse=True):
        try:
            run_date = datetime.strptime(r.race_date, "%Y-%m-%d")
        except Exception:
            logger.debug("invalid race_date format: %s", r.race_date, exc_info=True)
            continue
        days_ago = (ref_date - run_date).days
        if days_ago < 0:
            continue
        if days_ago > days_limit:
            continue
        if r.surface != target_surface:
            cross_surface.append(r)
            continue
        filtered.append(r)

    # 同馬場走がある場合: 従来通り
    if filtered:
        return filtered[:RACE_HISTORY_MAX_RUNS], is_long_break, False

    # 同馬場走ゼロ + 異馬場走あり: 転向フォールバック
    if cross_surface:
        return cross_surface[:RACE_HISTORY_MAX_RUNS], is_long_break, True

    # 過去走なし（新馬）
    return [], is_long_break, False


# ============================================================
# C-1b: 芝ダ転換適性スコア算出
# ============================================================


def calc_surface_switch_score(
    switch_direction: str,
    context: Optional[Dict] = None,
) -> float:
    """
    馬場転向時の適性補正値を算出する（-4.0 〜 +6.0 pt）。

    7因子の加重合算。各因子は -1.0〜+1.0 の正規化スコアを出力し、
    データ根拠の重みで合算後、MAX_BONUS/MAX_PENALTY でクランプ。

    Args:
        switch_direction: "turf_to_dirt" or "dirt_to_turf"
        context: 各因子の入力値を含むdict
            - predicted_position: float (0.0=逃げ 〜 1.0=追込)
            - horse_weight: int (kg)
            - age: int
            - sire_target_pr: float (種牡馬の転向先面の複勝率)
            - sire_source_pr: float (種牡馬の転向元面の複勝率)
            - jockey_target_pr: float (騎手の転向先面の複勝率)
            - jockey_source_pr: float (騎手の転向元面の複勝率)
            - gate_no: int
            - field_count: int
            - bms_target_pr: float (母父の転向先面の複勝率)
            - bms_source_pr: float (母父の転向元面の複勝率)
    Returns:
        float: 転換適性補正値（偏差値pt）
    """
    from config.settings import (
        SURFACE_SWITCH_FACTOR_WEIGHTS,
        SURFACE_SWITCH_MAX_BONUS,
        SURFACE_SWITCH_MAX_PENALTY,
    )

    if not context:
        return 0.0
    if switch_direction not in ("turf_to_dirt", "dirt_to_turf"):
        return 0.0

    to_dirt = switch_direction == "turf_to_dirt"
    factor_scores: Dict[str, float] = {}

    # --- 1. 脚質/位置取り (weight=0.347) ---
    # predicted_position: 0.0=最前 〜 1.0=最後方
    # ダート転向: 前目に行けるほどプラス（data: 逃げ51% vs 追込2.4%）
    # 芝転向: 前目がやや有利だが差も大きくない
    pred_pos = context.get("predicted_position")
    if pred_pos is not None:
        if to_dirt:
            # 0.0→+1.0(逃げ), 0.35→+0.3(先行), 0.65→-0.3(差し), 1.0→-1.0(追込)
            factor_scores["position"] = max(-1.0, min(1.0, 1.0 - pred_pos * 2.0))
        else:
            # 芝転向: 前目が有利だが影響度は半分
            factor_scores["position"] = max(-1.0, min(1.0, (0.5 - pred_pos) * 1.2))

    # --- 2. 馬体重 (weight=0.141) ---
    # ダート転向: 重い馬ほどプラス（data: 520+→20.5% vs <440→6.7%）
    # 芝転向: 軽い方がやや有利だが影響は小さい
    hw = context.get("horse_weight")
    if hw and hw > 0:
        if to_dirt:
            if hw >= 520:
                factor_scores["horse_weight"] = 1.0
            elif hw >= 480:
                factor_scores["horse_weight"] = 0.5
            elif hw >= 440:
                factor_scores["horse_weight"] = 0.0
            else:
                factor_scores["horse_weight"] = -0.7
        else:
            if hw < 440:
                factor_scores["horse_weight"] = -0.5  # 軽量は芝でも苦戦
            elif hw < 480:
                factor_scores["horse_weight"] = 0.0
            elif hw < 520:
                factor_scores["horse_weight"] = 0.3
            else:
                factor_scores["horse_weight"] = 0.2  # 大型は芝でもまあまあ

    # --- 3. 年齢 (weight=0.134) ---
    # 2-4歳: 成長期で転向成功率高い（data: 2歳18.6%, 4歳15.9%）
    # 5歳+: 急落（data: 5歳6.3%, 7歳2.1%）
    age = context.get("age")
    if age and age > 0:
        if age <= 3:
            factor_scores["age"] = 0.6
        elif age == 4:
            factor_scores["age"] = 0.3
        elif age == 5:
            factor_scores["age"] = -0.3
        elif age == 6:
            factor_scores["age"] = -0.6
        else:
            factor_scores["age"] = -1.0

    # --- 4. 種牡馬の馬場適性 (weight=0.134) ---
    # 転向先面の複勝率が転向元面より高いほどプラス
    sire_tgt = context.get("sire_target_pr")
    sire_src = context.get("sire_source_pr")
    if sire_tgt is not None and sire_src is not None and sire_src > 0:
        # 差を0.10で正規化（data: 最大差21.5pt = 0.215）
        diff = (sire_tgt - sire_src)
        factor_scores["sire"] = max(-1.0, min(1.0, diff / 0.10))

    # --- 5. 騎手の馬場適性 (weight=0.105) ---
    j_tgt = context.get("jockey_target_pr")
    j_src = context.get("jockey_source_pr")
    if j_tgt is not None and j_src is not None and j_src > 0:
        diff = (j_tgt - j_src)
        factor_scores["jockey"] = max(-1.0, min(1.0, diff / 0.08))

    # --- 6. 枠順 (weight=0.083) ---
    # ダート転向: 外枠が有利（data: 外20.0% vs 内14.7%）
    # 芝転向: 外枠がやや有利（data: 外13.6% vs 内7.8%）
    gate_no = context.get("gate_no")
    field_count = context.get("field_count")
    if gate_no and field_count and field_count > 0:
        gate_pct = gate_no / field_count  # 0.0=最内 〜 1.0=大外
        if to_dirt:
            # 外枠ほどプラス: 0.5→0, 1.0→+1.0, 0.0→-0.6
            factor_scores["gate"] = max(-1.0, min(1.0, (gate_pct - 0.5) * 2.0))
        else:
            # 芝転向でも外枠がやや有利
            factor_scores["gate"] = max(-1.0, min(1.0, (gate_pct - 0.4) * 1.5))

    # --- 7. 母父の馬場適性 (weight=0.055) ---
    bms_tgt = context.get("bms_target_pr")
    bms_src = context.get("bms_source_pr")
    if bms_tgt is not None and bms_src is not None and bms_src > 0:
        diff = (bms_tgt - bms_src)
        factor_scores["bms"] = max(-1.0, min(1.0, diff / 0.10))

    # --- 加重合算 ---
    if not factor_scores:
        return 0.0

    weighted_sum = 0.0
    weight_total = 0.0
    for key, score in factor_scores.items():
        w = SURFACE_SWITCH_FACTOR_WEIGHTS.get(key, 0.0)
        weighted_sum += score * w
        weight_total += w

    if weight_total <= 0:
        return 0.0

    # 利用可能な因子で正規化（欠損因子があっても比率を保つ）
    normalized = weighted_sum / weight_total

    # スケーリング: -1.0〜+1.0 → MAX_PENALTY〜MAX_BONUS
    if normalized >= 0:
        result = normalized * SURFACE_SWITCH_MAX_BONUS
    else:
        result = normalized * abs(SURFACE_SWITCH_MAX_PENALTY)

    return round(max(SURFACE_SWITCH_MAX_PENALTY, min(SURFACE_SWITCH_MAX_BONUS, result)), 2)


def detect_long_break(runs: List[PastRun], race_date: str, threshold_days: int = 90) -> Tuple[bool, int]:
    """
    長期休養明け判定 (直近走から90日以上空いている場合)
    Returns: (is_long_break: bool, break_days: int)
    """
    if not runs:
        return True, 999
    try:
        ref = datetime.strptime(race_date, "%Y-%m-%d")
        latest = max(datetime.strptime(r.race_date, "%Y-%m-%d") for r in runs)
        days = (ref - latest).days
        return days >= threshold_days, days
    except Exception:
        return False, 0


# ============================================================
# C-2: 加重平均偏差値 (WA)
# ============================================================



def _get_wa_weights_by_distance(distance: int) -> list:
    """距離帯別のWA重みを返す（Phase 4-1）。"""
    from config.settings import get_wa_weights
    return get_wa_weights(distance)


def _get_effective_weights(n_runs: int, base_weights: list) -> list:
    """実際の走数に合わせた重みリスト（正規化済み）を返す"""
    w = base_weights[:n_runs]  # 走数分だけ切り取る
    total = sum(w)
    if total <= 0:
        return [1.0 / n_runs] * n_runs  # 均等配分にフォールバック
    return [x / total for x in w]  # 正規化


def calc_weighted_average_deviation(
    run_deviations: List[float],
    chakusa_indices: List[float],
    is_long_break: bool = False,
    break_days: int = 0,
    horse_age: int = 4,
    distance: int = 1600,
) -> float:
    """
    C-2: 5走加重平均偏差値
    WA_WEIGHTS = [0.35, 0.25, 0.20, 0.12, 0.08]
    E-3着差評価指数を×1.5で反映した補正後偏差値で算出
    長期休養明け: 案3 休養日数に応じた段階的減衰
      91日=0.85, 180日=0.70, 365日=0.50, 730日=0.40
    """
    if not run_deviations:
        # 過去走なし（未出走・デビュー前）: 能力未知として平均以下を先行値とする
        # 血統補正で上振れる余地を残しつつ、経験馬より保守的な基準を使う
        return 48.0

    # E-3 着差評価指数を偏差値に反映
    corrected = []
    for i, dev in enumerate(run_deviations):
        if i < len(chakusa_indices):
            corrected.append(dev + chakusa_indices[i] * CHAKUSA_INDEX_WEIGHT)
        else:
            corrected.append(dev)

    # 加重平均（距離帯別の重みを使用、実走数に合わせて切り詰め・正規化）
    base_weights = _get_wa_weights_by_distance(distance)
    effective_weights = _get_effective_weights(len(corrected), base_weights)
    wa = sum(w * d for w, d in zip(effective_weights, corrected))
    wa = wa if effective_weights else 50.0

    # 長期休養明け: 休養日数に応じた段階的減衰（案3改）
    # 91日=0.85, 180日=0.70, 365日=0.50, 730日=0.40, 730日超=さらに減衰
    if is_long_break and break_days > 90:
        if break_days > 730:
            # 超長期休養（2年超）: さらに減衰
            decay = max(0.20, 0.40 - (break_days - 730) / 1500.0 * 0.20)
        else:
            decay = max(0.40, 1.0 - (break_days - 90) / 640.0)
        # 年齢別減衰: 6歳以上で段階的に減衰
        _age_factor = {6: 0.97, 7: 0.93, 8: 0.88, 9: 0.82}.get(
            horse_age, 1.0 if horse_age < 6 else 0.78
        )
        decay *= _age_factor
        wa = 50.0 + (wa - 50.0) * decay

    return max(DEVIATION["ability"]["min"], min(DEVIATION["ability"]["max"], wa))


# ============================================================
# C-3: α可変ロジック
# ============================================================


def calc_alpha(
    max_dev: float,
    wa_dev: float,
    is_declining: bool,
) -> float:
    """
    C-3: α = ALPHA_DEFAULT ± 乖離補正 - E-1下降補正
    """
    alpha = ALPHA_DEFAULT

    # 乖離幅補正
    divergence = abs(max_dev - wa_dev)
    if divergence > ALPHA_DIVERGENCE_THRESHOLD * 2:
        alpha += 0.15 if max_dev > wa_dev else -0.15
    elif divergence > ALPHA_DIVERGENCE_THRESHOLD:
        alpha += 0.07 if max_dev > wa_dev else -0.07

    # E-1下降フラグ補正
    if is_declining:
        alpha -= ALPHA_DECLINE_PENALTY

    return max(0.1, min(0.9, alpha))


# ============================================================
# E-1: トレンド検知
# ============================================================


def detect_trend(
    run_deviations: List[float],
    recent_runs: list = None,  # PastRun オブジェクトのリスト（オプション）
) -> Trend:
    """
    E-1: 直近3走の偏差値傾きと着順傾向を合算してトレンドを判定
    - 偏差値傾き（60%）＋着順傾向（40%）の複合スコア
    - 連勝中（直近2走以上で1着）は下降判定を緩和
    - G1/G2好走補正: 直近G1/G2で1〜3着なら偏差値にボーナスを加算
    """
    if len(run_deviations) < 2:
        return Trend.STABLE

    devs = list(run_deviations[:5])  # 最大5走に拡張

    # G1/G2 高着順ボーナス
    if recent_runs:
        HIGH_GRADES = {"G1", "G2", "JpnI", "JpnII"}
        for i, run in enumerate(recent_runs[:5]):
            if i >= len(devs):
                break
            grade = getattr(run, "grade", "")
            pos = getattr(run, "finish_pos", 99)
            if grade in HIGH_GRADES:
                if pos == 1:
                    devs[i] = devs[i] + 4.0
                elif pos <= 3:
                    devs[i] = devs[i] + 2.0

    # 偏差値傾き（直近3走: 70% + 長期5走: 30%）
    diff_1 = devs[0] - devs[1]
    if len(devs) >= 3:
        diff_2 = devs[1] - devs[2]
        short_diff = (diff_1 + diff_2) / 2
    else:
        short_diff = diff_1

    # 長期傾き: 5走目→1走目の線形回帰的傾き
    if len(devs) >= 4:
        n = len(devs)
        # 最小二乗回帰: x=[0,1,...,n-1] (新→古), y=devs
        x_mean = (n - 1) / 2.0
        y_mean = sum(devs) / n
        num = sum((i - x_mean) * (d - y_mean) for i, d in enumerate(devs))
        den = sum((i - x_mean) ** 2 for i in range(n))
        long_slope = -num / den if den != 0 else 0.0  # 負符号: 古→新で上昇が正
        avg_dev_diff = short_diff * 0.70 + long_slope * 0.30
    else:
        avg_dev_diff = short_diff

    # 着順傾向スコア（着順改善=正、悪化=負）
    # 着順は小さいほど良い。recent_runs[0] が最新。
    # diff = 前走着順 - 最新着順 → 正=改善、負=悪化
    pos_trend = 0.0
    if recent_runs and len(recent_runs) >= 2:
        positions = [getattr(r, "finish_pos", 9) for r in recent_runs[:3]]
        field_counts = [getattr(r, "field_count", 16) or 16 for r in recent_runs[:3]]
        # 着順を相対値（着順/頭数）で正規化して比較
        rel_pos = [p / fc for p, fc in zip(positions, field_counts)]
        pd1 = rel_pos[1] - rel_pos[0]  # 正=最新が改善（前走より上位）
        if len(rel_pos) >= 3:
            pd2 = rel_pos[2] - rel_pos[1]
            avg_pos_diff = (pd1 + pd2) / 2
        else:
            avg_pos_diff = pd1
        # 相対着順変化 0.1 ≈ 偏差値 2pt 相当に換算
        pos_trend = avg_pos_diff * 20.0

    # 連勝フラグ（直近2走以上で1着）
    consecutive_wins = 0
    if recent_runs:
        for r in recent_runs[:3]:
            if getattr(r, "finish_pos", 99) == 1:
                consecutive_wins += 1
            else:
                break

    # 複合スコア（偏差値60% + 着順40%）
    combined = avg_dev_diff * 0.6 + pos_trend * 0.4

    # 連勝中（2連勝以上）の場合は下降判定を大幅に緩和
    if consecutive_wins >= 2:
        combined = max(combined, 0.0)  # 最低でもSTABLEに
    elif consecutive_wins == 1:
        combined = max(combined, -1.5)  # DOWN 止まり（RAPID_DOWNは出さない）

    if combined >= 4:
        return Trend.RAPID_UP
    if combined >= 2:
        return Trend.UP
    if combined <= -4:
        return Trend.RAPID_DOWN
    if combined <= -2:
        return Trend.DOWN
    return Trend.STABLE


# ============================================================
# E-2: 信頼度集約
# ============================================================


def aggregate_reliability(
    sample_count: int,
    has_long_break: bool,
    standard_time_rel: Reliability,
) -> Tuple[Reliability, float]:
    """
    E-2: サンプル数・休養・基準タイム信頼度を統合
    Returns: (Reliability enum, 信頼度スコア 0.0〜1.0)
    信頼度スコアはC級内でも細分化するためのfloat値。
    A=0.9〜1.0, B=0.6〜0.8, C=0.1〜0.5
    """
    # 基本スコア: サンプル数ベース（0〜5走→0.0〜0.5）
    sample_score = min(1.0, sample_count / 5.0) * 0.5

    # 基準タイム信頼度ボーナス
    st_bonus = {Reliability.A: 0.3, Reliability.B: 0.15, Reliability.C: 0.0}.get(
        standard_time_rel, 0.0
    )

    # 休養ペナルティ
    break_penalty = 0.15 if has_long_break else 0.0

    score = min(1.0, max(0.0, sample_score + st_bonus - break_penalty))

    # enum 判定（従来ロジック維持）
    if sample_count >= 4 and not has_long_break and standard_time_rel == Reliability.A:
        grade = Reliability.A
        score = max(score, 0.9)  # A級は最低0.9
    elif sample_count >= 2 and standard_time_rel != Reliability.C:
        grade = Reliability.B
        score = max(min(score, 0.89), 0.6)  # B級は0.6〜0.89
    else:
        grade = Reliability.C
        score = min(score, 0.59)  # C級は最大0.59

    return grade, score


# ============================================================
# E-3: 着差プロファイル算出
# ============================================================


def calc_chakusa_profile(
    runs: List[PastRun],
) -> Tuple[ChakusaPattern, Tuple[int, int], float, BakenType]:
    """
    E-3: 着差プロファイルを算出
    Returns:
        (ChakusaPattern, (接戦勝, 接戦全), 3走平均着差評価指数, BakenType)
    """
    if not runs:
        return ChakusaPattern.KENJO, (0, 0), 0.0, BakenType.BALANCE

    indices = [r.chakusa_index for r in runs[:5]]
    avg_idx = statistics.mean(indices[:3]) if len(indices) >= 1 else 0.0

    # 着差パターン判定
    win_runs = [r for r in runs if r.finish_pos == 1]
    if win_runs:
        avg_margin = statistics.mean([r.margin_behind for r in win_runs])
        mura_score = statistics.stdev([r.chakusa_index for r in runs]) if len(runs) >= 3 else 0
        if avg_margin > 1.0:
            pattern = ChakusaPattern.ASSHŌ
        elif mura_score > 3.0:
            pattern = ChakusaPattern.MURA
        else:
            pattern = ChakusaPattern.KENJO
    else:
        pattern = ChakusaPattern.KENJO

    # 接戦勝率 (根性指標): 着差0.3秒以内のレースで1着
    close_races = [
        r
        for r in runs
        if abs(r.margin_ahead or 0.0) <= 0.3 or (r.finish_pos == 1 and (r.margin_behind or 0.0) <= 0.3)
    ]
    close_wins = [r for r in close_races if r.finish_pos == 1]
    close_tuple = (len(close_wins), len(close_races))

    # 馬券タイプ (勝切/バランス/着拾/一発/安定)
    baken_type = _calc_baken_type(runs)

    return pattern, close_tuple, avg_idx, baken_type


def _calc_baken_type(runs: List[PastRun]) -> BakenType:
    """三連率ベースで馬券タイプを判定 (I-1 三連率プロファイル)"""
    if not runs:
        return BakenType.BALANCE
    # 取消・除外（着順90以上）を除外して有効走のみで計算
    valid = [r for r in runs if r.finish_pos < 90]
    n = len(valid)
    if n == 0:
        return BakenType.BALANCE
    wins = sum(1 for r in valid if r.finish_pos == 1) / n
    top2 = sum(1 for r in valid if r.finish_pos <= 2) / n
    top3 = sum(1 for r in valid if r.finish_pos <= 3) / n

    if top3 == 0:
        return BakenType.IPPATSU

    kachikiri = wins / top3
    chakuhiroi = (top3 - top2) / top3 if top3 > 0 else 0

    if kachikiri >= 0.6:
        return BakenType.KACHIKIRI
    if chakuhiroi >= 0.5:
        return BakenType.CHAKUHIROI
    if top3 >= 0.6:
        return BakenType.ANTEI
    if wins >= 0.25:
        return BakenType.BALANCE
    return BakenType.IPPATSU


# ============================================================
# D-3: 出走間隔フラグ
# ============================================================


def calc_interval_flag(race_date: str, runs: List[PastRun]) -> str:
    """
    D-3: 出走間隔フラグ
    """
    if not runs:
        return "休み明け"
    try:
        ref = datetime.strptime(race_date, "%Y-%m-%d")
        latest = max(datetime.strptime(r.race_date, "%Y-%m-%d") for r in runs)
        days = (ref - latest).days
    except Exception:
        return "不明"

    if days <= 14:
        return "連闘"
    if days <= 35:
        return "中2週以内"
    if days <= 70:
        return "標準"
    if days <= 180:
        return "間隔空け"
    return "休み明け"


# ============================================================
# D-4: 距離変更補正 (SMILE+SS 6区分)
# ============================================================

# 距離帯区分 (SMILE+SS)
DISTANCE_ZONE = {
    "S": (1000, 1399),  # Sprint
    "M": (1400, 1799),  # Mile
    "I": (1800, 2099),  # Intermediate
    "L": (2100, 2399),  # Long
    "E": (2400, 2999),  # Extended
    "SS": (3000, 9999),  # Super Stayer
}


def get_distance_zone(distance: int) -> str:
    for zone, (lo, hi) in DISTANCE_ZONE.items():
        if lo <= distance <= hi:
            return zone
    return "M"


def calc_distance_change_flag(prev_distance: int, current_distance: int) -> Dict:
    """D-4: 距離変更補正フラグ"""
    prev_zone = get_distance_zone(prev_distance)
    curr_zone = get_distance_zone(current_distance)
    diff_m = current_distance - prev_distance

    return {
        "prev_zone": prev_zone,
        "curr_zone": curr_zone,
        "zone_change": prev_zone != curr_zone,
        "diff_m": diff_m,
        "direction": "延長" if diff_m > 0 else ("短縮" if diff_m < 0 else "同距離"),
    }


# ============================================================
# メイン統合関数: 馬の能力偏差値を算出する
# ============================================================


def calc_ability_deviation(
    horse: Horse,
    race_date: str,
    race_surface: str,
    course_id: str,
    std_calc: StandardTimeCalculator,
    track_corr: TrackCorrector,
    current_condition: str,
    current_cv: Optional[float],
    current_moisture: Optional[float],
    is_jra: bool = True,
    race_grade: str = "",
    race_distance: int = 1600,
    bloodline_db: Optional[Dict] = None,
    pace_db: Optional[Dict] = None,
    pace_type=None,
    surface_switch_context: Optional[Dict] = None,
) -> AbilityDeviation:
    """
    全A-E章ロジックを統合して AbilityDeviation を返す

    Steps:
    1. 長期休養判定
    2. 過去走フィルタ (C-1)
    3. 各走の走破偏差値算出 (A-1, B-1, B-2)
    4. E-3 着差プロファイル
    5. WA偏差値 (C-2)
    6. MAX偏差値
    7. α算出 (C-3)
    8. トレンド (E-1)
    9. 信頼度集約 (E-2)
    """

    # 1. 長期休養判定
    is_long_break, break_days = detect_long_break(horse.past_runs, race_date)

    # 2. 過去走フィルタ（同馬場走0の場合は異馬場走をフォールバック返却）
    filtered_runs, _, is_surface_switch = filter_past_runs(
        horse.past_runs, race_date, race_surface, is_long_break
    )

    # 3. 各走の走破偏差値算出
    run_deviations: List[float] = []
    run_records_list: list = []  # (PastRun, deviation, std_time) を格納

    # ---- ばんえい専用: JRA同等のタイムベース偏差値計算 ----
    # 計算式: dev = 50 + (斤量帯別基準タイム - 走破タイム) × 係数
    # クラス差はタイム差として自然に反映される（40,630走の実データで検証済み）
    # 係数0.50: 全体std=24.3s → 1σ=12.2pt → 大半が20-100範囲に収まる
    from data.masters.venue_master import is_banei as _is_banei_ability
    _banei_mode = _is_banei_ability(course_id[:2] if course_id else "")
    _BANEI_TIME_COEFF = 0.50  # 1秒差 = 0.5偏差値ポイント
    if _banei_mode:
        _banei_baselines = _load_banei_time_baselines()
        for run in filtered_runs:
            if run.finish_pos >= 99:
                run_deviations.append(30.0)
                run_records_list.append((run, 30.0, None))
                continue
            if not run.finish_time_sec or run.finish_time_sec <= 0:
                # タイムなし → 平均以下の保守的評価
                run_deviations.append(45.0)
                run_records_list.append((run, 45.0, None))
                continue
            _banei_base = _get_banei_baseline(run.weight_kg, _banei_baselines)
            dev = 50.0 + (_banei_base - run.finish_time_sec) * _BANEI_TIME_COEFF
            dev = max(-50.0, min(100.0, dev))  # A案: 下限 -50 に拡張
            run_deviations.append(dev)
            run_records_list.append((run, dev, _banei_base))
        # ばんえいはStep 3のfor loopをスキップ（以下のelseブロックが通常馬用）

    if not _banei_mode:
      for run in filtered_runs:
        # 非完走（取消・除外・中止・競走中止 = 着順90以上）はスキップ
        if run.finish_pos >= 90:
            # 取消・除外は最低評価（Noneだと後続でTypeErrorリスク）
            run_deviations.append(30.0)
            run_records_list.append((run, None, None))  # 表示はNone→「—」
            continue
        # 走破タイム・距離が無効な場合はスキップ（JRA公式データ等で未取得の場合）
        if not run.finish_time_sec or run.finish_time_sec <= 0:
            run_deviations.append(30.0)  # タイム不明も低評価
            run_records_list.append((run, None, None))  # 表示はNone→「—」
            continue
        if not run.distance or run.distance <= 0:
            run_deviations.append(50.0)
            run_records_list.append((run, 50.0, None))
            continue

        # course_idが空の場合（NAR公式経由等）はvenue/surface/distanceから復元
        _cid = run.course_id
        if not _cid and run.venue and run.surface and run.distance:
            from data.masters.venue_master import VENUE_NAME_TO_CODE
            _venue = run.venue.lstrip("Ｊ").lstrip("J").strip()
            _vc = VENUE_NAME_TO_CODE.get(_venue, "")
            if _vc:
                _cid = f"{_vc}_{run.surface}_{run.distance}"
        std_time, st_rel = std_calc.calc_standard_time(
            _cid, run.grade, run.condition, run.distance
        )
        if std_time is None:
            run_deviations.append(50.0)
            run_records_list.append((run, 50.0, None))
            continue

        # B-1: 馬場補正（良馬場相当に正規化）
        track_offset = track_corr.calc_empirical_correction(
            surface=run.surface,
            condition=run.condition,
            distance=run.distance,
            is_jra=is_jra,
        )

        # B-3: 競馬場別時計レベル補正（全場平均に正規化）
        venue_offset = 0.0
        org_key = "JRA" if is_jra else "NAR"
        venue_speed_key = f"{org_key}_{run.surface}"
        venue_table = std_calc.VENUE_SPEED_TABLE.get(venue_speed_key, {})
        rate_per200 = venue_table.get(run.venue, 0.0)
        # rate_per200: 正=その場は遅い → 正規化のため負補正(速く換算)
        # 良馬場平均より遅い場でのタイムは、平均レベルに引き上げる（減算）
        venue_offset = -rate_per200 * run.distance / 200.0

        # B-4: 世代限定戦補正（is_generation=True の芝レースは約0.5秒遅い）
        # 2〜3歳限定戦では馬がまだ発達途上のため、芝では実力より遅いタイムが出やすい
        gen_offset = 0.0
        if run.is_generation and run.surface == "\u829d":  # 芝
            gen_offset = 0.50  # 0.5秒加算して一般戦相当タイムへ正規化
        elif run.is_generation and run.surface == "\u30c0\u30fc\u30c8":  # ダート
            gen_offset = 0.10  # ダートは差が小さい

        corrected_time = run.finish_time_sec + track_offset + venue_offset + gen_offset

        # D-5: 斤量補正 (基準斤量との差 × 0.15秒)
        from src.calculator.calibration import get_base_weight

        base_weight = get_base_weight(horse.sex, horse.age, run.race_date)
        weight_corr = (run.weight_kg - base_weight) * 0.15
        corrected_time += weight_corr

        dev = calc_run_deviation(
            corrected_time, std_time, run.distance,
            venue_code=getattr(run, "venue_code", ""),
        )

        # B-5: 中央↔地方クロス補正（全馬場種別）
        # 中央と地方はコース基準・競争レベルが異なるため交差実績に割引を適用
        _JRA_VENUES = {
            "札幌", "函館", "福島", "新潟", "東京",
            "中山", "中京", "京都", "阪神", "小倉",
        }
        run_is_jra = run.venue in _JRA_VENUES
        if run.surface == race_surface:
            if is_jra and not run_is_jra:
                # 現在=中央, 過去走=地方 → JRA↔NARクロス補正: 距離・クラス別に細分化
                if race_surface == "ダート":
                    base_factor = 0.78
                    if race_distance >= 2000:
                        base_factor += 0.05    # 長距離は耐久性が通用しやすい
                    if race_grade in ("OP", "G3", "G2", "G1", "オープン", "重賞"):
                        base_factor += 0.04  # 上級クラスはNAR強豪が通用しやすい
                    factor = min(0.90, base_factor)
                else:
                    factor = 0.85 + (0.05 if race_distance >= 2000 else 0.0)
                dev = 50.0 + (dev - 50.0) * factor
            elif not is_jra and run_is_jra:
                # 現在=地方, 過去走=中央 → わずかに割引
                dev = 50.0 + (dev - 50.0) * 0.90

        # B-6: 着順ガード — 大敗した走で異常に高い偏差値が出るのを防止
        # 基準タイム誤差（フォールバック/NAR/データ不足）による過大評価を安全キャップ
        if run.field_count and run.field_count >= 4:
            finish_ratio = run.finish_pos / run.field_count
            if finish_ratio > 0.75 and dev > 55:
                # 下位25%: dev上限55（B-相当）
                dev = 55.0
            elif finish_ratio > 0.5 and dev > 60:
                # 下位50%: dev上限60（A-相当）
                dev = 60.0

        run_deviations.append(dev)
        run_records_list.append((run, dev, std_time))

    # 4. E-3 着差プロファイル
    chakusa_pattern, close_tuple, chakusa_avg_3, baken_type = calc_chakusa_profile(filtered_runs)
    chakusa_indices = [r.chakusa_index for r in filtered_runs]

    # 5. WA偏差値 (C-2) — 距離帯別の加重平均重みを使用
    wa_dev = calc_weighted_average_deviation(
        run_deviations, chakusa_indices, is_long_break,
        break_days=break_days, horse_age=horse.age or 4,
        distance=race_distance,
    )

    # 5b. 芝ダ転換馬の異馬場WA割引
    # 異馬場走の偏差値は「その馬場での実力」を表す。転向先での実力に直接置き換えはできないが、
    # 48.0（完全未知）よりは有用な情報。50基準で圧縮して控えめな参考値とする。
    surface_switch_adj = 0.0
    if is_surface_switch and run_deviations:
        from config.settings import SURFACE_SWITCH_BASE_DISCOUNT
        # WA/MAXを50基準で割引（例: wa=55 → 48 + (55-48)*0.65 = 52.55）
        wa_dev = 48.0 + (wa_dev - 48.0) * SURFACE_SWITCH_BASE_DISCOUNT
        logger.debug(
            "芝ダ転換: %s → WA割引適用 (discount=%.2f, wa=%.1f)",
            getattr(horse, 'horse_name', '?'), SURFACE_SWITCH_BASE_DISCOUNT, wa_dev,
        )
        # 転換適性スコア算出
        if surface_switch_context:
            switch_dir = surface_switch_context.get("switch_direction", "")
            surface_switch_adj = calc_surface_switch_score(switch_dir, surface_switch_context)
            logger.debug(
                "芝ダ転換適性スコア: %s → adj=%.2f (%s)",
                getattr(horse, 'horse_name', '?'), surface_switch_adj, switch_dir,
            )

    # 6. MAX偏差値（過去走なしの場合は wa_dev と同値で統一）
    max_dev = max(run_deviations) if run_deviations else wa_dev
    if is_surface_switch and run_deviations:
        from config.settings import SURFACE_SWITCH_BASE_DISCOUNT
        max_dev = 48.0 + (max_dev - 48.0) * SURFACE_SWITCH_BASE_DISCOUNT

    # 7. α算出 (C-3)
    trend = detect_trend(run_deviations, recent_runs=filtered_runs)
    is_declining = trend in (Trend.DOWN, Trend.RAPID_DOWN)
    alpha = calc_alpha(max_dev, wa_dev, is_declining)

    # 8. 信頼度（enum + スコア）
    if _banei_mode:
        # ばんえい: 基準タイム信頼度は不要、過去走数のみで判定
        from src.models import Reliability as _BaneiRel
        st_rel = _BaneiRel.A if len(filtered_runs) >= 3 else _BaneiRel.B
    else:
        _, st_rel = std_calc.calc_standard_time(course_id, "", current_condition, 2000)  # ダミー
    # 転換馬は信頼度を1段階下げる（異馬場データは同馬場より不確実）
    if is_surface_switch:
        from src.models import Reliability as _SwitchRel
        reliability = _SwitchRel.C
        reliability_score = 0.3
    else:
        reliability, reliability_score = aggregate_reliability(len(filtered_runs), is_long_break, st_rel)

    # 9. クラス落差補正
    last_grade = filtered_runs[0].grade if filtered_runs else ""
    from src.scraper.improvement_dbs import calc_class_adjustment

    class_adjustment = calc_class_adjustment(race_grade, last_grade) if race_grade else 0.0

    # 10. 血統×距離適性補正（新馬・未勝利・過去走少なめで有効）
    bloodline_adjustment = 0.0
    if bloodline_db and (len(filtered_runs) <= 3 or (race_grade and "新馬" in race_grade)):
        from src.scraper.improvement_dbs import calc_bloodline_adjustment

        bloodline_adjustment = calc_bloodline_adjustment(
            horse,
            race_distance,
            race_surface,
            bloodline_db,
            condition=current_condition,
        )
    # 常に算出（過去走数問わず）※参考値として格納
    bloodline_adj_always = 0.0
    if bloodline_db:
        try:
            from src.scraper.improvement_dbs import calc_bloodline_adjustment

            bloodline_adj_always = calc_bloodline_adjustment(
                horse,
                race_distance,
                race_surface,
                bloodline_db,
                condition=current_condition,
            )
        except Exception:
            logger.debug("bloodline adjustment calc failed", exc_info=True)
    pace_adjustment = 0.0
    if pace_db and pace_type:
        from src.scraper.improvement_dbs import calc_pace_adjustment

        pace_adjustment = calc_pace_adjustment(horse, pace_type, pace_db)
    total_adjustment = class_adjustment + bloodline_adjustment + pace_adjustment

    # 表示用: 異表面の走にも偏差値を計算して run_records に含める
    # （能力値計算には影響しない。前走テーブル表示で "—基準なし" を減らすため）
    recorded_dates = {entry[0].race_date for entry in run_records_list}
    try:
        ref_date = datetime.strptime(race_date, "%Y-%m-%d")
    except Exception:
        ref_date = datetime.now()
    days_limit = RACE_HISTORY_DAYS_休養明け if is_long_break else RACE_HISTORY_DAYS_DEFAULT
    for run in sorted(horse.past_runs, key=lambda x: x.race_date, reverse=True):
        if run.race_date in recorded_dates:
            continue
        try:
            run_date = datetime.strptime(run.race_date, "%Y-%m-%d")
        except Exception:
            continue
        days_ago = (ref_date - run_date).days
        if days_ago < 0 or days_ago > days_limit:
            continue
        # 異表面の走の偏差値を計算
        if run.finish_pos >= 90 or not run.finish_time_sec or run.finish_time_sec <= 0:
            run_records_list.append((run, None, None))
            continue
        if not run.distance or run.distance <= 0:
            run_records_list.append((run, None, None))
            continue
        _cross_cid = run.course_id
        if not _cross_cid and run.venue and run.surface and run.distance:
            from data.masters.venue_master import VENUE_NAME_TO_CODE
            _cross_venue = run.venue.lstrip("Ｊ").lstrip("J").strip()
            _cross_vc = VENUE_NAME_TO_CODE.get(_cross_venue, "")
            if _cross_vc:
                _cross_cid = f"{_cross_vc}_{run.surface}_{run.distance}"
        _st, _ = std_calc.calc_standard_time(
            _cross_cid, run.grade, run.condition, run.distance
        )
        if _st is None:
            run_records_list.append((run, None, None))
            continue
        _dev = calc_run_deviation(
            run.finish_time_sec, _st, run.distance,
            venue_code=getattr(run, "venue_code", ""),
        )
        run_records_list.append((run, _dev, _st))

    # 日付降順でソートし直して最大5走
    run_records_list.sort(key=lambda x: x[0].race_date, reverse=True)

    return AbilityDeviation(
        max_dev=max_dev,
        wa_dev=wa_dev,
        alpha=alpha,
        trend=trend,
        reliability=reliability,
        reliability_score=reliability_score,
        chakusa_pattern=chakusa_pattern,
        close_race_win_rate=close_tuple,
        chakusa_index_avg=chakusa_avg_3,
        class_adjustment=total_adjustment,
        bloodline_adj=bloodline_adj_always,
        surface_switch_adj=surface_switch_adj,
        is_surface_switch=is_surface_switch,
        run_records=run_records_list[:5],  # 最大5走保存（表示は3走）
    )
