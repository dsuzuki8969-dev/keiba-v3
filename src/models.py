"""
競馬解析マスターシステム v3.0 - データモデル
計算層と分析層で共通して使うデータ構造を定義する
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


def _same_jockey(name_a: str, name_b: str) -> bool:
    """
    騎手名の同一判定。短縮名(松岡)とフルネーム(松岡正海)、
    記号付き(△長浜)と(長浜鴻緒)を同一と見做す。
    """

    def _core(s: str) -> str:
        return re.sub(r"^[△▲★☆◉◎○☆\s]+", "", s).strip()

    a, b = _core(name_a), _core(name_b)
    if not a or not b:
        return False
    if a == b:
        return True
    return (len(a) <= len(b) and b.startswith(a)) or (len(b) <= len(a) and a.startswith(b))


# ============================================================
# Enum定義
# ============================================================


class Reliability(Enum):
    """データ信頼度 (E-2)"""

    A = "A"  # 十分なサンプル
    B = "B"  # やや不足
    C = "C"  # 不足


class Trend(Enum):
    """トレンドフラグ (E-1)"""

    RAPID_UP = "↗↗急上昇"
    UP = "↗上昇"
    STABLE = "→安定"
    DOWN = "↘下降"
    RAPID_DOWN = "↘↘急下降"


class PaceType(Enum):
    """ペース3段階 (F-1) — Phase 14: 5段階→3段階に統合"""

    H = "H"   # ハイペース
    M = "M"   # ミドルペース
    S = "S"   # スローペース


class RunningStyle(Enum):
    """脚質 (F-2) — 7分類"""

    NIGASHI = "逃げ"
    SENKOU = "先行"
    KOUI = "好位"        # 先行と差しの間
    CHUUDAN = "中団"     # 差しの前半
    SASHIKOMI = "差し"
    OIKOMI = "追込"
    MACURI = "マクリ"


class ChakusaPattern(Enum):
    """着差パターン (E-3)"""

    ASSHŌ = "圧勝型"
    KENJO = "堅実型"
    MURA = "ムラ型"


class BakenType(Enum):
    """馬券タイプ (I-1)"""

    KACHIKIRI = "勝切型"
    BALANCE = "バランス型"
    CHAKUHIROI = "着拾型"
    IPPATSU = "一発型"
    ANTEI = "安定型"


class KishuPattern(Enum):
    """乗り替わり理由 (H-3)"""

    A = "A:戦略的強化"
    B = "B:戦術的"
    C = "C:ローテ都合"
    D = "D:調教目的"
    E = "E:見切り"
    F = "F:事情不明"


class JushaRank(Enum):
    """厩舎ランク (J-1)"""

    A = "A"
    B = "B"
    C = "C"
    D = "D"


class KaisyuType(Enum):
    """回収率タイプ (J-1, H-1)"""

    SHINRAITYPE = "信頼型"
    ANA_TYPE = "穴型"
    KAJOHYOKA = "過大評価型"
    HEIBONTYPE = "平凡型"


class ConfidenceLevel(Enum):
    """展開信頼度 / 自信度 6段階"""

    SS = "SS"
    S = "S"
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"


class Mark(Enum):
    """印 (I-3)"""

    TEKIPAN = "◉"  # 鉄板
    HONMEI = "◎"  # 本命
    TAIKOU = "○"  # 対抗
    TANNUKE = "▲"  # 単穴
    RENDASHI = "△"  # 連下
    RENDASHI2 = "★"  # 連下2（指数5位）
    ANA = "☆"  # 穴（0～2頭）
    KIKEN = "×"  # 危険（0～1頭）
    NONE = "－"


class AnaType(Enum):
    """穴馬タイプ (I-1)"""

    ANA_A = "穴A(隠れ実力馬)"
    ANA_B = "穴B(条件絶好馬)"
    NONE = "該当なし"


class KikenType(Enum):
    """危険馬タイプ (I-2)"""

    KIKEN_A = "危A(能力過大評価)"
    KIKEN_B = "危B(条件絶望的)"
    NONE = "該当なし"


# ============================================================
# コースマスタ (G-1)
# ============================================================


@dataclass
class CourseMaster:
    """コース情報 - 全25場226コース"""

    venue: str  # 競馬場名
    venue_code: str  # 場コード
    distance: int  # 距離(m)
    surface: str  # 芝/ダート
    direction: str  # 右/左
    straight_m: int  # 直線距離(m)
    corner_count: int  # コーナー回数
    corner_type: str  # 大回り/小回り/スパイラル
    _first_corner: str  # 旧定性ラベル（互換用。first_corner_mから自動算出）
    slope_type: str  # 坂なし/軽坂/急坂
    inside_outside: str  # 内/外/なし
    is_jra: bool = True  # JRA/地方フラグ
    first_corner_m: int = 0  # スタート〜初角距離(m)。0=データなし/直線コース
    # 残り600m（上がり3F）地点データ
    l3f_corners: int = -1  # 残り600mで通過するコーナー数 (-1=自動計算)
    l3f_elevation: float = 0.0  # 残り600m区間の純高低差(m, 正=上り坂あり)
    l3f_hill_start: int = 0  # ゴール前何mから急坂開始 (0=坂なし)

    def __post_init__(self):
        """残り600mコーナー数の自動推定"""
        if self.l3f_corners == -1:
            if self.distance < 600:
                self.l3f_corners = 0  # ばんえい等
            elif self.straight_m >= 600:
                self.l3f_corners = 0  # 直線のみ（新潟外1000m等）
            elif self.corner_count == 0:
                self.l3f_corners = 0  # 直線コース
            else:
                # 600m - 直線距離 = コーナー区間距離
                corner_m = 600 - self.straight_m
                # コーナー1つの弧長は概ね200-350m
                # corner_m ≤ 300m → 4角だけで収まる(1コーナー)
                # corner_m > 300m → 3角+4角を跨ぐ(2コーナー)
                self.l3f_corners = 1 if corner_m <= 300 else 2

    @property
    def l3f_corner_m(self) -> int:
        """残り600mのうちコーナー区間の距離(m)"""
        if self.distance < 600:
            return 0  # ばんえい等、600m未満のコースは対象外
        return max(0, 600 - self.straight_m)

    @property
    def l3f_straight_pct(self) -> float:
        """残り600mのうち直線の比率 (0.0-1.0)"""
        if self.distance < 600:
            return 1.0  # 距離600m未満は対象外
        return min(1.0, self.straight_m / 600)

    @property
    def l3f_has_hill(self) -> bool:
        """残り600m区間に坂があるか"""
        return self.l3f_hill_start > 0

    @property
    def l3f_desc(self) -> str:
        """残り600m地点の説明文（展開描写用）"""
        if self.straight_m >= 600:
            return "直線のみ"
        if self.l3f_corners == 0:
            return "直線のみ"
        corner_m = 600 - self.straight_m
        if self.l3f_corners >= 2:
            desc = f"3角途中→4角→直線{self.straight_m}m"
        else:
            desc = f"4角途中→直線{self.straight_m}m"
        if self.l3f_has_hill:
            desc += f"（ゴール前{self.l3f_hill_start}mから坂）"
        return desc

    @property
    def first_corner(self) -> str:
        """初角距離ラベル: first_corner_mから自動算出"""
        if self.first_corner_m <= 0:
            return self._first_corner or "直線のみ"
        if self.first_corner_m <= 200:
            return "短い"
        elif self.first_corner_m <= 400:
            return "平均"
        else:
            return "長い"

    @property
    def course_id(self) -> str:
        return f"{self.venue_code}_{self.surface}_{self.distance}"

    def similarity_score(self, other: "CourseMaster") -> float:
        """コース類似度スコア(最大7.5pt) G-1"""
        score = 0.0
        if self.surface == other.surface:
            score += 2.0
        if self.direction == other.direction:
            score += 1.5
        if abs(self.distance - other.distance) <= 200:
            score += 1.5
        if self.corner_type == other.corner_type:
            score += 1.0
        if self.slope_type == other.slope_type:
            score += 0.5
        if self.inside_outside == other.inside_outside:
            score += 0.5
        return min(score, 7.5)


# ============================================================
# 過去走データ
# ============================================================


@dataclass
class PastRun:
    """1走分の過去走データ"""

    race_date: str  # YYYY-MM-DD
    venue: str
    course_id: str
    distance: int
    surface: str
    condition: str  # 良/稍重/重/不良
    class_name: str  # クラス名
    grade: str  # G1/G2/G3/OP/3勝/2勝/1勝/未勝利/新馬
    field_count: int  # 出走頭数
    gate_no: int  # 枠番
    horse_no: int  # 馬番
    jockey: str
    weight_kg: float  # 斤量
    position_4c: int  # 4角通過順位（後方互換）
    finish_pos: int  # 着順
    finish_time_sec: float  # 走破タイム(秒)
    last_3f_sec: float  # 上がり3F(秒)
    margin_behind: float  # 後着差(秒換算)
    margin_ahead: float  # 前着差(秒換算, 1着は0)
    pace: Optional[PaceType] = None
    horse_weight: Optional[int] = None  # 馬体重(kg)
    weight_change: Optional[int] = None  # 増減
    positions_corners: List[int] = field(
        default_factory=list
    )  # 全コーナー通過順位 [1角,2角,3角,4角...]
    first_3f_sec: Optional[float] = None  # 前半3F(秒) ペース列から取得
    jockey_id: str = ""  # 騎手ID（馬場状態別集計用）
    trainer_id: str = ""  # 調教師ID（馬場状態別集計用）
    is_generation: bool = False  # 世代限定戦フラグ (2歳・3歳限定)
    race_level_dev: Optional[float] = (
        None  # レースレベル偏差値（勝ち馬タイム基準）同一レース内で共通
    )
    tansho_odds: Optional[float] = None   # その走の確定単勝オッズ（LGBMオッズ予測用）
    popularity_at_race: Optional[int] = None  # その走の人気順位
    race_id: str = ""  # netkeibaのレースID（結果ページリンク用）
    result_cname: str = ""  # JRA公式結果ページCNAME（JRA公式リンク用）
    race_no: int = 0  # レース番号（前走リンク生成用）
    source: str = ""  # データ取得元 ("netkeiba"/"official"/"keibabook"/"rakuten")

    @property
    def relative_position(self) -> float:
        """相対位置 = 4角通過順位÷出走頭数 (F-2)。positions_cornersあれば4角相当を使用"""
        pos = None
        if self.positions_corners:
            # 通過順0はデータ欠損 → 最終角から順に有効値を探す
            for _p in reversed(self.positions_corners):
                if isinstance(_p, (int, float)) and _p > 0:
                    pos = _p
                    break
        if pos is None:
            pos = self.position_4c
        if pos is None:
            return 0.5  # ばんえい等で通過順なし → 中団デフォルト
        return pos / self.field_count if self.field_count > 0 else 0.5

    @property
    def chakusa_index(self) -> float:
        """着差評価指数 = 対下位着差 - 対上位着差 (E-3)"""
        return (self.margin_behind or 0.0) - (self.margin_ahead or 0.0)


# ============================================================
# 馬データ
# ============================================================


@dataclass
class Horse:
    """出走馬データ"""

    horse_id: str
    horse_name: str
    sex: str  # 牡/牝/セン
    age: int
    color: str
    trainer: str
    trainer_id: str
    owner: str
    breeder: str
    sire: str
    dam: str
    sire_id: str = ""  # 父馬ID（血統×距離DB用）
    dam_id: str = ""  # 母馬ID
    maternal_grandsire_id: str = ""  # 母父馬ID（母の父）
    maternal_grandsire: str = ""  # 母父馬名（表示用）
    past_runs: List[PastRun] = field(default_factory=list)

    # 当日エントリー情報
    race_date: str = ""
    venue: str = ""
    race_no: int = 0
    gate_no: int = 0
    horse_no: int = 0
    jockey: str = ""
    jockey_id: str = ""
    weight_kg: float = 55.0
    base_weight_kg: float = 55.0  # 性齢定量
    odds: Optional[float] = None  # 確定オッズ (なければNone)
    popularity: Optional[int] = None
    horse_weight: Optional[int] = None
    weight_change: Optional[int] = None

    # 前走騎手
    prev_jockey: str = ""

    # 調教師の所属（美浦・栗東・大井・船橋 etc. 出馬表から取得）
    trainer_affiliation: str = ""

    # 馬主ID（勝負服画像用）
    owner_id: str = ""

    # データ取得元 ("netkeiba"/"official"/"keibabook"/"rakuten")
    source: str = ""

    @property
    def weight_diff(self) -> float:
        """斤量補正量(秒) (D-5)"""
        return (self.weight_kg - self.base_weight_kg) * 0.15

    @property
    def is_jockey_change(self) -> bool:
        """乗り替わり判定 (H-3)。短縮名(松岡)とフルネーム(松岡正海)を同一と見做す"""
        if not self.prev_jockey or not self.jockey:
            return False
        return not _same_jockey(self.prev_jockey, self.jockey)


# ============================================================
# 騎手データ (H-1, H-2)
# ============================================================


@dataclass
class JockeyStats:
    """騎手成績"""

    jockey_id: str
    jockey_name: str

    # 人気別×期間の偏差値 (H-1 2×2=4象限)
    upper_long_dev: float = 50.0  # 上位人気×長期(1年)
    upper_short_dev: float = 50.0  # 上位人気×短期(2ヶ月)
    lower_long_dev: float = 50.0  # 下位人気×長期
    lower_short_dev: float = 50.0  # 下位人気×短期

    # 勢いフラグ (H-1)
    momentum_upper: str = ""  # 好調/不調/""
    momentum_lower: str = ""

    # 回収率タイプ (H-1)
    kaisyu_type: KaisyuType = KaisyuType.HEIBONTYPE

    # コース適性スコア用の生データ
    course_records: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # {course_id: {all_dev, upper_dev, lower_dev, sample_n}}

    # 馬場状態別実績（course_db から集計。良/稍重/重/不良 → {wins, runs}）
    condition_records: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # 所属（美浦・栗東・大井・船橋 etc.）
    location: str = ""

    def get_deviation(self, is_upper_ninki: bool) -> float:
        """人気別偏差値を返す (H-1 長期×0.6+短期×0.4)"""
        if is_upper_ninki:
            return self.upper_long_dev * 0.6 + self.upper_short_dev * 0.4
        else:
            return self.lower_long_dev * 0.6 + self.lower_short_dev * 0.4

    def get_momentum_flag(self, is_upper_ninki: bool) -> str:
        """勢いフラグ (H-1 短期-長期≧+5→好調, ≦-5→不調)"""
        if is_upper_ninki:
            diff = self.upper_short_dev - self.upper_long_dev
        else:
            diff = self.lower_short_dev - self.lower_long_dev
        if diff >= 5:
            return "好調"
        if diff <= -5:
            return "不調"
        return ""


# ============================================================
# 厩舎データ (J-1〜J-4)
# ============================================================


@dataclass
class TrainerStats:
    """調教師・厩舎データ"""

    trainer_id: str
    trainer_name: str
    stable_name: str
    location: str  # JRA/地方

    rank: JushaRank = JushaRank.C
    kaisyu_type: KaisyuType = KaisyuType.HEIBONTYPE

    # 条件別回収率テーブル (J-1)
    recovery_by_class: Dict[str, float] = field(default_factory=dict)
    recovery_break: float = 0.0  # 休み明け
    recovery_distance_change: float = 0.0  # 距離変更
    recovery_jockey_change: float = 0.0  # 乗り替わり

    # ローテーションタイプ (J-3)
    rotation_type: str = "標準"  # 短間隔/標準/間隔空け
    break_type: str = "初戦型"  # 初戦型/叩き良化型
    short_momentum: str = ""  # 短期勢い(2ヶ月)

    # 得意・苦手競馬場
    good_venues: List[str] = field(default_factory=list)
    bad_venues: List[str] = field(default_factory=list)

    # 馬場状態別実績（course_db から集計。良/稍重/重/不良 → {wins, runs}）
    condition_records: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # 騎手と同様の偏差値スコア (win_rate → Z変換: 平均50±10)
    deviation: float = 50.0

    # 騎手×調教師の組み合わせ成績 (J-2)
    jockey_combo: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # {jockey_id: {wins, runs, recovery, is_main_jockey}}


# ============================================================
# 調教データ (J-4)
# ============================================================


@dataclass
class TrainingRecord:
    """調教記録"""

    date: str
    venue: str  # 調教場所
    course: str  # 坂路/CW/芝/ダートなど
    splits: Dict[str, float] = field(default_factory=dict)
    # {"5F": xx.x, "4F": xx.x, "3F": xx.x, "2F": xx.x, "1F": xx.x}
    partner: str = ""  # 一緒に調教した馬
    position: str = ""  # 先行/後追い
    rider: str = ""  # 調教の乗り手（助手/騎手名）
    track_condition: str = ""  # 馬場状態（良/稍/重/不）
    lap_count: str = ""  # 周回数（[7]等）

    # 強度判定(計算層が算出)
    intensity_label: str = "通常"  # 猛時計/やや速い/通常/やや軽め/軽め
    sigma_from_mean: float = 0.0  # 厩舎平均からのσ
    comment: str = ""  # 調教短評（tanpyo）/ 併せ馬コメント
    stable_comment: str = ""  # 厩舎の話（danwa、競馬ブック厩舎の話ページから取得）


# ============================================================
# コース適性スコア (G-0, G-1, G-2, G-3)
# ============================================================


@dataclass
class CourseAptitude:
    """コース適性偏差値の内訳"""

    base_score: float = 50.0  # ベーススコア(計算層)
    course_record: float = 0.0  # ❶コース実績(-8〜+8)
    course_record_n: int = 0  # コース実績のサンプル数（グレード信頼度加重用）
    venue_aptitude: float = 0.0  # ❷競馬場適性(-5〜+5) 4因子類似度ベース（※別途拡大検討）
    venue_aptitude_n: int = 0  # 競馬場適性のサンプル数（グレード信頼度加重用）
    venue_contrib_level: str = ""  # Solo/Pair/Trio/Quartet+
    jockey_course: float = 0.0  # ❸騎手コース影響(-5〜+5) H-2から
    ai_adjustment: float = 0.0  # AI層調整(±12pt)
    norm_adjustment: float = 0.0  # フィールド内正規化補正（実行時に設定）

    @property
    def shape_compatibility(self) -> float:
        """後方互換: venue_aptitude を返す"""
        return self.venue_aptitude

    @property
    def total(self) -> float:
        v = (
            self.base_score
            + self.course_record
            + self.venue_aptitude
            + self.jockey_course
            + self.ai_adjustment
            + self.norm_adjustment
        )
        from config.settings import DEVIATION

        return max(DEVIATION["course"]["min"], min(DEVIATION["course"]["max"], v))


# ============================================================
# 展開偏差値スコア (F-0, F-4)
# ============================================================


@dataclass
class PaceDeviation:
    """展開偏差値の内訳"""

    base_score: float = 50.0  # ベーススコア(計算層)
    last3f_eval: float = 0.0  # ❶末脚評価(-12〜+12)
    position_balance: float = 0.0  # ❷位置取り×末脚バランス(-12〜+12)
    gate_bias: float = 0.0  # ❸枠順バイアス(-8〜+8)
    course_style_bias: float = 0.0  # ❹コース脚質バイアス(-8〜+8)
    jockey_pace: float = 0.0  # ❺騎手展開影響(-6〜+6) H-3から
    trajectory_score: float = 0.0  # ❻軌跡方向スコア(-10〜+5) 下がる馬=大ペナ、上がる馬=ボーナス
    ai_adjustment: float = 0.0  # AI層調整(±18pt)
    norm_adjustment: float = 0.0  # フィールド内正規化補正（実行時に設定）

    # Phase 11c: ゴールタイム偏差値ブレンド
    # pace.total が離散的な last3f_eval 優位で構成され、推定された上がり3F秒値を
    # 直接反映できない問題への対策。全馬の予想ゴールタイム((pos_4c-1)×sec_per_rank+last3f)
    # からフィールド内偏差値を算出し、セットされていれば既存 total と 5:5 blend する。
    # engine.py の全馬ループ後に外部から注入する。
    goal_time_override: Optional[float] = None

    # 推定値
    estimated_position_4c: Optional[float] = None  # 推定4角番手
    estimated_last3f: Optional[float] = None  # 推定上がり3F
    estimated_front_3f: Optional[float] = None  # 各馬の推定前半3F（秒）
    estimated_mid_sec: Optional[float] = None  # 各馬の推定道中タイム（秒）
    running_style: Optional["RunningStyle"] = None  # 脚質（formatter表示用）

    @property
    def total(self) -> float:
        v = (
            self.base_score
            + self.last3f_eval
            + self.position_balance
            + self.gate_bias
            + self.course_style_bias
            + self.jockey_pace
            + self.trajectory_score
            + self.ai_adjustment
            + self.norm_adjustment
        )
        # Phase 11c: ゴールタイム偏差値を 5:5 で blend
        if self.goal_time_override is not None:
            v = v * 0.5 + self.goal_time_override * 0.5
        from config.settings import DEVIATION

        return max(DEVIATION["pace"]["min"], min(DEVIATION["pace"]["max"], v))


# ============================================================
# 能力偏差値スコア (A,B,C,D,E章)
# ============================================================


@dataclass
class AbilityDeviation:
    """能力偏差値の内訳"""

    max_dev: float = 48.0  # MAX偏差値（過去走なし時は48=C+をデフォルト）
    wa_dev: float = 48.0  # 加重平均偏差値 (C-2)（過去走なし時は48=C+をデフォルト）
    alpha: float = 0.5  # α (C-3)
    trend: Trend = Trend.STABLE  # トレンドフラグ (E-1)
    reliability: Reliability = Reliability.B  # 信頼度 (E-2)
    reliability_score: float = 0.5  # 信頼度スコア (0.0〜1.0): C級内の細分化等に使用

    # E-3 着差プロファイル
    chakusa_pattern: ChakusaPattern = ChakusaPattern.KENJO
    close_race_win_rate: Tuple[int, int] = (0, 0)  # (勝, 全)
    chakusa_index_avg: float = 0.0  # 3走平均着差評価指数

    class_adjustment: float = 0.0  # クラス落差補正 (-2〜+2 pt)
    bloodline_adj: float = 0.0  # 血統×距離×馬場補正 (-5.0〜+5.0 pt)
    norm_adjustment: float = 0.0  # フィールド内正規化 + MLフィードバック補正（実行時に設定）
    surface_switch_adj: float = 0.0  # 芝ダ転換補正 (-4.0〜+6.0 pt)
    is_surface_switch: bool = False  # 異馬場転向馬フラグ（同馬場走0 → 異馬場走で推定）
    sire_breakdown: dict = field(default_factory=dict)  # 父馬 surface×SMILE 別複勝率
    run_records: List[Tuple[PastRun, float, Optional[float]]] = field(default_factory=list)
    # [(PastRun, deviation: float, std_time: float | None), ...] 計算に使った走と走破偏差値

    @property
    def total(self) -> float:
        """C-3: 能力偏差値 = MAX×α + WA×(1-α) + クラス落差補正 + 正規化補正 + 転換補正"""
        v = (
            self.max_dev * self.alpha
            + self.wa_dev * (1 - self.alpha)
            + self.class_adjustment
            + self.norm_adjustment
            + self.surface_switch_adj
        )
        from config.settings import DEVIATION

        return max(DEVIATION["ability"]["min"], min(DEVIATION["ability"]["max"], v))


# ============================================================
# 総合評価結果
# ============================================================


@dataclass
class HorseEvaluation:
    """1頭の全評価結果"""

    horse: Horse

    # 各偏差値
    ability: AbilityDeviation = field(default_factory=AbilityDeviation)
    pace: PaceDeviation = field(default_factory=PaceDeviation)
    course: CourseAptitude = field(default_factory=CourseAptitude)

    # 騎手・厩舎
    jockey_stats: Optional[JockeyStats] = None
    trainer_stats: Optional[TrainerStats] = None
    training_records: List[TrainingRecord] = field(default_factory=list)
    jockey_change_pattern: Optional[KishuPattern] = None
    jockey_change_score: float = 0.0  # テン乗りペナルティ込み

    # 穴馬・危険馬
    win_prob: float = 0.0  # 勝率推定
    place2_prob: float = 0.0  # 連対率推定
    place3_prob: float = 0.0  # 複勝率推定
    baken_type: BakenType = BakenType.BALANCE
    ana_score: float = 0.0
    ana_type: AnaType = AnaType.NONE
    tokusen_score: float = 0.0
    is_tokusen: bool = False
    tokusen_kiken_score: float = 0.0
    is_tokusen_kiken: bool = False
    kiken_score: float = 0.0
    kiken_type: KikenType = KikenType.NONE

    # 予測オッズ (5-1)
    predicted_odds: Optional[float] = None

    # 勝負気配スコア (J-2)
    shobu_score: float = 0.0

    # 印
    mark: Mark = Mark.NONE

    odds_consistency_adj: float = 0.0  # オッズ整合性スコア (-4〜+4 pt)
    ml_composite_adj: float = 0.0     # ML偏差値補正 (-6〜+6 pt)
    market_anchor_adj: float = 0.0    # 市場アンカー補正 (-3〜+3 pt)

    # Plan-γ Phase 2: 当該レース内 ability_total z-score 正規化偏差値 (50中心 σ=10, 範囲20〜80)
    race_relative_dev: float = 50.0

    venue_name: str = ""  # 競馬場名 (場別重み適用用)

    ml_place_prob: Optional[float] = None  # LightGBM P(3着以内)
    ml_win_prob: Optional[float] = None    # LightGBM P(1着)
    ml_top2_prob: Optional[float] = None   # LightGBM P(2着以内)

    # Step3: SHAP寄与度グループ {group_name: shap_sum}
    shap_groups: Optional[Dict[str, float]] = None

    # 予想オッズ (ML確率ベース)
    predicted_tansho_odds: Optional[float] = None   # 単勝予想オッズ
    odds_divergence: Optional[float] = None         # 乖離率 (実オッズ/予想オッズ)
    divergence_signal: str = ""                     # 乖離シグナル (S/A/B/C/×)

    @property
    def composite(self) -> float:
        """総合偏差値: 7因子加重平均 + 調教好調ボーナス乗算 + 馬体重変動補正 + オッズ整合性"""
        from config.settings import DEVIATION, get_composite_weights
        from src.scraper.improvement_dbs import calc_weight_change_adjustment

        # 改善1: レース条件を渡して条件別ペースウェイト動的調整
        _surface = getattr(self, "_race_surface", None)
        _field_size = getattr(self, "_race_field_size", None)
        _distance = getattr(self, "_race_distance", None)
        w = get_composite_weights(
            self.venue_name,
            surface=_surface,
            field_size=_field_size,
            distance=_distance,
        )
        # 調教偏差値 → 好調ボーナス係数（不調ペナルティなし）
        # バックテスト検証済み: asym(up=0.006, dn=0.000) が複勝率+0.4pp/単ROI+0.8pp
        jdev = getattr(self, "_jockey_dev", None)
        tdev = getattr(self, "_trainer_dev", None)
        bdev = getattr(self, "_bloodline_dev", None)
        trdev = getattr(self, "_training_dev", None)

        # 調教好調ボーナス: 能力・展開に乗算（50超のみ、不調時は1.0=変化なし）
        _TRAINING_ALPHA = 0.006
        training_multiplier = 1.0
        if trdev is not None and trdev > 50:
            training_multiplier = 1.0 + (trdev - 50) * _TRAINING_ALPHA

        v = (
            self.ability.total * w["ability"] * training_multiplier
            + self.pace.total * w["pace"] * training_multiplier
            + self.course.total * w["course"]
            + (jdev if jdev is not None else 50.0) * w.get("jockey", 0.10)
            + (tdev if tdev is not None else 50.0) * w.get("trainer", 0.05)
            + (bdev if bdev is not None else 50.0) * w.get("bloodline", 0.05)
        )

        # 馬体重・増減の定量評価 (設計書 D-6 強化)
        v += calc_weight_change_adjustment(
            self.horse.weight_change,
            self.horse.horse_weight,
        )
        v += self.odds_consistency_adj
        v += self.ml_composite_adj
        v += self.market_anchor_adj

        return max(DEVIATION["composite"]["min"], min(DEVIATION["composite"]["max"], v))

    @property
    def effective_odds(self) -> Optional[float]:
        """確定オッズ優先、なければ予測オッズ"""
        return self.horse.odds if self.horse.odds is not None else self.predicted_odds

    @property
    def is_ana_candidate(self) -> bool:
        eff = self.effective_odds
        return (
            eff is not None
            and eff >= 10.0
            and (self.horse.popularity or 99) >= 5
        )

    @property
    def is_kiken_candidate(self) -> bool:
        eff = self.effective_odds
        return (
            eff is not None
            and eff < 10.0
            and (self.horse.popularity or 99) <= 3
        )

    @property
    def hybrid_total(self) -> float:
        """Plan-γ Phase 3: ハイブリッド合算指数

        hybrid_total = ability_total × (1-β) + race_relative_dev × β
        β = HYBRID_BETA (デフォルト 0.30)

        USE_HYBRID_SCORING=True 時に印付与・順位判定で採用される。
        False (default) では参考値として算出のみ、印付与には影響しない。

        返値は DEVIATION["ability"]["min"] 〜 DEVIATION["ability"]["max"] にクランプ。
        """
        from config.settings import HYBRID_BETA, DEVIATION
        at = self.ability.total if self.ability else 50.0
        rrd = self.race_relative_dev
        blended = at * (1 - HYBRID_BETA) + rrd * HYBRID_BETA
        return max(DEVIATION["ability"]["min"], min(DEVIATION["ability"]["max"], blended))


# ============================================================
# レース情報
# ============================================================


@dataclass
class RaceInfo:
    """レース情報"""

    race_id: str
    race_date: str  # YYYY-MM-DD
    venue: str
    race_no: int
    race_name: str
    grade: str
    condition: str  # 条件説明
    course: CourseMaster
    field_count: int
    weather: str = ""
    post_time: str = ""  # 発走時刻 "09:50"
    track_condition_turf: str = ""  # 芝馬場状態
    track_condition_dirt: str = ""  # ダート馬場状態
    moisture_turf: Optional[float] = None  # 含水率(芝)
    moisture_dirt: Optional[float] = None  # 含水率(ダート)
    cv_value: Optional[float] = None  # CV値
    is_jra: bool = True


@dataclass
class RaceAnalysis:
    """レース分析の最終結果"""

    race: RaceInfo
    evaluations: List[HorseEvaluation] = field(default_factory=list)
    pace_type_predicted: Optional[PaceType] = None
    pace_reliability: ConfidenceLevel = ConfidenceLevel.B
    leading_horses: List[int] = field(default_factory=list)  # 馬番
    front_horses: List[int] = field(default_factory=list)
    mid_horses: List[int] = field(default_factory=list)
    rear_horses: List[int] = field(default_factory=list)
    favorable_gate: str = ""
    favorable_style: str = ""
    favorable_style_reason: str = ""  # 有利脚質の根拠（ペース・並び・前半/後半3F等）
    estimated_front_3f: Optional[float] = None  # 前半3F推定(秒)
    estimated_last_3f: Optional[float] = None  # 後半3F推定(秒)
    pace_comment: str = ""
    overall_confidence: ConfidenceLevel = ConfidenceLevel.B
    confidence_score: float = 0.0  # 自信度スコア (0.0-1.0)

    # 買い目
    tickets: List[Dict[str, Any]] = field(default_factory=list)
    total_budget: int = 0  # 買い目配分時の予算（回収率表示の分母に使用）
    # [{"type": "馬連", "combo": (3,7), "ev": 125.3, "stake": 2000, "signal": "◎買い"}]
    formation: Optional[Dict[str, List[Any]]] = None
    # {"col1": [...], "col2": [...], "col3": [...], "umaren": [...], "sanrenpuku": [...]}

    # 買い目指南 Phase 1-b: 買う/買わない判定
    bet_decision: Optional[Dict[str, Any]] = None
    # {"skip": bool, "reason": str|None, "message": str, "reference_tickets": list}

    # 買い目指南 Phase 1-c: 3モード（的中率/バランス/回収率）買い目
    tickets_by_mode: Optional[Dict[str, List[Dict[str, Any]]]] = None
    # {"accuracy": [...], "balanced": [...], "recovery": [...]}

    # 予想オッズ・期待値ランキング
    predicted_odds_umaren: List[Dict[str, Any]] = field(default_factory=list)
    predicted_odds_sanrenpuku: List[Dict[str, Any]] = field(default_factory=list)
    value_bets: List[Dict[str, Any]] = field(default_factory=list)
    is_pre_day_mode: bool = False  # 前日モード（実オッズなし）

    # 展開セクション強化フィールド
    predicted_race_time: Optional[float] = None   # 予想走破タイム(秒)
    final_formation: Optional[Dict[str, List[int]]] = None  # 最終隊列予測 {"先頭":[], "好位":[], "中団":[], "後方":[]}
    llm_pace_comment: str = ""   # LLM生成 展開見解
    llm_mark_comment: str = ""   # LLM生成 印見解
    pace_reliability_label: str = ""  # 展開信頼度ラベル (A/B/C等)

    @property
    def sorted_evaluations(self) -> List[HorseEvaluation]:
        """総合偏差値順"""
        return sorted(self.evaluations, key=lambda e: e.composite, reverse=True)
