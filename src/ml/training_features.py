"""
調教データからML特徴量を抽出するモジュール

24本の調教特徴量を以下の8カテゴリで構築（内部では50本計算、MLに渡すのは24本）:
  A. タイム系（自馬比較） [7本]
  B. ラスト加速・余力系 [1本]
  C. 強さ効率系 [4本]
  D. 厩舎基準偏差系 [3本]
  E. ボリューム・パターン系 [3本]
  F. 併せ馬系 [2本]
  G. コメント・評価系 [2本]
  H. 複合・状態推定系 [2本]

設計原則:
  - 調教は「能力の代理変数」ではなく「状態変化のセンサー」
  - 自馬比較が最重要（過去の自分との差が状態変化を捉える）
  - 厩舎基準は未来リーク防止のためローリング計算
  - 欠損値はNaN（LightGBMがネイティブ処理）
"""

import json
import os
import re
import sqlite3
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np

from src.log import get_logger

logger = get_logger(__name__)

DB_PATH = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")),
    "data", "keiba.db",
)

# ── 定数 ──

# 強さラベル → 数値コード
INTENSITY_MAP = {
    "馬なり": 1.0, "馬なり余力": 1.0, "楽走": 1.0,
    "ゲートなり": 1.0,
    "中間軽め": 0.5, "極軽め": 0.5, "連闘のため中間軽め": 0.5,
    "稍強め": 1.5,
    "強め": 2.0, "強めに追う": 2.0,
    "末強め": 2.0, "直強め余力": 2.0,
    "G前仕掛け": 2.5, "G前気合付": 2.5, "G前一杯追": 2.5,
    "末強め余力": 1.5,
    "稍一杯": 2.5, "稍一杯追う": 2.5,
    "一杯": 3.0, "一杯に追う": 3.0,
    "末一杯": 3.0, "末一杯追う": 3.0,
    "直一杯": 3.0, "直一杯追う": 3.0,
    "叩き一杯": 3.0, "叩一杯": 3.0,
    "仕掛け": 2.0, "追って": 2.0,
    "末強め追う": 2.0, "直強め追う": 2.0, "直強め": 2.0,
}

# コース分類マッピング
COURSE_CATEGORY_MAP = {
    # 坂路系 → 0
    "栗坂": 0, "美坂": 0, "小林坂": 0, "門別坂": 0, "牧場坂": 0,
    # ウッド系 → 1
    "美Ｗ": 1, "栗ＣＷ": 1, "函館Ｗ": 1,
    # ダート系 → 2
    "小倉ダ": 2, "札幌ダ": 2, "函館ダ": 2,
    # 芝系 → 3
    "栗芝": 3, "美芝": 3, "函館芝": 3,
    # NAR調教場 → 4
    "大井外": 4, "川崎調教場": 4, "船橋外": 4, "浦和調教場": 4,
    "園田": 4, "西脇": 4, "小林外": 4, "船橋内": 4,
    "川崎本": 4, "浦和本": 4,
    # ポリ・その他 → 5
    "栗Ｐ": 5, "美Ｐ": 5, "栗Ｂ": 5, "美Ｂ": 5,
    "栗Ｅ": 5, "栗障": 5,
}

# 厩舎コメント評価マーク
STABLE_MARK_MAP = {"◎": 3, "○": 2, "△": 1}

# ポジティブキーワード → スコア
POSITIVE_KEYWORDS = {
    "良化": 2, "上向": 1, "好調": 2, "軽快": 1, "抜群": 3,
    "素軽": 1, "万全": 2, "順調": 1, "好気配": 2, "好内容": 2,
    "力強": 1, "活気": 1, "上々": 1, "好仕上": 2,
}

# ネガティブキーワード → スコア（負）
NEGATIVE_KEYWORDS = {
    "不満": -2, "ひと息": -1, "今ひと": -1, "重い": -1,
    "欠く": -1, "太め": -1, "緩い": -1, "不安": -1,
    "物足りな": -1, "地味": -1,
}

# 併せ馬解析パターン
_RE_SENCHAKU = re.compile(r"(\d+\.?\d*)秒先着|先着")
_RE_OKURE = re.compile(r"(\d+\.?\d*)秒遅れ|遅れ")
_RE_DONYU = re.compile(r"同入")
_RE_MARGIN_SEC = re.compile(r"(\d+\.?\d*)秒")

# MLに渡す調教特徴量カラム（24本）
# ※ extract_features()内部では全50本を計算（複合特徴量の依存解決のため）
# ※ このリストがモデルに投入されるカラムを決定する
TRAINING_FEATURE_COLS = [
    # A: タイム系（自馬比較） [7本]
    "train_final_4f",                   # 4Fタイム（first1f_paceの基盤）
    "train_final_3f_self_best_ratio",   # 自馬ベスト比（ピーク接近度）
    "train_final_3f_trend",             # 3Fトレンド（方向性）
    "train_final_3f_rank_in_race",      # レース内3F順位
    "train_final_3f_dev",               # レース内3F偏差値 ★最重要
    "train_final_1f_dev",               # レース内1F偏差値
    "train_final_1f_trend",             # 1Fトレンド
    # B: ラスト加速・余力系 [1本]
    "train_first1f_pace",               # 入り1Fペース（f4-f3）
    # C: 強さ効率系 [4本]
    "train_intensity_max",              # 全追い切りの最大強さ
    "train_3f_per_intensity",           # 3F÷強さ（効率値）
    "train_efficiency_self_diff",       # 効率の自馬差
    "train_narinori_3f",                # 馬なり時3F（無負荷能力読み）
    # D: 厩舎基準偏差系 [3本]
    "train_3f_trainer_dev",             # 厩舎3F偏差値（ローリング）
    "train_1f_trainer_dev",             # 厩舎1F偏差値（ローリング）
    "train_trainer_intensity_diff",     # 厩舎の通常強さとの差
    # E: ボリューム・パターン系 [3本]
    "train_volume_self_diff",           # 本数の自馬差
    "train_intensity_pattern",          # 強さ推移パターン（0-3）
    "train_course_primary",             # 主使用コース種別
    # F: 併せ馬系 [2本]
    "train_partner_margin",             # 併せ馬着差（符号付き秒差）
    "train_partner_win_rate",           # 併せ馬先着率
    # G: コメント・評価系 [2本]
    "train_stable_mark",                # 厩舎評価マーク（◎3,○2,△1,無0）
    "train_comment_sentiment",          # コメント感情スコア
    # H: 複合・状態推定系 [2本]
    "train_state_score",                # 総合状態スコア
    "train_readiness_index",            # 仕上がり指数
]


def _empty_training_features() -> dict:
    """調教データ無し時のデフォルト値（全NaN）"""
    return {col: None for col in TRAINING_FEATURE_COLS}


def _parse_intensity(label: str) -> float:
    """強さラベルを数値に変換"""
    if not label:
        return 1.0
    # 完全一致
    if label in INTENSITY_MAP:
        return INTENSITY_MAP[label]
    # 部分一致フォールバック
    if "一杯" in label:
        return 3.0
    if "強め" in label:
        return 2.0
    if "馬なり" in label or "なり" in label:
        return 1.0
    if "軽め" in label:
        return 0.5
    if "仕掛" in label or "気合" in label:
        return 2.5
    return 1.0


def _parse_splits(splits_json: str) -> dict:
    """splits_jsonをパース。キーをintに変換"""
    if not splits_json or splits_json == "{}":
        return {}
    try:
        raw = json.loads(splits_json)
        return {int(k): float(v) for k, v in raw.items()}
    except (json.JSONDecodeError, ValueError):
        return {}


def _get_3f(splits: dict) -> Optional[float]:
    """3F(600m)タイムを取得"""
    return splits.get(600)


def _get_1f(splits: dict) -> Optional[float]:
    """1F(200m)タイムを取得"""
    return splits.get(200)


def _get_4f(splits: dict) -> Optional[float]:
    """4F(800m)タイムを取得"""
    return splits.get(800)


def _calc_accel(splits: dict) -> Optional[float]:
    """加速度 = 3F平均ペース - 1Fペース。正=末脚加速"""
    f3 = _get_3f(splits)
    f1 = _get_1f(splits)
    if f3 is None or f1 is None or f3 <= 0:
        return None
    avg_pace_per_f = f3 / 3.0
    return avg_pace_per_f - f1


def _parse_partner_from_comment(comment: str) -> Tuple[Optional[int], Optional[float]]:
    """コメントから併せ馬結果を抽出。(result, margin_sec)"""
    if not comment:
        return None, None
    m = _RE_SENCHAKU.search(comment)
    if m:
        margin = None
        mm = _RE_MARGIN_SEC.search(comment)
        if mm:
            try:
                margin = float(mm.group(1))
            except ValueError:
                pass
        return 1, margin
    m = _RE_OKURE.search(comment)
    if m:
        margin = None
        mm = _RE_MARGIN_SEC.search(comment)
        if mm:
            try:
                margin = float(mm.group(1))
            except ValueError:
                pass
        return -1, margin
    if _RE_DONYU.search(comment):
        return 0, 0.0
    return None, None


def _linear_slope(values: list) -> Optional[float]:
    """値リストの線形回帰の傾き"""
    n = len(values)
    if n < 2:
        return None
    x = np.arange(n, dtype=float)
    y = np.array(values, dtype=float)
    # NaN除外
    mask = ~np.isnan(y)
    if mask.sum() < 2:
        return None
    x, y = x[mask], y[mask]
    n = len(x)
    if n < 2:
        return None
    denom = n * np.sum(x**2) - np.sum(x)**2
    if denom == 0:
        return None
    slope = (n * np.sum(x * y) - np.sum(x) * np.sum(y)) / denom
    return float(slope)


def _comment_scores(comment: str, stable_comment: str) -> Tuple[float, float]:
    """コメントからポジティブ/ネガティブスコアを計算"""
    pos = 0.0
    neg = 0.0
    texts = []
    if comment:
        texts.append(comment)
    if stable_comment:
        texts.append(stable_comment)
    combined = " ".join(texts)
    for kw, score in POSITIVE_KEYWORDS.items():
        if kw in combined:
            pos += score
    for kw, score in NEGATIVE_KEYWORDS.items():
        if kw in combined:
            neg += score
    return pos, neg


def _stable_mark_score(stable_comment: str) -> int:
    """厩舎コメント先頭の評価マークを数値化"""
    if not stable_comment:
        return 0
    first_char = stable_comment[0] if stable_comment else ""
    return STABLE_MARK_MAP.get(first_char, 0)


class TrainingFeatureExtractor:
    """
    調教特徴量抽出器。

    使い方:
      1. load_all() でDB全データをメモリにロード
      2. extract_features(race_id, horse_name, race_date) で1馬分の特徴量を取得
      3. compute_race_relative(race_features_list) でレース内相対値を計算
    """

    def __init__(self, db_path: Optional[str] = None):
        self._db_path = db_path or DB_PATH
        # {race_id → {horse_name → [TrainingRow, ...]}}
        self._race_training: Dict[str, Dict[str, list]] = {}
        # {horse_id → [(race_date, race_id, final_splits, final_intensity, all_records), ...]}
        self._horse_history: Dict[str, list] = {}
        # {trainer_name → {course → [(race_date, 3f, 1f, intensity_code), ...]}}
        self._trainer_stats: Dict[str, Dict[str, list]] = {}
        # {race_id+horse_name → horse_id}
        self._horse_id_map: Dict[str, str] = {}
        # {race_id+horse_name → trainer_name}
        self._trainer_map: Dict[str, str] = {}
        self._loaded = False
        self._race_date_cache: Dict[str, str] = {}
        self._race_date_loaded = False
        import threading
        self._lock = threading.Lock()

    def load_all(self):
        """DBから全調教データ+紐付け情報をロード"""
        if self._loaded:
            return
        with self._lock:
            if self._loaded:  # ダブルチェックロッキング
                return
            self._load_all_impl()

    def _load_all_impl(self):
        """ロード本体（ロック内で呼ばれる）"""
        logger.info("調教特徴量: DBからデータロード中...")
        conn = sqlite3.connect(self._db_path)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            # 1. training_records 全件ロード
            cur.execute("""
                SELECT race_id, horse_name, date, course, splits_json,
                       intensity_label, sigma_from_mean, comment, stable_comment
                FROM training_records
                ORDER BY race_id, horse_name, id
            """)
            raw_count = 0
            for row in cur.fetchall():
                rid = row["race_id"]
                hname = row["horse_name"]
                if rid not in self._race_training:
                    self._race_training[rid] = {}
                if hname not in self._race_training[rid]:
                    self._race_training[rid][hname] = []
                self._race_training[rid][hname].append(dict(row))
                raw_count += 1

            # 2. race_log から horse_id, trainer_name を取得
            #    キー: race_id+horse_name（レース結果がある馬の直接マッピング）
            cur.execute("""
                SELECT DISTINCT race_id, horse_name, horse_id, trainer_name, race_date
                FROM race_log
                WHERE horse_name IS NOT NULL AND horse_name != ''
            """)
            for row in cur.fetchall():
                key = row["race_id"] + row["horse_name"]
                self._horse_id_map[key] = row["horse_id"] or ""
                self._trainer_map[key] = row["trainer_name"] or ""

            # 2b. horse_name → 最新の horse_id / trainer_name マッピング
            #     推論時（race_logに未登録のレース）のフォールバック用
            #     同名馬の衝突は最新レースを優先することで実用上問題なし
            cur.execute("""
                SELECT horse_name, horse_id, trainer_name
                FROM race_log
                WHERE horse_name IS NOT NULL AND horse_name != ''
                  AND horse_id IS NOT NULL AND horse_id != ''
                ORDER BY race_date DESC
            """)
            self._horse_name_to_id: Dict[str, str] = {}
            self._horse_name_to_trainer: Dict[str, str] = {}
            for row in cur.fetchall():
                hname = row["horse_name"]
                if hname not in self._horse_name_to_id:
                    self._horse_name_to_id[hname] = row["horse_id"] or ""
                    self._horse_name_to_trainer[hname] = row["trainer_name"] or ""
        finally:
            conn.close()

        # 3. 馬別の調教履歴を構築（horse_id ベース、時系列順）
        self._build_horse_history()
        # 4. 調教師×コース別の統計を構築
        self._build_trainer_stats()

        self._loaded = True
        n_races = len(self._race_training)
        n_horses = len(self._horse_history)
        n_trainers = len(self._trainer_stats)
        logger.info(
            f"調教特徴量: ロード完了 "
            f"({raw_count:,}レコード, {n_races:,}レース, "
            f"{n_horses:,}馬, {n_trainers:,}調教師)"
        )

    def _build_horse_history(self):
        """馬ID別の調教履歴を構築"""
        # {horse_id → [(race_date, race_id, records), ...]} を日付順に
        temp: Dict[str, list] = defaultdict(list)

        for rid, horses in self._race_training.items():
            for hname, records in horses.items():
                key = rid + hname
                hid = self._horse_id_map.get(key, "")
                if not hid:
                    continue
                # race_dateをrace_logから取得
                # race_idから日付推定（先頭4桁=年, race_logのrace_dateを使用）
                # → _horse_id_mapと同時にrace_dateも取れるようにすべきだが
                #   race_id先頭8桁で近似（YYYYMMDD形式でない場合もある）
                # race_logから取得したrace_dateを使う
                race_date = self._get_race_date(rid)
                if not race_date:
                    continue

                # 最終追い切り（splits有の最後の行）を特定
                final_splits = {}
                final_intensity = 1.0
                for rec in reversed(records):
                    s = _parse_splits(rec.get("splits_json", ""))
                    if s:
                        final_splits = s
                        final_intensity = _parse_intensity(rec.get("intensity_label", ""))
                        break

                temp[hid].append((race_date, rid, final_splits, final_intensity, records))

        # 日付順にソート
        for hid in temp:
            temp[hid].sort(key=lambda x: x[0])
        self._horse_history = dict(temp)

    def _build_trainer_stats(self):
        """調教師×コース別のタイム統計を構築（ローリング計算用）"""
        for rid, horses in self._race_training.items():
            race_date = self._get_race_date(rid)
            if not race_date:
                continue
            for hname, records in horses.items():
                key = rid + hname
                trainer = self._trainer_map.get(key, "")
                if not trainer:
                    continue
                for rec in records:
                    course = rec.get("course", "")
                    if not course:
                        continue
                    splits = _parse_splits(rec.get("splits_json", ""))
                    f3 = _get_3f(splits)
                    f1 = _get_1f(splits)
                    ic = _parse_intensity(rec.get("intensity_label", ""))
                    if f3 is None:
                        continue
                    if trainer not in self._trainer_stats:
                        self._trainer_stats[trainer] = {}
                    if course not in self._trainer_stats[trainer]:
                        self._trainer_stats[trainer][course] = []
                    self._trainer_stats[trainer][course].append(
                        (race_date, f3, f1, ic)
                    )

        # 各リストを日付順にソート
        for trainer in self._trainer_stats:
            for course in self._trainer_stats[trainer]:
                self._trainer_stats[trainer][course].sort(key=lambda x: x[0])

    def _get_race_date(self, race_id: str) -> str:
        """race_idからrace_dateを取得（キャッシュ付き）"""
        if not self._race_date_loaded:
            # 一括ロード
            try:
                conn = sqlite3.connect(self._db_path)
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT DISTINCT race_id, race_date FROM race_log WHERE race_date IS NOT NULL")
                    for rid, rd in cur.fetchall():
                        self._race_date_cache[rid] = rd
                finally:
                    conn.close()
                self._race_date_loaded = True
            except Exception:
                pass
        return self._race_date_cache.get(race_id, "")

    def extract_features(
        self,
        race_id: str,
        horse_name: str,
        race_date: str,
    ) -> dict:
        """
        1馬分の調教特徴量を抽出する。

        Args:
            race_id: レースID
            horse_name: 馬名
            race_date: レース日付（YYYY-MM-DD）

        Returns:
            50特徴量のdict（キー=TRAINING_FEATURE_COLS）
        """
        if not self._loaded:
            self.load_all()

        # 当該レースの調教データ
        race_horses = self._race_training.get(race_id, {})
        records = race_horses.get(horse_name, [])

        if not records:
            return _empty_training_features()

        # ── 基礎データ抽出 ──

        # 追い切り行（splits有）を分離
        workout_rows = []
        comment_row = None
        for rec in records:
            s = _parse_splits(rec.get("splits_json", ""))
            if s:
                workout_rows.append((rec, s))
            elif not comment_row:
                comment_row = rec

        if not workout_rows:
            # splitsが無い場合でもコメント系特徴量は抽出
            return self._extract_comment_only_features(records)

        # 最終追い切り（最後のworkout行）
        final_rec, final_splits = workout_rows[-1]
        f3 = _get_3f(final_splits)
        f1 = _get_1f(final_splits)
        f4 = _get_4f(final_splits)
        final_intensity = _parse_intensity(final_rec.get("intensity_label", ""))

        # ── 自馬過去データ取得 ──
        key = race_id + horse_name
        hid = self._horse_id_map.get(key, "")
        # フォールバック: race_logに未登録のレース → horse_nameで逆引き
        if not hid:
            hid = getattr(self, "_horse_name_to_id", {}).get(horse_name, "")
        past_history = []
        if hid and hid in self._horse_history:
            past_history = [
                (rd, rid, fs, fi, recs)
                for rd, rid, fs, fi, recs in self._horse_history[hid]
                if rd < race_date
            ]

        # 過去3走の最終追い切り3F/1F
        past_3f = [_get_3f(fs) for _, _, fs, _, _ in past_history[-3:] if _get_3f(fs) is not None]
        past_1f = [_get_1f(fs) for _, _, fs, _, _ in past_history[-3:] if _get_1f(fs) is not None]
        all_past_3f = [_get_3f(fs) for _, _, fs, _, _ in past_history if _get_3f(fs) is not None]
        all_past_1f = [_get_1f(fs) for _, _, fs, _, _ in past_history if _get_1f(fs) is not None]

        # 過去のaccel
        past_accel = []
        for _, _, fs, _, _ in past_history[-3:]:
            a = _calc_accel(fs)
            if a is not None:
                past_accel.append(a)

        # 過去の効率
        past_efficiency = []
        for _, _, fs, fi, _ in past_history[-3:]:
            pf3 = _get_3f(fs)
            if pf3 is not None and fi > 0:
                past_efficiency.append(pf3 / fi)

        # 過去のσ
        past_sigma = []
        for _, _, _, _, recs in past_history[-3:]:
            for rec in recs:
                s = rec.get("sigma_from_mean")
                if s is not None and s != 0.0:
                    past_sigma.append(s)
                    break

        # 過去の追い切り本数
        past_workout_counts = []
        for _, _, _, _, recs in past_history[-5:]:
            cnt = sum(1 for r in recs if _parse_splits(r.get("splits_json", "")))
            past_workout_counts.append(cnt)

        # 過去の強さ
        past_intensities = [fi for _, _, _, fi, _ in past_history[-3:]]

        # ── A: 最終追い切りタイム系 ──
        features = {}

        features["train_final_3f"] = f3
        features["train_final_1f"] = f1
        features["train_final_4f"] = f4

        # 自馬差
        features["train_final_3f_self_diff"] = (
            (f3 - np.mean(past_3f)) if f3 is not None and past_3f else None
        )
        features["train_final_1f_self_diff"] = (
            (f1 - np.mean(past_1f)) if f1 is not None and past_1f else None
        )

        # ベスト比
        features["train_final_3f_self_best_ratio"] = (
            (f3 / min(all_past_3f)) if f3 is not None and all_past_3f and min(all_past_3f) > 0 else None
        )
        features["train_final_1f_self_best_ratio"] = (
            (f1 / min(all_past_1f)) if f1 is not None and all_past_1f and min(all_past_1f) > 0 else None
        )

        # トレンド（直近3走の傾き）
        trend_3f = [_get_3f(fs) for _, _, fs, _, _ in past_history[-3:] if _get_3f(fs) is not None]
        if f3 is not None:
            trend_3f.append(f3)
        features["train_final_3f_trend"] = _linear_slope(trend_3f)

        trend_1f = [_get_1f(fs) for _, _, fs, _, _ in past_history[-3:] if _get_1f(fs) is not None]
        if f1 is not None:
            trend_1f.append(f1)
        features["train_final_1f_trend"] = _linear_slope(trend_1f)

        # レース内順位・偏差値（後でcompute_race_relativeで上書き）
        features["train_final_3f_rank_in_race"] = None
        features["train_final_3f_dev"] = None
        features["train_final_1f_dev"] = None

        # ── B: ラスト加速・余力系 ──
        accel = _calc_accel(final_splits)
        features["train_accel_ratio"] = (
            (f1 / (f3 / 3.0)) if f3 is not None and f1 is not None and f3 > 0 else None
        )
        features["train_accel_diff"] = accel
        features["train_accel_self_diff"] = (
            (accel - np.mean(past_accel)) if accel is not None and past_accel else None
        )
        features["train_first1f_pace"] = (
            (f4 - f3) if f4 is not None and f3 is not None else None
        )
        features["train_decel_pattern"] = (
            ((f4 - f3) / f1) if f4 is not None and f3 is not None and f1 is not None and f1 > 0 else None
        )
        features["train_accel_rank_in_race"] = None  # 後でcompute_race_relative

        # ── C: 強さ効率系 ──
        features["train_intensity_code"] = final_intensity

        all_intensities = [_parse_intensity(r.get("intensity_label", "")) for r, _ in workout_rows]
        features["train_intensity_max"] = max(all_intensities) if all_intensities else None

        efficiency = (f3 / final_intensity) if f3 is not None and final_intensity > 0 else None
        features["train_3f_per_intensity"] = efficiency
        features["train_1f_per_intensity"] = (
            (f1 / final_intensity) if f1 is not None and final_intensity > 0 else None
        )
        features["train_efficiency_self_diff"] = (
            (efficiency - np.mean(past_efficiency))
            if efficiency is not None and past_efficiency else None
        )

        # 前走→今走の強さ変化
        features["train_intensity_escalation"] = (
            (final_intensity - past_intensities[-1])
            if past_intensities else None
        )

        # 馬なり時の3F（馬なり系の場合のみ有効値）
        features["train_narinori_3f"] = f3 if final_intensity <= 1.0 and f3 is not None else None

        # 一杯フラグ
        features["train_ippai_flag"] = int(any(i >= 3.0 for i in all_intensities))

        # ── D: 厩舎基準偏差系 ──
        trainer = self._trainer_map.get(key, "")
        # フォールバック: race_logに未登録のレース → horse_nameで逆引き
        if not trainer:
            trainer = getattr(self, "_horse_name_to_trainer", {}).get(horse_name, "")
        final_course = final_rec.get("course", "")
        sigma = final_rec.get("sigma_from_mean")
        features["train_sigma_trainer"] = sigma if sigma and sigma != 0.0 else None

        # 厩舎×コースの基準偏差値（ローリング）
        t3f_dev, t1f_dev, t_int_diff = self._calc_trainer_deviation(
            trainer, final_course, race_date, f3, f1, final_intensity
        )
        features["train_3f_trainer_dev"] = t3f_dev
        features["train_1f_trainer_dev"] = t1f_dev
        features["train_trainer_intensity_diff"] = t_int_diff

        # σの自馬差
        features["train_sigma_self_diff"] = (
            (sigma - np.mean(past_sigma))
            if sigma is not None and sigma != 0.0 and past_sigma else None
        )

        # ── E: ボリューム・パターン系 ──
        workout_count = len(workout_rows)
        features["train_workout_count"] = workout_count
        features["train_workout_count_strong"] = sum(
            1 for i in all_intensities if i >= 2.0
        )
        features["train_volume_self_diff"] = (
            (workout_count - np.mean(past_workout_counts))
            if past_workout_counts else None
        )

        # 最終週に強い追い切りがあるか
        features["train_final_week_strong"] = int(final_intensity >= 2.0)

        # 強さの推移パターン
        features["train_intensity_pattern"] = self._classify_intensity_pattern(all_intensities)

        # 主使用コース
        course_cats = [
            COURSE_CATEGORY_MAP.get(r.get("course", ""), 5) for r, _ in workout_rows
        ]
        features["train_course_primary"] = (
            max(set(course_cats), key=course_cats.count) if course_cats else None
        )
        features["train_course_variety"] = len(set(
            r.get("course", "") for r, _ in workout_rows if r.get("course", "")
        ))

        # ── F: 併せ馬系 ──
        partner_results = []
        partner_margins = []
        for rec, _ in workout_rows:
            result, margin = _parse_partner_from_comment(rec.get("comment", ""))
            if result is not None:
                partner_results.append(result)
                if margin is not None:
                    partner_margins.append(margin)

        # コメント行からも併せ馬を検出
        if comment_row:
            result, margin = _parse_partner_from_comment(comment_row.get("comment", ""))
            if result is not None:
                partner_results.append(result)
                if margin is not None:
                    partner_margins.append(margin)

        # 最終追い切りの併せ馬結果
        final_partner, final_margin = _parse_partner_from_comment(
            final_rec.get("comment", "")
        )
        features["train_partner_result"] = final_partner
        features["train_partner_margin"] = (
            final_margin * (1 if final_partner == 1 else -1)
            if final_margin is not None and final_partner is not None and final_partner != 0
            else (0.0 if final_partner == 0 else None)
        )
        features["train_partner_count"] = len(partner_results)
        wins = sum(1 for r in partner_results if r == 1)
        total = len(partner_results)
        features["train_partner_win_rate"] = (wins / total) if total > 0 else None
        features["train_partner_flag"] = int(final_partner is not None)

        # ── G: コメント・評価系 ──
        # 全レコードのコメントを集約
        all_comments = " ".join(
            (r.get("comment", "") or "") for r in ([comment_row] if comment_row else []) + [rec for rec, _ in workout_rows]
        )
        all_stable = ""
        for rec in records:
            sc = rec.get("stable_comment", "")
            if sc:
                all_stable = sc
                break  # 厩舎コメントは1つだけ

        features["train_stable_mark"] = _stable_mark_score(all_stable)
        pos_score, neg_score = _comment_scores(all_comments, all_stable)
        features["train_comment_positive_score"] = pos_score
        features["train_comment_negative_score"] = neg_score
        features["train_comment_sentiment"] = pos_score + neg_score
        features["train_good_comment_flag"] = int(pos_score >= 2)

        # ── H: 複合・状態推定系 ──
        features["train_state_score"] = self._calc_state_score(features)
        features["train_readiness_index"] = self._calc_readiness_index(features)

        return features

    def _extract_comment_only_features(self, records: list) -> dict:
        """splitsが無い場合のコメント系特徴量のみ抽出"""
        features = _empty_training_features()

        all_comments = " ".join((r.get("comment", "") or "") for r in records)
        all_stable = ""
        for rec in records:
            sc = rec.get("stable_comment", "")
            if sc:
                all_stable = sc
                break

        features["train_stable_mark"] = _stable_mark_score(all_stable)
        pos_score, neg_score = _comment_scores(all_comments, all_stable)
        features["train_comment_positive_score"] = pos_score
        features["train_comment_negative_score"] = neg_score
        features["train_comment_sentiment"] = pos_score + neg_score
        features["train_good_comment_flag"] = int(pos_score >= 2)

        # 強さ情報
        for rec in records:
            il = rec.get("intensity_label", "")
            if il:
                features["train_intensity_code"] = _parse_intensity(il)
                break

        return features

    def _calc_trainer_deviation(
        self,
        trainer: str,
        course: str,
        race_date: str,
        f3: Optional[float],
        f1: Optional[float],
        intensity: float,
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """厩舎×コースの基準に対する偏差値を計算（ローリング）"""
        if not trainer or not course:
            return None, None, None

        stats = self._trainer_stats.get(trainer, {}).get(course, [])
        if not stats:
            return None, None, None

        # race_date より前のデータのみ使用
        past = [(rd, t3, t1, ic) for rd, t3, t1, ic in stats if rd < race_date]
        if len(past) < 5:
            return None, None, None

        t3_vals = [t3 for _, t3, _, _ in past if t3 is not None]
        t1_vals = [t1 for _, _, t1, _ in past if t1 is not None]
        ic_vals = [ic for _, _, _, ic in past]

        t3_dev = None
        if f3 is not None and len(t3_vals) >= 5:
            mean = np.mean(t3_vals)
            std = np.std(t3_vals)
            if std > 0.1:
                t3_dev = 50.0 + 10.0 * (mean - f3) / std

        t1_dev = None
        if f1 is not None and len(t1_vals) >= 5:
            mean = np.mean(t1_vals)
            std = np.std(t1_vals)
            if std > 0.05:
                t1_dev = 50.0 + 10.0 * (mean - f1) / std

        t_int_diff = None
        if ic_vals:
            t_int_diff = intensity - np.mean(ic_vals)

        return t3_dev, t1_dev, t_int_diff

    def _classify_intensity_pattern(self, intensities: list) -> int:
        """強さ推移パターンを分類
        0 = 全馬なり（全て≤1.0）
        1 = 徐々に上げ（最後が最強 or 後半に強い）
        2 = 最初から強い（最初が最強 or 前半に強い）
        3 = 一定（変化なし）
        """
        if not intensities:
            return 0
        if all(i <= 1.0 for i in intensities):
            return 0
        if len(intensities) == 1:
            return 3
        # 後半の平均 vs 前半の平均
        mid = len(intensities) // 2
        first_half = np.mean(intensities[:mid]) if mid > 0 else intensities[0]
        second_half = np.mean(intensities[mid:])
        if second_half > first_half + 0.3:
            return 1  # 徐々に上げ
        elif first_half > second_half + 0.3:
            return 2  # 最初から強い
        return 3  # 一定

    def _calc_state_score(self, features: dict) -> Optional[float]:
        """総合状態スコア"""
        components = []
        weights = []

        # 自馬σ差（3F）
        v = features.get("train_final_3f_self_diff")
        if v is not None:
            # 負=改善なので反転
            components.append(-v)
            weights.append(0.3)

        # 加速度の自馬差
        v = features.get("train_accel_self_diff")
        if v is not None:
            components.append(v)
            weights.append(0.2)

        # 効率の自馬差
        v = features.get("train_efficiency_self_diff")
        if v is not None:
            components.append(-v)  # 低い=効率良い
            weights.append(0.2)

        # 厩舎3F偏差値
        v = features.get("train_3f_trainer_dev")
        if v is not None:
            components.append((v - 50.0) / 10.0)  # 偏差値を正規化
            weights.append(0.15)

        # コメント感情
        v = features.get("train_comment_sentiment")
        if v is not None:
            components.append(v)
            weights.append(0.15)

        if not components:
            return None

        total_w = sum(weights)
        return sum(c * w for c, w in zip(components, weights)) / total_w

    def _calc_readiness_index(self, features: dict) -> Optional[float]:
        """仕上がり指数"""
        score = 0.0
        count = 0

        # 本数（多い=しっかり仕上げ）
        wc = features.get("train_workout_count")
        if wc is not None:
            score += min(wc / 3.0, 1.5)  # 3本で1.0、上限1.5
            count += 1

        # 強さパターン（1=徐々に上げが最高）
        ip = features.get("train_intensity_pattern")
        if ip is not None:
            pattern_score = {0: 0.3, 1: 1.0, 2: 0.7, 3: 0.5}
            score += pattern_score.get(ip, 0.5)
            count += 1

        # 最終タイムの自馬比（良い=負の差）
        diff = features.get("train_final_3f_self_diff")
        if diff is not None:
            score += max(-diff, -2.0)  # 2秒以上速くても上限
            count += 1

        # 一杯追い切りフラグ
        ippai = features.get("train_ippai_flag")
        if ippai:
            score += 0.5
            count += 1

        return score / count if count > 0 else None

    def compute_race_relative(
        self,
        race_id: str,
        all_horse_features: Dict[str, dict],
    ):
        """
        レース内相対値を計算して各馬のfeaturesを更新する。

        Args:
            race_id: レースID
            all_horse_features: {horse_name: features_dict}
        """
        # 3F値を持つ馬を収集
        f3_values = {}
        f1_values = {}
        accel_values = {}

        for hname, feats in all_horse_features.items():
            f3 = feats.get("train_final_3f")
            if f3 is not None:
                f3_values[hname] = f3
            f1 = feats.get("train_final_1f")
            if f1 is not None:
                f1_values[hname] = f1
            accel = feats.get("train_accel_diff")
            if accel is not None:
                accel_values[hname] = accel

        # 3F偏差値・順位
        if len(f3_values) >= 2:
            vals = list(f3_values.values())
            mean_3f = np.mean(vals)
            std_3f = np.std(vals)
            sorted_3f = sorted(vals)
            n = len(sorted_3f)

            for hname, f3 in f3_values.items():
                if hname in all_horse_features:
                    # 順位（パーセンタイル、0=最速）
                    rank_idx = sorted_3f.index(f3)
                    all_horse_features[hname]["train_final_3f_rank_in_race"] = rank_idx / max(n - 1, 1)
                    # 偏差値
                    if std_3f > 0.1:
                        all_horse_features[hname]["train_final_3f_dev"] = 50.0 + 10.0 * (mean_3f - f3) / std_3f

        # 1F偏差値
        if len(f1_values) >= 2:
            vals = list(f1_values.values())
            mean_1f = np.mean(vals)
            std_1f = np.std(vals)
            if std_1f > 0.05:
                for hname, f1 in f1_values.items():
                    if hname in all_horse_features:
                        all_horse_features[hname]["train_final_1f_dev"] = 50.0 + 10.0 * (mean_1f - f1) / std_1f

        # 加速度順位
        if len(accel_values) >= 2:
            sorted_accel = sorted(accel_values.values(), reverse=True)  # 大きい=良い
            n = len(sorted_accel)
            for hname, accel in accel_values.items():
                if hname in all_horse_features:
                    rank_idx = sorted_accel.index(accel)
                    all_horse_features[hname]["train_accel_rank_in_race"] = rank_idx / max(n - 1, 1)

    def get_race_training_features(
        self,
        race_id: str,
        horse_names: List[str],
        race_date: str,
    ) -> Dict[str, dict]:
        """
        レース全馬分の調教特徴量を一括取得する（レース内相対値計算込み）。

        Args:
            race_id: レースID
            horse_names: 馬名リスト
            race_date: レース日付

        Returns:
            {horse_name: features_dict}
        """
        all_features = {}
        for hname in horse_names:
            all_features[hname] = self.extract_features(race_id, hname, race_date)

        # レース内相対値を計算
        self.compute_race_relative(race_id, all_features)

        return all_features


# ============================================================
# 調教偏差値 (20-100 スケール)
# ============================================================

# 強さ推移パターンの連続値変換
_INTENSITY_PATTERN_SCORE = {0: 0.3, 1: 1.0, 2: 0.7, 3: 0.5}

# 5軸の定義: (特徴量名, 方向(+1=高い方が良い), 軸内重み)
_DEV_AXES = {
    "speed": {                     # 追切スピード
        "weight": 0.30,
        "features": [
            ("train_final_3f_dev",              +1, 0.65),  # レース内3F偏差値 ★ML重要度1位
            ("train_final_1f_dev",              +1, 0.35),  # レース内1F偏差値
        ],
    },
    "condition": {                 # 状態変化
        "weight": 0.25,
        "features": [
            ("train_3f_trainer_dev",            +1, 0.35),  # 厩舎3F偏差値
            ("train_1f_trainer_dev",            +1, 0.15),  # 厩舎1F偏差値
            ("train_final_3f_self_best_ratio",  -1, 0.25),  # 自馬ベスト比(低い=好調)
            ("train_final_3f_trend",            -1, 0.25),  # 3Fトレンド(負=改善)
        ],
    },
    "fitness": {                   # 仕上がり
        "weight": 0.20,
        "features": [
            ("train_efficiency_self_diff",      -1, 0.35),  # 効率自馬差(負=改善)
            ("train_volume_self_diff",          +1, 0.25),  # 本数自馬差(正=入念)
            ("train_intensity_pattern_score",   +1, 0.25),  # 強さ推移スコア(高=漸増)
            ("train_trainer_intensity_diff",    +1, 0.15),  # 厩舎強さ差(正=強め)
        ],
    },
    "evaluation": {                # 関係者評価
        "weight": 0.15,
        "features": [
            ("train_stable_mark",              +1, 0.55),  # 厩舎マーク(0-3) ★ML重要度3位
            ("train_comment_sentiment",        +1, 0.45),  # コメント感情
        ],
    },
    "partner": {                   # 併せ馬
        "weight": 0.10,
        "features": [
            ("train_partner_margin",           +1, 0.60),  # 着差(正=先着)
            ("train_partner_win_rate",         +1, 0.40),  # 先着率
        ],
    },
}

# 偏差値計算に必要な最低軸数（これ未満ならNone）
_MIN_AXES_FOR_DEV = 2
# 各特徴量のZスコア算出に必要な最低馬数
_MIN_HORSES_FOR_ZSCORE = 3


def calc_training_dev(
    horse_features: Dict[str, dict],
) -> Dict[str, Optional[float]]:
    """
    レース出走馬の調教データから調教偏差値(20-100)を算出する。

    アルゴリズム:
      1. 各特徴量をレース内でZスコア化（方向補正あり）
      2. 軸内で重み付き平均Zスコアを算出
      3. 5軸のZスコアを軸重みで加重平均
      4. 最終偏差値 = 50 + 10 × 加重平均Z → [20, 100] クランプ

    Args:
        horse_features: {horse_name: features_dict}（get_race_training_features の戻り値）

    Returns:
        {horse_name: training_dev or None}
    """
    horse_names = list(horse_features.keys())
    n = len(horse_names)
    if n < _MIN_HORSES_FOR_ZSCORE:
        return {h: None for h in horse_names}

    # 前処理: intensity_patternをスコアに変換
    for hname in horse_names:
        fd = horse_features[hname]
        ip = fd.get("train_intensity_pattern")
        if ip is not None and not (isinstance(ip, float) and np.isnan(ip)):
            fd["train_intensity_pattern_score"] = _INTENSITY_PATTERN_SCORE.get(int(ip), 0.5)
        else:
            fd["train_intensity_pattern_score"] = None

    # 軸別Zスコアを算出
    # axis_z[hname] = {axis_name: z_score}
    axis_z: Dict[str, Dict[str, float]] = {h: {} for h in horse_names}

    for axis_name, axis_def in _DEV_AXES.items():
        # この軸内の各特徴量をZスコア化
        feat_zscores: Dict[str, list] = {h: [] for h in horse_names}  # [(z, weight), ...]
        feat_weights: Dict[str, list] = {h: [] for h in horse_names}

        for feat_name, direction, feat_weight in axis_def["features"]:
            # 値を収集
            vals = []
            idxs = []
            for i, hname in enumerate(horse_names):
                v = horse_features[hname].get(feat_name)
                if v is not None and not (isinstance(v, float) and np.isnan(v)):
                    vals.append(float(v) * direction)
                    idxs.append(i)

            if len(vals) < _MIN_HORSES_FOR_ZSCORE:
                continue  # データ不足 → この特徴量スキップ

            mean_v = np.mean(vals)
            std_v = np.std(vals)
            if std_v < 1e-9:
                continue  # 分散なし → 全馬同値

            for v, i in zip(vals, idxs):
                z = (v - mean_v) / std_v
                hname = horse_names[i]
                feat_zscores[hname].append(z * feat_weight)
                feat_weights[hname].append(feat_weight)

        # 軸内加重平均Zスコア
        for hname in horse_names:
            zs = feat_zscores[hname]
            ws = feat_weights[hname]
            if zs:
                total_w = sum(ws)
                axis_z[hname][axis_name] = sum(zs) / total_w

    # 全軸の加重平均 → 偏差値
    result: Dict[str, Optional[float]] = {}
    for hname in horse_names:
        az = axis_z[hname]
        if len(az) < _MIN_AXES_FOR_DEV:
            result[hname] = None
            continue

        # 利用可能な軸のみで重み再配分
        total_axis_w = sum(_DEV_AXES[a]["weight"] for a in az)
        final_z = sum(az[a] * _DEV_AXES[a]["weight"] for a in az) / total_axis_w
        dev = 50.0 + 10.0 * final_z
        dev = max(20.0, min(100.0, dev))
        result[hname] = round(dev, 1)

    return result
