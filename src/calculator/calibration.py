"""
競馬解析マスターシステム v3.0 - キャリブレーション・マスタデータ

1. 換算定数自動校正 (CONVERSION_CONSTANTをcourse_dbから算出)
2. 性齢定量テーブル (base_weight_kg)
3. コース改修イベントテーブル (G-1 改修履歴管理)
"""

import statistics
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from src.log import get_logger

logger = get_logger(__name__)

# ============================================================
# 1. 換算定数自動校正 (B-2)
# ============================================================


def calibrate_conversion_constant(
    course_db: Dict[str, List],
    target_std: float = 10.0,  # 偏差値の目標σ=10
    target_mean: float = 50.0,
) -> float:
    """
    course_db から換算定数を自動校正する。

    原理:
      走破偏差値 = 50 + (基準タイム - 走破タイム) × 距離係数 × k
      レース内の全走タイムを偏差値化したとき σ ≈ 10 になる k を求める。

    Returns:
        校正後の換算定数 k (デフォルト3.5の代替)
    """
    DISTANCE_BASE = 1600
    diffs: List[float] = []  # (基準タイム - 走破タイム) × 距離係数

    for cid, runs in course_db.items():
        if len(runs) < 10:
            continue

        # 同コース内の走破タイムの分布
        times = []
        for r in runs:
            if isinstance(r, dict):
                t = r.get("finish_time_sec", 0)
                d = r.get("distance", 2000)
            else:
                t = r.finish_time_sec
                d = r.distance
            if t > 0 and d > 0:
                dist_coeff = DISTANCE_BASE / d
                times.append(t * dist_coeff)

        if len(times) < 10:
            continue

        mean_t = statistics.mean(times)
        for t in times:
            diffs.append(mean_t - t)

    if len(diffs) < 30:
        return 3.5  # サンプル不足時はデフォルト値

    # diffs の標準偏差から換算定数を逆算
    sigma_diff = statistics.stdev(diffs)
    if sigma_diff == 0:
        return 3.5

    # σ_偏差値 = σ_diff × k = target_std(=10)
    k = target_std / sigma_diff
    k = max(1.0, min(8.0, k))  # 安全範囲でクリップ
    return round(k, 3)


def recalibrate_and_update(course_db: Dict, settings_path: str = None) -> float:
    """
    換算定数を再校正して返す。
    settings_path が指定されていれば settings.py の値も更新する。
    """
    k = calibrate_conversion_constant(course_db)
    logger.info(
        "換算定数: %.3f (サンプル数: %d走)", k, sum(len(v) for v in course_db.values())
    )

    if settings_path:
        import re

        with open(settings_path, "r", encoding="utf-8") as f:
            content = f.read()
        content = re.sub(r"CONVERSION_CONSTANT\s*=\s*[\d.]+", f"CONVERSION_CONSTANT = {k}", content)
        with open(settings_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info("settings.py 更新: CONVERSION_CONSTANT = %s", k)

    return k


# ============================================================
# 2. 性齢定量テーブル (D-5)
# ============================================================

# JRA 性齢定量テーブル
# {(性別, 年齢, 月): 斤量}
# 一般レース（馬齢重量）: 2歳〜4歳以上
# ※グレードレースは別途ハンデキャップが適用される場合あり

JRA_BASE_WEIGHT: Dict[Tuple, float] = {
    # 2歳
    ("牡", 2, "通年"): 55.0,
    ("牝", 2, "通年"): 54.0,
    ("セン", 2, "通年"): 55.0,
    # 3歳 (前半: 1-6月, 後半: 7-12月)
    ("牡", 3, "前半"): 56.0,
    ("牡", 3, "後半"): 57.0,
    ("牝", 3, "前半"): 54.0,
    ("牝", 3, "後半"): 55.0,
    ("セン", 3, "前半"): 56.0,
    ("セン", 3, "後半"): 57.0,
    # 4歳以上
    ("牡", 4, "通年"): 57.0,
    ("牝", 4, "通年"): 55.0,
    ("セン", 4, "通年"): 57.0,
}

# 4歳以上は全年齢同じ
for age in range(5, 15):
    JRA_BASE_WEIGHT[("牡", age, "通年")] = 57.0
    JRA_BASE_WEIGHT[("牝", age, "通年")] = 55.0
    JRA_BASE_WEIGHT[("セン", age, "通年")] = 57.0


def get_base_weight(sex: str, age: int, race_date: str) -> float:
    """
    性別・年齢・出走月から基準斤量を返す。

    Args:
        sex: "牡" / "牝" / "セン"
        age: 馬齢
        race_date: "YYYY-MM-DD"
    Returns:
        基準斤量 (kg)
    """
    try:
        month = int(race_date[5:7])
    except Exception:
        month = 6

    if age <= 1:
        return 55.0

    if age == 2:
        season = "通年"
    elif age == 3:
        season = "前半" if month <= 6 else "後半"
    else:
        season = "通年"

    # セン馬は牡と同じ扱い
    effective_sex = sex if sex in ("牡", "牝") else "牡"

    # 4歳以上は"通年"キーで統一
    lookup_age = min(age, 4)

    key = (effective_sex, lookup_age, season)
    return JRA_BASE_WEIGHT.get(key, 57.0)


def calc_weight_correction(
    actual_kg: float,
    sex: str,
    age: int,
    race_date: str,
    is_female_race: bool = False,
) -> float:
    """
    D-5: 斤量補正 (秒)
    牝馬限定戦の場合は補正なし

    Returns:
        補正秒数 (正=ハンデ重い、負=軽い)
    """
    if is_female_race:
        return 0.0

    base = get_base_weight(sex, age, race_date)
    diff_kg = actual_kg - base
    return diff_kg * 0.15  # 1kgあたり0.15秒


# ============================================================
# 3. コース改修イベントテーブル (設計書 0章)
# ============================================================

# 改修によってコース特性が変わった日付を記録する
# (改修後は改修前のデータを基準タイム算出から除外するため)
# format: {venue_code: [(改修開始日, 改修終了日, 変更内容)]}

RENOVATION_EVENTS: Dict[str, List[Tuple]] = {
    # JRA
    "05": [  # 東京
        ("2014-01-01", "2014-12-31", "直線整備・芝張替"),
        ("2020-01-01", "2020-06-30", "Aコース→Bコース移行"),
    ],
    "06": [  # 中山
        ("2017-01-01", "2017-03-31", "内ラチ移動工事"),
    ],
    "08": [  # 阪神
        ("2006-01-01", "2006-12-31", "外回りコース新設"),
        ("2022-01-01", "2022-09-30", "芝全面張替"),
    ],
    "07": [  # 京都
        ("2021-01-01", "2023-10-06", "全面改修休場"),
    ],
    "09": [  # 中京
        ("2012-01-01", "2012-12-31", "全面改修"),
    ],
    "10": [  # 小倉
        ("2018-01-01", "2018-12-31", "芝張替・排水改善"),
    ],
    "01": [  # 福島
        ("2020-01-01", "2020-12-31", "排水改善工事"),
    ],
    "02": [  # 新潟
        ("2001-01-01", "2001-12-31", "直線1000mコース新設"),
    ],
    "03": [  # 札幌
        ("2006-01-01", "2006-12-31", "内ラチ移動"),
    ],
    "04": [  # 函館
        ("2012-01-01", "2013-12-31", "全面改修"),
    ],
    # 地方
    "22": [  # 大井
        ("2016-01-01", "2016-06-30", "コース改修"),
    ],
    "21": [  # 川崎
        ("2019-01-01", "2019-06-30", "照明・排水改善"),
    ],
}


def is_pre_renovation(
    venue_code: str,
    race_date: str,
    analysis_date: str,
) -> bool:
    """
    race_date のレースが改修前データかどうかを判定する。
    改修前データは基準タイム算出から除外される。

    Args:
        venue_code: 場コード
        race_date: 判定対象のレース日付
        analysis_date: 分析対象レースの日付 (現在)
    """
    events = RENOVATION_EVENTS.get(venue_code, [])
    if not events:
        return False

    try:
        r_date = datetime.strptime(race_date, "%Y-%m-%d")
        a_date = datetime.strptime(analysis_date, "%Y-%m-%d")
    except Exception:
        return False

    # 分析対象レースより前に改修があり、過去走がその改修前のデータなら除外
    for start, end, description in events:
        try:
            s = datetime.strptime(start, "%Y-%m-%d")
            e = datetime.strptime(end, "%Y-%m-%d")
        except Exception:
            logger.debug("invalid renovation date format: %s / %s", start, end, exc_info=True)
            continue

        # 改修期間が分析日より前に完了している
        if e < a_date:
            # 過去走が改修前なら除外
            if r_date < s:
                return True

    return False


def filter_post_renovation_runs(
    runs: List,
    venue_code: str,
    analysis_date: str,
) -> List:
    """
    改修後のコースに対して、改修前の過去走データを除外する。
    """
    filtered = []
    for r in runs:
        if isinstance(r, dict):
            rd = r.get("race_date", "")
        else:
            rd = r.race_date

        if not is_pre_renovation(venue_code, rd, analysis_date):
            filtered.append(r)

    return filtered


# ============================================================
# 4. 展開見解・コメント自動生成
# ============================================================


def generate_pace_comment(
    pace_type,
    leaders: List[int],
    front_horses: List[int],
    rear_horses: List[int],
    course,
    all_evaluations: List,
    pace_reliability=None,
    lineup: Optional[List] = None,
    mid_horses: Optional[List[int]] = None,
    front_3f_est: Optional[float] = None,
    last_3f_est: Optional[float] = None,
) -> Tuple[str, str, str, str]:
    """
    展開予測コメント・有利な枠・有利な脚質を自動生成する。
    競馬ブック風の流れるような文章で300-400字の見解を生成。

    Returns:
        (pace_comment, favorable_gate, favorable_style, favorable_style_reason)
    """

    pace_v = pace_type.value if pace_type else "MM"
    total_horses = len(all_evaluations)

    # 馬番→評価辞書
    _ev_map = {e.horse.horse_no: e for e in all_evaluations}
    _name = lambda no: _ev_map[no].horse.horse_name if no in _ev_map else f"{no}番"
    _jockey = lambda no: getattr(_ev_map[no].horse, "jockey", "") if no in _ev_map else ""
    _gate = lambda no: getattr(_ev_map[no].horse, "gate_no", no) if no in _ev_map else no
    _style = lambda no: getattr(getattr(_ev_map[no], "pace", None), "running_style", None) if no in _ev_map else None

    # ペース名称
    pace_desc = {
        "HH": "ハイペース", "HM": "ハイ〜ミドル",
        "MM": "ミドルペース", "MS": "ミドル〜スロー",
        "SS": "スローペース",
    }.get(pace_v, "ミドルペース")

    straight_m = course.straight_m or 0
    condition = getattr(course, "condition", "")
    sorted_evs = sorted(all_evaluations, key=lambda e: e.composite, reverse=True)

    # ============================================================
    # 7段階の展開見解を人間的な文章で生成
    # ============================================================

    # 推定4角位置と推定上がり3Fを馬番から引けるようにする
    _pos4c = lambda no: getattr(_ev_map[no].pace, "estimated_position_4c", None) if no in _ev_map else None
    _est3f = lambda no: getattr(_ev_map[no].pace, "estimated_last3f", None) if no in _ev_map else None
    _wp = lambda no: _ev_map[no].win_prob if no in _ev_map else 0

    # composite順の上位馬
    sorted_by_wp = sorted(all_evaluations, key=lambda e: e.win_prob, reverse=True)
    top3_nos = [ev.horse.horse_no for ev in sorted_by_wp[:3]]

    # 推定上がり3F上位3頭
    evs_with_3f = [(ev, ev.pace.estimated_last3f) for ev in all_evaluations if ev.pace.estimated_last3f]
    evs_with_3f.sort(key=lambda x: x[1])
    fast3f_top3 = evs_with_3f[:3] if evs_with_3f else []

    # 脚質グループ名
    leader_set = set(leaders)
    front_set = set(front_horses)
    mid_set = set(mid_horses or [])
    rear_set = set(rear_horses)

    def _style_label(no):
        if no in leader_set:
            return "逃げ"
        if no in front_set:
            return "先行"
        if no in mid_set:
            return "差し"
        if no in rear_set:
            return "追込"
        return "中団"

    parts = []

    # ── (1) 先行争い・主導権 ──
    if len(leaders) == 0:
        if front_horses:
            fn = [f"**{_name(no)}**" for no in front_horses[:2]]
            parts.append(
                f"逃げ馬不在のメンバー構成。{'と'.join(fn)}あたりが"
                "控えめにハナを取る形になりそうで、主導権を握るというよりは"
                "お見合いからの入りになる"
            )
        else:
            parts.append("逃げ馬も先行馬も見当たらない異質なメンバー構成。序盤は手探りの入りになる")
    elif len(leaders) == 1:
        no = leaders[0]
        n, j = _name(no), _jockey(no)
        g = _gate(no)
        challengers = [no2 for no2 in front_horses if no2 != no]
        if not challengers:
            gate_desc = f"{g}枠から" if g <= 4 else f"外目の{g}枠からでも"
            parts.append(
                f"**{n}**（{j}）が{gate_desc}すんなりハナを取り切る。"
                "競りかけてくる馬が見当たらず、単騎逃げで主導権は完全に**{n}**のもの".format(n=n)
            )
        else:
            ch_names = "・".join([f"**{_name(no2)}**" for no2 in challengers[:2]])
            parts.append(
                f"ハナは**{n}**（{j}）が主張する。"
                f"{ch_names}も前を意識するタイプだが、無理に競りかけるほどではなく、"
                f"**{n}**が主導権を握る形になりそうだ"
            )
    elif len(leaders) == 2:
        nos = sorted(leaders, key=lambda x: _gate(x))
        n1, n2 = _name(nos[0]), _name(nos[1])
        inner = nos[0] if _gate(nos[0]) <= _gate(nos[1]) else nos[1]
        parts.append(
            f"**{n1}**と**{n2}**がともにハナを主張したい構え。"
            f"内枠の**{_name(inner)}**が先手を取りやすいが、"
            "外から被せに来る形になればテンから消耗戦に突入する"
        )
    else:
        leader_names = "・".join([f"**{_name(no)}**" for no in leaders[:3]])
        parts.append(
            f"{leader_names}と逃げたい馬が{len(leaders)}頭。"
            "これだけ揃えばテンから激しいポジション争いは必至で、"
            "先行馬群を巻き込んだ縦長の隊列になる可能性が高い"
        )

    # ── (2) ペース・有利脚質 ──
    pace_sentence = ""
    if front_3f_est:
        pace_sentence = f"前半3Fは{front_3f_est:.1f}秒前後の{pace_desc}が想定される。"
    else:
        pace_sentence = f"流れは{pace_desc}が想定される。"

    if pace_v in ("HH", "HM"):
        pace_sentence += "速い流れで先行勢は息が入らず、中団から後ろで脚をためた組に展開が向く"
    elif pace_v in ("SS", "MS"):
        if len(leaders) <= 1:
            pace_sentence += "逃げ馬が楽に運べるペースで、前に行った馬がそのまま残りやすい流れ"
        else:
            pace_sentence += "流れ自体は緩いが、3〜4角で一気にペースアップする瞬発力勝負になりやすい"
    else:
        pace_sentence += "平均的な流れで力通りの決着になりやすく、脚質による有利不利は小さい"
    parts.append(pace_sentence)

    # ── (3) 隊列 ──
    # 脚質グループの馬数から隊列を推定
    n_front_group = len(leaders) + len(front_horses)
    n_rear_group = len(rear_horses)
    n_mid_group = len(mid_horses or [])

    formation = ""
    if n_front_group >= total_horses * 0.5:
        formation = "前に行きたい馬が多く、先行集団が分厚い隊列になる"
        if pace_v in ("HH", "HM"):
            formation += "。道中は縦長になり、後方との差が開いたまま4角を迎えそうだ"
        else:
            formation += "。ただし激しい先行争いにはならず、一団の隊列でコーナーに入る形か"
    elif n_rear_group >= total_horses * 0.4:
        formation = "差し・追込タイプが多いメンバー構成で、前の頭数が少ない分だけ隊列は縦長になりにくい"
        if len(leaders) <= 1:
            formation += "。道中は一団のまま3角を迎え、各馬が外々を回す混戦模様になりそうだ"
    else:
        if pace_v in ("HH",):
            formation = "ハイペースに引っ張られて前後の差が広がり、縦に長い隊列で道中が進む"
        elif pace_v in ("SS",):
            formation = "スローで一団の隊列。後方待機だと射程圏に入れないリスクがある"
        else:
            formation = "先行・中団・後方がバランスよく散らばり、標準的な隊列で道中を進む形"
    parts.append(formation)

    # ── (4) 勝負所（ラスト600m）の隊形 ──
    shobubasho = ""
    if course.corner_count >= 3:
        # 3〜4角がある（1200m以上）
        if pace_v in ("HH", "HM"):
            shobubasho = (
                "3〜4角で先行馬の脚色が怪しくなるタイミングが仕掛けどころ。"
                "後続は外からマクリ気味に押し上げてくるため、"
                "ラスト600m地点では先頭から後方まで一気に圧縮される場面がありそうだ"
            )
        elif pace_v in ("SS", "MS"):
            shobubasho = (
                "ペースが上がるのは4角手前から。"
                "各馬がここで一斉にスパートをかけるため、"
                "仕掛け遅れた馬は置かれる危険がある。ラスト600m地点ではまだ先行勢が優位"
            )
        else:
            shobubasho = (
                "3〜4角の手応えで勝負が決まる流れ。"
                "ここで手応え良く外に持ち出せた馬が直線で伸びる"
            )
    elif course.corner_count == 2:
        # 短距離（1000-1200m）
        shobubasho = (
            "コーナーが少ないコース形態で、"
            "ラスト600m地点ではまだ隊列が固まったまま直線に向く。"
            "直線に入ってからの瞬発力が問われる"
        )
    else:
        # 直線コース
        shobubasho = "直線コースのため各馬が一斉にスピードを上げ、純粋な末脚比べになる"

    # 馬場の影響を補足
    if condition in ("重", "不良"):
        if course.surface == "芝":
            shobubasho += "。馬場が悪化して内が荒れており、4角で外に持ち出す馬が多くなりそうだ"
        else:
            shobubasho += "。重いダートでスタミナ消耗が激しく、先行馬が4角で手応えが怪しくなる場面も"
    elif condition == "稍重" and course.surface == "芝":
        shobubasho += "。稍重で内側がやや傷んでおり、外めのコース取りが鍵になる"
    parts.append(shobubasho)

    # ── (5) 逃げ先行馬と差し追込馬の脚色 ──
    ashiiro = ""
    # 先行勢の脚色
    front_group_nos = list(leader_set | front_set)
    front_est3f = [(no, _est3f(no)) for no in front_group_nos if _est3f(no)]
    front_est3f.sort(key=lambda x: x[1])
    rear_group_nos = list(mid_set | rear_set)
    rear_est3f = [(no, _est3f(no)) for no in rear_group_nos if _est3f(no)]
    rear_est3f.sort(key=lambda x: x[1])

    if front_est3f and rear_est3f:
        best_front = front_est3f[0]
        best_rear = rear_est3f[0]
        avg_front_3f = sum(t for _, t in front_est3f) / len(front_est3f)
        avg_rear_3f = sum(t for _, t in rear_est3f) / len(rear_est3f)
        diff_3f = avg_front_3f - avg_rear_3f  # 正なら後方の上がりが速い

        if diff_3f > 0.5:
            ashiiro = (
                f"先行勢の上がりは平均{avg_front_3f:.1f}秒で、"
                f"後方待機組の{avg_rear_3f:.1f}秒と比べると見劣りする。"
                f"直線で先行馬の脚が鈍るところを**{_name(best_rear[0])}**"
                f"（{best_rear[1]:.1f}秒）あたりが差してくる形が浮かぶ"
            )
        elif diff_3f < -0.3:
            ashiiro = (
                f"先行馬の上がりが{avg_front_3f:.1f}秒と速く、"
                f"後方組の{avg_rear_3f:.1f}秒では追いつけない計算。"
                "前で運んだ馬がそのまま押し切る展開になりやすい"
            )
        else:
            ashiiro = (
                f"先行勢の上がり{avg_front_3f:.1f}秒、後方組{avg_rear_3f:.1f}秒とほぼ互角。"
                "脚色比べでは決め手に欠く展開で、位置取りと仕掛けのタイミングが勝負を分けそうだ"
            )
    elif front_est3f:
        best_front = front_est3f[0]
        ashiiro = (
            f"先行勢では**{_name(best_front[0])}**が上がり{best_front[1]:.1f}秒と脚が使える。"
            "後方待機組の推定データが少なく、先行有利の流れが濃厚"
        )
    else:
        if pace_v in ("HH", "HM"):
            ashiiro = "ハイペースで先行勢はスタミナを消耗し、直線で後続に飲み込まれる危険がある"
        else:
            ashiiro = "先行馬が脚をため直せる流れで、前残りの余地は十分にある"
    parts.append(ashiiro)

    # ── (6) 予想上がり3Fと位置取りからみた有利馬 ──
    advantage = ""
    if fast3f_top3:
        # 上がり上位×位置取りで有利な馬を特定
        best_combo_horse = None
        best_combo_score = -999
        for ev, est3f in evs_with_3f:
            no = ev.horse.horse_no
            pos4c = ev.pace.estimated_position_4c or total_horses * 0.5
            # スコア = 上がり速さ(低いほど良い) × 位置取り前(低いほど良い)
            # 正規化して合算（上がりの速さを重視しつつ位置取りもプラス）
            score = -est3f * 2.0 - pos4c * 0.5
            if score > best_combo_score:
                best_combo_score = score
                best_combo_horse = (ev, est3f, pos4c)

        if best_combo_horse:
            ev, est3f, pos4c = best_combo_horse
            no = ev.horse.horse_no
            pos_label = "前目" if pos4c <= total_horses * 0.33 else ("中団" if pos4c <= total_horses * 0.66 else "後方")

            # 上がり最速馬
            fastest_ev, fastest_3f = fast3f_top3[0]
            fastest_no = fastest_ev.horse.horse_no

            if fastest_no == no:
                advantage = (
                    f"上がり最速{fastest_3f:.1f}秒の脚を持つ**{_name(no)}**が{pos_label}から繰り出す形。"
                    "位置取りと末脚の両方を兼ね備えており、この馬にとって理想的な展開になる"
                )
            else:
                # 最速馬と位置取り×上がりの最良馬が異なる
                fastest_pos = fastest_ev.pace.estimated_position_4c or total_horses * 0.5
                fastest_pos_label = "前目" if fastest_pos <= total_horses * 0.33 else ("中団" if fastest_pos <= total_horses * 0.66 else "後方")
                advantage = (
                    f"上がり最速は{fastest_pos_label}の**{_name(fastest_no)}**（{fastest_3f:.1f}秒）だが、"
                    f"位置取りの利を含めると{pos_label}の**{_name(no)}**（{est3f:.1f}秒）が有利。"
                )
                if straight_m >= 400:
                    advantage += "直線が長い分だけ後方からでも届く計算で、末脚勝負の要素が大きい"
                else:
                    advantage += f"直線{straight_m}mでは位置取りの差がダイレクトに出るため、前で脚をためた馬が有利"
        else:
            advantage = "各馬の推定上がりに大きな差がなく、位置取りの優劣がそのまま結果に反映されそうだ"
    else:
        advantage = "推定上がりデータが揃わないメンバーだが、コース形態から先行力のある馬を重視したい"
    parts.append(advantage)

    # ── (7) ゴール前 ──
    # 上位3頭の脚質と展開を絡めた結論
    finish = ""
    top1 = sorted_by_wp[0] if sorted_by_wp else None
    top2 = sorted_by_wp[1] if len(sorted_by_wp) >= 2 else None
    top3 = sorted_by_wp[2] if len(sorted_by_wp) >= 3 else None

    if top1:
        n1 = _name(top1.horse.horse_no)
        s1 = _style_label(top1.horse.horse_no)

        if s1 in ("逃げ", "先行"):
            if straight_m >= 400 and course.slope_type == "急坂":
                finish = (
                    f"ゴール前は急坂で**{n1}**の脚が鈍るかどうかがポイント。"
                )
            elif straight_m <= 300:
                finish = f"直線が短いこのコースなら、{s1}で運ぶ**{n1}**がそのまま押し切る形が濃厚。"
            else:
                finish = f"**{n1}**が{s1}から直線で脚を伸ばす形。"
        elif s1 in ("差し", "追込"):
            if pace_v in ("HH", "HM"):
                finish = f"前が止まる流れなら、**{n1}**が外から一気に差し切る場面。"
            elif straight_m >= 400:
                finish = f"長い直線を味方に**{n1}**がゴール前で差し脚を爆発させる形。"
            else:
                finish = f"**{n1}**が中団から鋭く脚を伸ばしてくるが、前を捉え切れるかが焦点。"
        else:
            finish = f"**{n1}**が中団から抜け出しにかかる。"

        if top2:
            n2 = _name(top2.horse.horse_no)
            s2 = _style_label(top2.horse.horse_no)
            if s1 in ("逃げ", "先行") and s2 in ("差し", "追込"):
                finish += f"それを**{n2}**がどこまで追い詰められるか。"
            elif s1 in ("差し", "追込") and s2 in ("逃げ", "先行"):
                finish += f"粘る**{n2}**を捉えられるかどうかの勝負。"
            else:
                finish += f"**{n2}**も同じ脚質で並びかける。"

        if top3:
            n3 = _name(top3.horse.horse_no)
            s3 = _style_label(top3.horse.horse_no)
            est3f_3 = _est3f(top3.horse.horse_no)
            if s3 in ("逃げ", "先行"):
                # 逃げ・先行馬は「末脚で割って入る」表現が不自然
                finish += f"先行で運ぶ**{n3}**が3着圏内に粘り込めるかどうか"
            elif est3f_3 and fast3f_top3 and est3f_3 <= fast3f_top3[0][1] + 0.3:
                finish += f"3番手争いに**{n3}**が末脚で割って入る"
            else:
                finish += f"**{n3}**が3着圏内に食い込めるかどうか"
    else:
        finish = "混戦模様でゴール前は横一線の追い比べになりそうだ"

    parts.append(finish)

    comment = "。".join(p.rstrip("。") for p in parts if p) + "。"

    # ============================================================
    # 有利な枠順（従来ロジック維持）
    # ============================================================
    if course.surface == "芝":
        if pace_v in ("HH", "HM"):
            favorable_gate = "外枠（差し馬向き）"
        elif pace_v in ("SS", "MS"):
            favorable_gate = "内枠（前につけやすい）"
        else:
            if course.inside_outside == "内" or course.corner_type == "小回り":
                favorable_gate = "内枠（内回り・小回り有利）"
            elif course.inside_outside == "外" or course.straight_m >= 400:
                favorable_gate = "外枠（外回り・長直線有利）"
            elif course.straight_m <= 300:
                favorable_gate = "内枠（短直線・前残り有利）"
            else:
                favorable_gate = "内外差なし（平均ペース）"
    else:
        if pace_v in ("HH", "HM"):
            favorable_gate = "外枠（砂被り回避・ハイペース）"
        elif pace_v in ("SS", "MS"):
            favorable_gate = "外枠（砂被り回避）"
        else:
            favorable_gate = "外枠（砂被り回避）"

    # ============================================================
    # 有利な脚質（従来ロジック維持）
    # ============================================================
    favorable_style_reason = ""
    if (
        lineup is not None
        and front_3f_est is not None
        and last_3f_est is not None
        and mid_horses is not None
    ):
        from src.calculator.pace_analysis import judge_favorable_style

        favorable_style, favorable_style_reason = judge_favorable_style(
            pace_type,
            course,
            leaders,
            front_horses,
            mid_horses,
            rear_horses,
            lineup,
            front_3f_est,
            last_3f_est,
        )
    else:
        if pace_v == "HH":
            favorable_style = "差し・追い込み（ハイペース消耗戦）"
        elif pace_v == "HM":
            favorable_style = "差し・中団〜後方待機"
        elif pace_v == "MM":
            favorable_style = "先行〜差し（力通り）"
        elif pace_v == "MS":
            favorable_style = "先行（楽逃げ・前残り）"
        else:
            favorable_style = "先行・逃げ（スロー前残り）"

    return comment, favorable_gate, favorable_style, favorable_style_reason


# ============================================================
# 4a-2. ばんえい競馬専用コメント生成
# ============================================================


def generate_banei_comment(race, evaluations) -> str:
    """
    ばんえい競馬専用の展開コメントを生成する。
    通常競馬のペース・脚質・3F概念を排除し、
    斤量・馬場水分量・馬体重に基づくコメントを返す。
    """
    # 水分量
    moisture = getattr(race, "moisture_dirt", None)
    if moisture is not None:
        if moisture <= 1.5:
            water_desc = f"馬場水分 {moisture:.1f}%（乾燥）。砂が重く、軽量馬・持久力型に有利。タイムはかかる傾向"
        elif moisture <= 2.5:
            water_desc = f"馬場水分 {moisture:.1f}%（標準）。障害力とスタミナのバランスが問われる馬場"
        elif moisture <= 3.5:
            water_desc = f"馬場水分 {moisture:.1f}%（重め）。ソリの滑りが良くパワー型に有利。タイムは速め"
        else:
            water_desc = f"馬場水分 {moisture:.1f}%（泥深め）。大型馬でもスタミナ切れの危険あり。高水分適性が鍵"
    else:
        water_desc = "馬場水分データなし"

    # 斤量レンジ
    weights = [getattr(ev.horse, "weight_kg", None) for ev in evaluations]
    weights = [w for w in weights if w is not None and w > 0]
    if weights:
        w_min, w_max = min(weights), max(weights)
        w_med = sorted(weights)[len(weights) // 2]
        weight_desc = f"斤量 {w_min:.0f}〜{w_max:.0f}kg（中央値 {w_med:.0f}kg）"
        w_diff = w_max - w_min
        if w_diff >= 30:
            weight_desc += f"。{w_diff:.0f}kgの斤量差は大きく、軽量馬が有利な展開になりやすい"
        elif w_diff <= 5:
            weight_desc += "。斤量差がほぼなく、実力勝負"
    else:
        weight_desc = "斤量データなし"

    # 馬体重レンジ
    h_weights = [getattr(ev.horse, "horse_weight", None) for ev in evaluations]
    h_weights = [w for w in h_weights if w is not None and w > 0]
    if h_weights:
        hw_min, hw_max = min(h_weights), max(h_weights)
        hw_desc = f"馬体重 {hw_min:.0f}〜{hw_max:.0f}kg"
    else:
        hw_desc = "馬体重データなし"

    # 斤量/馬体重比（負担割合）
    ratio_info = []
    for ev in evaluations:
        w = getattr(ev.horse, "weight_kg", None)
        hw = getattr(ev.horse, "horse_weight", None)
        if w and hw and hw > 0:
            ratio = w / hw
            ratio_info.append((ev.horse.horse_name, ev.horse.horse_no, ratio, w, hw))
    burden_desc = ""
    if ratio_info:
        ratio_info.sort(key=lambda x: x[2])
        lightest = ratio_info[0]
        heaviest = ratio_info[-1]
        burden_desc = (
            f"負担率は**{lightest[0]}**({lightest[3]:.0f}kg/{lightest[4]:.0f}kg={lightest[2]:.1%})が最も軽く、"
            f"**{heaviest[0]}**({heaviest[3]:.0f}kg/{heaviest[4]:.0f}kg={heaviest[2]:.1%})が最も重い"
        )

    # 上位馬の近走タイム
    sorted_by_comp = sorted(evaluations, key=lambda e: e.composite, reverse=True)
    top3 = sorted_by_comp[:3]
    top3_info = []
    for ev in top3:
        runs = ev.horse.past_runs[:3] if ev.horse.past_runs else []
        times = [r.finish_time_sec for r in runs if r.finish_time_sec and r.finish_time_sec > 0]
        if times:
            avg_time = sum(times) / len(times)
            m = int(avg_time) // 60
            s = avg_time - m * 60
            top3_info.append(f"**{ev.horse.horse_name}**（近走平均{m}:{s:04.1f}）")
        else:
            top3_info.append(f"**{ev.horse.horse_name}**")
    top3_desc = "上位評価: " + "、".join(top3_info) if top3_info else ""

    # コメント組立
    parts = [water_desc, weight_desc, hw_desc]
    if burden_desc:
        parts.append(burden_desc)
    if top3_desc:
        parts.append(top3_desc)
    return "。\n".join(parts) + "。"


def generate_banei_comment_dict(race_data: dict) -> str:
    """
    ばんえい専用コメント生成（dict版）。
    オッズ取得後の再生成用。race_data は pred.json のレース dict。
    """
    # 水分量
    moisture = race_data.get("water_content")
    if moisture is not None:
        if moisture <= 1.5:
            water_desc = f"馬場水分 {moisture:.1f}%（乾燥）。砂が重く、軽量馬・持久力型に有利。タイムはかかる傾向"
        elif moisture <= 2.5:
            water_desc = f"馬場水分 {moisture:.1f}%（標準）。障害力とスタミナのバランスが問われる馬場"
        elif moisture <= 3.5:
            water_desc = f"馬場水分 {moisture:.1f}%（重め）。ソリの滑りが良くパワー型に有利。タイムは速め"
        else:
            water_desc = f"馬場水分 {moisture:.1f}%（泥深め）。大型馬でもスタミナ切れの危険あり。高水分適性が鍵"
    else:
        water_desc = "馬場水分データなし"

    horses = race_data.get("horses", [])

    # 斤量レンジ
    weights = [h.get("weight_kg") for h in horses]
    weights = [w for w in weights if w is not None and w > 0]
    if weights:
        w_min, w_max = min(weights), max(weights)
        w_med = sorted(weights)[len(weights) // 2]
        weight_desc = f"斤量 {w_min:.0f}〜{w_max:.0f}kg（中央値 {w_med:.0f}kg）"
        w_diff = w_max - w_min
        if w_diff >= 30:
            weight_desc += f"。{w_diff:.0f}kgの斤量差は大きく、軽量馬が有利な展開になりやすい"
        elif w_diff <= 5:
            weight_desc += "。斤量差がほぼなく、実力勝負"
    else:
        weight_desc = "斤量データなし"

    # 馬体重レンジ
    h_weights = [h.get("horse_weight") for h in horses]
    h_weights = [w for w in h_weights if w is not None and w > 0]
    if h_weights:
        hw_min, hw_max = min(h_weights), max(h_weights)
        hw_desc = f"馬体重 {hw_min:.0f}〜{hw_max:.0f}kg"
    else:
        hw_desc = "馬体重データなし"

    # 斤量/馬体重比
    ratio_info = []
    for h in horses:
        w = h.get("weight_kg")
        hw = h.get("horse_weight")
        if w and hw and hw > 0:
            ratio = w / hw
            ratio_info.append((h.get("horse_name", "?"), h.get("horse_no", 0), ratio, w, hw))
    burden_desc = ""
    if ratio_info:
        ratio_info.sort(key=lambda x: x[2])
        lightest = ratio_info[0]
        heaviest = ratio_info[-1]
        burden_desc = (
            f"負担率は**{lightest[0]}**({lightest[3]:.0f}kg/{lightest[4]:.0f}kg={lightest[2]:.1%})が最も軽く、"
            f"**{heaviest[0]}**({heaviest[3]:.0f}kg/{heaviest[4]:.0f}kg={heaviest[2]:.1%})が最も重い"
        )

    # 上位馬の近走タイム
    sorted_by_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    top3 = sorted_by_comp[:3]
    top3_info = []
    for h in top3:
        past = h.get("past_3_runs") or []
        times = [r.get("finish_time_sec") for r in past if r.get("finish_time_sec") and r["finish_time_sec"] > 0]
        if times:
            avg_time = sum(times) / len(times)
            m = int(avg_time) // 60
            s = avg_time - m * 60
            top3_info.append(f"**{h.get('horse_name', '?')}**（近走平均{m}:{s:04.1f}）")
        else:
            top3_info.append(f"**{h.get('horse_name', '?')}**")
    top3_desc = "上位評価: " + "、".join(top3_info) if top3_info else ""

    parts = [water_desc, weight_desc, hw_desc]
    if burden_desc:
        parts.append(burden_desc)
    if top3_desc:
        parts.append(top3_desc)
    return "。\n".join(parts) + "。"


# ============================================================
# 4b. 馬個別見解の自動生成
# ============================================================


def _generate_banei_horse_comment(
    horse: dict,
    race_context: dict,
    detail_level: str = "normal",
) -> str:
    """ばんえい競馬用の馬個別見解テキストを生成する。"""
    all_points = []
    composite = horse.get("composite", 0) or 0
    ability = horse.get("ability_total", 0) or 0
    trend = horse.get("ability_trend", "")
    jockey = horse.get("jockey", "")
    jockey_grade = horse.get("jockey_grade", "")
    divergence = horse.get("odds_divergence")
    div_signal = horse.get("divergence_signal", "")
    odds = horse.get("odds")
    popularity = horse.get("popularity")
    kiken_type = horse.get("kiken_type", "")
    ana_type = horse.get("ana_type", "")
    mark = horse.get("mark", "")
    change = horse.get("jockey_change")
    weight_kg = horse.get("weight_kg")  # 斤量（ソリ重量）
    horse_weight = horse.get("horse_weight")  # 馬体重

    all_composites = race_context.get("all_composites", [])
    field_count = race_context.get("field_count", 0)
    water_content = race_context.get("water_content")

    # ---- ① 能力の位置づけ ----
    sorted_comps = sorted(all_composites, reverse=True) if all_composites else []
    rank = 0
    for i, c in enumerate(sorted_comps):
        if abs(c - composite) < 0.01:
            rank = i + 1
            break
    if rank == 0:
        rank = len(sorted_comps)

    if rank == 1:
        gap_to_2nd = sorted_comps[0] - sorted_comps[1] if len(sorted_comps) >= 2 else 0
        if gap_to_2nd >= 5:
            ability_pos = f"総合偏差値{composite:.1f}はメンバー断トツ"
        elif gap_to_2nd >= 2:
            ability_pos = f"総合偏差値{composite:.1f}はメンバートップ"
        else:
            ability_pos = f"総合偏差値{composite:.1f}でメンバー上位"
    elif rank <= 3:
        ability_pos = f"総合偏差値{composite:.1f}でメンバー{rank}位"
    elif rank <= field_count * 0.6:
        ability_pos = f"総合偏差値{composite:.1f}で{field_count}頭中{rank}位"
    else:
        ability_pos = f"総合偏差値{composite:.1f}で{field_count}頭中{rank}位と下位"

    # ---- ② 近走トレンド ----
    trend_text = ""
    trend_positive = True
    if "急上昇" in trend:
        trend_text = "近走パフォーマンス急上昇中"
    elif "上昇" in trend:
        trend_text = "近走は上昇傾向"
    elif "急下降" in trend:
        trend_text = "近走は下降傾向で不安あり"
        trend_positive = False
    elif "下降" in trend:
        trend_text = "近走はやや下降傾向"
        trend_positive = False

    # ---- ③ 斤量・馬体重・負担率 ----
    burden_text = ""
    if weight_kg and horse_weight and horse_weight > 0:
        ratio = weight_kg / horse_weight
        # 全馬の斤量を集計して相対位置を判断
        all_weights = [h.get("weight_kg") for h in race_context.get("all_horses", [])]
        all_weights = [w for w in all_weights if w is not None and w > 0]
        if all_weights:
            w_min, w_max = min(all_weights), max(all_weights)
            if weight_kg <= w_min:
                burden_text = f"斤量{weight_kg:.0f}kgはメンバー最軽量で有利"
            elif weight_kg >= w_max:
                burden_text = f"斤量{weight_kg:.0f}kgはメンバー最重量でハンデ大"
            elif weight_kg <= w_min + (w_max - w_min) * 0.3:
                burden_text = f"斤量{weight_kg:.0f}kgは軽め"
            elif weight_kg >= w_min + (w_max - w_min) * 0.7:
                burden_text = f"斤量{weight_kg:.0f}kgは重め"
        if horse_weight:
            if burden_text:
                burden_text += f"（馬体重{horse_weight:.0f}kg、負担率{ratio:.1%}）"
            else:
                burden_text = f"斤量{weight_kg:.0f}kg/馬体重{horse_weight:.0f}kg（負担率{ratio:.1%}）"
    elif weight_kg:
        burden_text = f"斤量{weight_kg:.0f}kg"

    # ---- ④ 馬場水分との相性 ----
    water_text = ""
    if water_content is not None and horse_weight:
        if water_content <= 1.5:
            # 乾燥 → 軽量馬・持久力型有利
            if weight_kg and weight_kg <= (min(all_weights) + 5 if all_weights else 600):
                water_text = "乾燥馬場で軽量は有利材料"
        elif water_content >= 3.5:
            # 泥深め → 大型馬はスタミナ切れリスク
            if horse_weight >= 1050:
                water_text = "高水分馬場で大型馬はスタミナ切れに注意"

    # ---- ⑤ オッズ乖離 ----
    odds_text = ""
    if div_signal and div_signal not in ("×", "なし", "-", "—"):
        if divergence and divergence >= 3.0:
            odds_text = f"オッズ乖離{divergence:.1f}倍と大きな妙味"
        elif divergence and divergence >= 1.5:
            odds_text = f"オッズ乖離{divergence:.1f}倍で妙味あり"
    elif divergence and divergence < 0.5 and popularity and popularity <= 3:
        odds_text = "人気を背負いすぎで妙味なし"
    if mark == "☆" and odds and odds >= 10.0 and not odds_text:
        odds_text = f"単勝{odds:.1f}倍と配当妙味あり"

    # ---- ⑥ 騎手評価 ----
    jockey_text = ""
    if jockey_grade in ("SS",):
        jockey_text = f"騎手{jockey_grade}評価の{jockey}を配し追い技術に期待"
    elif jockey_grade == "S":
        jockey_text = f"騎手S評価の{jockey}が騎乗"
    elif jockey_grade == "D" and detail_level in ("full", "normal"):
        jockey_text = "騎手Dグレードが不安材料"

    # ---- ⑦ 危険/穴フラグ ----
    has_mark = mark in ("◉", "◎", "○", "▲", "△", "☆")
    flag_text = ""
    if not has_mark:
        if kiken_type and "危" in kiken_type:
            kiken_score = horse.get("kiken_score", 0)
            if kiken_score >= 4:
                flag_text = "人気ほどの信頼はなく消し候補"
            else:
                flag_text = "過大評価の恐れあり"
        elif ana_type and "穴" in ana_type:
            flag_text = "穴馬候補として一考"

    # ---- ⑧ 乗り替わり ----
    change_text = ""
    if change and change <= -2:
        change_text = "乗り替わりで大幅マイナス"
    elif change and change == -1:
        change_text = "乗り替わりでやや不安"

    # ============================================================
    # 文章組み立て
    # ============================================================
    if detail_level == "full":
        s1 = ability_pos
        if trend_text:
            if rank <= 3 and not trend_positive:
                s1 += f"。ただし{trend_text}"
            else:
                s1 += f"で、{trend_text}"
        all_points.append(s1)

        if burden_text:
            all_points.append(burden_text)
        if water_text:
            all_points.append(water_text)
        if jockey_text:
            all_points.append(jockey_text)
        if odds_text:
            all_points.append(odds_text)
        if flag_text:
            all_points.append(flag_text)
        if change_text:
            all_points.append(change_text)

    elif detail_level == "normal":
        s1 = ability_pos
        if trend_text:
            if rank <= 3 and not trend_positive:
                s1 += f"。ただし{trend_text}"
            else:
                s1 += f"で、{trend_text}"
        all_points.append(s1)

        # 2番目: フラグ > 斤量 > 乖離 > 騎手
        if flag_text:
            all_points.append(flag_text)
        elif burden_text:
            all_points.append(burden_text)
        elif odds_text:
            all_points.append(odds_text)
        elif jockey_text:
            all_points.append(jockey_text)

    else:  # short
        if flag_text:
            all_points.append(flag_text)
        elif burden_text:
            all_points.append(f"{ability_pos}。{burden_text}")
        elif trend_text and "急" in trend_text:
            all_points.append(f"{ability_pos}。{trend_text}")
        else:
            all_points.append(ability_pos)

    return "。".join(all_points) + "。" if all_points else ""


def generate_horse_comment(
    horse: dict,
    race_context: dict,
    detail_level: str = "normal",
) -> str:
    """
    馬1頭ぶんの見解テキストを自動生成する。
    印見解・全頭診断の両方で使用。

    Args:
        horse: 馬データ dict (horse_no, horse_name, composite, ability_total, ...)
        race_context: レース全体の文脈 dict
            - field_count, straight_m, slope_type, surface, pace_predicted,
            - leading_horses, front_horses, mid_horses, rear_horses,
            - estimated_front_3f, all_composites (全馬のcomposite一覧)
        detail_level: "full" (3-4文) | "normal" (1-2文) | "short" (1文)

    Returns:
        見解テキスト
    """
    # ばんえい判定
    is_banei_race = race_context.get("is_banei", False)
    if is_banei_race:
        return _generate_banei_horse_comment(horse, race_context, detail_level)

    parts = []
    name = horse.get("horse_name", "?")
    no = horse.get("horse_no", 0)
    jockey = horse.get("jockey", "")
    composite = horse.get("composite", 0) or 0
    ability = horse.get("ability_total", 0) or 0
    pace_total = horse.get("pace_total", 0) or 0
    course_total = horse.get("course_total", 0) or 0
    trend = horse.get("ability_trend", "")
    style = horse.get("running_style", "")
    jockey_grade = horse.get("jockey_grade", "")
    divergence = horse.get("odds_divergence")
    div_signal = horse.get("divergence_signal", "")
    odds = horse.get("odds")
    popularity = horse.get("popularity")
    est_last3f = horse.get("pace_estimated_last3f")
    est_last3f_rank = horse.get("estimated_last3f_rank")
    kiken_type = horse.get("kiken_type", "")
    ana_type = horse.get("ana_type", "")
    change = horse.get("jockey_change")

    # レースコンテキスト
    field_count = race_context.get("field_count", 0)
    straight_m = race_context.get("straight_m", 0)
    slope_type = race_context.get("slope_type", "")
    surface = race_context.get("surface", "")
    pace_v = race_context.get("pace_predicted", "MM")
    leading = set(race_context.get("leading_horses", []))
    front = set(race_context.get("front_horses", []))
    mid = set(race_context.get("mid_horses", []))
    rear = set(race_context.get("rear_horses", []))
    all_composites = race_context.get("all_composites", [])
    front_3f = race_context.get("estimated_front_3f")

    # ---- ① 能力の位置づけ ----
    if all_composites:
        sorted_comps = sorted(all_composites, reverse=True)
        rank = sorted_comps.index(composite) + 1 if composite in sorted_comps else len(sorted_comps)
        # 同着がある場合は先頭の順位
        for i, c in enumerate(sorted_comps):
            if abs(c - composite) < 0.01:
                rank = i + 1
                break
    else:
        rank = 0

    if rank == 1:
        gap_to_2nd = sorted_comps[0] - sorted_comps[1] if len(sorted_comps) >= 2 else 0
        if gap_to_2nd >= 5:
            ability_pos = f"総合偏差値{composite:.1f}はメンバー断トツ"
        elif gap_to_2nd >= 2:
            ability_pos = f"総合偏差値{composite:.1f}はメンバートップ"
        else:
            ability_pos = f"総合偏差値{composite:.1f}でメンバー上位"
    elif rank <= 3:
        ability_pos = f"総合偏差値{composite:.1f}でメンバー{rank}位"
    elif rank <= field_count * 0.6:
        ability_pos = f"総合偏差値{composite:.1f}で{field_count}頭中{rank}位"
    else:
        ability_pos = f"総合偏差値{composite:.1f}で{field_count}頭中{rank}位と下位"

    # ---- ② 近走トレンド ----
    trend_text = ""
    trend_positive = True  # 上昇系ならTrue
    if "急上昇" in trend:
        trend_text = "近走パフォーマンス急上昇中"
    elif "上昇" in trend:
        trend_text = "近走は上昇傾向"
    elif "急下降" in trend:
        trend_text = "近走は下降傾向で不安あり"
        trend_positive = False
    elif "下降" in trend:
        trend_text = "近走はやや下降傾向"
        trend_positive = False

    # ---- ③ 展開利/不利 ----
    pace_text = ""
    if no in leading:
        if len(leading) == 1:
            pace_text = "単騎逃げが見込め自分のペースで運べる"
        else:
            pace_text = "逃げ争いに巻き込まれるリスクあり"
    elif no in front:
        if pace_v in ("SS", "MS"):
            pace_text = "スローの好位で楽に運べる"
        else:
            pace_text = "好位から流れに乗れる"
    elif no in mid:
        if straight_m >= 400:
            pace_text = "中団から長い直線を活かせる"
        elif straight_m <= 320:
            pace_text = f"中団からだと直線{straight_m}mは短い"
        else:
            pace_text = "中団待機から直線勝負"
    elif no in rear:
        if straight_m >= 450:
            pace_text = f"追い込み脚質だが直線{straight_m}mなら届く条件"
        elif straight_m <= 350:
            pace_text = f"追い込み一手で直線{straight_m}mは厳しい条件"
        else:
            pace_text = "後方からの一発狙い"

    # 坂の影響
    if slope_type == "急坂" and no in (leading | front):
        pace_text += "。ただし急坂で先行馬は消耗しやすい"

    # 推定上がり
    last3f_text = ""
    if est_last3f and est_last3f_rank:
        if est_last3f_rank <= 2:
            last3f_text = f"推定上がり{est_last3f:.1f}秒は全馬{est_last3f_rank}位の脚"
        elif est_last3f_rank == field_count:
            last3f_text = f"推定上がり{est_last3f:.1f}秒は全馬最遅"

    # ---- ④ オッズ乖離 ----
    odds_text = ""
    if div_signal and div_signal not in ("×", "なし", "-", "—"):
        if divergence and divergence >= 3.0:
            odds_text = f"オッズ乖離{divergence:.1f}倍と大きな妙味"
        elif divergence and divergence >= 1.5:
            odds_text = f"オッズ乖離{divergence:.1f}倍で妙味あり"
    elif divergence and divergence < 0.5 and popularity and popularity <= 3:
        odds_text = "人気を背負いすぎで妙味なし"
    # ☆印(穴馬)向け: 妙味を強調
    mark_ = horse.get("mark", "")
    if mark_ == "☆" and odds and odds >= 10.0:
        if not odds_text:
            odds_text = f"単勝{odds:.1f}倍と配当妙味あり"

    # ---- ⑤ 騎手評価 ----
    jockey_text = ""
    if jockey_grade in ("SS",):
        jockey_text = f"騎手{jockey_grade}評価の{jockey}を配し"
    elif jockey_grade == "S":
        jockey_text = f"騎手S評価の{jockey}が騎乗"
    elif jockey_grade == "D" and detail_level in ("full", "normal"):
        jockey_text = "騎手Dグレードが不安材料"

    # ---- ⑥ 適性 ----
    course_grades = horse.get("course_detail_grades") or {}
    venue_g = course_grades.get("venue", "")
    dist_g = course_grades.get("distance", "")
    course_text = ""
    if venue_g in ("SS", "S", "A"):
        course_text = f"競馬場実績{venue_g}で条件合う"
    elif venue_g in ("D",) and dist_g in ("D",):
        course_text = "競馬場・距離ともにD評価で条件厳しい"
    elif dist_g in ("SS", "S", "A"):
        course_text = f"距離実績{dist_g}で距離は合う"

    # ---- ⑦ 危険/穴フラグ ----
    # 印付き馬には穴/危険フラグを出さない（印の意味と矛盾するため）
    mark = horse.get("mark", "")
    has_mark = mark in ("◉", "◎", "○", "▲", "△", "☆")
    flag_text = ""
    if not has_mark:
        if kiken_type and "危" in kiken_type:
            kiken_score = horse.get("kiken_score", 0)
            if kiken_score >= 4:
                flag_text = "人気ほどの信頼はなく消し候補"
            else:
                flag_text = "過大評価の恐れあり"
        elif ana_type and "穴" in ana_type:
            flag_text = "穴馬候補として一考"

    # ---- ⑧ 乗り替わり ----
    change_text = ""
    if change and change <= -2:
        change_text = "乗り替わりで大幅マイナス"
    elif change and change == -1:
        change_text = "乗り替わりでやや不安"

    # ============================================================
    # 文章組み立て（detail_level に応じて取捨選択）
    # ============================================================
    all_points = []

    # full: 全ポイントを入れる
    if detail_level == "full":
        # 第1文: 能力位置 + トレンド
        s1 = ability_pos
        if trend_text:
            # 上位+下降 → 逆接、上位+上昇 → 順接、下位+下降 → 順接
            if rank <= 3 and not trend_positive:
                s1 += f"。ただし{trend_text}"
            else:
                s1 += f"で、{trend_text}"
        all_points.append(s1)

        # 第2文: 展開利 + 騎手
        s2_parts = []
        if jockey_text:
            s2_parts.append(jockey_text)
        if pace_text:
            s2_parts.append(pace_text)
        if s2_parts:
            all_points.append("、".join(s2_parts))

        # 第3文: 適性 or 上がり
        if course_text:
            all_points.append(course_text)
        if last3f_text:
            all_points.append(last3f_text)

        # 第4文: 乖離 or フラグ or 乗り替わり
        if odds_text:
            all_points.append(odds_text)
        if flag_text:
            all_points.append(flag_text)
        if change_text:
            all_points.append(change_text)

    elif detail_level == "normal":
        # 2ポイントに絞る: 最も重要なものを選択
        # 能力位置は常に入れる
        s1 = ability_pos
        if trend_text:
            if rank <= 3 and not trend_positive:
                s1 += f"。ただし{trend_text}"
            else:
                s1 += f"で、{trend_text}"
        all_points.append(s1)

        # 2番目のポイント: フラグ > 乖離 > 展開 > 騎手
        if flag_text:
            all_points.append(flag_text)
        elif odds_text:
            all_points.append(odds_text)
        elif pace_text:
            all_points.append(pace_text)
        elif jockey_text:
            all_points.append(jockey_text)

    else:  # short
        # 1ポイントのみ: 最も目立つ特徴
        if flag_text:
            all_points.append(flag_text)
        elif trend_text and ("急" in trend_text):
            all_points.append(f"{ability_pos}。{trend_text}")
        elif odds_text and divergence and divergence >= 3.0:
            all_points.append(f"{ability_pos}。{odds_text}")
        elif pace_text and ("厳しい" in pace_text or "楽に" in pace_text or "断トツ" in ability_pos):
            all_points.append(f"{ability_pos}。{pace_text}")
        else:
            all_points.append(ability_pos)

    return "。".join(all_points) + "。" if all_points else ""


def _generate_banei_horse_diagnosis(
    horse: dict,
    race_context: dict,
) -> str:
    """ばんえい競馬用の短評テキストを生成する。"""
    composite = horse.get("composite", 0) or 0
    ability = horse.get("ability_total", 0) or 0
    trend = horse.get("ability_trend", "")
    jockey = horse.get("jockey", "")
    jockey_grade = horse.get("jockey_grade", "")
    divergence = horse.get("odds_divergence")
    popularity = horse.get("popularity")
    no = horse.get("horse_no", 0)
    ana_type = horse.get("ana_type", "")
    kiken_type = horse.get("kiken_type", "")
    mark = horse.get("mark", "")
    change = horse.get("jockey_change")
    odds = horse.get("odds")
    weight_kg = horse.get("weight_kg")  # 斤量
    horse_weight = horse.get("horse_weight")  # 馬体重

    all_composites = race_context.get("all_composites", [])
    field_count = race_context.get("field_count", 0)
    water_content = race_context.get("water_content")

    # 順位計算
    sorted_comps = sorted(all_composites, reverse=True) if all_composites else []
    rank = 0
    for i, c in enumerate(sorted_comps):
        if abs(c - composite) < 0.01:
            rank = i + 1
            break
    if rank == 0:
        rank = len(sorted_comps)

    # 全馬の斤量
    all_weights = [h.get("weight_kg") for h in race_context.get("all_horses", [])]
    all_weights = [w for w in all_weights if w is not None and w > 0]
    w_min = min(all_weights) if all_weights else 0
    w_max = max(all_weights) if all_weights else 0

    # 負担率
    ratio = weight_kg / horse_weight if weight_kg and horse_weight and horse_weight > 0 else None

    # ── 1位: 信頼感 ──
    if rank == 1:
        gap = sorted_comps[0] - sorted_comps[1] if len(sorted_comps) >= 2 else 0
        parts = []
        if gap >= 5:
            parts.append("指数面では他馬を大きく引き離しており、ここでは力が違う")
        elif gap >= 2:
            parts.append("総合力でメンバーを一歩リードしており、能力的には最上位の存在")
        else:
            parts.append("僅差の混戦模様だが、その中でも総合力は一枚上")

        # 斤量面
        if weight_kg and all_weights:
            if weight_kg <= w_min:
                parts.append("斤量も最軽量で障害への負担が軽く、安定した走りが期待できる")
            elif weight_kg >= w_max and w_max - w_min >= 10:
                parts.append("ただし斤量は最重量で、第2障害での消耗が懸念される")
            else:
                parts.append("斤量面にも大きな不安はなく、実力通りの走りが見込める")

        if "急上昇" in trend:
            parts.append("近走の成績は右肩上がりで、今がまさに充実期")
        if jockey_grade in ("SS", "S"):
            parts.append(f"鞍上{jockey}の追い技術も心強く、軸として信頼できる一頭")
        elif not parts[-1].endswith("一頭"):
            parts.append("ここは素直に信頼したい")

        result = "。".join(parts) + "。"

    # ── 2-3位: 上位争い ──
    elif rank <= 3:
        parts = []
        if ability >= 55:
            parts.append("地力の高さは折り紙付きで、勝ち負けに加わる力は十分にある")
        elif ability >= 50:
            parts.append("能力的には上位グループで、十分勝ち負けの圏内")
        else:
            parts.append("総合力は上位に位置しており、馬券の中心に据えられる存在")

        # 斤量面
        if weight_kg and all_weights:
            if weight_kg <= w_min + (w_max - w_min) * 0.3:
                parts.append("斤量が軽く障害でのロスが少ない分、逆転の可能性を秘めている")
            elif weight_kg >= w_max - (w_max - w_min) * 0.2:
                parts.append("斤量が重めで第2障害に苦戦すると厳しくなる")
            else:
                parts.append("斤量面は標準的で、地力勝負に持ち込みたいところ")

        if horse_weight and horse_weight >= 1050 and water_content and water_content >= 3.5:
            parts.append("大型馬で高水分馬場はスタミナ面に不安が残る")
        elif "急上昇" in trend:
            parts.append("近走の勢いは本物で、ここでも上位争いは必至")
        elif "下降" in trend:
            parts.append("ただ近走はやや精彩を欠いており、全幅の信頼は置きにくい")
        elif divergence and divergence >= 2.0:
            parts.append("人気以上に走る可能性を秘めており、配当妙味も見込める")
        elif jockey_grade in ("SS", "S"):
            parts.append(f"鞍上{jockey}の追い技術次第で逆転の目は十分")

        result = "。".join(parts) + "。"

    # ── 4位〜中位: 条件付き浮上 ──
    elif rank <= max(field_count * 0.6, 4):
        parts = []
        if "上昇" in trend or "急上昇" in trend:
            parts.append("近走は内容が充実しており、ここにきて力をつけてきた印象")
        else:
            parts.append("力量的には上位とやや差があるが、条件ひとつで浮上の余地はある")

        # 斤量面の利点
        if weight_kg and all_weights and weight_kg <= w_min + 5:
            parts.append("斤量の軽さは大きな武器で、障害をスムーズにこなせれば前走以上が望める")
        elif horse_weight and horse_weight >= 1050 and water_content and water_content <= 1.5:
            parts.append("乾燥馬場は大型馬にとってタフな条件で、スタミナ勝負になると有利")

        if divergence and divergence >= 3.0:
            parts.append("人気薄なら馬券妙味は大きく、押さえておきたい一頭")
        elif ana_type and "穴" in ana_type:
            parts.append("穴を開ける資格は持っており、ヒモに加えてみたい")
        elif jockey_grade in ("SS", "S"):
            parts.append(f"鞍上{jockey}の追い技術次第で上位食い込みの可能性も")
        else:
            parts.append("障害力と馬場適性がハマれば、相手候補には入れたい")

        result = "。".join(parts) + "。"

    # ── 下位 ──
    else:
        parts = []
        if kiken_type and "危" in kiken_type and popularity and popularity <= 3:
            parts.append(f"{popularity}番人気に支持されているが、データ的には過大評価の疑いが濃い")
            if "下降" in trend:
                parts.append("近走の成績も下降線で、額面通りには信頼しにくい")
            elif weight_kg and all_weights and weight_kg >= w_max:
                parts.append("斤量も最重量でハンデが大きく、嫌ってみる手も一考")
            else:
                parts.append("人気を考えると馬券妙味は薄く、過信は禁物")
        elif "急上昇" in trend:
            parts.append("ここにきて急上昇カーブを描いており、勢いだけなら侮れない")
            parts.append("格上挑戦の形だが、近走の充実ぶりで一発に期待")
        elif divergence and divergence >= 3.0:
            parts.append("実力的には苦しいが、人気薄なら配当妙味は十分ある")
            if weight_kg and all_weights and weight_kg <= w_min + 5:
                parts.append("斤量の軽さで障害をこなせれば、波乱の一角を担う可能性も")
            else:
                parts.append("展開のアヤを拾って穴を開ける可能性はゼロではない")
        else:
            if weight_kg and all_weights and weight_kg <= w_min + 5:
                parts.append("力関係的に見劣りするが、斤量の軽さだけは武器になり得る")
            else:
                parts.append("現状の力量ではこのメンバーに入ると厳しい")

            if "下降" in trend:
                parts.append("近走の内容も精彩を欠き、巻き返しには時間がかかりそう")
            elif change and change <= -2:
                parts.append("乗り替わりもマイナス材料で、今回は見送りが妥当")
            else:
                parts.append("条件が好転するまで静観が賢明か")

        result = "。".join(parts) + "。"

    # 300字で切る（安全弁）
    if len(result) > 300:
        result = result[:280]
        last_period = result.rfind("。")
        if last_period > 150:
            result = result[:last_period + 1]

    return result


def generate_horse_diagnosis(
    horse: dict,
    race_context: dict,
) -> str:
    """
    馬1頭ぶんの短評テキストを自動生成する（競馬ブック風）。
    データの羅列ではなく、読者に「この馬はどうなのか」が伝わる文章を目指す。

    Args:
        horse: 馬データ dict
        race_context: レース全体の文脈 dict

    Returns:
        競馬ブック風の短評テキスト（150-250字）
    """
    # ばんえい判定
    if race_context.get("is_banei", False):
        return _generate_banei_horse_diagnosis(horse, race_context)

    composite = horse.get("composite", 0) or 0
    ability = horse.get("ability_total", 0) or 0
    course_total = horse.get("course_total", 0) or 0
    pace_total = horse.get("pace_total", 0) or 0
    trend = horse.get("ability_trend", "")
    style = horse.get("running_style", "")
    jockey = horse.get("jockey", "")
    jockey_grade = horse.get("jockey_grade", "")
    divergence = horse.get("odds_divergence")
    popularity = horse.get("popularity")
    est_last3f = horse.get("pace_estimated_last3f")
    est_last3f_rank = horse.get("estimated_last3f_rank")
    no = horse.get("horse_no", 0)
    ana_type = horse.get("ana_type", "")
    kiken_type = horse.get("kiken_type", "")
    mark = horse.get("mark", "")
    change = horse.get("jockey_change")
    odds = horse.get("odds")

    # レースコンテキスト
    field_count = race_context.get("field_count", 0)
    straight_m = race_context.get("straight_m", 0)
    pace_v = race_context.get("pace_predicted", "MM")
    leading = set(race_context.get("leading_horses", []))
    front = set(race_context.get("front_horses", []))
    mid = set(race_context.get("mid_horses", []))
    rear = set(race_context.get("rear_horses", []))
    all_composites = race_context.get("all_composites", [])

    # 順位計算
    sorted_comps = sorted(all_composites, reverse=True) if all_composites else []
    rank = 0
    for i, c in enumerate(sorted_comps):
        if abs(c - composite) < 0.01:
            rank = i + 1
            break
    if rank == 0:
        rank = len(sorted_comps)

    pace_labels = {"HH": "ハイペース", "HM": "やや速い流れ", "MM": "平均的な流れ",
                   "MS": "落ち着いた流れ", "SS": "スローペース"}
    pace_ja = pace_labels.get(pace_v, "")

    # 上がり評価
    last3f_good = est_last3f_rank and est_last3f_rank <= 3
    last3f_top = est_last3f_rank and est_last3f_rank == 1
    last3f_bad = est_last3f_rank and field_count > 0 and est_last3f_rank >= field_count * 0.8

    # ============================================================
    # 順位帯別に文章を組み立て
    # ============================================================

    # ── 1位: 信頼感・軸としての安心感 ──
    if rank == 1:
        gap = sorted_comps[0] - sorted_comps[1] if len(sorted_comps) >= 2 else 0
        parts = []

        if gap >= 5:
            parts.append("指数面では他馬を大きく引き離しており、ここでは力が違う")
        elif gap >= 2:
            parts.append("総合力でメンバーを一歩リードしており、能力的には最上位の存在")
        else:
            parts.append("僅差の混戦模様だが、その中でも総合力は一枚上")

        # 展開面
        if no in leading and len(leading) == 1:
            parts.append("単騎でマイペースの逃げに持ち込めるのも大きなアドバンテージ")
        elif no in front:
            if pace_v in ("SS", "MS"):
                parts.append(f"{pace_ja}なら好位でじっくり溜めて、直線で楽に抜け出せる")
            else:
                parts.append("好位から流れに乗って直線で力を発揮する形が理想")
        elif no in mid:
            if last3f_top:
                parts.append("中団待機からメンバー最速の末脚で一気に突き抜ける")
            else:
                parts.append("中団から早めに動いて押し切るイメージ")
        elif no in rear:
            parts.append("後方からの末脚勝負になるが、この馬の切れ味なら十分届く")

        # 補強
        if "急上昇" in trend:
            parts.append("近走の成績は右肩上がりで、今がまさに充実期")
        if jockey_grade in ("SS", "S"):
            parts.append(f"鞍上{jockey}の手腕も心強く、軸として信頼できる一頭")
        elif not parts[-1].endswith("一頭"):
            parts.append("ここは素直に信頼したい")

        result = "。".join(parts) + "。"

    # ── 2-3位: 上位争い・逆転の可能性 ──
    elif rank <= 3:
        parts = []

        if ability >= 55:
            parts.append("地力の高さは折り紙付きで、勝ち負けに加わる力は十分にある")
        elif ability >= 50:
            parts.append("能力的には上位グループで、十分勝ち負けの圏内")
        else:
            parts.append("総合力は上位に位置しており、馬券の中心に据えられる存在")

        # 展開面
        if no in leading:
            if len(leading) == 1:
                parts.append("すんなりハナを切れれば自分のペースに持ち込める")
            else:
                parts.append("逃げ争いのリスクはあるものの、先手を奪えれば簡単には止まらない")
        elif no in front:
            parts.append("好位からそつなく立ち回れる堅実さが武器")
        elif no in mid:
            if straight_m >= 400:
                parts.append(f"中団からじっくり構えて、長い直線を味方につけたい")
            else:
                parts.append("中団から早めの仕掛けで勝負を賭ける形")
        elif no in rear:
            if last3f_good:
                parts.append("後方一気の脚力は侮れず、展開がハマれば一気突き抜ける")
            else:
                parts.append("追い込み一手で展開の助けが欲しいところ")

        # トレンド・結論
        if "急上昇" in trend:
            parts.append("近走の勢いは本物で、ここでも上位争いは必至")
        elif "下降" in trend:
            parts.append("ただ近走はやや精彩を欠いており、全幅の信頼は置きにくい")
        elif divergence and divergence >= 2.0:
            parts.append("人気以上に走る可能性を秘めており、配当妙味も見込める")
        elif jockey_grade in ("SS", "S"):
            parts.append(f"鞍上{jockey}の腕も加味すれば、逆転の目は十分")

        result = "。".join(parts) + "。"

    # ── 4位〜中位: 条件付きで浮上の余地 ──
    elif rank <= max(field_count * 0.6, 4):
        parts = []

        # 力関係の位置づけ（数値を出さない）
        if course_total >= 53:
            parts.append("コース適性の高さが光り、条件さえ噛み合えば上位に食い込める器")
        elif "上昇" in trend or "急上昇" in trend:
            parts.append("近走は内容が充実しており、ここにきて力をつけてきた印象")
        else:
            parts.append("力量的には上位とやや差があるが、展開ひとつで浮上の余地はある")

        # 展開面
        if no in leading and len(leading) == 1:
            parts.append("単騎逃げが叶えば粘り込みのシーンも")
        elif no in front:
            if pace_v in ("SS", "MS"):
                parts.append(f"{pace_ja}になれば好位で楽に運べ、残り目が出てくる")
            else:
                parts.append("好位から堅実に立ち回れるのが強み")
        elif no in mid or no in rear:
            if last3f_good:
                parts.append("末脚の切れ味には定評があり、展開が向けば台頭のシーンも描ける")
            elif straight_m >= 400:
                parts.append(f"直線が長いコースは味方で、差し込む余地はある")
            elif straight_m <= 300:
                parts.append(f"ただ直線{straight_m}mは追い込みには厳しい条件")
            else:
                parts.append("脚質的に展開の助けが欲しいところ")

        # 結論
        if divergence and divergence >= 3.0:
            parts.append("人気薄なら馬券妙味は大きく、押さえておきたい一頭")
        elif ana_type and "穴" in ana_type:
            parts.append("穴を開ける資格は持っており、ヒモに加えてみたい")
        elif jockey_grade in ("SS", "S"):
            parts.append(f"鞍上{jockey}の手腕次第で上位食い込みの可能性も")
        else:
            parts.append("好走には展開の後押しが必要だが、相手候補には入れたい")

        result = "。".join(parts) + "。"

    # ── 下位: 厳しいが光る部分があれば ──
    else:
        parts = []

        if kiken_type and "危" in kiken_type and popularity and popularity <= 3:
            parts.append(f"{popularity}番人気に支持されているが、データ的には過大評価の疑いが濃い")
            if "下降" in trend:
                parts.append("近走の成績も下降線で、額面通りには信頼しにくい")
            elif jockey_grade in ("D",):
                parts.append(f"鞍上{jockey}も心許なく、嫌ってみる手も一考")
            else:
                parts.append("人気を考えると馬券妙味は薄く、過信は禁物")
        elif course_total >= 53:
            parts.append("力関係的に厳しいメンバー構成だが、コース適性の高さは見逃せない")
            parts.append("展開と枠順が噛み合えば、一変があっても驚けない")
        elif "急上昇" in trend:
            parts.append("ここにきて急上昇カーブを描いており、勢いだけなら侮れない")
            parts.append("格上挑戦の形だが、近走の充実ぶりで一発に期待")
        elif divergence and divergence >= 3.0:
            parts.append("実力的には苦しいが、人気薄なら配当妙味は十分ある")
            if no in leading and len(leading) == 1:
                parts.append("逃げ残りのワンチャンスに賭ける手はあり")
            else:
                parts.append("展開のアヤを拾って穴を開ける可能性はゼロではない")
        else:
            # 脚質に応じた最低限のコメント
            if no in leading and len(leading) == 1:
                parts.append("力関係的に見劣りするが、単騎逃げからの粘り込みだけは警戒が必要")
            elif no in rear:
                if straight_m >= 400:
                    parts.append("力不足は否めないが、追い込み脚質で展開のアヤを拾えれば")
                else:
                    parts.append(f"力量差に加えて直線{straight_m}mも追い込みには厳しく、好走は難しい")
            else:
                parts.append("現状の力量ではこのメンバーに入ると厳しい")

            if "下降" in trend:
                parts.append("近走の内容も精彩を欠き、巻き返しには時間がかかりそう")
            elif change and change <= -2:
                parts.append("乗り替わりもマイナス材料で、今回は見送りが妥当")
            else:
                parts.append("条件が好転するまで静観が賢明か")

        result = "。".join(parts) + "。"

    # 300字で切る（安全弁）
    if len(result) > 300:
        result = result[:280]
        last_period = result.rfind("。")
        if last_period > 150:
            result = result[:last_period + 1]

    return result


def generate_mark_comment_rich(
    all_horses: List[dict],
    race_context: dict,
) -> str:
    """
    印見解テキストを自動生成（競馬ブック風の流れる文章版）。
    印付き馬を中心に、レース展望を一つのナラティブとして300-400字で生成。

    Args:
        all_horses: 全馬データのリスト（composite順にソート済み）
        race_context: レースコンテキスト

    Returns:
        印見解テキスト
    """
    mark_order = ["◉", "◎", "○", "▲", "△", "★", "☆"]
    mark_priority = {m: i for i, m in enumerate(mark_order)}

    # 印付き馬をマーク順にソート
    marked = []
    marked_nos = set()
    for mark in mark_order:
        for h in all_horses:
            if h.get("mark") == mark:
                marked.append(h)
                marked_nos.add(h.get("horse_no"))

    if not marked:
        return ""

    # レースコンテキスト
    field_count = race_context.get("field_count", 0)
    straight_m = race_context.get("straight_m", 0)
    slope_type = race_context.get("slope_type", "")
    surface = race_context.get("surface", "")
    pace_v = race_context.get("pace_predicted", "MM")
    leading = set(race_context.get("leading_horses", []))
    front = set(race_context.get("front_horses", []))
    mid = set(race_context.get("mid_horses", []))
    rear = set(race_context.get("rear_horses", []))
    all_composites = race_context.get("all_composites", [])
    sorted_comps = sorted(all_composites, reverse=True) if all_composites else []

    pace_labels = {"HH": "ハイペース", "HM": "ハイ寄り", "MM": "ミドルペース", "MS": "スロー寄り", "SS": "スローペース"}

    def _rank(comp):
        for i, c in enumerate(sorted_comps):
            if abs(c - comp) < 0.01:
                return i + 1
        return len(sorted_comps)

    def _horse_narrative(h, mark):
        """馬1頭ぶんの見解を競馬ブック風に生成（読者を惹きつける文体）"""
        no = h.get("horse_no", 0)
        name = h.get("horse_name", "?")
        jockey = h.get("jockey", "")
        composite = h.get("composite", 0) or 0
        trend = h.get("ability_trend", "")
        jockey_grade = h.get("jockey_grade", "")
        divergence = h.get("odds_divergence")
        est_last3f = h.get("pace_estimated_last3f")
        est_last3f_rank = h.get("estimated_last3f_rank")
        rank = _rank(composite)
        course_total = h.get("course_total", 0) or 0
        ability_total = h.get("ability_total", 0) or 0
        pace_total = h.get("pace_total", 0) or 0
        popularity = h.get("popularity")
        odds = h.get("odds")

        # ◉◎: 勝ちパターンを熱く語る（3-4文）
        if mark in ("◉", "◎"):
            parts = []
            # 軸となる強み
            if ability_total >= 55:
                parts.append(f"能力値{ability_total:.1f}はメンバー中でも図抜けている")
            elif ability_total >= 52:
                parts.append("地力の高さは折り紙付き")

            # 脚質・展開シナリオ（具体的に）
            if no in leading and len(leading) == 1:
                parts.append("単騎でマイペースの逃げに持ち込めるのは大きなアドバンテージ")
            elif no in leading:
                parts.append("ハナ争いの懸念はあるが、行き脚の速さで主導権を握れるはず")
            elif no in front:
                pace_label = pace_labels.get(pace_v, "")
                if pace_v in ("SS", "MS"):
                    parts.append(f"{pace_label}想定なら好位でじっくり脚を溜めて直線一気に弾ける")
                else:
                    parts.append("好位でレースの流れに乗り、直線で力強く抜け出す形が理想")
            elif no in mid:
                if straight_m >= 400:
                    parts.append(f"中団でじっくり待機し、直線{straight_m}mの長い直線を味方につけて差し切る")
                else:
                    parts.append("中団から早めに仕掛けて押し切るイメージ")
            elif no in rear:
                parts.append("後方一気の末脚勝負に徹する形。展開がハマれば一気突き抜ける破壊力がある")

            # 補強材料
            if "急上昇" in trend:
                parts.append("近走の成績は急上昇カーブを描いており、今がまさに旬")
            elif "上昇" in trend:
                parts.append("近走の内容も充実しており勢いは十分")
            if est_last3f and est_last3f_rank and est_last3f_rank <= 2:
                parts.append(f"推定上がり{est_last3f:.1f}秒はメンバー屈指の切れ味")
            if jockey_grade in ("SS", "S"):
                parts.append(f"鞍上**{jockey}**の手腕も心強く、大崩れは考えにくい")
            elif jockey:
                parts.append(f"**{jockey}**騎手とのコンビで堅実な競馬を期待")

            if course_total >= 55:
                parts.append("コース適性の高さも見逃せない")

            narr = "。".join(parts[:4])
            return f"**{name}**が筆頭。{narr}。"

        # ○▲: 対抗・単穴として逆転の可能性を語る（2-3文）
        elif mark in ("○", "▲"):
            parts = []
            mark_label = "相手筆頭に指名" if mark == "○" else "一発があるのがこの馬"

            if no in leading:
                if len(leading) >= 2:
                    parts.append("逃げ争いのリスクを孕むが、先手を奪えば簡単には止まらない")
                else:
                    parts.append("すんなりハナを切れる公算が高く、自分の形なら粘り腰を発揮")
            elif no in front:
                parts.append("好位からそつなく立ち回れる堅実派")
                if pace_v in ("HH", "HM"):
                    parts.append("ペースが上がれば前残りの展開で浮上の目も")
            elif no in mid:
                parts.append("中団からの差し脚に期待がかかる")
                if straight_m >= 400:
                    parts.append(f"直線{straight_m}mならギリギリ届く計算")
            elif no in rear:
                if straight_m >= 400:
                    parts.append("追い込み一手だが、長い直線なら末脚が活きる舞台")
                else:
                    parts.append("直線の短さがネックだが、展開次第で台頭の余地あり")

            if divergence and divergence >= 2.0:
                parts.append("人気以上に走る可能性を秘めており配当妙味も十分")
            elif "上昇" in trend or "急上昇" in trend:
                parts.append("近走上向きの気配で侮れない")

            if ability_total >= 52:
                parts.append("地力は確かなものがある")

            narr = "。".join(parts[:3])
            return f"**{name}**を{mark_label}。{narr}。"

        # △★: 押さえ馬としての魅力を一言で（1-2文）
        elif mark in ("△", "★"):
            parts = []
            if no in leading and len(leading) == 1:
                parts.append("逃げ残りに警戒が必要")
            elif no in front:
                parts.append("好位から堅実に走れるタイプで、相手には押さえておきたい")
            elif no in rear and straight_m >= 400:
                parts.append("追い込みが決まる展開になれば馬券圏内に突っ込んでくるシーンも")
            else:
                style = h.get("running_style", "")
                if style == "逃げ" or style == "先行":
                    parts.append("前々の競馬で堅実にまとめてくるタイプ")
                elif style == "差し" or style == "追込":
                    parts.append("末脚勝負の一発に警戒")
                else:
                    parts.append("素質は確かで侮れない存在")

            if divergence and divergence >= 2.5:
                parts.append("人気薄なら配当妙味は大きい")

            narr = "。".join(parts[:2])
            return f"**{name}**は{narr}。"

        # ☆: 穴馬としてのロマンを語る（1-2文）
        else:
            parts = []
            if divergence and divergence >= 3.0:
                parts.append("人気は薄いが、秘めたポテンシャルは侮れない")
            elif course_total >= 52:
                parts.append("コース巧者の面があり、展開一つで台頭するシーンも")
            else:
                parts.append("穴を開ける資格は十分に持っている")

            if no in leading and len(leading) == 1:
                parts.append("逃げ残りの一発に要注意")
            elif no in rear and straight_m >= 400:
                parts.append("長い直線で一気に差し込む展開に期待")

            narr = "。".join(parts[:2])
            return f"**{name}**は伏兵。{narr}。"

    # ============================================================
    # 競馬ブック風の流れる文章を構築
    # ============================================================
    sections = []

    # 本命馬（◉/◎）を先頭に
    honmei = [h for h in marked if h.get("mark") in ("◉", "◎")]
    taikou = [h for h in marked if h.get("mark") == "○"]
    tannuke = [h for h in marked if h.get("mark") == "▲"]
    renka = [h for h in marked if h.get("mark") in ("△", "★")]
    ana = [h for h in marked if h.get("mark") == "☆"]

    for h in honmei:
        sections.append(_horse_narrative(h, h.get("mark", "")))
    for h in taikou:
        sections.append(_horse_narrative(h, h.get("mark", "")))
    for h in tannuke:
        sections.append(_horse_narrative(h, h.get("mark", "")))
    for h in renka:
        sections.append(_horse_narrative(h, h.get("mark", "")))
    for h in ana:
        sections.append(_horse_narrative(h, h.get("mark", "")))

    # 危険馬（印なし）
    kiken_horses = [
        h for h in all_horses
        if h.get("kiken_type", "").startswith("危") and h.get("horse_no") not in marked_nos
    ]
    for h in kiken_horses[:1]:
        name = h.get("horse_name", "?")
        pop = h.get("popularity")
        if pop and pop <= 3:
            sections.append(f"**{name}**は{pop}番人気だが、データ的には過大評価の匂い。嫌ってみる手も面白い。")

    # 各馬を改行で分離（フロントエンドで段落表示される）
    return "\n\n".join(sections) if sections else ""


# ============================================================
# 5. 偏差値の健全性チェック（診断ツール）
# ============================================================


def diagnose_deviations(evaluations: List) -> dict:
    """
    偏差値分布を診断して問題があれば報告する。

    Returns:
        {"status": "OK" | "WARNING", "message": str, "spread": float}
    """
    composites = [e.composite for e in evaluations]
    if len(composites) < 2:
        return {"status": "OK", "message": "評価馬1頭以下", "spread": 0.0}

    spread = max(composites) - min(composites)
    std_dev = statistics.stdev(composites) if len(composites) >= 2 else 0.0
    top_mean = statistics.mean(composites)

    issues = []

    if spread < 2.0:
        issues.append(
            f"[!] 総合偏差値のバラつきが小さすぎます（最大差{spread:.1f}pt）。換算定数の校正を推奨"
        )
    if std_dev < 0.5:
        issues.append(f"[!] 偏差値のσ={std_dev:.2f}。基準タイムDBのサンプルが不足している可能性")
    if not (35 < top_mean < 65):
        issues.append(f"[!] 偏差値の平均が{top_mean:.1f}と標準から外れています")

    status = "WARNING" if issues else "OK"
    msg = " / ".join(issues) if issues else f"正常（σ={std_dev:.1f}, 最大差{spread:.1f}pt）"

    return {
        "status": status,
        "message": msg,
        "spread": spread,
        "std_dev": std_dev,
    }
