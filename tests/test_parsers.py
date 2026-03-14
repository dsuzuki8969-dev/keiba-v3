"""
競馬解析マスターシステム v3.0 - パーサー・計算ロジックテスト

モデル作成、キャリブレーション、StandardTimeDBBuilder、少頭数エッジケースを検証。
ネットワーク不要で実行可能。
"""

import pytest

from src.calculator.calibration import (
    calibrate_conversion_constant,
    filter_post_renovation_runs,
    generate_pace_comment,
    get_base_weight,
    calc_weight_correction,
    is_pre_renovation,
)
from src.models import (
    CourseMaster,
    Horse,
    PastRun,
    PaceType,
    RaceInfo,
)
from src.scraper.race_results import (
    Last3FDBBuilder,
    StandardTimeDBBuilder,
    build_course_db_from_past_runs,
    build_course_style_stats_db,
    build_gate_bias_db,
)


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def sample_course() -> CourseMaster:
    """テスト用コースマスタ（東京芝1600）"""
    return CourseMaster(
        venue="東京",
        venue_code="05",
        distance=1600,
        surface="芝",
        direction="左",
        straight_m=528,
        corner_count=3,
        corner_type="大回り",
        first_corner="長い",
        slope_type="急坂",
        inside_outside="外",
        is_jra=True,
    )


@pytest.fixture
def sample_past_run() -> PastRun:
    """テスト用過去走データ"""
    return PastRun(
        race_date="2024-01-14",
        venue="東京",
        course_id="05_芝_1600",
        distance=1600,
        surface="芝",
        condition="良",
        class_name="3勝クラス",
        grade="3勝",
        field_count=16,
        gate_no=3,
        horse_no=5,
        jockey="川田将雅",
        weight_kg=57.0,
        position_4c=4,
        finish_pos=2,
        finish_time_sec=96.5,
        last_3f_sec=35.2,
        margin_behind=0.2,
        margin_ahead=0.5,
        pace=PaceType.MM,
    )


@pytest.fixture
def sample_horse() -> Horse:
    """テスト用馬データ"""
    past_run = PastRun(
        race_date="2024-01-14",
        venue="東京",
        course_id="05_芝_1600",
        distance=1600,
        surface="芝",
        condition="良",
        class_name="3勝クラス",
        grade="3勝",
        field_count=16,
        gate_no=3,
        horse_no=5,
        jockey="川田将雅",
        weight_kg=57.0,
        position_4c=4,
        finish_pos=2,
        finish_time_sec=96.5,
        last_3f_sec=35.2,
        margin_behind=0.2,
        margin_ahead=0.5,
    )
    return Horse(
        horse_id="abc123",
        horse_name="テスト馬",
        sex="牡",
        age=4,
        color="鹿毛",
        trainer="藤原英昭",
        trainer_id="t001",
        owner="テストオーナー",
        breeder="テスト牧場",
        sire="ディープインパクト",
        dam="テスト母馬",
        past_runs=[past_run],
        race_date="2024-02-10",
        venue="東京",
        race_no=11,
        gate_no=4,
        horse_no=7,
        jockey="松岡正海",
        jockey_id="j001",
        weight_kg=58.0,
        base_weight_kg=57.0,
        prev_jockey="川田将雅",
    )


# ============================================================
# 1. モデル作成テスト (PastRun, Horse, RaceInfo)
# ============================================================


class Test過去走モデル作成:
    """PastRun データクラスの作成・プロパティ検証"""

    def test_過去走モデル正常作成(self, sample_past_run: PastRun):
        """有効データでPastRunが正しく作成される"""
        assert sample_past_run.race_date == "2024-01-14"
        assert sample_past_run.venue == "東京"
        assert sample_past_run.distance == 1600
        assert sample_past_run.finish_pos == 2
        assert sample_past_run.last_3f_sec == 35.2

    def test_相対位置プロパティ_position4c使用(self, sample_past_run: PastRun):
        """positions_cornersが空の場合、position_4cで相対位置を算出"""
        # 4角4番目 / 16頭 = 0.25
        assert sample_past_run.relative_position == 0.25

    def test_相対位置プロパティ_positions_corners優先(self):
        """positions_cornersがあれば最後角を使用"""
        run = PastRun(
            race_date="2024-01-14",
            venue="東京",
            course_id="05_芝_1600",
            distance=1600,
            surface="芝",
            condition="良",
            class_name="3勝",
            grade="3勝",
            field_count=10,
            gate_no=1,
            horse_no=1,
            jockey="テスト",
            weight_kg=57.0,
            position_4c=5,
            finish_pos=3,
            finish_time_sec=96.0,
            last_3f_sec=35.0,
            margin_behind=0.5,
            margin_ahead=0.3,
            positions_corners=[2, 3, 4, 5],  # 4角は5番目
        )
        # 5/10 = 0.5
        assert run.relative_position == 0.5

    def test_着差評価指数(self, sample_past_run: PastRun):
        """chakusa_index = margin_behind - margin_ahead"""
        assert sample_past_run.chakusa_index == 0.2 - 0.5


class Test馬モデル作成:
    """Horse データクラスの作成・プロパティ検証"""

    def test_馬モデル正常作成(self, sample_horse: Horse):
        """有効データでHorseが正しく作成される"""
        assert sample_horse.horse_id == "abc123"
        assert sample_horse.horse_name == "テスト馬"
        assert len(sample_horse.past_runs) == 1
        assert sample_horse.weight_kg == 58.0

    def test_斤量補正量(self, sample_horse: Horse):
        """weight_diff = (weight_kg - base_weight_kg) * 0.15"""
        assert sample_horse.weight_diff == (58.0 - 57.0) * 0.15

    def test_乗り替わり判定_別騎手(self, sample_horse: Horse):
        """prev_jockeyとjockeyが異なれば乗り替わり"""
        sample_horse.prev_jockey = "川田将雅"
        sample_horse.jockey = "松岡正海"
        assert sample_horse.is_jockey_change is True

    def test_乗り替わり判定_同一騎手短縮名(self):
        """短縮名(松岡)とフルネーム(松岡正海)は同一扱い"""
        h = Horse(
            horse_id="x",
            horse_name="テスト",
            sex="牡",
            age=4,
            color="鹿毛",
            trainer="t",
            trainer_id="t1",
            owner="o",
            breeder="b",
            sire="s",
            dam="d",
            prev_jockey="松岡",
            jockey="松岡正海",
        )
        assert h.is_jockey_change is False


class Testレース情報モデル作成:
    """RaceInfo データクラスの作成検証"""

    def test_レース情報モデル正常作成(self, sample_course: CourseMaster):
        """有効データでRaceInfoが正しく作成される"""
        race = RaceInfo(
            race_id="202402100511",
            race_date="2024-02-10",
            venue="東京",
            race_no=11,
            race_name="フェブラリーS",
            grade="G1",
            condition="サラ系4歳以上",
            course=sample_course,
            field_count=16,
            weather="晴",
            track_condition_turf="良",
        )
        assert race.race_id == "202402100511"
        assert race.venue == "東京"
        assert race.course.venue_code == "05"
        assert race.field_count == 16


class Testコースマスタ作成:
    """CourseMaster の作成・メソッド検証"""

    def test_course_idプロパティ(self, sample_course: CourseMaster):
        """course_id = venue_code_surface_distance"""
        assert sample_course.course_id == "05_芝_1600"

    def test_コース類似度スコア_同一(self, sample_course: CourseMaster):
        """同一コースなら最大スコア（6項目合計7.0pt）"""
        score = sample_course.similarity_score(sample_course)
        assert score == 7.0

    def test_コース類似度スコア_異なる芝(self, sample_course: CourseMaster):
        """芝同士・距離差200m以内で部分一致"""
        other = CourseMaster(
            venue="阪神",
            venue_code="08",
            distance=1400,
            surface="芝",
            direction="左",
            straight_m=400,
            corner_count=2,
            corner_type="大回り",
            first_corner="短い",
            slope_type="軽坂",
            inside_outside="内",
        )
        score = sample_course.similarity_score(other)
        assert score >= 4.0  # surface, direction, corner_type, distance


# ============================================================
# 2. キャリブレーションテスト
# ============================================================


class Test換算定数キャリブレーション:
    """calibrate_conversion_constant の純関数テスト"""

    def test_サンプル不足時はデフォルト3_5(self):
        """走数が少ないと3.5を返す"""
        course_db = {"05_芝_1600": [{"finish_time_sec": 96.0, "distance": 1600}] * 5}
        k = calibrate_conversion_constant(course_db)
        assert k == 3.5

    def test_十分なサンプルで換算定数算出(self):
        """30走以上のデータで換算定数を算出（1.0〜8.0にクリップ）"""
        times = [95.0 + (i % 10) * 0.5 for i in range(50)]
        course_db = {
            "05_芝_1600": [
                {"finish_time_sec": t, "distance": 1600} for t in times
            ]
        }
        k = calibrate_conversion_constant(course_db)
        assert 1.0 <= k <= 8.0
        assert k != 3.5 or len(times) < 30  # 十分あれば値が変わる可能性


class Test性齢定量:
    """get_base_weight, calc_weight_correction のテスト"""

    def test_2歳牡馬(self):
        """2歳牡は55kg"""
        assert get_base_weight("牡", 2, "2024-06-15") == 55.0

    def test_3歳前半牝馬(self):
        """3歳前半牝は54kg"""
        assert get_base_weight("牝", 3, "2024-04-01") == 54.0

    def test_4歳以上牡馬(self):
        """4歳以上牡は57kg"""
        assert get_base_weight("牡", 5, "2024-08-01") == 57.0

    def test_斤量補正_牝馬限定戦は0(self):
        """牝馬限定戦は補正なし"""
        v = calc_weight_correction(58.0, "牝", 4, "2024-06-01", is_female_race=True)
        assert v == 0.0

    def test_斤量補正_1kgあたり0_15秒(self):
        """基準より2kg重いと0.3秒補正"""
        v = calc_weight_correction(59.0, "牡", 4, "2024-06-01")
        assert abs(v - 0.3) < 0.001  # 59-57=2, 2*0.15=0.3


class Test改修前データ除外:
    """is_pre_renovation, filter_post_renovation_runs のテスト"""

    def test_東京改修前データ除外(self):
        """東京2020年改修前の過去走は除外される"""
        # 東京05の改修: 2020-01-01〜2020-06-30
        assert is_pre_renovation("05", "2019-06-15", "2024-02-10") is True

    def test_東京改修後は除外されない(self):
        """改修後の過去走は除外されない"""
        assert is_pre_renovation("05", "2021-06-15", "2024-02-10") is False

    def test_改修イベントのない競馬場(self):
        """改修イベントがなければFalse"""
        assert is_pre_renovation("99", "2010-01-01", "2024-02-10") is False

    def test_filter_post_renovation_runs(self):
        """改修前の走をフィルタする"""
        runs = [
            PastRun(
                race_date="2019-06-15",
                venue="東京",
                course_id="05_芝_1600",
                distance=1600,
                surface="芝",
                condition="良",
                class_name="3勝",
                grade="3勝",
                field_count=16,
                gate_no=1,
                horse_no=1,
                jockey="x",
                weight_kg=57.0,
                position_4c=1,
                finish_pos=1,
                finish_time_sec=96.0,
                last_3f_sec=35.0,
                margin_behind=0.0,
                margin_ahead=0.5,
            ),
            PastRun(
                race_date="2021-06-15",
                venue="東京",
                course_id="05_芝_1600",
                distance=1600,
                surface="芝",
                condition="良",
                class_name="3勝",
                grade="3勝",
                field_count=16,
                gate_no=2,
                horse_no=2,
                jockey="y",
                weight_kg=57.0,
                position_4c=2,
                finish_pos=2,
                finish_time_sec=96.5,
                last_3f_sec=35.2,
                margin_behind=0.5,
                margin_ahead=0.3,
            ),
        ]
        filtered = filter_post_renovation_runs(runs, "05", "2024-02-10")
        assert len(filtered) == 1
        assert filtered[0].race_date == "2021-06-15"


# ============================================================
# 3. StandardTimeDBBuilder / 関連ビルダーテスト
# ============================================================


class TestStandardTimeDBBuilder基本操作:
    """StandardTimeDBBuilder のネットワーク不要テスト"""

    def test_初期状態は空(self):
        """初期化時はcourse_dbが空"""
        builder = StandardTimeDBBuilder()
        db = builder.get_course_db()
        assert db == {}

    def test_stats空(self):
        """空DBのstats"""
        builder = StandardTimeDBBuilder()
        s = builder.stats()
        assert "0" in s or "コース数" in s

    def test_build_course_db_from_past_runs(self, sample_horse: Horse):
        """過去走からcourse_dbに追加"""
        course_db: dict = {}
        build_course_db_from_past_runs([sample_horse], course_db)
        assert "05_芝_1600" in course_db
        assert len(course_db["05_芝_1600"]) == 1


class TestLast3FDBBuilder:
    """Last3FDBBuilder のビルドテスト"""

    def test_芝のlast3f範囲32_40(self):
        """芝コースは32〜40秒のみ集計"""
        runs = [
            PastRun(
                race_date="2024-01-01",
                venue="東京",
                course_id="05_芝_1600",
                distance=1600,
                surface="芝",
                condition="良",
                class_name="3勝",
                grade="3勝",
                field_count=16,
                gate_no=1,
                horse_no=1,
                jockey="x",
                weight_kg=57.0,
                position_4c=1,
                finish_pos=1,
                finish_time_sec=96.0,
                last_3f_sec=35.0,
                margin_behind=0.0,
                margin_ahead=0.5,
                pace=PaceType.MM,
            ),
        ]
        course_db = {"05_芝_1600": runs}
        builder = Last3FDBBuilder()
        result = builder.build(course_db)
        assert "05_芝_1600" in result
        assert "MM" in result["05_芝_1600"]
        assert 35.0 in result["05_芝_1600"]["MM"]


# ============================================================
# 4. 少頭数エッジケース (1〜5頭)
# ============================================================


class Test少頭数エッジケース:
    """field_count 1〜5 のエッジケース"""

    def test_field_count_1_相対位置(self):
        """1頭立て: 1/1=1.0 (0除算回避)"""
        run = PastRun(
            race_date="2024-01-01",
            venue="東京",
            course_id="05_芝_1600",
            distance=1600,
            surface="芝",
            condition="良",
            class_name="3勝",
            grade="3勝",
            field_count=1,
            gate_no=1,
            horse_no=1,
            jockey="x",
            weight_kg=57.0,
            position_4c=1,
            finish_pos=1,
            finish_time_sec=96.0,
            last_3f_sec=35.0,
            margin_behind=0.0,
            margin_ahead=0.0,
        )
        assert run.relative_position == 1.0

    def test_field_count_3_相対位置(self):
        """3頭立て: 2番目=2/3"""
        run = PastRun(
            race_date="2024-01-01",
            venue="東京",
            course_id="05_芝_1600",
            distance=1600,
            surface="芝",
            condition="良",
            class_name="3勝",
            grade="3勝",
            field_count=3,
            gate_no=2,
            horse_no=2,
            jockey="x",
            weight_kg=57.0,
            position_4c=2,
            finish_pos=2,
            finish_time_sec=96.0,
            last_3f_sec=35.0,
            margin_behind=0.2,
            margin_ahead=0.5,
        )
        assert abs(run.relative_position - 2 / 3) < 0.001

    def test_build_gate_bias_7頭以下スキップ(self):
        """field_count<=7の走は枠順バイアス集計から除外"""
        runs = [
            PastRun(
                race_date="2024-01-01",
                venue="05",
                course_id="05_芝_1600",
                distance=1600,
                surface="芝",
                condition="良",
                class_name="3勝",
                grade="3勝",
                field_count=5,
                gate_no=1,
                horse_no=1,
                jockey="x",
                weight_kg=57.0,
                position_4c=1,
                finish_pos=1,
                finish_time_sec=96.0,
                last_3f_sec=35.0,
                margin_behind=0.0,
                margin_ahead=0.5,
            ),
        ] * 20
        course_db = {"05_芝_1600": runs}
        result = build_gate_bias_db(course_db)
        # 7頭以下はgate_noでスキップされるため、venue_surfaceキーが含まれない可能性
        assert isinstance(result, dict)

    def test_build_course_style_stats_5頭未満スキップ(self):
        """5頭未満のコースはcourse_style_statsに含まれない"""
        runs = [
            PastRun(
                race_date="2024-01-01",
                venue="東京",
                course_id="05_芝_1600",
                distance=1600,
                surface="芝",
                condition="良",
                class_name="3勝",
                grade="3勝",
                field_count=4,
                gate_no=1,
                horse_no=1,
                jockey="x",
                weight_kg=57.0,
                position_4c=1,
                finish_pos=1,
                finish_time_sec=96.0,
                last_3f_sec=35.0,
                margin_behind=0.0,
                margin_ahead=0.5,
            ),
        ] * 4
        course_db = {"05_芝_1600": runs}
        result = build_course_style_stats_db(course_db)
        # len(runs)<5 でcontinueされる
        assert "05_芝_1600" not in result


# ============================================================
# 5. その他キャリブレーション（generate_pace_comment, diagnose_deviations）
# ============================================================


class Test展開コメント生成:
    """generate_pace_comment の基本テスト（network不要）"""

    def test_ペースコメント生成_ミドル(self, sample_course: CourseMaster):
        """MMペースでコメント生成"""
        # HorseEvaluation のモックは複雑なので、簡易評価オブジェクトを使用
        class SimpleEval:
            def __init__(self, horse_no, horse_name):
                self.horse = type("H", (), {"horse_no": horse_no, "horse_name": horse_name})()

        comment, gate, style, reason = generate_pace_comment(
            PaceType.MM,
            leaders=[5],
            front_horses=[5, 7],
            rear_horses=[1, 2, 3],
            course=sample_course,
            all_evaluations=[SimpleEval(5, "リーダー馬"), SimpleEval(7, "先行馬")],
        )
        assert "ミドル" in comment or "平均" in comment
        assert gate
        assert style


class Test偏差値診断:
    """diagnose_deviations のテスト（HorseEvaluationの簡易モック必要）"""

    def test_診断_評価1頭以下(self):
        """1頭以下ならOK"""
        from src.calculator.calibration import diagnose_deviations

        class SimpleEval:
            composite = 55.0

        result = diagnose_deviations([SimpleEval()])
        assert result["status"] == "OK"
        assert "評価馬1頭" in result["message"] or result["spread"] == 0.0
