"""
競馬解析マスターシステム v3.0 - デモ実行スクリプト v2
各馬に固有の過去走データ（タイム差をつけて偏差値が分散するよう設計）
東京 有馬記念シミュレーション (8頭)
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.models import (
    Horse, RaceInfo, CourseMaster, PastRun,
    JockeyStats, TrainerStats, KaisyuType, JushaRank
)
from src.engine import RaceAnalysisEngine


def make_tokyo_2000() -> CourseMaster:
    return CourseMaster(
        venue="東京", venue_code="05", distance=2000, surface="芝",
        direction="左", straight_m=526, corner_count=4, corner_type="大回り",
        first_corner="350", slope_type="急坂", inside_outside="内", is_jra=True,
    )


def prun(date, venue, cid, dist, surf, cond, cls, grade, fc,
         gn, hn, jockey, wkg, pos4c, fin, ftime, l3f, mbehind, mahead):
    return PastRun(
        race_date=date, venue=venue, course_id=cid, distance=dist, surface=surf,
        condition=cond, class_name=cls, grade=grade, field_count=fc,
        gate_no=gn, horse_no=hn, jockey=jockey, weight_kg=wkg,
        position_4c=pos4c, finish_pos=fin, finish_time_sec=ftime,
        last_3f_sec=l3f, margin_behind=mbehind, margin_ahead=mahead,
    )


def make_horses():
    """
    タイム差を意図的につけて偏差値が分散するよう設計。
    基準タイム東京芝2000 良 OP ≈ 120.0秒
      強い馬: 119.0〜119.5 → 偏差値60台
      普通の馬: 120.0〜120.5 → 偏差値50台
      弱い馬: 121.5〜122.5 → 偏差値40台
    """

    # ------ A: エースクロノス（最強馬）------
    h1_runs = [
        prun("2025-11-10","東京","05_芝_2000",2000,"芝","良","G1","G1",16,2,3,"Ｃ．ルメール",57.0,2,1,119.1,33.8,0.8,0.0),
        prun("2025-09-14","中山","06_芝_2000",2000,"芝","良","G2","G2",14,4,7,"Ｃ．ルメール",56.0,3,1,119.5,34.1,0.5,0.0),
        prun("2025-06-29","阪神","08_芝_2000",2000,"芝","良","G3","G3",16,6,10,"Ｃ．ルメール",56.0,4,1,119.8,34.3,0.3,0.0),
        prun("2025-04-06","東京","05_芝_2000",2000,"芝","良","3勝","3勝",14,1,2,"Ｃ．ルメール",56.0,2,1,120.2,34.5,0.6,0.0),
        prun("2025-01-19","中山","06_芝_2000",2000,"芝","良","2勝","2勝",14,3,5,"Ｃ．ルメール",55.0,3,2,120.3,34.4,0.0,0.3),
    ]

    # ------ B: ブルースターレイン（実力2番手）------
    h2_runs = [
        prun("2025-10-12","東京","05_芝_2000",2000,"芝","良","G2","G2",16,5,8,"川田将雅",56.0,4,2,119.4,34.2,0.0,0.4),
        prun("2025-08-03","小倉","10_芝_2000",2000,"芝","良","G3","G3",16,3,5,"川田将雅",56.0,3,1,119.7,34.0,0.4,0.0),
        prun("2025-06-01","東京","05_芝_2000",2000,"芝","稍重","3勝","3勝",14,7,12,"川田将雅",56.0,5,2,120.1,34.7,0.0,0.3),
        prun("2025-03-16","中山","06_芝_2000",2000,"芝","良","3勝","3勝",12,2,3,"武豊",56.0,2,1,120.4,34.5,0.5,0.0),
        prun("2025-01-05","京都","07_芝_2000",2000,"芝","良","2勝","2勝",12,4,6,"川田将雅",55.0,3,1,120.6,34.8,0.4,0.0),
    ]

    # ------ C: クリアスカイ（中堅上位）------
    h3_runs = [
        prun("2025-11-22","東京","05_芝_2000",2000,"芝","良","G3","G3",16,3,5,"横山武史",56.0,3,3,120.0,34.5,0.0,0.4),
        prun("2025-09-28","中山","06_芝_2000",2000,"芝","良","G3","G3",14,6,9,"横山武史",56.0,4,2,120.3,34.6,0.0,0.3),
        prun("2025-07-27","新潟","02_芝_2000",2000,"芝","良","3勝","3勝",12,1,2,"横山武史",56.0,2,1,120.5,34.4,0.5,0.0),
        prun("2025-05-11","東京","05_芝_2000",2000,"芝","良","3勝","3勝",16,5,8,"横山武史",55.0,4,3,120.7,34.8,0.0,0.3),
        prun("2025-02-23","中山","06_芝_2000",2000,"芝","稍重","2勝","2勝",14,7,11,"横山武史",55.0,5,2,121.0,35.0,0.0,0.2),
    ]

    # ------ D: ダンシングソード（牝馬・平均的）------
    h4_runs = [
        prun("2025-11-30","東京","05_芝_2000",2000,"芝","良","G2","G2",16,4,6,"Ｃ．ルメール",54.0,5,3,120.2,34.7,0.0,0.4),
        prun("2025-09-21","阪神","08_芝_2000",2000,"芝","良","G3","G3",14,2,4,"Ｃ．ルメール",54.0,3,2,120.4,34.5,0.0,0.3),
        prun("2025-07-20","小倉","10_芝_2000",2000,"芝","良","3勝","3勝",12,1,2,"Ｃ．ルメール",54.0,2,1,120.6,34.3,0.5,0.0),
        prun("2025-05-04","東京","05_芝_2000",2000,"芝","良","3勝","3勝",16,3,5,"池添謙一",54.0,4,4,121.0,34.9,0.0,0.5),
        prun("2025-02-16","東京","05_芝_2000",2000,"芝","稍重","2勝","2勝",14,6,9,"Ｃ．ルメール",53.0,3,2,121.2,35.2,0.0,0.3),
    ]

    # ------ E: エメラルドフラッシュ（中堅下位）------
    h5_runs = [
        prun("2025-11-08","東京","05_芝_2000",2000,"芝","良","G3","G3",16,7,11,"M.デムーロ",56.0,7,5,120.5,35.1,0.0,0.6),
        prun("2025-09-07","中京","09_芝_2000",2000,"芝","良","G3","G3",14,4,7,"M.デムーロ",56.0,5,4,120.8,35.0,0.0,0.5),
        prun("2025-06-22","阪神","08_芝_2000",2000,"芝","良","3勝","3勝",12,3,5,"M.デムーロ",56.0,4,2,121.0,34.8,0.0,0.2),
        prun("2025-04-20","東京","05_芝_2000",2000,"芝","良","3勝","3勝",16,6,10,"M.デムーロ",55.0,6,4,121.3,35.3,0.0,0.5),
        prun("2025-02-02","京都","07_芝_2000",2000,"芝","良","2勝","2勝",14,5,8,"M.デムーロ",55.0,4,3,121.5,35.4,0.0,0.4),
    ]
    # ------ F: フォールンエンジェル（下位）------
    h6_runs = [
        prun("2025-10-19","東京","05_芝_2000",2000,"芝","良","G3","G3",16,8,14,"戸崎圭太",56.0,9,7,121.0,35.5,0.0,0.8),
        prun("2025-08-24","新潟","02_芝_2000",2000,"芝","良","3勝","3勝",12,5,8,"戸崎圭太",56.0,6,5,121.4,35.3,0.0,0.7),
        prun("2025-06-15","阪神","08_芝_2000",2000,"芝","稍重","3勝","3勝",14,4,7,"戸崎圭太",56.0,7,6,121.8,35.7,0.0,0.8),
        prun("2025-04-13","中山","06_芝_2000",2000,"芝","良","3勝","3勝",14,3,6,"松山弘平",56.0,5,4,122.0,35.6,0.0,0.5),
        prun("2025-01-26","東京","05_芝_2000",2000,"芝","良","2勝","2勝",12,7,11,"戸崎圭太",55.0,8,6,122.3,36.0,0.0,0.8),
    ]

    # ------ G: グリーンサファイア（人気薄・穴候補）------
    h7_runs = [
        prun("2025-11-01","東京","05_芝_2000",2000,"芝","良","OP","OP",16,1,2,"藤岡佑介",56.0,3,2,119.6,34.0,0.0,0.3),  # 激走
        prun("2025-09-15","中山","06_芝_2000",2000,"芝","良","G3","G3",14,8,13,"岩田康誠",56.0,10,8,121.5,36.0,0.0,1.2),  # 凡走
        prun("2025-07-13","小倉","10_芝_2000",2000,"芝","良","3勝","3勝",12,2,3,"藤岡佑介",56.0,2,1,120.0,34.2,0.4,0.0),  # 激走
        prun("2025-05-18","東京","05_芝_2000",2000,"芝","良","3勝","3勝",16,4,7,"三浦皇成",55.0,8,7,121.8,35.8,0.0,1.0),  # 凡走
        prun("2025-02-09","中山","06_芝_2000",2000,"芝","稍重","2勝","2勝",14,2,4,"藤岡佑介",55.0,3,2,120.3,34.5,0.0,0.2),
    ]

    # ------ H: ハーモニクス（弱い人気馬）------
    h8_runs = [
        prun("2025-11-15","東京","05_芝_2000",2000,"芝","良","G2","G2",16,6,9,"武豊",56.0,6,6,120.8,35.2,0.0,0.7),
        prun("2025-09-28","阪神","08_芝_2000",2000,"芝","良","G2","G2",16,5,8,"武豊",56.0,7,7,121.0,35.4,0.0,0.8),
        prun("2025-07-06","中京","09_芝_2000",2000,"芝","良","G3","G3",14,3,5,"武豊",56.0,5,5,121.3,35.6,0.0,0.6),
        prun("2025-05-25","東京","05_芝_2000",2000,"芝","良","G3","G3",16,2,4,"武豊",57.0,3,4,121.5,35.5,0.0,0.5),  # 過負担
        prun("2025-03-02","中山","06_芝_2000",2000,"芝","稍重","3勝","3勝",14,4,6,"武豊",56.0,4,3,121.7,35.7,0.0,0.5),
    ]

    horses = [
        Horse(horse_id="H001", horse_name="エースクロノス",
              sex="牡", age=5, color="鹿毛",
              trainer="藤原英昭", trainer_id="T001",
              owner="G1ファーム", breeder="ノーザンファーム",
              sire="ディープインパクト", dam="クロノス",
              past_runs=h1_runs,
              race_date="2025-12-28", venue="東京", race_no=11,
              gate_no=1, horse_no=1,
              jockey="Ｃ．ルメール", jockey_id="J001",
              weight_kg=57.0, base_weight_kg=57.0,
              odds=2.8, popularity=1,
              prev_jockey="Ｃ．ルメール"),

        Horse(horse_id="H002", horse_name="ブルースターレイン",
              sex="牡", age=4, color="青鹿毛",
              trainer="国枝栄", trainer_id="T002",
              owner="キャロットファーム", breeder="社台ファーム",
              sire="キングカメハメハ", dam="ブルースター",
              past_runs=h2_runs,
              race_date="2025-12-28", venue="東京", race_no=11,
              gate_no=2, horse_no=2,
              jockey="川田将雅", jockey_id="J002",
              weight_kg=57.0, base_weight_kg=57.0,
              odds=5.1, popularity=2,
              prev_jockey="川田将雅"),

        Horse(horse_id="H003", horse_name="クリアスカイ",
              sex="牡", age=5, color="栗毛",
              trainer="矢作芳人", trainer_id="T003",
              owner="シルクレーシング", breeder="ノーザンファーム",
              sire="ハービンジャー", dam="クリアブルー",
              past_runs=h3_runs,
              race_date="2025-12-28", venue="東京", race_no=11,
              gate_no=3, horse_no=3,
              jockey="横山武史", jockey_id="J003",
              weight_kg=57.0, base_weight_kg=57.0,
              odds=8.3, popularity=3,
              prev_jockey="横山武史"),

        Horse(horse_id="H004", horse_name="ダンシングソード",
              sex="牝", age=4, color="鹿毛",
              trainer="中内田充正", trainer_id="T004",
              owner="サンデーレーシング", breeder="ノーザンファーム",
              sire="ハーツクライ", dam="ダンシングクイーン",
              past_runs=h4_runs,
              race_date="2025-12-28", venue="東京", race_no=11,
              gate_no=4, horse_no=4,
              jockey="Ｃ．ルメール", jockey_id="J001b",
              weight_kg=55.0, base_weight_kg=55.0,
              odds=9.7, popularity=4,
              prev_jockey="池添謙一"),

        Horse(horse_id="H005", horse_name="エメラルドフラッシュ",
              sex="牡", age=6, color="黒鹿毛",
              trainer="堀宣行", trainer_id="T005",
              owner="個人馬主", breeder="追分ファーム",
              sire="ステイゴールド", dam="エメラルドグリーン",
              past_runs=h5_runs,
              race_date="2025-12-28", venue="東京", race_no=11,
              gate_no=5, horse_no=5,
              jockey="M.デムーロ", jockey_id="J004",
              weight_kg=57.0, base_weight_kg=57.0,
              odds=15.0, popularity=5,
              prev_jockey="M.デムーロ"),

        Horse(horse_id="H006", horse_name="フォールンエンジェル",
              sex="牡", age=4, color="芦毛",
              trainer="藤沢和雄", trainer_id="T006",
              owner="クラブ馬主", breeder="社台ファーム",
              sire="ドゥラメンテ", dam="フォールンスター",
              past_runs=h6_runs,
              race_date="2025-12-28", venue="東京", race_no=11,
              gate_no=6, horse_no=6,
              jockey="戸崎圭太", jockey_id="J005",
              weight_kg=57.0, base_weight_kg=57.0,
              odds=22.0, popularity=6,
              prev_jockey="戸崎圭太"),

        Horse(horse_id="H007", horse_name="グリーンサファイア",
              sex="牡", age=5, color="鹿毛",
              trainer="石坂正", trainer_id="T007",
              owner="個人馬主", breeder="浦河町",
              sire="ゴールドシップ", dam="グリーンジェム",
              past_runs=h7_runs,
              race_date="2025-12-28", venue="東京", race_no=11,
              gate_no=7, horse_no=7,
              jockey="藤岡佑介", jockey_id="J006",
              weight_kg=57.0, base_weight_kg=57.0,
              odds=55.0, popularity=8,
              prev_jockey="藤岡佑介"),

        Horse(horse_id="H008", horse_name="ハーモニクス",
              sex="牡", age=5, color="青鹿毛",
              trainer="池江泰寿", trainer_id="T008",
              owner="Gホールディング", breeder="ノーザンファーム",
              sire="オルフェーヴル", dam="ハーモニー",
              past_runs=h8_runs,
              race_date="2025-12-28", venue="東京", race_no=11,
              gate_no=8, horse_no=8,
              jockey="武豊", jockey_id="J007",
              weight_kg=57.0, base_weight_kg=57.0,
              odds=12.0, popularity=4,
              prev_jockey="池添謙一"),
    ]
    return horses


def make_race_info(course: CourseMaster) -> RaceInfo:
    return RaceInfo(
        race_id="202501050511",
        race_date="2025-12-28",
        venue="東京",
        race_no=11,
        race_name="有馬記念シミュレーション",
        grade="G1",
        condition="3歳以上オープン",
        course=course,
        field_count=8,
        is_jra=True,
        track_condition_turf="良",
        cv_value=None,
        moisture_turf=None,
    )


def main():
    course = make_tokyo_2000()
    horses = make_horses()
    race   = make_race_info(course)

    # course_db を今回の過去走から構築
    course_db = {}
    for h in horses:
        for run in h.past_runs:
            if run.course_id not in course_db:
                course_db[run.course_id] = []
            course_db[run.course_id].append(run)

    # 騎手・厩舎DB (簡易版)
    jockey_db = {
        "J001":  JockeyStats("J001",  "Ｃ．ルメール",   upper_long_dev=68.0, upper_short_dev=66.0, lower_long_dev=65.0, lower_short_dev=62.0, kaisyu_type=KaisyuType.SHINRAITYPE),
        "J001b": JockeyStats("J001b", "Ｃ．ルメール",   upper_long_dev=68.0, upper_short_dev=66.0, lower_long_dev=65.0, lower_short_dev=62.0, kaisyu_type=KaisyuType.SHINRAITYPE),
        "J002":  JockeyStats("J002",  "川田将雅",       upper_long_dev=65.0, upper_short_dev=64.0, lower_long_dev=58.0, lower_short_dev=56.0, kaisyu_type=KaisyuType.SHINRAITYPE),
        "J003":  JockeyStats("J003",  "横山武史",       upper_long_dev=58.0, upper_short_dev=57.0, lower_long_dev=52.0, lower_short_dev=51.0, kaisyu_type=KaisyuType.HEIBONTYPE),
        "J004":  JockeyStats("J004",  "M.デムーロ",    upper_long_dev=62.0, upper_short_dev=60.0, lower_long_dev=58.0, lower_short_dev=55.0, kaisyu_type=KaisyuType.ANA_TYPE),
        "J005":  JockeyStats("J005",  "戸崎圭太",       upper_long_dev=55.0, upper_short_dev=54.0, lower_long_dev=50.0, lower_short_dev=49.0, kaisyu_type=KaisyuType.HEIBONTYPE),
        "J006":  JockeyStats("J006",  "藤岡佑介",       upper_long_dev=50.0, upper_short_dev=50.0, lower_long_dev=55.0, lower_short_dev=54.0, kaisyu_type=KaisyuType.ANA_TYPE),
        "J007":  JockeyStats("J007",  "武豊",           upper_long_dev=62.0, upper_short_dev=60.0, lower_long_dev=55.0, lower_short_dev=54.0, kaisyu_type=KaisyuType.SHINRAITYPE),
    }
    trainer_db = {
        "T001": TrainerStats("T001","藤原英昭","藤原英昭","JRA", rank=JushaRank.A, kaisyu_type=KaisyuType.SHINRAITYPE),
        "T002": TrainerStats("T002","国枝栄",  "国枝栄",  "JRA", rank=JushaRank.A, kaisyu_type=KaisyuType.SHINRAITYPE),
        "T003": TrainerStats("T003","矢作芳人","矢作芳人","JRA", rank=JushaRank.A, kaisyu_type=KaisyuType.SHINRAITYPE),
        "T004": TrainerStats("T004","中内田充正","中内田充正","JRA", rank=JushaRank.B, kaisyu_type=KaisyuType.HEIBONTYPE),
        "T005": TrainerStats("T005","堀宣行",  "堀宣行",  "JRA", rank=JushaRank.B, kaisyu_type=KaisyuType.HEIBONTYPE),
        "T006": TrainerStats("T006","藤沢和雄","藤沢和雄","JRA", rank=JushaRank.B, kaisyu_type=KaisyuType.KAJOHYOKA),
        "T007": TrainerStats("T007","石坂正",  "石坂正",  "JRA", rank=JushaRank.C, kaisyu_type=KaisyuType.ANA_TYPE),
        "T008": TrainerStats("T008","池江泰寿","池江泰寿","JRA", rank=JushaRank.A, kaisyu_type=KaisyuType.SHINRAITYPE),
    }

    print("[実行中] レース分析...")
    engine = RaceAnalysisEngine(
        course_db=course_db,
        all_courses={"05_芝_2000": course},
        jockey_db=jockey_db,
        trainer_db=trainer_db,
        trainer_baseline_db={},
        pace_last3f_db={},
        course_style_stats_db={},
        is_jra=True,
    )

    analysis = engine.analyze(race, horses, custom_stake=None)
    os.makedirs("output", exist_ok=True)
    out_path = os.path.join("output", "keiba_demo.html")
    engine.render_html(analysis, out_path)

    print("\n[完了] 分析完了")
    print(f"   出走頭数: {len(horses)}頭")
    print(f"   ペース予測: {analysis.pace_type_predicted.value if analysis.pace_type_predicted else '不明'}")
    print(f"   自信度: {analysis.overall_confidence.value}")
    print(f"   買い目数: {len(analysis.tickets)}点")
    print()

    # 偏差値分布表示
    print("--- 全馬偏差値 ---")
    for ev in sorted(analysis.evaluations, key=lambda e: e.composite, reverse=True):
        mk = ev.mark.value
        print(f"  {mk} {ev.horse.horse_name:<16} 総合{ev.composite:.1f}"
              f"  能{ev.ability.total:.1f} 展{ev.pace.total:.1f} コ{ev.course.total:.1f}"
              f"  {ev.horse.odds:.1f}倍")

    print()
    if analysis.tickets:
        print("--- 買い目 ---")
        for t in analysis.tickets:
            print(f"  {t['type']} {t['a']}-{t['b']}  EV{t['ev']:.0f}%  {t.get('stake',0):,}円")
    else:
        print("--- 見送り（期待値基準未達）---")

    print(f"\nHTML出力: {os.path.abspath(out_path)}")


if __name__ == "__main__":
    main()
