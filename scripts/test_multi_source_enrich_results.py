"""
MultiSourceEnricher.enrich_results の動作確認スクリプト
（自動テストではなく手動確認用）

使い方:
    python scripts/test_multi_source_enrich_results.py 202644042706 2026-04-26
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models import Horse
from src.scraper.multi_source import MultiSourceEnricher
from src.scraper.official_nar import OfficialNARScraper


def main() -> None:
    if len(sys.argv) < 3:
        print("使い方: python scripts/test_multi_source_enrich_results.py <race_id> <YYYY-MM-DD>")
        sys.exit(1)

    race_id = sys.argv[1]
    race_date = sys.argv[2]

    # ダミー Horse オブジェクト（horse_no 1-16、最小限のフィールドのみ）
    horses: list[Horse] = []
    for no in range(1, 17):
        h = Horse(
            horse_id="",
            horse_name=f"テスト馬{no:02d}",
            sex="牡",
            age=3,
            color="鹿毛",
            trainer="テスト調教師",
            trainer_id="",
            owner="テスト馬主",
            breeder="テスト生産者",
            sire="テスト父",
            dam="テスト母",
            horse_no=no,
        )
        horses.append(h)

    nar = OfficialNARScraper()
    enricher = MultiSourceEnricher(nar_scraper=nar)

    print(f"[INFO] race_id={race_id}  race_date={race_date}")
    print("[INFO] NAR 結果補完開始...")

    stats = enricher.enrich_results(race_id, race_date, horses)

    print(f"[結果] finish_time={stats['finish_time']}件, last_3f={stats['last_3f']}件, corners={stats['corners']}件")
    print()

    # 個別馬の補完内容を表示
    print(f"{'馬番':>4}  {'馬名':<14}  {'走破時計(秒)':>12}  {'上がり3F(秒)':>12}  {'通過順':>20}")
    print("-" * 70)
    for h in horses:
        ts = getattr(h, "finish_time_sec", "-")
        l3f = getattr(h, "last_3f_sec", "-")
        passing = getattr(h, "passing", getattr(h, "corners", "-"))
        print(f"{h.horse_no:>4}  {h.horse_name:<14}  {str(ts):>12}  {str(l3f):>12}  {str(passing):>20}")


if __name__ == "__main__":
    main()
