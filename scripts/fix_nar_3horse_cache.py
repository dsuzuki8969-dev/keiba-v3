"""壊れたNARキャッシュ（3頭のみ）を削除して再取得するスクリプト

commit 30d9d99 で official_nar.py の len(cells) < 12 → < 4 修正済みだが、
修正前に作成されたキャッシュ5件に3頭しか入っていない問題を解消する。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.masters.course_master import get_all_courses
from src.scraper.auth import PremiumNetkeibaScraper
from src.scraper.race_cache import invalidate_race_cache, load_race_cache

BROKEN_RACES = [
    "202643050502",  # 船橋 2R
    "202643050503",  # 船橋 3R
    "202648050509",  # 名古屋 9R
    "202648050510",  # 名古屋 10R
    "202648050511",  # 名古屋 11R
]

def main():
    print("=== NAR 3頭キャッシュ修復スクリプト ===\n")

    # Step 1: 壊れたキャッシュを削除
    print("[1/3] 壊れたキャッシュを削除...")
    for race_id in BROKEN_RACES:
        cached = load_race_cache(race_id)
        count = len(cached[1]) if cached else 0
        deleted = invalidate_race_cache(race_id)
        status = "削除" if deleted else "ファイル無し"
        print(f"  {race_id}: {status} (旧キャッシュ {count}頭)")

    # Step 2: スクレイパー初期化（NAR公式のみ使用、netkeiba認証不要）
    print("\n[2/3] スクレイパー初期化...")
    all_courses = get_all_courses()
    scraper = PremiumNetkeibaScraper(all_courses, ignore_ttl=True)
    scraper._official_only = True

    # Step 3: 再取得
    print("\n[3/3] NAR公式から再取得...")
    for i, race_id in enumerate(BROKEN_RACES):
        print(f"\n  [{i+1}/{len(BROKEN_RACES)}] {race_id} 再取得中...")
        try:
            race_info, horses = scraper.fetch_race(
                race_id,
                fetch_history=True,
                fetch_odds=False,
                fetch_training=False,
                use_cache=True,
                prefer_cache=True,
            )
            print(f"    → {len(horses)}頭取得: {', '.join(f'#{h.horse_no} {h.horse_name}' for h in horses[:5])}{'...' if len(horses) > 5 else ''}")
        except Exception as e:
            print(f"    → エラー: {e}")

        if i < len(BROKEN_RACES) - 1:
            time.sleep(2.5)

    # Step 4: 検証
    print("\n=== 検証 ===")
    all_ok = True
    for race_id in BROKEN_RACES:
        cached = load_race_cache(race_id)
        if cached:
            count = len(cached[1])
            status = "OK" if count >= 5 else "NG (まだ少ない)"
            if count < 5:
                all_ok = False
            print(f"  {race_id}: {count}頭 → {status}")
        else:
            all_ok = False
            print(f"  {race_id}: キャッシュ無し → NG")

    print(f"\n{'全レース修復完了' if all_ok else '一部レースで問題あり'}")
    return 0 if all_ok else 1

if __name__ == "__main__":
    sys.exit(main())
