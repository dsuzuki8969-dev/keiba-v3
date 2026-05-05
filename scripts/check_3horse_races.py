"""5/5 で 3 頭しか取り込めていないレースの実出走頭数を keibabook で確認"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.scraper.keibabook_training import KeibabookClient
from src.scraper.keibabook_result import KeibabookResultScraper

kb = KeibabookClient()
if not kb.ensure_login():
    print("[ERR] keibabook login failed")
    sys.exit(1)

scraper = KeibabookResultScraper(kb)
TARGETS = [
    ('202648050505', '2026-05-05'),  # 名古屋 5R
    ('202643050511', '2026-05-05'),  # 船橋 11R (かしわ記念)
    ('202650050506', '2026-05-05'),  # 園田 6R
    ('202646050509', '2026-05-05'),  # 金沢 9R
    ('202635050505', '2026-05-05'),  # 盛岡 5R
]

for rid, rdate in TARGETS:
    print(f"\n=== {rid} ({rdate}) ===")
    try:
        result = scraper.fetch_result(netkeiba_race_id=rid, race_date=rdate)
        if not result:
            print("  → 取得失敗")
            continue
        order = result.get('order') or []
        print(f"  keibabook 実出走頭数: {len(order)}")
        if order:
            for o in order[:5]:
                print(f"    着={o.get('finish_pos') or o.get('finish')} 馬番={o.get('horse_no')} 馬名={o.get('horse_name', '')}")
    except Exception as e:
        import traceback
        print(f"  ERR: {e}")
        traceback.print_exc()
