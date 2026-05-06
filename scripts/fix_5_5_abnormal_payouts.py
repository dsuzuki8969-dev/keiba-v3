"""5/5 名古屋5R + 金沢9R の異常 payouts を keibabook で再取得して修復"""
import sys, os, json, sqlite3
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# import 順序を厳守 (training を先にロード)
from src.scraper import keibabook_training
from src.scraper.keibabook_result import KeibabookResultScraper
from src.scraper.auth import KeibabookClient

kb = KeibabookClient()
if not kb.ensure_login():
    print("[エラー] keibabook ログイン失敗")
    sys.exit(1)

scraper = KeibabookResultScraper(kb)
conn = sqlite3.connect('data/keiba.db')
c = conn.cursor()

TARGETS = [
    ('202648050505', '2026-05-05'),
    ('202646050509', '2026-05-05'),
]

for rid, rdate in TARGETS:
    print(f"\n=== {rid} 再取得 ===")
    try:
        result = scraper.fetch_result(race_id=rid, race_date=rdate)
        if not result:
            print("  → 取得失敗")
            continue
        payouts = result.get('payouts', {})
        print(f"  payouts: {json.dumps(payouts, ensure_ascii=False)[:300]}")
        c.execute(
            "UPDATE race_results SET payouts_json = ? WHERE race_id = ?",
            (json.dumps(payouts, ensure_ascii=False), rid)
        )
        print(f"  DB UPDATE rowcount={c.rowcount}")
    except Exception as e:
        import traceback
        print(f"  ERR: {e}")
        traceback.print_exc()

conn.commit()
conn.close()
print("\n=== 完了 ===")
