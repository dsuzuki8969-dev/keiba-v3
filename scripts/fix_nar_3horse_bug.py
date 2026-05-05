"""
fix_nar_3horse_bug.py — 5/5 マスター激怒指摘の修復スクリプト

scraper 修正 (commit 30d9d99) で 4 着以降の馬が正しく取り込まれるようになった。
このスクリプトは race_log で 5 頭未満 (= 競馬ルール不成立) のレースを抽出し、
NAR 公式から再取得して race_results / race_log を修復する。

netkeiba 不使用 (★★ 累犯 2 回・5/5 厳禁) / NAR 公式のみ使用。
レート制限 2.0 秒/件以上厳守。
"""
import sys, os, json, sqlite3, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scraper.official_nar import OfficialNARScraper
from src.log import get_logger

logger = get_logger(__name__)

DB = 'data/keiba.db'
RATE_LIMIT_SEC = 2.0

# 5 頭未満 NAR レース全件抽出
conn = sqlite3.connect(DB)
c = conn.cursor()
c.execute("""
SELECT race_id, race_date, COUNT(*) as 馬数
FROM race_log
WHERE is_jra = 0
GROUP BY race_id
HAVING COUNT(*) < 5
ORDER BY race_date DESC
""")
TARGETS = c.fetchall()
print(f"[開始] 修復対象 NAR レース: {len(TARGETS)} 件 (推定 {len(TARGETS) * RATE_LIMIT_SEC:.0f}秒 = {len(TARGETS) * RATE_LIMIT_SEC / 60:.1f}分)")

client = OfficialNARScraper()
fixed_count = 0
fail_count = 0
skipped_count = 0

t_start = time.time()
for i, (rid, rdate, nh) in enumerate(TARGETS, 1):
    try:
        time.sleep(RATE_LIMIT_SEC)
        result = client.get_result(rid, rdate)
        if not result:
            print(f"  [{i}/{len(TARGETS)}] {rid} ({rdate}) → 取得失敗")
            fail_count += 1
            continue
        order = result.get('order') or []
        if len(order) <= nh:
            # 改善なし
            skipped_count += 1
            continue
        new_count = len(order)

        # race_results.order_json を UPDATE
        c.execute("""
            UPDATE race_results SET order_json = ?, fetched_at = datetime('now', 'localtime')
            WHERE race_id = ?
        """, (json.dumps(order, ensure_ascii=False), rid))

        # race_log の 3 頭立てデータを削除して再 INSERT (簡易方式 = 一旦既存 INSERT のみ)
        # 実 INSERT は run_results_today.py が分担するため本スクリプトでは race_results のみ更新
        # → 後で再取込時に race_log も自動更新される
        if i % 10 == 0 or i == len(TARGETS):
            elapsed = time.time() - t_start
            print(f"  [{i}/{len(TARGETS)}] race_results UPDATE 中 ({nh} → {new_count} 頭) elapsed={elapsed:.0f}s")

        fixed_count += 1
        conn.commit()
    except Exception as e:
        print(f"  [{i}/{len(TARGETS)}] {rid} ERR: {e}")
        fail_count += 1

elapsed_total = time.time() - t_start
print()
print("=" * 60)
print(f"[完了] 修復対象 {len(TARGETS)} 件 / 経過 {elapsed_total:.0f}秒")
print(f"  race_results UPDATE 成功: {fixed_count} 件")
print(f"  失敗: {fail_count} 件")
print(f"  スキップ (改善なし): {skipped_count} 件")
print()
print("【次工程】 race_log の修復は results_tracker.py の再取込 or run_results_today.py 経由")
print("=" * 60)
conn.close()
