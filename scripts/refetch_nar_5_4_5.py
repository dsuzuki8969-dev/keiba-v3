"""5/4-5/5 NAR 全レースを keibabook で再取得 — 馬名・騎手・人気フィールド追加版"""
import sys, os, json, sqlite3, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scraper.keibabook_training import KeibabookClient
from src.scraper.keibabook_result import KeibabookResultScraper

DB = 'data/keiba.db'
RATE = 2.0

kb = KeibabookClient()
if not kb.ensure_login():
    print("[ERR] keibabook login failed", flush=True)
    sys.exit(1)
print("[OK] login", flush=True)

scraper = KeibabookResultScraper(kb)
conn = sqlite3.connect(DB)
c = conn.cursor()

# 5/4 + 5/5 の NAR 全レース
c.execute("""
SELECT DISTINCT race_id, race_date FROM race_log
WHERE is_jra = 0 AND race_date IN ('2026-05-04', '2026-05-05')
ORDER BY race_date DESC, race_id
""")
TARGETS = c.fetchall()
print(f"[開始] 対象: {len(TARGETS)} 件 (推定 {int(len(TARGETS) * RATE / 60)} 分)", flush=True)

fixed = 0
fail = 0
t_start = time.time()

for i, (rid, rdate) in enumerate(TARGETS, 1):
    try:
        time.sleep(RATE)
        result = scraper.fetch_result(netkeiba_race_id=rid, race_date=rdate)
        if not result:
            fail += 1
            continue
        order = result.get('order') or []
        if len(order) < 3:
            fail += 1
            continue
        # 旧データと比較して horse_name 入っているかチェック
        c.execute("UPDATE race_results SET order_json = ? WHERE race_id = ?",
                  (json.dumps(order, ensure_ascii=False), rid))

        # race_log の既存 row 更新 (horse_name 等を追加)
        for h in order:
            try:
                hno = int(h.get('horse_no') or 0)
                if not hno:
                    continue
                c.execute("""
                    UPDATE race_log SET
                        horse_name = COALESCE(NULLIF(?,''), horse_name),
                        jockey_name = COALESCE(NULLIF(?,''), jockey_name),
                        popularity = COALESCE(?, popularity),
                        finish_time_sec = COALESCE(?, finish_time_sec),
                        last_3f_sec = COALESCE(?, last_3f_sec),
                        tansho_odds = COALESCE(?, tansho_odds),
                        win_odds = COALESCE(?, win_odds)
                    WHERE race_id = ? AND horse_no = ?
                """, (
                    h.get('horse_name', ''),
                    h.get('jockey_name', ''),
                    h.get('popularity'),
                    h.get('time_sec') or h.get('finish_time_sec') or None,
                    h.get('last_3f') or h.get('last_3f_sec') or None,
                    h.get('odds') or h.get('win_odds') or None,
                    h.get('odds') or h.get('win_odds') or None,
                    rid, hno
                ))
            except Exception as e:
                pass

        # data/results JSON も更新
        date_key = rdate.replace('-', '')
        json_path = f'data/results/{date_key}_results.json'
        try:
            with open(json_path, encoding='utf-8') as f:
                jd = json.load(f)
            if rid in jd:
                jd[rid]['order'] = order
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(jd, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        fixed += 1
        if i % 20 == 0 or i == len(TARGETS):
            elapsed = int(time.time() - t_start)
            print(f"  [{i}/{len(TARGETS)}] elapsed={elapsed}s fixed={fixed} fail={fail}", flush=True)
            conn.commit()
    except Exception as e:
        fail += 1
        print(f"  [{i}/{len(TARGETS)}] {rid} ERR: {e}", flush=True)

conn.commit()
elapsed = int(time.time() - t_start)
print(f"\n[完了] {len(TARGETS)} 件 / {elapsed}秒 / 修復={fixed} 失敗={fail}")
conn.close()
