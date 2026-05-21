# -*- coding: utf-8 -*-
"""壊れた results.json エントリを再取得して修復するスクリプト

3頭バグにより results.json の order が3頭分しかないレースを
keiba.go.jp/netkeiba から再取得して正しいデータに更新する。
"""
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import RESULTS_DIR


def fix_broken_results():
    """壊れた results.json エントリを特定して修復"""
    # 対象: results.json で order <= 3 だが payouts に多頭数の情報があるレース
    broken = []
    
    for fname in sorted(os.listdir(RESULTS_DIR)):
        if not fname.endswith("_results.json"):
            continue
        fpath = os.path.join(RESULTS_DIR, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        for race_id, result in data.items():
            order = result.get("order", [])
            if len(order) <= 3 and len(order) > 0:
                # payouts の馬番をチェック — 3頭以上いるか
                payouts = result.get("payouts", {})
                max_horse_no = max(
                    (int(o.get("horse_no", 0)) for o in order), default=0
                )
                if max_horse_no > 3:
                    # 馬番が3より大きい → 実際は多頭数レース
                    broken.append({
                        "file": fpath,
                        "date": fname.replace("_results.json", ""),
                        "race_id": race_id,
                        "order_count": len(order),
                        "max_horse_no": max_horse_no,
                    })
    
    if not broken:
        print("✅ 壊れた results.json エントリはありません")
        return
    
    print(f"❌ 壊れた results.json エントリ: {len(broken)}件")
    for b in broken:
        print(f"  {b['date']} {b['race_id']}: order={b['order_count']}頭, "
              f"max_horse_no={b['max_horse_no']}")
    
    # 修復: fetch_single_race_result を使って再取得
    print()
    print("=== 修復開始 ===")
    
    try:
        from src.results_tracker import fetch_single_race_result
        from src.scraper.netkeiba import NetkeibaScraper
        
        client = NetkeibaScraper()
        
        for b in broken:
            date_str = b["date"]
            race_id = b["race_id"]
            date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            print(f"  再取得: {date_fmt} {race_id}...")
            
            result = fetch_single_race_result(
                date=date_fmt,
                race_id=race_id,
                client=client,
            )
            
            if result and len(result.get("order", [])) > 3:
                # results.json を更新
                with open(b["file"], "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # 既存の payouts は保持、order を更新
                old_payouts = data.get(race_id, {}).get("payouts", {})
                data[race_id] = result
                if not result.get("payouts") and old_payouts:
                    data[race_id]["payouts"] = old_payouts
                
                with open(b["file"], "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                print(f"    ✅ 修復完了: {len(result['order'])}頭")
            else:
                order_cnt = len(result.get("order", [])) if result else 0
                print(f"    ⚠ 再取得結果: {order_cnt}頭 (改善なし)")
            
            time.sleep(2.5)  # レート制限
    
    except Exception as e:
        print(f"  ❌ エラー: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    fix_broken_results()
