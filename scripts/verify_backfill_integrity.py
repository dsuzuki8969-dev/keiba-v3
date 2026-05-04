#!/usr/bin/env python3
"""
Backfill 完了済データの整合性検証スクリプト (T-063)
====================================================
検証項目:
  1. 月別レース数・的中率・回収率の連続性チェック
  2. 印別出現頻度と的中率の整合性チェック
  3. 自信度別 (SS/S/A/B/C/D/E) 的中率・回収率チェック
  4. 三連複払戻 NULL 率チェック (5/2 修復後 <1% を期待)
  5. pred.json 整合性: 勝率 < 連対率 < 複勝率 の順序チェック

出力: 標準出力 + logs/verify_backfill_integrity_YYYYMMDD.json
"""

import sqlite3
import json
import sys
import os
import traceback
from datetime import datetime
from pathlib import Path

# パス設定
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "keiba.db"
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")
OUTPUT_JSON = LOGS_DIR / f"verify_backfill_integrity_{today_str}.json"

# 異常検出閾値
THRESHOLD_ROI_DROP_RATIO = 0.10   # 前月比 ROI が 10% 以下に急落したら警告
THRESHOLD_RACE_CHANGE_RATIO = 0.5  # 前月比レース数が 50% 未満 or 200% 超なら警告
MARK_KIKEN_WIN_MAX = 0.05           # ×印の勝率 5% 未満を確認
MARK_ANA_PLACE_MIN = 0.30          # ☆印の複勝率 30% 超を確認
SANRENPUKU_NULL_MAX = 0.01         # 三連複 NULL 率 1% 未満 (最新 6 ヶ月)
PROB_ORDER_OK_MIN = 0.99           # 勝率<連対率<複勝率が成立するレコード率 99% 以上

# 全体結果
results = {
    "generated_at": datetime.now().isoformat(),
    "db_path": str(DB_PATH),
    "checks": {},
    "anomalies": [],
    "summary": {}
}
anomaly_count = 0


def add_anomaly(category: str, detail: str, expected, actual):
    """異常値を登録"""
    global anomaly_count
    anomaly_count += 1
    entry = {
        "category": category,
        "detail": detail,
        "expected": str(expected),
        "actual": str(actual)
    }
    results["anomalies"].append(entry)
    print(f"  [異常] {category} / {detail}: 期待={expected} 実測={actual}")


def log(msg: str):
    """標準出力に出力"""
    sys.stdout.buffer.write((msg + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


# ============================================================
# 1. 月別レース数・的中率・回収率の連続性チェック
# ============================================================
def check_monthly_continuity(cur):
    log("\n=== [1/5] 月別レース数・的中率・回収率の連続性チェック ===")
    cur.execute("""
        SELECT
            substr(date, 1, 7) as ym,
            COUNT(*) as races,
            SUM(hit_tickets) as hits,
            SUM(total_tickets) as total_tickets,
            SUM(stake) as stake,
            SUM(ret) as ret
        FROM match_results
        WHERE date >= '2024-01-01'
        GROUP BY ym
        ORDER BY ym
    """)
    rows = cur.fetchall()

    monthly = []
    prev_races = None
    prev_roi = None
    anomalies = []

    log(f"{'月':8} {'レース':>6} {'的中率':>8} {'回収率':>8} {'投資':>10} {'回収':>10} {'状態'}")
    log("-" * 72)

    for row in rows:
        ym, races, hits, total_t, stake, ret = row
        hit_rate = round(hits / total_t * 100, 1) if total_t and total_t > 0 else 0
        roi = round(ret / stake * 100, 1) if stake and stake > 0 else 0

        # 異常検出: レース数急変
        race_anomaly = ""
        if prev_races is not None and prev_races > 0:
            ratio = races / prev_races
            if ratio < THRESHOLD_RACE_CHANGE_RATIO or ratio > 2.0:
                race_anomaly = f"[レース数急変 {prev_races}→{races}]"
                anomalies.append({"ym": ym, "type": "race_count_jump", "prev": prev_races, "cur": races})

        # 異常検出: ROI 急落 (前月比 10% 以下)
        roi_anomaly = ""
        if prev_roi is not None and prev_roi > 50:
            if roi > 0 and roi < prev_roi * THRESHOLD_ROI_DROP_RATIO:
                roi_anomaly = f"[ROI急落 {prev_roi}%→{roi}%]"
                anomalies.append({"ym": ym, "type": "roi_drop", "prev_roi": prev_roi, "cur_roi": roi})

        status = race_anomaly + roi_anomaly if (race_anomaly or roi_anomaly) else "OK"
        log(f"{ym:8} {races:>6} {hit_rate:>7.1f}% {roi:>7.1f}% {int(stake):>10,} {int(ret):>10,}  {status}")

        monthly.append({
            "ym": ym, "races": races, "hit_rate": hit_rate,
            "roi": roi, "stake": int(stake), "ret": int(ret)
        })
        prev_races = races
        prev_roi = roi

    for a in anomalies:
        add_anomaly("月別連続性", a["type"], "", str(a))

    results["checks"]["monthly"] = {
        "rows": len(monthly),
        "data": monthly,
        "anomaly_count": len(anomalies)
    }
    log(f"  → 検証月数: {len(monthly)} / 異常: {len(anomalies)} 件")
    return len(anomalies)


# ============================================================
# 2. 印別出現頻度と的中率の整合性チェック
# ============================================================
def check_mark_integrity(cur):
    log("\n=== [2/5] 印別出現頻度と的中率の整合性チェック ===")

    # by_mark_json が空の場合は match_results から直接集計できないため
    # predictions の horses_json を解析して印別の複勝率を集計する
    cur.execute("""
        SELECT p.date, p.race_id, p.horses_json, r.order_json
        FROM predictions p
        JOIN race_results r ON p.race_id = r.race_id
        WHERE r.cancelled = 0
          AND r.order_json IS NOT NULL
          AND p.date >= '2024-01-01'
          AND p.date <= '2026-05-04'
    """)
    rows = cur.fetchall()

    mark_stats = {}
    processed = 0
    errors = 0

    for date, race_id, horses_json_str, order_json_str in rows:
        try:
            horses = json.loads(horses_json_str)
            order = json.loads(order_json_str)
            # order_json: [{"horse_no":X,...}, ...] — 着順リスト
            if not order:
                continue
            # 上位3着の馬番を取得
            top3 = set()
            top2 = set()
            top1 = set()
            for i, h in enumerate(order[:3]):
                no = h.get("horse_no") or h.get("no")
                if no is not None:
                    top3.add(int(no))
                    if i < 2:
                        top2.add(int(no))
                    if i == 0:
                        top1.add(int(no))

            for h in horses:
                mark = h.get("mark", "－") or "－"
                horse_no = h.get("horse_no")
                if horse_no is None:
                    continue
                horse_no = int(horse_no)

                if mark not in mark_stats:
                    mark_stats[mark] = {"count": 0, "win": 0, "place2": 0, "place3": 0}

                mark_stats[mark]["count"] += 1
                if horse_no in top1:
                    mark_stats[mark]["win"] += 1
                if horse_no in top2:
                    mark_stats[mark]["place2"] += 1
                if horse_no in top3:
                    mark_stats[mark]["place3"] += 1

            processed += 1
        except Exception:
            errors += 1

    log(f"{'印':4} {'出現':>8} {'勝率':>8} {'連対率':>8} {'複勝率':>8} {'状態'}")
    log("-" * 60)

    mark_order = ["◉", "◎", "○", "▲", "△", "★", "×", "☆", "－", ""]
    mark_anomalies = []

    for mark in mark_order:
        if mark not in mark_stats:
            continue
        s = mark_stats[mark]
        cnt = s["count"]
        if cnt == 0:
            continue
        win_rate = s["win"] / cnt
        place2_rate = s["place2"] / cnt
        place3_rate = s["place3"] / cnt

        status_parts = []

        # ×印: 勝率 5% 未満を確認
        if mark == "×":
            if win_rate >= MARK_KIKEN_WIN_MAX:
                status_parts.append(f"[×勝率高={win_rate:.1%}]")
                mark_anomalies.append({"mark": mark, "type": "kiken_win_too_high",
                                       "expected": f"<{MARK_KIKEN_WIN_MAX:.0%}", "actual": f"{win_rate:.1%}"})
            else:
                status_parts.append("危険印OK")

        # ☆印: 複勝率 30% 超を確認
        if mark == "☆":
            if place3_rate < MARK_ANA_PLACE_MIN:
                status_parts.append(f"[☆複勝率低={place3_rate:.1%}]")
                mark_anomalies.append({"mark": mark, "type": "ana_place_too_low",
                                       "expected": f">{MARK_ANA_PLACE_MIN:.0%}", "actual": f"{place3_rate:.1%}"})
            else:
                status_parts.append("穴印OK")

        status = " ".join(status_parts) if status_parts else "OK"
        log(f"{mark or '（空）':4} {cnt:>8,} {win_rate:>7.1%} {place2_rate:>7.1%} {place3_rate:>7.1%}  {status}")

    for a in mark_anomalies:
        add_anomaly("印別整合性", a["type"], a["expected"], a["actual"])

    results["checks"]["mark_integrity"] = {
        "processed_races": processed,
        "errors": errors,
        "mark_stats": {k: v for k, v in mark_stats.items()},
        "anomaly_count": len(mark_anomalies)
    }
    log(f"  → 処理レース: {processed} / 解析エラー: {errors} / 異常: {len(mark_anomalies)} 件")
    return len(mark_anomalies)


# ============================================================
# 3. 自信度別 (SS/S/A/B/C/D/E) 的中率・回収率チェック
# ============================================================
def check_confidence_grade(cur):
    log("\n=== [3/5] 自信度別的中率・回収率チェック ===")

    cur.execute("""
        SELECT
            p.confidence as grade,
            COUNT(*) as races,
            SUM(m.hit_tickets) as hits,
            SUM(m.total_tickets) as total_t,
            SUM(m.stake) as stake,
            SUM(m.ret) as ret
        FROM match_results m
        JOIN predictions p ON m.race_id = p.race_id AND m.date = p.date
        WHERE m.date >= '2024-01-01'
        GROUP BY p.confidence
        ORDER BY
            CASE p.confidence
                WHEN 'SS' THEN 1
                WHEN 'S'  THEN 2
                WHEN 'A'  THEN 3
                WHEN 'B'  THEN 4
                WHEN 'C'  THEN 5
                WHEN 'D'  THEN 6
                WHEN 'E'  THEN 7
                ELSE 8
            END
    """)
    rows = cur.fetchall()

    log(f"{'自信度':6} {'レース':>8} {'的中率':>8} {'回収率':>8} {'投資':>12} {'回収':>12}")
    log("-" * 70)

    conf_data = []
    conf_anomalies = []

    # SS/S は A/B より高 ROI を期待 (緩いチェック: SS<A ならフラグ)
    roi_by_grade = {}
    for row in rows:
        grade, races, hits, total_t, stake, ret = row
        hit_rate = round(hits / total_t * 100, 1) if total_t and total_t > 0 else 0
        roi = round(ret / stake * 100, 1) if stake and stake > 0 else 0
        log(f"{str(grade):6} {races:>8,} {hit_rate:>7.1f}% {roi:>7.1f}%  {int(stake):>12,}  {int(ret):>12,}")
        roi_by_grade[grade] = roi
        conf_data.append({"grade": grade, "races": races, "hit_rate": hit_rate,
                          "roi": roi, "stake": int(stake), "ret": int(ret)})

    # SS < A の ROI 逆転チェック
    if "SS" in roi_by_grade and "A" in roi_by_grade:
        if roi_by_grade["SS"] < roi_by_grade["A"] * 0.7:
            conf_anomalies.append({"type": "ss_roi_below_a",
                                   "ss_roi": roi_by_grade["SS"], "a_roi": roi_by_grade["A"]})
            add_anomaly("自信度別", "SS ROI が A の 70% 未満",
                        f"SS≥A*0.7 ({roi_by_grade['A']*0.7:.1f}%)", f"SS={roi_by_grade['SS']:.1f}%")

    results["checks"]["confidence_grade"] = {
        "data": conf_data,
        "anomaly_count": len(conf_anomalies)
    }
    log(f"  → 検証グレード: {len(conf_data)} / 異常: {len(conf_anomalies)} 件")
    return len(conf_anomalies)


# ============================================================
# 4. 三連複払戻 NULL 率チェック (最新 6 ヶ月)
# ============================================================
def check_sanrenpuku_null_rate(cur):
    log("\n=== [4/5] 三連複払戻 NULL 率チェック (最新 6 ヶ月: 2025-11〜) ===")

    # 最新 6 ヶ月を対象 (2025-11 以降)
    cur.execute("""
        SELECT
            substr(date, 1, 7) as ym,
            COUNT(*) as total,
            SUM(CASE WHEN json_extract(payouts_json, '$.三連複') IS NOT NULL THEN 1 ELSE 0 END) as has_sanrenpuku,
            SUM(CASE WHEN json_extract(payouts_json, '$.三連複') IS NULL THEN 1 ELSE 0 END) as null_sanrenpuku
        FROM race_results
        WHERE date >= '2025-11-01'
        GROUP BY ym
        ORDER BY ym
    """)
    rows = cur.fetchall()

    log(f"{'月':8} {'総レース':>8} {'三連複あり':>10} {'NULL':>8} {'NULL率':>8} {'状態'}")
    log("-" * 55)

    sr_anomalies = []
    sr_data = []

    for row in rows:
        ym, total, has_sr, null_sr = row
        null_rate = null_sr / total if total > 0 else 0
        status = "OK"
        if null_rate > SANRENPUKU_NULL_MAX:
            status = f"[NULL率高={null_rate:.1%}]"
            sr_anomalies.append({"ym": ym, "null_rate": null_rate, "null_count": null_sr, "total": total})
        log(f"{ym:8} {total:>8} {has_sr:>10} {null_sr:>8} {null_rate:>7.1%}  {status}")
        sr_data.append({"ym": ym, "total": total, "has_sanrenpuku": has_sr,
                        "null_sanrenpuku": null_sr, "null_rate": round(null_rate, 4)})

    for a in sr_anomalies:
        add_anomaly("三連複NULL率", f"{a['ym']} NULL率高",
                    f"<{SANRENPUKU_NULL_MAX:.0%}", f"{a['null_rate']:.1%}")

    results["checks"]["sanrenpuku_null"] = {
        "target_period": "2025-11以降",
        "data": sr_data,
        "anomaly_count": len(sr_anomalies)
    }
    log(f"  → 検証月数: {len(sr_data)} / 異常: {len(sr_anomalies)} 件")
    return len(sr_anomalies)


# ============================================================
# 5. pred.json 整合性: 勝率 < 連対率 < 複勝率 の順序チェック
# ============================================================
def check_prob_order(cur):
    log("\n=== [5/5] 確率順序チェック (勝率 <= 連対率 <= 複勝率) ===")

    cur.execute("""
        SELECT date, race_id, horses_json
        FROM predictions
        WHERE date >= '2024-01-01'
    """)
    rows = cur.fetchall()

    total_horses = 0
    order_ok = 0
    order_ng = 0
    ng_examples = []

    for date, race_id, horses_json_str in rows:
        try:
            horses = json.loads(horses_json_str)
            for h in horses:
                win = h.get("win_prob")
                place2 = h.get("place2_prob")
                place3 = h.get("place3_prob")

                if win is None or place2 is None or place3 is None:
                    continue

                total_horses += 1
                # 順序: win <= place2 <= place3
                # 少し余裕を持たせて -0.005 の許容範囲
                if win <= place2 + 0.005 and place2 <= place3 + 0.005:
                    order_ok += 1
                else:
                    order_ng += 1
                    if len(ng_examples) < 5:
                        ng_examples.append({
                            "date": date,
                            "race_id": race_id,
                            "horse_no": h.get("horse_no"),
                            "win": round(win, 4),
                            "place2": round(place2, 4),
                            "place3": round(place3, 4)
                        })
        except Exception:
            pass

    ok_rate = order_ok / total_horses if total_horses > 0 else 0
    status = "OK" if ok_rate >= PROB_ORDER_OK_MIN else f"[整合性不足 {ok_rate:.2%}]"
    log(f"  対象馬数: {total_horses:,}")
    log(f"  順序OK: {order_ok:,} ({ok_rate:.2%})")
    log(f"  順序NG: {order_ng:,}")
    log(f"  状態: {status}")

    if ng_examples:
        log("  NGサンプル:")
        for ex in ng_examples:
            log(f"    {ex['date']} {ex['race_id']} 馬番{ex['horse_no']}: "
                f"勝={ex['win']} 連対={ex['place2']} 複勝={ex['place3']}")

    prob_anomalies = []
    if ok_rate < PROB_ORDER_OK_MIN:
        prob_anomalies.append({"ok_rate": ok_rate, "ng_count": order_ng})
        add_anomaly("確率順序", "win<=place2<=place3 不成立率過大",
                    f">={PROB_ORDER_OK_MIN:.0%}", f"{ok_rate:.2%}")

    results["checks"]["prob_order"] = {
        "total_horses": total_horses,
        "order_ok": order_ok,
        "order_ng": order_ng,
        "ok_rate": round(ok_rate, 4),
        "ng_examples": ng_examples,
        "anomaly_count": len(prob_anomalies)
    }
    log(f"  → 対象馬: {total_horses:,} / 異常: {len(prob_anomalies)} 件")
    return len(prob_anomalies)


# ============================================================
# メイン実行
# ============================================================
def main():
    log("=" * 72)
    log("Backfill 整合性検証 (T-063)")
    log(f"DB: {DB_PATH}")
    log(f"日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 72)

    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
    except Exception as e:
        log(f"[エラー] DB接続失敗: {e}")
        sys.exit(1)

    try:
        a1 = check_monthly_continuity(cur)
        a2 = check_mark_integrity(cur)
        a3 = check_confidence_grade(cur)
        a4 = check_sanrenpuku_null_rate(cur)
        a5 = check_prob_order(cur)
    except Exception:
        log(f"[エラー] 検証中に例外:\n{traceback.format_exc()}")
        results["error"] = traceback.format_exc()
        conn.close()
        # エラーでも途中結果をJSON保存
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        sys.exit(1)
    finally:
        conn.close()

    total_anomalies = a1 + a2 + a3 + a4 + a5

    # ============================================================
    # サマリ出力
    # ============================================================
    log("\n" + "=" * 72)
    log("検証サマリ")
    log("=" * 72)
    log(f"  月別連続性     : {'OK' if a1 == 0 else f'異常 {a1} 件'}")
    log(f"  印別整合性     : {'OK' if a2 == 0 else f'異常 {a2} 件'}")
    log(f"  自信度別整合性 : {'OK' if a3 == 0 else f'異常 {a3} 件'}")
    log(f"  三連複NULL率   : {'OK' if a4 == 0 else f'異常 {a4} 件'}")
    log(f"  確率順序       : {'OK' if a5 == 0 else f'異常 {a5} 件'}")
    log("-" * 30)
    log(f"  総異常件数     : {total_anomalies} 件")

    if total_anomalies == 0:
        log("\n[PASS] 全期間整合性 OK — Backfill データに重大な問題は検出されませんでした")
    else:
        log(f"\n[WARN] 異常 {total_anomalies} 件検出 — 詳細は上記ログまたは {OUTPUT_JSON} を確認してください")

    results["summary"] = {
        "total_anomalies": total_anomalies,
        "monthly_anomalies": a1,
        "mark_anomalies": a2,
        "confidence_anomalies": a3,
        "sanrenpuku_null_anomalies": a4,
        "prob_order_anomalies": a5,
        "status": "PASS" if total_anomalies == 0 else "WARN"
    }

    # JSON 保存
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    log(f"\nJSON出力: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
