"""2026-02, 2026-03, 2026-04 の results.json 欠損/不完全日を全て backfill
マスター指示 2026-04-22: 「全部しっかり仕上げとけ」— scheduled tasks が失敗して
  多くの日で結果が不完全。全部掃除する。

判定: with_order < pred_races * 0.8 を「不完全」として再取得対象にする
"""
from __future__ import annotations
import io, json, os, sys, time
from datetime import date, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, ".")

from config.settings import RESULTS_DIR, PREDICTIONS_DIR
from src.results_tracker import fetch_actual_results
from src.scraper.netkeiba import NetkeibaClient
from src.scraper.official_odds import OfficialOddsScraper

INCOMPLETE_THRESHOLD = 0.80  # with_order/pred_races がこれ未満なら不完全
SCAN_FROM = date(2026, 2, 1)
SCAN_TO = date(2026, 4, 21)  # 04-22 / 04-16~04-21 は別プロセスで処理中


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        Path("tmp").mkdir(parents=True, exist_ok=True)
        with open("tmp/backfill_2026_gaps.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def check_complete(date_iso: str) -> tuple:
    """(完全, with_order, pred_races) を返す"""
    dk = date_iso.replace("-", "")
    rfp = os.path.join(RESULTS_DIR, f"{dk}_results.json")
    pfp = os.path.join(PREDICTIONS_DIR, f"{dk}_pred.json")
    if not os.path.exists(pfp):
        return (True, 0, 0)  # pred 無しはスキップ
    try:
        p = json.load(open(pfp, encoding="utf-8"))
        pred_cnt = len(p.get("races", []))
    except Exception:
        return (True, 0, 0)
    if pred_cnt == 0:
        return (True, 0, 0)
    if not os.path.exists(rfp):
        return (False, 0, pred_cnt)
    try:
        r = json.load(open(rfp, encoding="utf-8"))
    except Exception:
        return (False, 0, pred_cnt)
    wo = sum(1 for v in r.values() if isinstance(v, dict) and v.get("order"))
    complete = wo >= pred_cnt * INCOMPLETE_THRESHOLD
    return (complete, wo, pred_cnt)


def main() -> None:
    t0 = time.time()
    # スキャンして不完全な日付を収集
    targets = []
    total_days = (SCAN_TO - SCAN_FROM).days + 1
    for i in range(total_days):
        d = SCAN_FROM + timedelta(days=i)
        ok, wo, pred_cnt = check_complete(d.isoformat())
        if not ok and pred_cnt > 0:
            targets.append((d.isoformat(), wo, pred_cnt))

    log(f"=== 不完全な日 {len(targets)} 件を backfill ===")
    for d, wo, pred in targets:
        log(f"  {d}: order={wo}/{pred}")
    log("")

    if not targets:
        log("backfill 対象なし")
        return

    client = NetkeibaClient(no_cache=True)
    official = OfficialOddsScraper()

    for i, (date_iso, wo, pred_cnt) in enumerate(targets):
        dk = date_iso.replace("-", "")
        rfp = os.path.join(RESULTS_DIR, f"{dk}_results.json")

        log(f"[{i+1}/{len(targets)}] {date_iso} 取得開始 (既存{wo}/{pred_cnt})")
        if os.path.exists(rfp):
            os.remove(rfp)

        t_start = time.time()
        try:
            result = fetch_actual_results(
                date_iso, client, official_scraper=official
            )
        except Exception as e:
            log(f"  ERR: {type(e).__name__}: {e}")
            continue

        after_ok, after_wo, _ = check_complete(date_iso)
        log(f"  完了: {after_wo}/{pred_cnt} ({time.time()-t_start:.0f}秒)")

    try:
        from src.results_tracker import invalidate_aggregate_cache
        invalidate_aggregate_cache()
        log("集計キャッシュクリア完了")
    except Exception as e:
        log(f"キャッシュクリア失敗: {e}")

    log(f"=== 全完了 総所要 {(time.time()-t0)/60:.1f}分 ===")


if __name__ == "__main__":
    main()
