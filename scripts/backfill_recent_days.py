"""直近日の不完全な results.json を強制再取得
マスター指示 2026-04-22: 04-16〜04-21 の結果が pred.json 件数に対し大幅欠落。
  全レース終了済みなのに結果が反映されていないので全件取り直し。

対象日: 2026-04-16 ~ 2026-04-22
動作: 既存 results.json を削除 → fetch_actual_results で公式優先取得 → netkeiba フォールバック
"""
from __future__ import annotations
import io, json, os, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, ".")

from config.settings import RESULTS_DIR, PREDICTIONS_DIR
from src.results_tracker import fetch_actual_results
from src.scraper.netkeiba import NetkeibaClient
from src.scraper.official_odds import OfficialOddsScraper

TARGET_DATES = [
    "2026-04-16", "2026-04-17", "2026-04-18",
    "2026-04-19", "2026-04-20", "2026-04-21",
]  # 04-22 は別プロセスで処理中


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        Path("tmp").mkdir(parents=True, exist_ok=True)
        with open("tmp/backfill_recent.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def is_incomplete(date: str) -> tuple:
    """(レース数, 完全な結果件数, 三連単付き件数, pred件数) を返す"""
    date_key = date.replace("-", "")
    rfp = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
    pfp = os.path.join(PREDICTIONS_DIR, f"{date_key}_pred.json")
    pred_count = 0
    if os.path.exists(pfp):
        try:
            p = json.load(open(pfp, encoding="utf-8"))
            pred_count = len(p.get("races", []))
        except Exception:
            pass
    if not os.path.exists(rfp):
        return (0, 0, 0, pred_count)
    try:
        r = json.load(open(rfp, encoding="utf-8"))
    except Exception:
        return (0, 0, 0, pred_count)
    with_order = sum(1 for v in r.values() if isinstance(v, dict) and v.get("order"))
    with_san = sum(
        1 for v in r.values()
        if isinstance(v, dict) and "三連単" in (v.get("payouts") or {})
    )
    return (len(r), with_order, with_san, pred_count)


def main() -> None:
    t0 = time.time()
    log(f"=== 直近日 backfill 開始 対象 {len(TARGET_DATES)} 日 ===")

    client = NetkeibaClient(no_cache=True)
    official = OfficialOddsScraper()

    for i, date in enumerate(TARGET_DATES):
        date_key = date.replace("-", "")
        rfp = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
        before = is_incomplete(date)
        log(f"[{i+1}/{len(TARGET_DATES)}] {date} 取得前: "
            f"レース{before[0]} 結果{before[1]} 三連単{before[2]} / pred{before[3]}")

        # 既存ファイル削除（強制再取得）
        if os.path.exists(rfp):
            os.remove(rfp)
            log(f"  既存 {rfp} を削除")

        t_start = time.time()
        try:
            result = fetch_actual_results(date, client, official_scraper=official)
        except Exception as e:
            log(f"  ERR {date}: {type(e).__name__}: {e}")
            continue

        elapsed = time.time() - t_start
        after = is_incomplete(date)
        log(f"  {date} 取得後: "
            f"レース{after[0]} 結果{after[1]} 三連単{after[2]} / pred{after[3]} "
            f"({elapsed:.0f}秒)")

    # 集計キャッシュ無効化
    try:
        from src.results_tracker import invalidate_aggregate_cache
        invalidate_aggregate_cache()
        log("集計キャッシュクリア完了")
    except Exception as e:
        log(f"キャッシュクリア失敗: {e}")

    log(f"=== 全完了 総所要 {(time.time()-t0)/60:.1f}分 ===")


if __name__ == "__main__":
    main()
