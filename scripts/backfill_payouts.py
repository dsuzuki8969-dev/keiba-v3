"""2024-01～2024-11 の不完全 payouts を公式/netkeiba で再取得
マスター指示 2026-04-22: 三連単F チャートを 2024-01 起点にするため、
  過去 results.json の payouts に 三連単・馬単・三連複・ワイド・枠連を追加する。

設計:
  1. 配慮したレート: 公式 3秒 + netkeiba 6秒 のシリアル走査
  2. 中断再開: tmp/backfill_checkpoint.txt に進捗保存
  3. プログレスバー: 残り時間 ETA 表示
  4. 公式成功なら netkeiba アクセス不要（負荷最小化）
  5. 失敗は skip 記録して次へ進む

対象判定: payouts に 三連単 が無い race_id
"""
from __future__ import annotations
import io, json, os, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, ".")

from config.settings import RESULTS_DIR, PREDICTIONS_DIR
from src.results_tracker import fetch_single_race_result, load_prediction
from src.scraper.netkeiba import NetkeibaClient

OFFICIAL_INTERVAL = 3.0   # 公式連続リクエストの間隔（秒）
NETKEIBA_INTERVAL = 2.5   # netkeiba フォールバック時の間隔（秒、礼儀あるスクレイピング）
CHECKPOINT_FILE = Path("tmp/backfill_checkpoint.json")
LOG_FILE = Path("tmp/backfill_payouts.log")


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {"done_race_ids": [], "skip_race_ids": []}
    return {"done_race_ids": [], "skip_race_ids": []}


def save_checkpoint(cp: dict) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        CHECKPOINT_FILE.write_text(
            json.dumps(cp, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
    except Exception:
        pass


def collect_incomplete_races(date_from: str = "20240101", date_to: str = "20241130") -> list:
    """三連単が欠ける race_id の一覧を返す。新→古の順で返す。"""
    incomplete = []
    rdir = Path(RESULTS_DIR)
    for fp in sorted(rdir.glob("*_results.json")):
        date_str = fp.name[:8]
        if not date_str.isdigit():
            continue
        if date_str < date_from or date_str > date_to:
            continue
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        # pred.json も確認（date が予想対象で、かつ pred が存在するもの）
        pred_path = Path(PREDICTIONS_DIR) / f"{date_str}_pred.json"
        if not pred_path.exists():
            continue
        for rid, entry in data.items():
            if not isinstance(entry, dict):
                continue
            payouts = entry.get("payouts", {})
            if not isinstance(payouts, dict):
                continue
            # 三連単 があればスキップ
            if "三連単" in payouts:
                continue
            # order が無いレース（中止/取消）もスキップ
            if not entry.get("order"):
                continue
            iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            incomplete.append((iso_date, rid))
    return incomplete


def clear_cached_entry(date: str, race_id: str) -> bool:
    """対象 race_id のキャッシュエントリを削除して再取得を強制する。"""
    fpath = Path(RESULTS_DIR) / f"{date.replace('-', '')}_results.json"
    if not fpath.exists():
        return False
    try:
        data = json.loads(fpath.read_text(encoding="utf-8"))
    except Exception:
        return False
    if race_id not in data:
        return False
    # 既存 order を保持しつつ、payouts のみ消去して再取得を促す
    # （fetch_single_race_result は order 有ればスキップする仕様なので、entry 自体を削除）
    del data[race_id]
    try:
        fpath.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return True
    except Exception:
        return False


def main():
    t0 = time.time()

    # 公式スクレイパ準備
    try:
        from src.scraper.official_odds import OfficialOddsScraper
        official = OfficialOddsScraper()
    except Exception as e:
        log(f"公式スクレイパ初期化失敗: {e}")
        official = None

    netkeiba = NetkeibaClient(no_cache=True)
    # リクエスト間隔を調整（netkeiba）
    if hasattr(netkeiba, "request_interval"):
        netkeiba.request_interval = NETKEIBA_INTERVAL

    cp = load_checkpoint()
    done_set = set(cp.get("done_race_ids", []))
    skip_set = set(cp.get("skip_race_ids", []))

    log("対象レース集計中...")
    incomplete = collect_incomplete_races()
    log(f"三連単未取得レース: {len(incomplete)}件")
    todo = [x for x in incomplete if x[1] not in done_set and x[1] not in skip_set]
    log(f"チェックポイント除外後: {len(todo)}件")

    if not todo:
        log("全件完了済み")
        return

    # 進捗バー
    total = len(todo)
    ok_count = 0
    skip_count = 0
    fail_count = 0

    for i, (date, rid) in enumerate(todo):
        # キャッシュエントリを削除して再取得を強制
        clear_cached_entry(date, rid)

        try:
            result = fetch_single_race_result(
                date, rid, netkeiba,
                official_scraper=official,
            )
            payouts = result.get("payouts", {}) if result else {}
            if payouts and "三連単" in payouts:
                ok_count += 1
                done_set.add(rid)
            else:
                # 三連単が取れなかった → 古すぎて公式にもない可能性
                skip_count += 1
                skip_set.add(rid)
        except Exception as e:
            fail_count += 1
            log(f"  ERR {date} {rid}: {e}")

        # 100件ごとに checkpoint 保存
        if (i + 1) % 25 == 0:
            cp["done_race_ids"] = list(done_set)
            cp["skip_race_ids"] = list(skip_set)
            save_checkpoint(cp)

        # プログレス表示（10件ごと）
        if (i + 1) % 10 == 0 or i == total - 1:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta_sec = (total - i - 1) / rate if rate > 0 else 0
            pct = (i + 1) / total * 100
            bar_len = 30
            filled = int(bar_len * (i + 1) / total)
            bar = "#" * filled + "-" * (bar_len - filled)
            log(
                f"[{bar}] {pct:5.1f}% {i+1}/{total} "
                f"OK={ok_count} SKIP={skip_count} FAIL={fail_count} "
                f"経過={elapsed/60:.1f}分 残={eta_sec/60:.1f}分"
            )

        # 公式 / netkeiba それぞれ内部で間隔制御しているため追加 sleep 不要

    # 最終 checkpoint
    cp["done_race_ids"] = list(done_set)
    cp["skip_race_ids"] = list(skip_set)
    save_checkpoint(cp)

    log("=" * 60)
    log(f"完了: 総所要 {(time.time()-t0)/60:.1f}分")
    log(f"  OK (三連単取得): {ok_count}")
    log(f"  SKIP (未取得OK): {skip_count}")
    log(f"  FAIL: {fail_count}")


if __name__ == "__main__":
    main()
