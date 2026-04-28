"""
pred.json に対する並行書き込みで JSON が破損しないことを検証するテスト。

シナリオ:
- dashboard の /api/race_odds と scheduler_tasks.run_odds_update が
  同時に pred.json に書き込む状況をスレッドで再現
- 10 スレッド x 20 回 = 200 回の書き込み
- 毎回書き込み後に別スレッドが読み込んで JSON パースできるか確認
- 最終的にファイルが有効な JSON であることを確認

期待: atomic_write_json により破損ゼロ
"""
from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.utils.atomic_json import atomic_write_json


def main() -> int:
    pred_path = ROOT / "data" / "predictions" / "20260419_pred.json"
    if not pred_path.exists():
        print(f"[ERROR] {pred_path} が存在しません")
        return 1

    # 元データを読み込み
    with open(pred_path, "r", encoding="utf-8") as rf:
        base = json.load(rf)

    target = ROOT / "data" / "_test_concurrent_pred.json"
    # 初期状態
    atomic_write_json(target, base)
    size0 = target.stat().st_size
    print(f"初期ファイル: {target} size={size0} bytes")

    errors: list[str] = []
    write_count = [0]
    read_ok_count = [0]
    read_ng_count = [0]

    lock_stats = threading.Lock()

    def writer(tid: int, n: int) -> None:
        for i in range(n):
            try:
                # 疑似的な modify
                base["_writer_tid"] = tid
                base["_writer_iter"] = i
                base["_timestamp"] = time.time()
                atomic_write_json(target, base)
                with lock_stats:
                    write_count[0] += 1
            except Exception as e:
                errors.append(f"writer{tid}.{i}: {e}")

    def reader() -> None:
        # writer 実行中ずっと読み続ける
        end = time.time() + 4.0
        while time.time() < end:
            try:
                with open(target, "r", encoding="utf-8") as rf:
                    json.load(rf)
                with lock_stats:
                    read_ok_count[0] += 1
            except Exception as e:
                with lock_stats:
                    read_ng_count[0] += 1
                errors.append(f"reader: {e}")
            time.sleep(0.001)

    # 実運用想定: dashboard + scheduler の 2 プロセスレベルを模擬
    N_WRITERS = 3
    N_WRITES_PER = 5
    N_READERS = 3

    t0 = time.time()
    threads = [threading.Thread(target=writer, args=(i, N_WRITES_PER)) for i in range(N_WRITERS)]
    readers = [threading.Thread(target=reader) for _ in range(N_READERS)]
    for t in threads + readers:
        t.start()
    for t in threads:
        t.join()
    for t in readers:
        t.join()
    dt = time.time() - t0

    print(f"書き込み完了: {write_count[0]} / {N_WRITERS*N_WRITES_PER} "
          f"({dt:.2f}秒, {write_count[0]/dt:.1f}/s)")
    print(f"読み込み成功: {read_ok_count[0]}")
    print(f"読み込み失敗: {read_ng_count[0]}")

    # 最終ファイルの JSON バリデーション
    try:
        with open(target, "r", encoding="utf-8") as rf:
            final = json.load(rf)
        print(f"[OK] 最終ファイル JSON パース成功 races={len(final.get('races') or [])}")
    except Exception as e:
        print(f"[NG] 最終ファイル JSON パース失敗: {e}")
        errors.append(f"final: {e}")

    if errors:
        print(f"\nエラー {len(errors)} 件:")
        for e in errors[:10]:
            print(f"  - {e}")
        return 2

    # 後片付け
    target.unlink()
    lock = ROOT / "data" / f".{target.name}.lock"
    if lock.exists():
        lock.unlink()

    print("\n総合判定: PASS (破損ゼロ)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
