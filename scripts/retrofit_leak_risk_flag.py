"""
既存 pred.json 全件に学習リークリスクフラグをレトロフィット (M-1 機能4)

使い方:
  python scripts/retrofit_leak_risk_flag.py             # 全件対象 (ドライラン)
  python scripts/retrofit_leak_risk_flag.py --execute   # 実際に書き込む
  python scripts/retrofit_leak_risk_flag.py --start 2024-01-01 --end 2025-12-31 --execute

処理内容:
  1. data/predictions/*_pred.json を走査
  2. "generated_at" フィールドがあればその値を使用
     なければファイルの mtime を fallback として使用
  3. race_date と generated_at の差分から _leak_risk_days / _leak_risk_flag を計算
  4. 既に付与済みのファイルはスキップ (--force で強制上書き)

判定ロジック:
  _leak_risk_days <= 0 → "OK"     (race 当日 or 前日生成 = 正常運用)
  _leak_risk_days <= 3 → "WARN"   (race 直後 3日以内 = 補完再生成、許容範囲)
  _leak_risk_days >  3 → "DANGER" (4日以上後の生成 = 学習リーク疑い濃厚)
"""

import argparse
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from config.settings import (
    LEAK_RISK_DAYS_DANGER,
    LEAK_RISK_DAYS_WARN,
    PREDICTIONS_DIR,
)
from src.log import get_logger

logger = get_logger(__name__)


def calc_leak_risk(race_date_str: str, generated_at_str: str | None, mtime: float | None) -> tuple[int, str]:
    """
    race_date と generated_at (なければ mtime) から (_leak_risk_days, _leak_risk_flag) を返す。
    計算不能な場合は (-999, "UNKNOWN") を返す。
    """
    try:
        race_dt = datetime.date.fromisoformat(race_date_str)
    except (ValueError, TypeError):
        return -999, "UNKNOWN"

    if generated_at_str:
        try:
            gen_dt = datetime.datetime.fromisoformat(generated_at_str)
        except (ValueError, TypeError):
            gen_dt = None
    else:
        gen_dt = None

    # fallback: ファイル mtime
    if gen_dt is None and mtime is not None:
        gen_dt = datetime.datetime.fromtimestamp(mtime)

    if gen_dt is None:
        return -999, "UNKNOWN"

    leak_days = (gen_dt.date() - race_dt).days
    if leak_days <= 0:
        flag = "OK"
    elif leak_days <= LEAK_RISK_DAYS_WARN:
        flag = "WARN"
    else:
        flag = "DANGER"

    return leak_days, flag


def process_file(fpath: str, execute: bool, force: bool) -> dict:
    """
    1ファイルを処理してリークフラグを付与する。
    戻り値: {"status": "skipped"|"dry_run"|"updated"|"error", "flag": str, "days": int}
    """
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error("JSON 読み込み失敗: %s — %s", fpath, e)
        return {"status": "error", "flag": "UNKNOWN", "days": -999}

    # 既に付与済みかチェック
    if not force and "_leak_risk_flag" in data:
        return {"status": "skipped", "flag": data["_leak_risk_flag"], "days": data.get("_leak_risk_days", -999)}

    race_date_str = data.get("date") or data.get("race_date", "")
    generated_at_str = data.get("generated_at")
    mtime = os.path.getmtime(fpath)

    leak_days, flag = calc_leak_risk(race_date_str, generated_at_str, mtime)

    if not execute:
        # ドライラン: ファイルは書き換えない
        return {"status": "dry_run", "flag": flag, "days": leak_days}

    # 実際に書き込む
    data["_leak_risk_days"] = leak_days
    data["_leak_risk_flag"] = flag
    # generated_at が未設定の場合は mtime を使って補完
    if not generated_at_str and mtime is not None:
        data["generated_at"] = datetime.datetime.fromtimestamp(mtime).astimezone().isoformat()
        data["_generated_at_source"] = "mtime_fallback"  # fallback 使用を明記

    try:
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return {"status": "updated", "flag": flag, "days": leak_days}
    except Exception as e:
        logger.error("JSON 書き込み失敗: %s — %s", fpath, e)
        return {"status": "error", "flag": flag, "days": leak_days}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="既存 pred.json に学習リークリスクフラグをレトロフィット (M-1 機能4)",
    )
    parser.add_argument("--start", default="", help="対象開始日 (YYYY-MM-DD, 省略=全件)")
    parser.add_argument("--end", default="", help="対象終了日 (YYYY-MM-DD, 省略=全件)")
    parser.add_argument("--execute", action="store_true", help="実際にファイルを書き換える (省略時はドライラン)")
    parser.add_argument("--force", action="store_true", help="既に _leak_risk_flag 付与済みファイルも強制上書き")
    args = parser.parse_args()

    if not os.path.isdir(PREDICTIONS_DIR):
        print(f"PREDICTIONS_DIR が見つかりません: {PREDICTIONS_DIR}")
        sys.exit(1)

    mode = "実行モード" if args.execute else "ドライランモード (--execute で実際に書き込み)"
    # UTF-8 安全 print (Windows cp932 環境での文字化けを回避)
    def _pr(msg: str = "") -> None:
        try:
            sys.stdout.buffer.write((msg + "\n").encode("utf-8", errors="replace"))
            sys.stdout.buffer.flush()
        except AttributeError:
            print(msg)

    _pr(f"=== retrofit_leak_risk_flag.py -- {mode} ===")
    _pr(f"対象ディレクトリ: {PREDICTIONS_DIR}")
    if args.start or args.end:
        _pr(f"対象範囲: {args.start or '(先頭)'} から {args.end or '(末尾)'}")
    _pr()

    # 対象ファイルを収集
    files = sorted(
        f for f in os.listdir(PREDICTIONS_DIR)
        if f.endswith("_pred.json") and "_backup" not in f and "_prev" not in f
    )

    # 日付範囲フィルタ
    if args.start or args.end:
        filtered = []
        for fname in files:
            dstr = fname[:8]
            try:
                d = datetime.date(int(dstr[:4]), int(dstr[4:6]), int(dstr[6:8]))
                iso = d.isoformat()
            except ValueError:
                continue
            if args.start and iso < args.start:
                continue
            if args.end and iso > args.end:
                continue
            filtered.append(fname)
        files = filtered

    total = len(files)
    _pr(f"対象ファイル数: {total}")
    if total == 0:
        _pr("対象なし")
        return

    # 統計カウンタ
    counts = {"updated": 0, "dry_run": 0, "skipped": 0, "error": 0}
    flag_counts = {"OK": 0, "WARN": 0, "DANGER": 0, "UNKNOWN": 0}

    for i, fname in enumerate(files, 1):
        fpath = os.path.join(PREDICTIONS_DIR, fname)
        result = process_file(fpath, execute=args.execute, force=args.force)
        counts[result["status"]] = counts.get(result["status"], 0) + 1
        flag_counts[result["flag"]] = flag_counts.get(result["flag"], 0) + 1

        # DANGER ファイルを明示表示
        if result["flag"] == "DANGER":
            _pr(f"  [DANGER] {fname}  (リーク={result['days']}日)  status={result['status']}")
        elif i % 50 == 0:
            # 50件ごとに進捗表示
            pct = i / total * 100
            _pr(f"  [{i}/{total}] {pct:.0f}%  DANGER={flag_counts['DANGER']}件 (処理済)")

    _pr()
    _pr("=== 完了 ===")
    _pr(f"  処理結果: updated={counts['updated']}, dry_run={counts['dry_run']}, "
        f"skipped={counts['skipped']}, error={counts['error']}")
    _pr(f"  フラグ分布: OK={flag_counts['OK']}, WARN={flag_counts['WARN']}, "
        f"DANGER={flag_counts['DANGER']}, UNKNOWN={flag_counts['UNKNOWN']}")

    if flag_counts["DANGER"] > 0:
        _pr()
        _pr(f"[WARNING] DANGER ファイルが {flag_counts['DANGER']} 件あります。")
        _pr("  これらは結果学習済モデルで後追い予想されており、本番運用評価には使用禁止です。")
        _pr("  詳細: memory/feedback_production_vs_wf_pred_distinction.md")

    if not args.execute:
        _pr()
        _pr("ドライランのみ実行 -- 実際に書き込むには --execute を付けてください")


if __name__ == "__main__":
    main()
