"""
pred.json に各レースの TOP10 オッズ（馬連/馬単/三連複/三連単）を注入する。

JRA: _get_jra_exotic_odds（CNAME抽出）→ JRAマトリクスパーサー
NAR: keiba.go.jp の OddsUmLenFuku / OddsUmLenTan / Odds3LenFuku / Odds3LenTan
     → NARテキストパーサー（2026-04 対応）
ばんえい: スキップ

使用例:
    python scripts/fetch_all_odds.py --date 2026-04-19
    python scripts/fetch_all_odds.py --date 2026-04-19 --only-jra
    python scripts/fetch_all_odds.py --pred-path data/predictions/20260419_pred.json

出力: 対象 pred.json に `top10_odds` キーを各レースに追加して上書き保存。
構造:
    race["top10_odds"] = {
        "umaren":     [{"combo": [m1, m2], "odds": f}, ... 最大10件],
        "umatan":     [{"combo": [m1, m2], "odds": f}, ... 最大10件],
        "sanrenpuku": [{"combo": [m1, m2, m3], "odds": f}, ... 最大10件],
        "sanrentan":  [{"combo": [m1, m2, m3], "odds": f}, ... 最大10件]
    }
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# プロジェクトルートをパスへ
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.scraper.official_odds import OfficialOddsScraper  # noqa: E402

# ----------------------------------------------------------------------------
# ユーティリティ
# ----------------------------------------------------------------------------

def _fmt_hhmmss(sec: float) -> str:
    """秒 → HH:MM:SS"""
    if sec < 0 or sec != sec:  # NaN対策
        sec = 0
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _progress_bar(cur: int, total: int, width: int = 20) -> str:
    pct = (cur / total) if total > 0 else 0.0
    filled = int(pct * width)
    return "[" + "■" * filled + "□" * (width - filled) + "]"


def _print_progress(label: str, cur: int, total: int, t0: float) -> None:
    """経過/残り/XX.X% を標準出力に出力（CLAUDE.md プログレスバールール準拠）"""
    elapsed = time.time() - t0
    rate = cur / elapsed if elapsed > 0 else 0
    remain = (total - cur) / rate if rate > 0 else 0
    pct = (cur / total * 100) if total > 0 else 0
    bar = _progress_bar(cur, total)
    msg = (
        f"{label} {bar} {cur}/{total} "
        f"{pct:5.1f}% "
        f"経過 {_fmt_hhmmss(elapsed)} / "
        f"残り {_fmt_hhmmss(remain)}"
    )
    print(msg, flush=True)


def _top_n(d: Dict, n: int) -> List[Dict]:
    """dict (combo → odds) からオッズ昇順 TOP N を返す"""
    items = [(k, v) for k, v in d.items() if isinstance(v, (int, float)) and v > 0]
    items.sort(key=lambda kv: kv[1])
    result = []
    for combo, odds in items[:n]:
        if isinstance(combo, tuple):
            combo_list = [int(x) for x in combo]
        else:
            combo_list = [int(combo)]
        result.append({"combo": combo_list, "odds": round(float(odds), 1)})
    return result


# ----------------------------------------------------------------------------
# メイン
# ----------------------------------------------------------------------------

def fetch_one_race(
    scraper: OfficialOddsScraper,
    race_id: str,
    top_n: int = 10,
) -> Dict[str, List[Dict]]:
    """1 レース分の TOP10 オッズを取得"""
    out: Dict[str, List[Dict]] = {}
    funcs = [
        ("umaren",     scraper.get_umaren_odds),
        ("umatan",     scraper.get_umatan_odds),
        ("sanrenpuku", scraper.get_sanrenpuku_odds),
        ("sanrentan",  scraper.get_sanrentan_odds),
    ]
    for name, fn in funcs:
        try:
            raw = fn(race_id)
        except Exception as e:
            print(f"    !! {name} 取得失敗: {e}", flush=True)
            raw = {}
        out[name] = _top_n(raw or {}, top_n)
    return out


def resolve_pred_path(args: argparse.Namespace) -> Path:
    if args.pred_path:
        return Path(args.pred_path)
    # --date YYYY-MM-DD
    if args.date:
        d = datetime.strptime(args.date, "%Y-%m-%d").strftime("%Y%m%d")
        return ROOT / "data" / "predictions" / f"{d}_pred.json"
    raise SystemExit("--date または --pred-path を指定してください")


def main() -> int:
    ap = argparse.ArgumentParser(description="pred.json に TOP10 オッズを注入")
    ap.add_argument("--date", type=str, default="", help="対象日 (YYYY-MM-DD)")
    ap.add_argument("--pred-path", type=str, default="", help="pred.json の直接指定")
    ap.add_argument("--top", type=int, default=10, help="取得件数 (default: 10)")
    ap.add_argument(
        "--only-jra", action="store_true", help="JRA レースのみ処理（NAR はスキップ）"
    )
    ap.add_argument(
        "--skip-banei", action="store_true", default=True, help="ばんえい は常にスキップ"
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="保存しない（疎通確認用）"
    )
    args = ap.parse_args()

    pred_path = resolve_pred_path(args)
    if not pred_path.exists():
        print(f"[ERROR] pred.json が存在しません: {pred_path}")
        return 1

    print("=" * 72)
    print(f"TOP{args.top} オッズ取得スクリプト (馬連 / 馬単 / 三連複 / 三連単)")
    print(f"対象: {pred_path}")
    print("=" * 72)

    with open(pred_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    races: List[dict] = data.get("races") or []
    if not races:
        print("[WARN] races が空です")
        return 0

    # 処理対象を絞る
    targets: List[Tuple[int, dict]] = []
    for idx, r in enumerate(races):
        if r.get("is_banei"):
            continue
        if args.only_jra and not r.get("is_jra"):
            continue
        rid = r.get("race_id") or ""
        if not rid or len(rid) < 10:
            continue
        targets.append((idx, r))

    total = len(targets)
    print(f"処理対象: {total} レース (全 {len(races)} 中)")
    if total == 0:
        print("[WARN] 処理対象なし")
        return 0

    scraper = OfficialOddsScraper()
    t0 = time.time()
    ok_cnt = 0
    fail_cnt = 0

    for i, (idx, race) in enumerate(targets, 1):
        rid = race.get("race_id", "")
        venue = race.get("venue", "")
        rno = race.get("race_no", "")
        label_prefix = f"[{i:>3}/{total}] {venue} {rno}R ({rid})"
        print(label_prefix, flush=True)

        try:
            top10 = fetch_one_race(scraper, rid, top_n=args.top)
            # 少なくとも1券種で取れていたら成功扱い
            got_any = any(len(v) > 0 for v in top10.values())
            if got_any:
                race["top10_odds"] = top10
                counts = " / ".join(f"{k}:{len(v)}" for k, v in top10.items())
                print(f"    OK {counts}", flush=True)
                ok_cnt += 1
            else:
                print(f"    NG 全券種 0 件", flush=True)
                fail_cnt += 1
        except Exception as e:
            print(f"    ERROR: {e}", flush=True)
            fail_cnt += 1

        _print_progress("進捗:", i, total, t0)

    # 更新タイムスタンプ
    data["top10_odds_updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if args.dry_run:
        print("\n[DRY-RUN] 保存はスキップ")
    else:
        # バックアップ
        bak = pred_path.with_suffix(pred_path.suffix + ".bak_top10odds")
        try:
            import shutil
            shutil.copy2(pred_path, bak)
        except Exception as e:
            print(f"[WARN] バックアップ失敗: {e}")
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\n保存: {pred_path}")

    elapsed = time.time() - t0
    print("=" * 72)
    print(f"完了 | 成功: {ok_cnt} / 失敗: {fail_cnt} / 総経過: {_fmt_hhmmss(elapsed)}")
    print("=" * 72)
    return 0 if fail_cnt == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
