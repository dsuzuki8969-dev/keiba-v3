"""◉ / ◎ 各印の自信度別複勝率集計スクリプト。

マスター指示 2026-05-02:
- data/predictions/YYYYMMDD_pred.json (_prev.json / _backup / .bak 除外)
- data/results/YYYYMMDD_results.json
を突合し、mark × confidence × segment (jra/nar/all) の複勝率を集計して表形式で出力する。
"""
from __future__ import annotations

import io
import json
import sys
import time
from pathlib import Path

# Windows での文字化け防止: stdout を UTF-8 に強制
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PRED_DIR = Path("data/predictions")
RES_DIR = Path("data/results")
LOG_DIR = Path("data/logs")
LOG_FILE = LOG_DIR / "markwise_place_rate.log"

# 集計対象の印
TARGET_MARKS = ("◉", "◎")

# 自信度の表示順
CONF_ORDER = ["SS", "S", "A", "B", "C", "D", "E"]

# JRA 会場コード (race_id の 5-6 桁目)
JRA_VENUE_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}


# ---------------------------------------------------------------------------
# ユーティリティ関数
# ---------------------------------------------------------------------------

def _is_jra(race_id: str) -> bool:
    """race_id の 5-6 桁目が JRA 会場コードなら True。"""
    rid = str(race_id)
    return len(rid) >= 6 and rid[4:6] in JRA_VENUE_CODES


def _normalize_conf(raw: str) -> str:
    """overall_confidence を正規化。SS/S/A/B/C/D/E 以外は 'D' 扱い。"""
    conf = (raw or "").replace("⁺", "+").strip()  # ⁺ → +
    if conf in ("SS", "S", "A", "B", "C", "D", "E"):
        return conf
    return "D"


def _new_stat() -> dict:
    """単一セルの集計カウンタを初期化して返す。"""
    return {"races": 0, "hit": 0}


def _new_mark_stats() -> dict:
    """mark × segment × confidence の集計テーブルを初期化。"""
    return {
        mark: {
            seg: {conf: _new_stat() for conf in CONF_ORDER + ["全体"]}
            for seg in ("jra", "nar", "all")
        }
        for mark in TARGET_MARKS
    }


# ---------------------------------------------------------------------------
# メイン集計
# ---------------------------------------------------------------------------

def collect_pred_files() -> list[Path]:
    """_prev.json / _backup / .bak を除いた pred.json を日付昇順で返す。"""
    files = sorted(PRED_DIR.glob("*_pred.json"))
    filtered = [
        f for f in files
        if "_prev" not in f.name
        and "_backup" not in f.name
        and ".bak" not in f.name
    ]
    return filtered


def run() -> None:
    pred_files = collect_pred_files()
    n_total = len(pred_files)
    print(f"対象 pred.json 数: {n_total}")

    stats = _new_mark_stats()

    started = time.time()
    n_processed = 0  # results が存在した日数

    for fi, fp in enumerate(pred_files):
        # ファイル名から日付を取得
        date_str = fp.name.split("_")[0]
        if len(date_str) != 8 or not date_str.isdigit():
            continue

        # 対応する results.json が存在するか確認
        res_fp = RES_DIR / f"{date_str}_results.json"
        if not res_fp.exists():
            continue

        # JSON 読み込み
        try:
            pred = json.loads(fp.read_text(encoding="utf-8"))
            results = json.loads(res_fp.read_text(encoding="utf-8"))
        except Exception:
            continue

        n_processed += 1

        # レースごとに集計
        for r in pred.get("races", []):
            race_id = str(r.get("race_id", ""))
            if not race_id:
                continue

            # results に該当レースがあるか確認
            rdata = results.get(race_id)
            if not rdata:
                continue

            # 着順マップ作成: horse_no → finish
            order_list = rdata.get("order", [])
            finish_map: dict[int, int] = {}
            for entry in order_list:
                hno = entry.get("horse_no")
                fin = entry.get("finish")
                if hno is not None and fin is not None:
                    finish_map[int(hno)] = int(fin)

            if not finish_map:
                continue

            # セグメント判定
            seg = "jra" if _is_jra(race_id) else "nar"

            # 自信度正規化
            conf = _normalize_conf(r.get("overall_confidence", ""))

            # 出走馬から TARGET_MARKS の馬を抽出
            for h in r.get("horses", []):
                # 取消馬は除外
                if h.get("is_scratched") or h.get("is_tokusen_kiken"):
                    continue

                mark = (h.get("mark") or "").strip()
                if mark not in TARGET_MARKS:
                    continue

                horse_no = h.get("horse_no")
                if horse_no is None:
                    continue
                horse_no = int(horse_no)

                # results に horse_no が存在しない場合は除外 (取消等)
                if horse_no not in finish_map:
                    continue

                # 複勝判定: finish <= 3 なら的中
                is_hit = finish_map[horse_no] <= 3

                # 集計: (jra or nar, conf), (jra or nar, 全体), (all, conf), (all, 全体)
                for sg in (seg, "all"):
                    for cf in (conf, "全体"):
                        cell = stats[mark][sg][cf]
                        cell["races"] += 1
                        if is_hit:
                            cell["hit"] += 1

        # 進捗表示 (100 ファイルごと)
        if (fi + 1) % 100 == 0 or (fi + 1) == n_total:
            elapsed = time.time() - started
            print(f"  {fi+1}/{n_total} ({date_str}) elapsed={elapsed:.1f}s", flush=True)

    print()
    print(f"集計対象 pred 日数: {n_processed}")

    # -----------------------------------------------------------------------
    # 表形式出力
    # -----------------------------------------------------------------------
    lines: list[str] = []

    SEG_LABEL = {
        "all": "全体",
        "jra": "JRA (中央)",
        "nar": "NAR (地方)",
    }

    def _rate_str(hit: int, races: int) -> str:
        """複勝率を文字列化。races が 0 なら '  -  ' を返す。"""
        if races == 0:
            return "   -  "
        return f"{hit/races*100:5.1f}%"

    def _diff_str(hit1: int, r1: int, hit2: int, r2: int) -> str:
        """差 (◉ rate - ◎ rate) を文字列化。"""
        if r1 == 0 or r2 == 0:
            return "      -"
        diff = (hit1 / r1 - hit2 / r2) * 100
        return f"{diff:+6.1f}pt"

    def _print_segment(seg: str) -> None:
        label = SEG_LABEL[seg]
        header = (
            f"\n{'='*56} {label} ◉◎ 自信度別 複勝率 {'='*56}\n"
            f"{'信頼度':<6}  "
            f"{'◉ R数':>7}  {'◉ 複勝':>7}  {'◉ 率':>7}  "
            f"{'◎ R数':>7}  {'◎ 複勝':>7}  {'◎ 率':>7}  "
            f"{'差(◉-◎)':>9}"
        )
        sep = "─" * 75

        lines.append(header)
        lines.append(sep)

        for conf in CONF_ORDER + ["全体"]:
            c1 = stats["◉"][seg][conf]
            c2 = stats["◎"][seg][conf]

            r1, h1 = c1["races"], c1["hit"]
            r2, h2 = c2["races"], c2["hit"]

            # どちらも 0 件なら行を省略
            if r1 == 0 and r2 == 0:
                continue

            diff = _diff_str(h1, r1, h2, r2)
            row = (
                f"{conf:<6}  "
                f"{r1:>7,}  {h1:>7,}  {_rate_str(h1, r1):>7}  "
                f"{r2:>7,}  {h2:>7,}  {_rate_str(h2, r2):>7}  "
                f"{diff:>9}"
            )
            lines.append(row)
            # 「全体」行の前に区切り線
            if conf == "E":
                lines.append(sep)

        lines.append(sep)

    for seg in ("all", "jra", "nar"):
        _print_segment(seg)

    output = "\n".join(lines)
    print(output)

    # -----------------------------------------------------------------------
    # ログファイル出力 (BOM 付き UTF-8)
    # -----------------------------------------------------------------------
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w", encoding="utf-8-sig") as lf:
        lf.write(output)
    print(f"\nログ保存: {LOG_FILE}")


if __name__ == "__main__":
    run()
