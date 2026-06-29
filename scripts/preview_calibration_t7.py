"""
preview_calibration_t7.py — T-7 表示率較正 Phase 0 本番非改変プレビュー

【絶対厳守】このスクリプトは読み取り専用。本番 pred.json を一切書き換えない。
  ① pred は json.load 後 copy.deepcopy した work にのみ較正適用(元 pred は read-only)
  ② open(...,"w") / json.dump / Path.write_text 等のディスク書込を一切持たない(print のみ)
  ③ 較正テーブルは monkeypatch でパス差替(本番ファイル自体は変更しない)

使用例:
    python scripts/preview_calibration_t7.py
    python scripts/preview_calibration_t7.py --date 20260630
    python scripts/preview_calibration_t7.py --date 20260630 --table wf
    python scripts/preview_calibration_t7.py --date 20260630 --gamma 1.5
"""

from __future__ import annotations

import sys

# 🛡️ cp932 (Shift-JIS) クラッシュ対策 (Windows scheduler / CLI)
# finalize_predictions.py:28-33 と同一パターン
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

import argparse
import copy
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# プロジェクトルートをパスに追加
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# ─────────────────────────────────────────────
# 定数
# ─────────────────────────────────────────────

_PRED_DIR = _ROOT / "data" / "predictions"
_DIAG_DIR = _ROOT / "data" / "_diag"
_CALIB_PROD = _DIAG_DIR / "calibration_composite.json"
_CALIB_WF   = _DIAG_DIR / "calibration_composite_wf.json"

_GAMMA_DEFAULTS = [1.0, 1.5, 2.0]

# 軸馬度の重み (dashboard.py:726 と同一)
_JIKU_W_JITSU    = 0.40
_JIKU_W_KENZEN   = 0.30
_JIKU_W_DANTOTSU = 0.30


# ─────────────────────────────────────────────
# 引数パース
# ─────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="T-7 表示率較正 Phase 0 本番非改変プレビュー"
    )
    p.add_argument(
        "--date",
        metavar="YYYYMMDD",
        help="対象日 (省略時は data/predictions/ の最新 *_pred.json を自動選択)",
    )
    p.add_argument(
        "--table",
        choices=["prod", "wf"],
        default="prod",
        help="較正テーブル選択: prod=本番版(リーク注意) / wf=WF版(Phase1生成が必要)",
    )
    p.add_argument(
        "--gamma",
        type=float,
        default=None,
        help="gamma 値(省略時は 1.0/1.5/2.0 を全て比較)",
    )
    return p.parse_args()


# ─────────────────────────────────────────────
# pred.json 選択・読み込み
# ─────────────────────────────────────────────

def _find_latest_pred() -> Optional[Path]:
    """最新の *_pred.json を返す。"""
    files = sorted(_PRED_DIR.glob("*_pred.json"))
    return files[-1] if files else None


def _load_pred(date_str: Optional[str]) -> Tuple[Path, Dict]:
    """pred.json を読み込んで (path, data) を返す。書き込みは一切しない。"""
    if date_str:
        date_key = date_str.replace("-", "")
        path = _PRED_DIR / f"{date_key}_pred.json"
        if not path.exists():
            print(f"[エラー] pred.json が見つかりません: {path}", file=sys.stderr)
            sys.exit(1)
    else:
        path = _find_latest_pred()
        if path is None:
            print(f"[エラー] {_PRED_DIR} に pred.json が見つかりません", file=sys.stderr)
            sys.exit(1)
    with open(path, "rb") as f:
        raw = f.read()
    text = raw.decode("utf-8", errors="ignore")
    return path, json.loads(text)


# ─────────────────────────────────────────────
# 較正テーブル monkeypatch
# ─────────────────────────────────────────────

def _resolve_calib_path(table: str) -> Path:
    """--table 引数に応じた較正テーブルパスを返す。"""
    if table == "wf":
        if not _CALIB_WF.exists():
            print(
                f"[エラー] WF版較正テーブルが存在しません: {_CALIB_WF}\n"
                "  → Phase 1 (walk_forward_backtest.py --composite-probe) を先に実行してください。",
                file=sys.stderr,
            )
            sys.exit(1)
        return _CALIB_WF
    return _CALIB_PROD


def _apply_calibration_with_table(
    work: Dict,
    calib_path: Path,
    gamma: float,
) -> Dict:
    """
    composite_calibration モジュールの CALIB_COMPOSITE_FILE を monkeypatch で
    差し替えて apply_composite_calibration を呼ぶ。
    work は呼び出し元で deepcopy 済みの独立したオブジェクト。
    """
    import src.calculator.composite_calibration as _cc

    # monkeypatch: モジュールのグローバル変数だけ差し替え (本番ファイルは変更しない)
    _orig_path = _cc.CALIB_COMPOSITE_FILE
    _cc.CALIB_COMPOSITE_FILE = calib_path
    try:
        result = _cc.apply_composite_calibration(work, gamma=gamma)
    finally:
        # 必ず元のパスに戻す
        _cc.CALIB_COMPOSITE_FILE = _orig_path
    return result


# ─────────────────────────────────────────────
# 軸馬度計算 (dashboard.py:713 と同一式)
# ─────────────────────────────────────────────

def _compute_jiku_score(
    composite: float,
    place3_prob: float,
    comp_rank: int,
    N: int,
) -> float:
    """軸馬度 jiku_score を返す。pop_rank は穴馬度用なので省略。"""
    _jitsu    = max(0.0, min((composite - 20.0) / 80.0 * 100.0, 100.0))
    _kenzen   = max(0.0, min(place3_prob * 100.0, 100.0))
    _dantotsu = ((N - comp_rank) / (N - 1) * 100.0) if N > 1 else 100.0
    return round(
        max(0.0, min(
            _JIKU_W_JITSU * _jitsu
            + _JIKU_W_KENZEN * _kenzen
            + _JIKU_W_DANTOTSU * _dantotsu,
            100.0,
        )),
        1,
    )


# ─────────────────────────────────────────────
# races_flat 構築 (elite_marks.py の引数形式に合わせる)
# ─────────────────────────────────────────────

def _to_races_flat(data: Dict) -> List[Dict]:
    """pred data の races[] を races_flat 形式に変換する。"""
    flat = []
    for race in data.get("races", []):
        race_id = race.get("race_id", "")
        flat.append({
            "race_id": race_id,
            "race": race,
            "horses": race.get("horses", []),
            "is_jra": bool(race.get("is_jra", False)),
        })
    return flat


# ─────────────────────────────────────────────
# 統計計算
# ─────────────────────────────────────────────

def _is_banei(race: Dict) -> bool:
    # 本番 composite_calibration.py:180 と同一判定 (is_banei フラグのみ)。
    # before/after 統計のスキップ対象を本番較正と一致させ母数ズレを防ぐ。
    return bool(race.get("is_banei", False))


def _compute_stats(data: Dict) -> Dict:
    """飽和解消サマリ統計を計算して返す。"""
    p3_over90 = 0       # place3_prob > 90%
    p3_max = 0.0        # place3_prob の最大値
    honmei_p3_sum = 0.0 # 本命(win_prob top)の place3_prob 合計
    honmei_count = 0
    win_over50 = 0      # win_prob > 50%

    _HONMEI_MARKS = {"◉", "◎"}

    for race in data.get("races", []):
        if _is_banei(race):
            continue
        horses = race.get("horses", [])
        active = [h for h in horses if not h.get("is_scratched", False)]
        if not active:
            continue

        # 本命: mark∈{◉,◎} の先頭、なければ win_prob 最大
        honmei = None
        for h in active:
            if h.get("mark", "") in _HONMEI_MARKS:
                honmei = h
                break
        if honmei is None:
            honmei = max(active, key=lambda h: float(h.get("win_prob") or 0))

        for h in active:
            p3 = float(h.get("place3_prob") or 0)
            wp = float(h.get("win_prob") or 0)
            if p3 * 100 > 90.0:
                p3_over90 += 1
            if p3 > p3_max:
                p3_max = p3
            if wp > 0.50:
                win_over50 += 1

        honmei_p3_sum += float(honmei.get("place3_prob") or 0)
        honmei_count += 1

    return {
        "p3_over90": p3_over90,
        "p3_max": p3_max,
        "honmei_p3_avg": honmei_p3_sum / honmei_count if honmei_count else 0.0,
        "win_over50": win_over50,
        "honmei_count": honmei_count,
    }


def _compute_jiku_list(data: Dict) -> List[Tuple[str, int, float]]:
    """各馬の (race_id, horse_no, jiku_score) リストを返す。ばんえい除外。"""
    result = []
    for race in data.get("races", []):
        if _is_banei(race):
            continue
        horses = race.get("horses", [])
        active = [h for h in horses if not h.get("is_scratched", False)]
        if not active:
            continue
        race_id = race.get("race_id", "")
        N = len(active)

        # composite 降順でランク付け
        sorted_by_comp = sorted(
            active,
            key=lambda h: float(h.get("composite") or 0),
            reverse=True,
        )
        comp_rank_map = {h.get("horse_no"): i + 1 for i, h in enumerate(sorted_by_comp)}

        for h in active:
            horse_no = h.get("horse_no")
            comp = float(h.get("composite") or 0)
            p3   = float(h.get("place3_prob") or 0)
            cr   = comp_rank_map.get(horse_no, 1)
            jiku = _compute_jiku_score(comp, p3, cr, N)
            result.append((race_id, horse_no, jiku))
    return result


# ─────────────────────────────────────────────
# ◉ 本命・穴馬の入替計算
# ─────────────────────────────────────────────

def _select_honmei_set(data: Dict, top_n: int = 5) -> set:
    """全レースの本命(mark∈{◉,◎}) を win_prob 降順 top_n 選定した (race_id, horse_no) セット。"""
    from src.calculator.elite_marks import select_pivot_honmei
    flat = _to_races_flat(data)
    candidates = select_pivot_honmei(flat, top_n=top_n)
    return {(race_id, horse_no) for race_id, horse_no, _ in candidates}


def _select_dark_set(data: Dict, top_n: int = 5) -> set:
    """穴馬 top_n の (race_id, horse_no) セット。"""
    from src.calculator.elite_marks import select_dark_horses
    flat = _to_races_flat(data)
    candidates = select_dark_horses(flat, top_n=top_n)
    return {(race_id, horse_no) for race_id, horse_no, _ in candidates}


# ─────────────────────────────────────────────
# サンプルレース表示
# ─────────────────────────────────────────────

def _print_sample_races(
    before: Dict,
    after: Dict,
    max_races: int = 2,
) -> None:
    """最初の max_races レースについて馬別の before→after 比較表を出力する。"""
    shown = 0
    for br, ar in zip(before.get("races", []), after.get("races", [])):
        if _is_banei(br):
            continue
        if shown >= max_races:
            break
        shown += 1
        race_id = br.get("race_id", "?")
        venue   = br.get("venue", "")
        race_no = br.get("race_no", "")
        print(f"\n  【サンプルレース: {venue} {race_no}R (race_id={race_id})】")
        print(
            f"  {'馬番':>3} {'偏差値':>6} "
            f"{'勝率B':>6} {'勝率A':>6} "
            f"{'連対B':>6} {'連対A':>6} "
            f"{'複勝B':>6} {'複勝A':>6} "
            f"{'印':>2}"
        )
        print("  " + "-" * 62)
        bh_map = {h.get("horse_no"): h for h in br.get("horses", [])}
        for ah in ar.get("horses", []):
            hn  = ah.get("horse_no")
            bh  = bh_map.get(hn, {})
            comp = float(bh.get("composite") or 0)
            bwp  = float(bh.get("win_prob") or 0) * 100
            awp  = float(ah.get("win_prob") or 0) * 100
            bp2  = float(bh.get("place2_prob") or 0) * 100
            ap2  = float(ah.get("place2_prob") or 0) * 100
            bp3  = float(bh.get("place3_prob") or 0) * 100
            ap3  = float(ah.get("place3_prob") or 0) * 100
            mark = bh.get("mark", "")
            scr  = "取" if bh.get("is_scratched") else ""
            print(
                f"  {hn:>3} {comp:>6.1f} "
                f"{bwp:>5.1f}% {awp:>5.1f}% "
                f"{bp2:>5.1f}% {ap2:>5.1f}% "
                f"{bp3:>5.1f}% {ap3:>5.1f}% "
                f"{mark:>2}{scr}"
            )


# ─────────────────────────────────────────────
# gamma1件分のプレビュー出力
# ─────────────────────────────────────────────

def _preview_gamma(
    gamma: float,
    pred_orig: Dict,
    calib_path: Path,
    top_n: int = 5,
) -> None:
    """gamma 1値分のプレビューを出力する。"""
    print(f"\n{'='*70}")
    print(f"  gamma = {gamma}")
    print(f"{'='*70}")

    # ① 元 pred の統計(before)
    stats_b = _compute_stats(pred_orig)

    # ② deepcopy してから較正適用 (元 pred_orig は一切変更しない)
    work = copy.deepcopy(pred_orig)
    # 二重適用ガードのフラグをリセット (deepcopy しているので安全)
    work.setdefault("_meta", {}).pop("composite_calibrated", None)

    # 標準出力への [composite_calibration] ログは抑制しない（診断用に有益）
    _apply_calibration_with_table(work, calib_path, gamma)

    stats_a = _compute_stats(work)

    # ─── 1. 飽和解消サマリ ───
    print("\n[1] 飽和解消サマリ")
    print(f"  複勝率>90%の馬数     : {stats_b['p3_over90']:>4}頭 → {stats_a['p3_over90']:>4}頭")
    print(
        f"  複勝率の最大値        : {stats_b['p3_max']*100:>5.1f}% → {stats_a['p3_max']*100:>5.1f}%"
    )
    print(
        f"  本命の平均複勝率      : {stats_b['honmei_p3_avg']*100:>5.1f}% → {stats_a['honmei_p3_avg']*100:>5.1f}%"
    )
    print(f"  勝率>50%の馬数       : {stats_b['win_over50']:>4}頭 → {stats_a['win_over50']:>4}頭")

    # ─── 2. サンプルレース表 ───
    print("\n[2] サンプルレース(B=before / A=after)")
    _print_sample_races(pred_orig, work, max_races=2)

    # ─── 3. ◉本命の入替 ───
    print("\n[3] ◉本命の入替 (win_prob top5 再選定)")
    try:
        set_b = _select_honmei_set(pred_orig, top_n=top_n)
        set_a = _select_honmei_set(work, top_n=top_n)
        added   = set_a - set_b
        removed = set_b - set_a
        print(f"  入替: +{len(added)}頭 / -{len(removed)}頭")
        if added:
            print(f"  新規追加: {list(added)[:3]}")
        if removed:
            print(f"  除外     : {list(removed)[:3]}")
    except Exception as e:
        print(f"  ◉本命入替計算エラー: {e}", file=sys.stderr)

    # ─── 4. 穴馬の入替 ───
    print("\n[4] 穴馬の入替 (miryoku / place3_prob 変化)")
    try:
        dark_b = _select_dark_set(pred_orig, top_n=top_n)
        dark_a = _select_dark_set(work, top_n=top_n)
        d_added   = dark_a - dark_b
        d_removed = dark_b - dark_a
        print(f"  入替: +{len(d_added)}頭 / -{len(d_removed)}頭")
        if d_added:
            print(f"  新規追加: {list(d_added)[:3]}")
        if d_removed:
            print(f"  除外     : {list(d_removed)[:3]}")
    except Exception as e:
        print(f"  穴馬入替計算エラー: {e}", file=sys.stderr)

    # ─── 5. 軸馬度の変動 ───
    print("\n[5] 軸馬度の変動 (place3_prob が堅実項に効く)")
    try:
        jiku_b_map = {(rid, hn): sc for rid, hn, sc in _compute_jiku_list(pred_orig)}
        jiku_a_map = {(rid, hn): sc for rid, hn, sc in _compute_jiku_list(work)}
        deltas = []
        for key, sc_b in jiku_b_map.items():
            sc_a = jiku_a_map.get(key, sc_b)
            deltas.append((abs(sc_a - sc_b), sc_b, sc_a, key))

        if deltas:
            avg_delta = sum(d[0] for d in deltas) / len(deltas)
            top_changed = len([d for d in deltas if d[0] >= 1.0])
            print(f"  |Δ|平均: {avg_delta:.2f}pt")
            print(f"  変動≥1.0ptのレース馬数: {top_changed}頭")
            # 変動が大きい上位3例
            deltas.sort(reverse=True)
            print("  変動大上位3例 (race_id, horse_no): before→after")
            for _abs_d, sc_b, sc_a, key in deltas[:3]:
                print(f"    {key}: {sc_b:.1f} → {sc_a:.1f}  (Δ={sc_a-sc_b:+.1f})")
    except Exception as e:
        print(f"  軸馬度変動計算エラー: {e}", file=sys.stderr)


# ─────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    # 較正テーブルパス解決
    calib_path = _resolve_calib_path(args.table)

    # pred.json 読み込み (書き込みゼロ)
    pred_path, pred_orig = _load_pred(args.date)

    # mtime 記録 (書き換えてないことを後で確認するため)
    pred_mtime_before = pred_path.stat().st_mtime

    # gamma リスト決定
    gammas = [args.gamma] if args.gamma is not None else _GAMMA_DEFAULTS

    # ─── ヘッダー出力 ───
    print()
    print("=" * 70)
    print("  T-7 表示率較正 Phase 0 本番非改変プレビュー")
    print("=" * 70)
    if args.table == "prod":
        print()
        print("  ⚠️  --table prod はリーク版テーブル=絶対値は楽観的・方向性確認用")
        print("      (本番 pred 全期間集計由来。WF版は Phase1 完了後 --table wf で確認)")
    else:
        print()
        print("  ✅ --table wf: WF版(leak-free)テーブルを使用")
    print()
    print(f"  pred ファイル  : {pred_path.name}")
    print(f"  較正テーブル  : {calib_path.name}")
    print(f"  gamma (試行値): {gammas}")
    print("  ばんえいレース: スキップ(較正対象外)")
    print()
    print("  【書換ゼロ担保】")
    print("  ① pred は deepcopy した work にのみ較正適用 (元 pred は read-only)")
    print("  ② このスクリプトに open(...,'w') / json.dump / write_text は存在しない")
    print("  ③ 較正テーブルは monkeypatch でパス差替 (本番ファイル自体は変更しない)")

    # ─── gamma ごとにプレビュー ───
    for gamma in gammas:
        _preview_gamma(gamma, pred_orig, calib_path, top_n=5)

    # ─── mtime 確認 ───
    pred_mtime_after = pred_path.stat().st_mtime
    print()
    print("=" * 70)
    print("  書換ゼロ検証")
    print("=" * 70)
    if pred_mtime_before == pred_mtime_after:
        print(f"  ✅ pred.json mtime 不変: {pred_path.name} は変更されていません")
    else:
        print(f"  ❌ [異常] pred.json の mtime が変化しました! 要調査: {pred_path}")
    print()


if __name__ == "__main__":
    main()
