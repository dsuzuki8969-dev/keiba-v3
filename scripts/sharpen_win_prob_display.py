#!/usr/bin/env python3
"""
表示勝率シャープ化スクリプト
==================================
本番 pred.json の win_prob / place2_prob / place3_prob を後処理でシャープ化する。
印(mark)・composite・その他指数は一切変更しない。

冪等ガード: data["_meta"]["display_sharpened"] == True の場合はスキップ。
importable 関数 sharpen_pred_file(date_key, backup=True) -> dict を提供。

フルパイプライン非実行・pred直接修正方式 (fix_probabilities.py と同系統)。

使い方:
  python scripts/sharpen_win_prob_display.py --date 20260622
  python scripts/sharpen_win_prob_display.py --date 20260622 --dry-run
  python scripts/sharpen_win_prob_display.py --date 20260622 --gamma 2.5
  python scripts/sharpen_win_prob_display.py --date 20260622 --no-backup
"""
from __future__ import annotations

import json
import os
import shutil
import statistics
import sys
from typing import Any, Dict, List, Optional, Tuple

# config/settings.py から定数を import（ハードコード除去）
from config.settings import DISPLAY_PROB_GAMMA, DISPLAY_PROB_ODDS_FLOORS

PRED_DIR = "data/predictions"


# ───────────────────────────────────────────────
# 内部ヘルパー: odds → floor マッピング
# ───────────────────────────────────────────────
def _get_floor_from_odds(odds: Any) -> float:
    """オッズから勝率下限を返す。oddsがNone/0/欠損なら0.0"""
    if not odds or odds <= 0:
        return 0.0
    for upper, floor in DISPLAY_PROB_ODDS_FLOORS:
        if odds <= upper:
            return floor
    return 0.0


# ───────────────────────────────────────────────
# 内部ヘルパー: 1レース処理
# ───────────────────────────────────────────────
def _process_race(horses: List[Dict], gamma: float) -> Dict:
    """
    1レースの馬リストを処理して、
    各馬の新しい win_prob / place2_prob / place3_prob を返す。

    Returns:
        dict: {horse_no -> (new_win, new_p2, new_p3, old_win, old_p2, old_p3)}
    """
    # 取消馬を除外してアクティブ馬を取得
    active = [h for h in horses if not h.get('is_scratched', False)]
    n = len(active)

    result: Dict = {}
    if n < 2:
        # 処理対象なし: 元の値をそのまま返す
        for h in horses:
            hn = h['horse_no']
            w = h.get('win_prob') or 0.0
            p2 = h.get('place2_prob') or 0.0
            p3 = h.get('place3_prob') or 0.0
            result[hn] = (w, p2, p3, w, p2, p3)
        return result

    # ── Step 1: 元値収集 ──
    old_wins = {h['horse_no']: (h.get('win_prob') or 0.0) for h in active}
    old_p2s  = {h['horse_no']: (h.get('place2_prob') or 0.0) for h in active}
    old_p3s  = {h['horse_no']: (h.get('place3_prob') or 0.0) for h in active}

    # ── Step 2: シャープ化 ──
    # s_i = max(w_i, 1e-9) ** gamma
    sharpened = {hn: max(w, 1e-9) ** gamma for hn, w in old_wins.items()}
    total_s = sum(sharpened.values())

    # ── Step 3: 再正規化 ──
    # w'_i = s_i / Σs  (Σw' = 1.0)
    if total_s <= 0:
        normalized = {hn: 1.0 / n for hn in old_wins}
    else:
        normalized = {hn: s / total_s for hn, s in sharpened.items()}

    # ── Step 4: 本命安泰下限 ──
    # 各馬のオッズ→floor 取得
    horse_odds = {h['horse_no']: h.get('odds') for h in active}
    floors = {hn: _get_floor_from_odds(horse_odds[hn]) for hn in old_wins}

    # floor 適用条件: 元の win_prob でレース上位3位以内 かつ floor > w'_i
    sorted_by_win = sorted(old_wins.keys(), key=lambda hn: old_wins[hn], reverse=True)
    top3_hns = set(sorted_by_win[:3])

    # ピン留め対象集合 P
    pinned: Dict = {}
    for hn in old_wins:
        f = floors[hn]
        if hn in top3_hns and f > 0 and f > normalized[hn]:
            pinned[hn] = f

    # Σfloor_P が 0.95 を超える場合は一律スケール
    if pinned:
        sum_floors = sum(pinned.values())
        if sum_floors > 0.95:
            scale = 0.95 / sum_floors
            pinned = {hn: f * scale for hn, f in pinned.items()}

    # ピン留め馬は floor 固定。非ピン留め馬は残り確率を w' 比で配分
    sum_pinned = sum(pinned.values())
    R = 1.0 - sum_pinned  # 非ピン留め馬への残り確率

    non_pinned = {hn: normalized[hn] for hn in old_wins if hn not in pinned}
    S_np = sum(non_pinned.values())

    final_wins: Dict = {}
    for hn in old_wins:
        if hn in pinned:
            final_wins[hn] = pinned[hn]
        else:
            if S_np > 0:
                final_wins[hn] = non_pinned[hn] / S_np * R
            else:
                final_wins[hn] = R / len(non_pinned) if non_pinned else 0.0

    # ── Step 5: 連対率・複勝率再計算 ──
    # engine.py 準拠のキャップ値 (アクティブ頭数 n を使用)
    p2_cap = min(0.85, 2.0 / n * 3.5)
    p3_cap = min(0.92, 3.0 / n * 3.5)

    new_p2s: Dict = {}
    new_p3s: Dict = {}
    for hn in old_wins:
        w_old = old_wins[hn]
        p2_old = old_p2s[hn]
        p3_old = old_p3s[hn]
        w_new = final_wins[hn]

        # 元の比率を計算 (w_old が極小なら安全なデフォルト比率を使う)
        if w_old > 0.001:
            r2 = p2_old / w_old
            r3 = p3_old / w_old
        else:
            r2 = 1.3
            r3 = 1.5

        # 比率ベース計算
        p2 = w_new * r2
        p3 = w_new * r3

        # 個馬制約 (win <= p2 <= p3)
        p2 = max(p2, w_new * 1.3, w_new)
        p3 = max(p3, p2 * 1.1, w_new * 1.5)

        # 上限キャップ (engine準拠)
        p2 = min(p2, p2_cap)
        p3 = min(p3, p3_cap)

        # 最終整合: win <= p2 <= p3
        p2 = max(p2, w_new)
        p3 = max(p3, p2)

        # anti-saturation: 大頭数などで p2=win に飽和した場合のみ発火
        eps = 1e-6
        if p2 <= w_new + eps and w_new < 0.85:
            p2 = min(0.92, w_new + (1.0 - w_new) * 0.25)
        if p3 <= p2 + eps and p2 < 0.92:
            p3 = min(0.97, p2 + (1.0 - p2) * 0.20)
        # 最終 win<=p2<=p3 を再保証
        p2 = max(p2, w_new)
        p3 = max(p3, p2)

        new_p2s[hn] = p2
        new_p3s[hn] = p3

    # ── 全馬分の結果を格納 ──
    # 取消馬は元の値をそのまま
    for h in horses:
        hn = h['horse_no']
        if h.get('is_scratched', False):
            w = h.get('win_prob') or 0.0
            p2 = h.get('place2_prob') or 0.0
            p3 = h.get('place3_prob') or 0.0
            result[hn] = (w, p2, p3, w, p2, p3)
        else:
            result[hn] = (
                final_wins[hn],
                new_p2s[hn],
                new_p3s[hn],
                old_wins[hn],
                old_p2s[hn],
                old_p3s[hn],
            )

    return result


# ───────────────────────────────────────────────
# 検証統計ビルダー (内部)
# ───────────────────────────────────────────────
def _build_stats(
    races: List[Dict],
    race_results: List[Tuple[int, Dict]],
    gamma: float,
) -> Dict:
    """処理後の検証統計 dict を返す。副作用なし。"""
    pre_max_wins: List[float] = []
    post_max_wins: List[float] = []
    scratched_count = 0
    violation_count = 0
    sigma_fail_count = 0
    TOLERANCE = 0.01

    mizusawa5_pre: List[Dict] = []
    mizusawa5_post: List[Dict] = []
    mizusawa5_race: Optional[Dict] = None

    for race_idx, result in race_results:
        race = races[race_idx]
        horses = race.get('horses', [])

        for h in horses:
            if h.get('is_scratched', False):
                scratched_count += 1

        active = [h for h in horses if not h.get('is_scratched', False)]
        if active:
            pre_max = max(result[h['horse_no']][3] for h in active)
            post_max = max(result[h['horse_no']][0] for h in active)
            sigma_win = sum(result[h['horse_no']][0] for h in active)
            if abs(sigma_win - 1.0) > TOLERANCE:
                sigma_fail_count += 1
            for h in horses:
                hn = h['horse_no']
                nw, np2, np3, _, _, _ = result[hn]
                if nw > np2 + 1e-9 or np2 > np3 + 1e-9:
                    violation_count += 1
        else:
            pre_max = post_max = 0.0

        pre_max_wins.append(pre_max)
        post_max_wins.append(post_max)

        venue = race.get('venue', '')
        race_no = race.get('race_no', 0)
        if '水沢' in str(venue) and race_no == 5:
            mizusawa5_race = race
            for h in horses:
                hn = h['horse_no']
                nw, np2, np3, ow, op2, op3 = result[hn]
                mizusawa5_pre.append({'horse_no': hn, 'horse_name': h['horse_name'],
                    'odds': h.get('odds'), 'mark': h.get('mark', ''),
                    'win': ow, 'p2': op2, 'p3': op3})
                mizusawa5_post.append({'horse_no': hn, 'horse_name': h['horse_name'],
                    'odds': h.get('odds'), 'mark': h.get('mark', ''),
                    'win': nw, 'p2': np2, 'p3': np3,
                    'is_scratched': h.get('is_scratched', False)})

    return {
        "processed_race_count": len(race_results),
        "scratched_count": scratched_count,
        "violation_count": violation_count,
        "sigma_fail_count": sigma_fail_count,
        "pre_max_wins": pre_max_wins,
        "post_max_wins": post_max_wins,
        "mizusawa5_race": mizusawa5_race,
        "mizusawa5_pre": mizusawa5_pre,
        "mizusawa5_post": mizusawa5_post,
        "gamma": gamma,
    }


def _print_stats(stats: Dict) -> None:
    """検証統計を標準出力に表示する。"""
    pre = stats["pre_max_wins"]
    post = stats["post_max_wins"]

    print("=" * 60)
    print("【検証出力】")
    print("=" * 60)
    print(f"\n1. 処理レース数: {stats['processed_race_count']}  /  取消除外馬数: {stats['scratched_count']}")

    if pre:
        print(f"\n2. 全{len(pre)}レースの max win_prob 統計:")
        print(f"   【処理前】min={min(pre):.4f}  median={statistics.median(pre):.4f}  max={max(pre):.4f}")
        print(f"   【処理後】min={min(post):.4f}  median={statistics.median(post):.4f}  max={max(post):.4f}")

    print(f"\n3. 制約違反カウント (win>p2 または p2>p3): {stats['violation_count']} 馬  (0であるべき)")
    print(f"\n4. Σwin 検査 (1.0±0.01 外れ): {stats['sigma_fail_count']} レース  (0であるべき)")

    print(f"\n5. 水沢5R top6 詳細:")
    mrace = stats.get("mizusawa5_race")
    if mrace:
        pre_sorted = sorted(stats["mizusawa5_pre"], key=lambda x: x['win'], reverse=True)
        post_sorted = sorted(stats["mizusawa5_post"], key=lambda x: x['win'], reverse=True)
        print(f"   レースID: {mrace.get('race_id')}  venue={mrace.get('venue')}  race_no={mrace.get('race_no')}")
        print()
        print("   --- 処理前 (win 降順 top6) ---")
        for rank, h in enumerate(pre_sorted[:6], 1):
            print(f"   {rank}位 馬番{h['horse_no']} {h['horse_name']}  odds={h['odds']}  "
                  f"win={h['win']:.4f}  p2={h['p2']:.4f}  p3={h['p3']:.4f}  mark={h['mark']}")
        print()
        print("   --- 処理後 (win 降順 top6) ---")
        for rank, h in enumerate(post_sorted[:6], 1):
            scr = " [取消]" if h.get('is_scratched') else ""
            print(f"   {rank}位 馬番{h['horse_no']} {h['horse_name']}  odds={h['odds']}  "
                  f"win={h['win']:.4f}  p2={h['p2']:.4f}  p3={h['p3']:.4f}  mark={h['mark']}{scr}")
        print()
        print("   --- 処理前 → 処理後 (全馬・馬番順) ---")
        pre_dict = {h['horse_no']: h for h in stats["mizusawa5_pre"]}
        post_dict = {h['horse_no']: h for h in stats["mizusawa5_post"]}
        for hn in sorted(pre_dict.keys()):
            pre_h = pre_dict[hn]
            post_h = post_dict[hn]
            scr = " [取消]" if post_h.get('is_scratched') else ""
            print(f"   馬番{hn} {post_h['horse_name']}  odds={post_h['odds']}"
                  f"  win: {pre_h['win']:.4f}→{post_h['win']:.4f}"
                  f" / p2: {pre_h['p2']:.4f}→{post_h['p2']:.4f}"
                  f" / p3: {pre_h['p3']:.4f}→{post_h['p3']:.4f}"
                  f"  mark={post_h['mark']}{scr}")
    else:
        print("   該当レース無し (水沢5R が存在しない)")
    print()


# ───────────────────────────────────────────────
# Public importable 関数
# ───────────────────────────────────────────────
def sharpen_pred_file(
    date_key: str,
    *,
    gamma: Optional[float] = None,
    backup: bool = True,
    dry_run: bool = False,
) -> Dict:
    """pred.json 表示勝率シャープ化後処理 (冪等)。

    冪等ガード: data["_meta"]["display_sharpened"] == True の場合はスキップ。
    処理後に data["_meta"]["display_sharpened"] = True をセットして保存。

    Parameters
    ----------
    date_key : str
        YYYYMMDD 形式の日付文字列
    gamma : float, optional
        シャープ化指数。省略時は config/settings.py の DISPLAY_PROB_GAMMA を使用
    backup : bool
        True の場合 .bak_sharpen バックアップを作成（既存なら原本保護のためスキップ）
    dry_run : bool
        True の場合、書き込まず検証統計のみ返す

    Returns
    -------
    dict
        検証統計 dict。"skipped" キーが True の場合は既に適用済みのためスキップ。
    """
    if gamma is None:
        gamma = DISPLAY_PROB_GAMMA

    target_path = os.path.join(PRED_DIR, f"{date_key}_pred.json")
    backup_path = target_path + ".bak_sharpen"

    if not os.path.exists(target_path):
        raise FileNotFoundError(f"pred.json が見つかりません: {target_path}")

    # ── ファイル読み込み ──
    with open(target_path, 'r', encoding='utf-8') as f:
        raw_text = f.read()

    indent = 2 if '  "' in raw_text[:100] else None
    data = json.loads(raw_text)

    # ── 冪等ガード ──
    meta = data.setdefault("_meta", {})
    if meta.get("display_sharpened") is True:
        print(f"[sharpen] display_sharpened=True 検出 → スキップ (冪等): {date_key}")
        return {"skipped": True, "date_key": date_key}

    races = data.get('races', [])
    print(f"[sharpen] 開始: {date_key}  gamma={gamma}  レース数={len(races)}")

    # ── 全レース処理 ──
    race_results: List[Tuple[int, Dict]] = []
    for race_idx, race in enumerate(races):
        horses = race.get('horses', [])
        if not horses:
            continue
        result = _process_race(horses, gamma)
        race_results.append((race_idx, result))

    # ── 検証統計 ──
    stats = _build_stats(races, race_results, gamma)
    _print_stats(stats)

    if dry_run:
        print("[sharpen] dry-run: ファイルは変更されません")
        stats["skipped"] = False
        stats["dry_run"] = True
        return stats

    # ── バックアップ ──
    if backup:
        if os.path.exists(backup_path):
            print(f"[sharpen] バックアップ既存 → スキップ (原本保護): {backup_path}")
        else:
            shutil.copy2(target_path, backup_path)
            print(f"[sharpen] バックアップ作成: {backup_path}")

    # ── pred.json に書き戻し ──
    for race_idx, result in race_results:
        race = races[race_idx]
        for h in race.get('horses', []):
            hn = h['horse_no']
            if hn in result and not h.get('is_scratched', False):
                new_win, new_p2, new_p3, _, _, _ = result[hn]
                h['win_prob'] = new_win
                h['place2_prob'] = new_p2
                h['place3_prob'] = new_p3

    # ── 冪等フラグをセット ──
    meta["display_sharpened"] = True

    with open(target_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)

    print(f"[sharpen] 完了: {target_path}  (display_sharpened=True セット済)")
    stats["skipped"] = False
    stats["dry_run"] = False
    return stats


# ───────────────────────────────────────────────
# CLI エントリーポイント
# ───────────────────────────────────────────────
def main() -> None:
    import argparse
    import io as _io

    # UTF-8出力強制 (Windows PowerShell 対策)
    sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

    parser = argparse.ArgumentParser(description='pred.json の表示勝率シャープ化後処理')
    parser.add_argument('--date', required=True, help='対象日付 YYYYMMDD (例: 20260622)')
    parser.add_argument('--gamma', type=float, default=DISPLAY_PROB_GAMMA,
                        help=f'シャープ化指数 (既定: {DISPLAY_PROB_GAMMA}, 大きいほど本命集中)')
    parser.add_argument('--dry-run', action='store_true',
                        help='書き込まず検証出力のみ表示')
    parser.add_argument('--no-backup', action='store_true',
                        help='バックアップを作成しない (既定: バックアップあり)')
    args = parser.parse_args()

    print(f"=== 表示勝率シャープ化スクリプト ===")
    print(f"対象ファイル: {os.path.join(PRED_DIR, args.date + '_pred.json')}")
    print(f"gamma: {args.gamma}")
    print(f"dry-run: {args.dry_run}")
    print(f"backup: {not args.no_backup}")
    print()

    stats = sharpen_pred_file(
        args.date,
        gamma=args.gamma,
        backup=(not args.no_backup),
        dry_run=args.dry_run,
    )

    if stats.get("skipped"):
        print("[INFO] 既にシャープ化済みのためスキップしました。再適用には .bak_sharpen を元に戻してください。")
    elif stats.get("dry_run"):
        print("=" * 60)
        print("【dry-run モード: ファイルは変更されません】")
        print("=" * 60)
    else:
        print("処理完了: 再検証は --dry-run で再度実行するか、外部で pred.json を確認してください。")


if __name__ == "__main__":
    main()
