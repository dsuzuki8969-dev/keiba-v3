"""
finalize_predictions.py — pred.json 後処理一本化モジュール

処理順序:
1. sharpen_pred_file  … 表示勝率シャープ化 (冪等・display_sharpened フラグで二重適用防止)
2. apply_elite_and_formation … elite(◉/穴) → 4パターンformation → force-buy → bet_decision

◉=本命勝率top5 は シャープ化後の win_prob で選定するため、順序は固定。

生成パイプライン末尾 + 手動再適用 兼用。冪等。

使用例:
    from src.calculator.finalize_predictions import finalize_predictions
    finalize_predictions("20260622")
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict

# 🛡️ cp932 (Shift-JIS) クラッシュ対策 (2026-06-24)
# dashboard scheduler スレッドや Windows CLI では stdout が cp932 になり、
# elite ステップ内の print("...◉...") が UnicodeEncodeError で落ちる。
# これにより オッズ確定後の ◉/穴 自動再選定 (b42d285) が毎回クラッシュしていた。
# finalize は dashboard 経由・CLI 直接の両経路の合流点なので、ここで UTF-8 を強制する。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        # reconfigure 非対応 (古い Python / 既にラップ済) は無視 (errors=replace相当の安全側)
        pass

# プロジェクトルートをパスに追加 (直接実行・import 双方対応)
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def finalize_predictions(date_key: str) -> Dict:
    """pred.json 後処理一本化: 表示勝率シャープ化 → elite(◉/穴) → 4パターンformation。

    生成パイプライン末尾 + 手動再適用 兼用。冪等。

    Parameters
    ----------
    date_key : str
        YYYYMMDD 形式の日付文字列 (例: "20260622")

    Returns
    -------
    dict
        {
            "sharpen": <sharpen_pred_file の戻り値 dict>,
            "elite":   <apply_elite_and_formation の戻り値 dict>,
        }

    Notes
    -----
    - sharpen は冪等ガード付き (display_sharpened フラグ)。二重適用しない。
    - elite は毎回再実行 (◉/穴選定は sharpen 後の win_prob に依存するため冪等ではない)。
    - 例外は呼び元に伝播させる (run_analysis_date.py 側で try/except して非致命扱い)。
    """
    # scripts/ ディレクトリをパスに追加 (sharpen/elite モジュール import 用)
    _scripts_dir = str(_ROOT / "scripts")
    if _scripts_dir not in sys.path:
        sys.path.insert(0, _scripts_dir)

    # ── Step 1: 表示勝率シャープ化 (冪等) ──
    from sharpen_win_prob_display import sharpen_pred_file
    print(f"[finalize] Step1: sharpen ({date_key})")
    sharpen_stats = sharpen_pred_file(date_key, backup=True)

    # ── Step 2: elite(◉/穴) → formation ──
    from apply_elite_marks_20260621 import apply_elite_and_formation
    print(f"[finalize] Step2: elite+formation ({date_key})")
    elite_stats = apply_elite_and_formation(date_key, backup=True)

    print(f"[finalize] 完了: {date_key}  "
          f"sharpen_skipped={sharpen_stats.get('skipped')}  "
          f"◉={elite_stats.get('pivot_count')}  穴={elite_stats.get('ana_count')}")

    return {
        "sharpen": sharpen_stats,
        "elite":   elite_stats,
    }
