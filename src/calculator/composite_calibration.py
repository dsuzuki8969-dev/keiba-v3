"""
composite_calibration.py — composite偏差値アンカー較正モジュール

【重要な注意事項】
calibration_composite.json は本番 pred.json の全期間結果から集計されている。
このデータは「本番運用で予測した結果」が含まれており、学習リーク懸念がある。
本番で COMPOSITE_CALIBRATION_ENABLED=True を使う前に、
Walk-Forward 評価期間別に再集計した calibration_composite.json に差し替えること。
("data/_diag/calibration_composite.json" を WF 版で作り直すこと。)

デフォルト: COMPOSITE_CALIBRATION_ENABLED=False → 本番動作は一切変わらない。

移植元: scripts/preview_calibration_v2.py (calc_after 関数の k_odds=0.0 固定相当)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# プロジェクトルート
_ROOT = Path(__file__).resolve().parent.parent.parent

# 較正テーブルパス
CALIB_COMPOSITE_FILE = _ROOT / "data" / "_diag" / "calibration_composite.json"

# composite bin 定義 (preview_calibration_v2.py と同一)
_BINS = [
    (0,    29.9,  "<30"),
    (30,   34.9,  "30-34"),
    (35,   39.9,  "35-39"),
    (40,   44.9,  "40-44"),
    (45,   49.9,  "45-49"),
    (50,   54.9,  "50-54"),
    (55,   59.9,  "55-59"),
    (60,   64.9,  "60-64"),
    (65,   69.9,  "65-69"),
    (70,   74.9,  "70-74"),
    (75,   999,   "75+"),
]


# ─────────────────────────────────────────────
# 内部ヘルパー
# ─────────────────────────────────────────────

def _bin_center(bin_label: str) -> float | None:
    """bin名 → 中心composite値 (線形補間用)。
    "<30" → 27.0 / "75+" → 77.0 / "60-64" → 62.0"""
    if bin_label.startswith("<"):
        return float(bin_label[1:]) - 3.0
    if bin_label.endswith("+"):
        return float(bin_label[:-1]) + 2.0
    if "-" in bin_label:
        lo, hi = bin_label.split("-", 1)
        return (float(lo) + float(hi)) / 2.0
    return None


def _interp_composite_rate(
    comp: float,
    org: str,
    calib_composite: dict,
) -> tuple[float, float, float] | None:
    """composite実値を隣接bin中心間で線形補間し (win, p2, p3) を 0〜1 で返す。

    - 階段状でなく連続値にすることで同偏差値帯内の同値問題を解消
    - org → "ALL" フォールバック。該当なしは None。
    - 返値は 0〜1 の小数 (% 単位ではない)
    """
    for try_org in (org, "ALL"):
        table = calib_composite.get(try_org, {})
        pts: list[tuple[float, float, float, float]] = []
        for bin_label, row in table.items():
            if not row or row.get("win") is None:
                continue
            c = _bin_center(bin_label)
            if c is None:
                continue
            pts.append((
                c,
                row["win"] / 100.0,
                row["place2"] / 100.0,
                row["place3"] / 100.0,
            ))
        if not pts:
            continue
        pts.sort(key=lambda x: x[0])
        if comp <= pts[0][0]:
            return pts[0][1], pts[0][2], pts[0][3]
        if comp >= pts[-1][0]:
            return pts[-1][1], pts[-1][2], pts[-1][3]
        for i in range(len(pts) - 1):
            c0, w0, p20, p30 = pts[i]
            c1, w1, p21, p31 = pts[i + 1]
            if c0 <= comp <= c1:
                t = (comp - c0) / (c1 - c0) if c1 > c0 else 0.0
                return (
                    w0 + t * (w1 - w0),
                    p20 + t * (p21 - p20),
                    p30 + t * (p31 - p30),
                )
    return None


# ─────────────────────────────────────────────
# 公開 API
# ─────────────────────────────────────────────

def load_calib_composite() -> dict:
    """calibration_composite.json を読み込んで返す。

    Returns
    -------
    dict
        {org: {bin_label: {"n": int, "win": float, "place2": float, "place3": float}}}
        ここで win/place2/place3 は % 単位。

    Raises
    ------
    FileNotFoundError
        テーブルファイルが存在しない場合。
    """
    with open(CALIB_COMPOSITE_FILE, encoding="utf-8") as f:
        return json.load(f)


def apply_composite_calibration(data: dict, gamma: float = 2.0) -> dict:
    """pred 構造の非取消馬の win_prob/place2_prob/place3_prob を
    composite偏差値アンカー較正で再計算する。

    処理:
      Step1. composite実値の線形補間でbase率取得 (win, p2, p3) を 0〜1
      Step2. k_odds=0 → オッズ加味なし (base率そのまま)
      Step3. gamma 乗でシャープ化 (デフォルト gamma=2.0)
      Step4. レース内正規化 (win Σ=1.0 / p2 Σ=2.0 / p3 Σ=3.0)
      Step5. 個馬整合 win ≤ p2 ≤ p3

    二重適用ガード:
      data["_meta"]["composite_calibrated"] == True のとき何もせずそのまま返す。

    取消馬 (is_scratched=True) は元値を保持する。

    Parameters
    ----------
    data : dict
        pred.json の内容。{"races": [...], "_meta": {...}, ...}
    gamma : float
        シャープ化の指数。1.0 = 変換なし。
        config/settings.py の COMPOSITE_CALIBRATION_GAMMA を渡すこと。

    Returns
    -------
    dict
        in-place 変更後の data (同じオブジェクト)。
    """
    # 二重適用ガード
    meta = data.setdefault("_meta", {})
    if meta.get("composite_calibrated"):
        print("[composite_calibration] composite_calibrated フラグあり → スキップ")
        return data

    # 較正テーブルをロード
    try:
        calib_composite = load_calib_composite()
    except FileNotFoundError:
        print(
            f"[composite_calibration] 警告: {CALIB_COMPOSITE_FILE} が見つかりません。"
            " 較正をスキップします。",
            file=sys.stderr,
        )
        return data

    races = data.get("races", [])
    total_updated = 0

    for race in races:
        # ばんえい・非アクティブは除外
        if race.get("is_banei"):
            continue

        org = "JRA" if race.get("is_jra", False) else "NAR"
        horses = race.get("horses", [])
        active = [h for h in horses if not h.get("is_scratched", False)]
        if not active:
            continue

        # Step1+2: composite線形補間でbase率取得 (k_odds=0 → market_win 使わず)
        horse_calc: list[dict] = []
        for h in active:
            comp = h.get("composite") or 0.0
            interp = _interp_composite_rate(comp, org, calib_composite)
            if interp is not None:
                base_win, base_p2, base_p3 = interp
            else:
                # フォールバック: ALL の 50-54 bin
                fallback = calib_composite.get("ALL", {}).get("50-54", {})
                base_win = (fallback.get("win") or 8.9) / 100.0
                base_p2  = (fallback.get("place2") or 19.9) / 100.0
                base_p3  = (fallback.get("place3") or 31.9) / 100.0

            horse_calc.append({
                "horse_no": h.get("horse_no"),
                "raw_win":  max(0.0, base_win),
                "raw_p2":   max(0.0, base_p2),
                "raw_p3":   max(0.0, base_p3),
            })

        # Step3: gamma シャープ化
        if gamma != 1.0:
            for c in horse_calc:
                c["raw_win"] = c["raw_win"] ** gamma
                c["raw_p2"]  = c["raw_p2"]  ** gamma
                c["raw_p3"]  = c["raw_p3"]  ** gamma

        # Step4: レース内正規化 (Σwin=1, Σp2=2, Σp3=3)
        sum_win = sum(c["raw_win"] for c in horse_calc)
        sum_p2  = sum(c["raw_p2"]  for c in horse_calc)
        sum_p3  = sum(c["raw_p3"]  for c in horse_calc)

        scale_win = 1.0 / sum_win if sum_win > 0 else 1.0
        scale_p2  = 2.0 / sum_p2  if sum_p2  > 0 else 1.0
        scale_p3  = 3.0 / sum_p3  if sum_p3  > 0 else 1.0

        # step4の正規化値を horse_no でマッピング
        normalized: dict[int | str, tuple[float, float, float]] = {}
        for c in horse_calc:
            w  = min(1.0, max(0.0, c["raw_win"] * scale_win))
            p2 = min(1.0, max(0.0, c["raw_p2"]  * scale_p2))
            p3 = min(1.0, max(0.0, c["raw_p3"]  * scale_p3))

            # Step5: 個馬整合 win ≤ p2 ≤ p3
            w  = min(w, p2, p3)
            p2 = max(p2, w)
            p2 = min(p2, p3)

            normalized[c["horse_no"]] = (w, p2, p3)

        # pred.json の horse に書き込む (取消馬はスキップ)
        for h in horses:
            if h.get("is_scratched"):
                continue
            key = h.get("horse_no")
            if key not in normalized:
                continue
            w, p2, p3 = normalized[key]
            h["win_prob"]    = round(w,  6)
            h["place2_prob"] = round(p2, 6)
            h["place3_prob"] = round(p3, 6)
            total_updated += 1

    # 二重適用防止フラグを立てる
    meta["composite_calibrated"] = True
    print(
        f"[composite_calibration] 適用完了: "
        f"更新馬数={total_updated}  gamma={gamma}"
    )
    return data
