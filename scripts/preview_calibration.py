"""
表示率較正プレビュースクリプト (本番非改変・読み取り専用)

目的:
  pred.json の win_prob/place2_prob/place3_prob が過大表示されている問題を
  実データ較正テーブル(calibration_rates.json)にアンカーして正す方針の検証用。
  マスターが「アンカー種別」と「実力乖離強度 k」を決めるためのプレビュー。

注意:
  - pred.json への書き戻しは一切しない（読み取り専用）
  - 本番ファイル(config/settings.py, src/ 配下)を変更しない
  - git add / git commit は絶対にしない
"""

import sys
import io
import json
import math
from pathlib import Path

# Windows cp932 環境での日本語 print 即死を防ぐ (必須)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

# ================== ファイルパス ==================
PROJECT_ROOT = Path(__file__).parent.parent
PRED_FILE = PROJECT_ROOT / "data" / "predictions" / "20260628_pred.json"
CALIB_FILE = PROJECT_ROOT / "data" / "_diag" / "calibration_rates.json"

# ================== ビン名パーサ ==================

def parse_bin_ranges(bin_keys: list[str]) -> list[tuple[float, float, str]]:
    """
    ビン名文字列集合を「(下限, 上限, ビン名)」リストに変換する（ハードコードなし）。
    例: '<1.5' -> (0, 1.5)
        '1.5-1.9' -> (1.5, 1.9)
        '300+' -> (300, inf)
    """
    result = []
    for key in bin_keys:
        k = key.strip()
        if k.startswith('<'):
            upper = float(k[1:])
            result.append((0.0, upper, key))
        elif k.endswith('+'):
            lower = float(k[:-1])
            result.append((lower, math.inf, key))
        elif '-' in k:
            parts = k.split('-')
            lower = float(parts[0])
            upper = float(parts[1])
            result.append((lower, upper, key))
        else:
            # 単一値ビン (万一存在した場合の保険)
            v = float(k)
            result.append((v, v, key))
    return sorted(result, key=lambda x: x[0])


def find_odds_bin(odds_val: float, bin_ranges: list[tuple[float, float, str]]) -> str | None:
    """オッズ値に対応するビン名を動的に返す。該当なしはNone。"""
    for (lower, upper, name) in bin_ranges:
        if lower <= odds_val <= upper:
            return name
    # '<1.5' の場合 lower=0, upper=1.5 で 1.5 を含む設計 → 上限以上は最大ビンへ
    # 全ビンを超えた場合は最大ビンを返す（高オッズのはみ出し対策）
    if bin_ranges:
        return bin_ranges[-1][2]
    return None


# ================== ビン範囲をキャッシュ ==================
def build_bin_cache(calib: dict) -> dict:
    """
    各 org キーの odds ビン名リストを解析し、 {org: [(lower, upper, name), ...]} を返す。
    """
    cache = {}
    for org_key, org_data in calib.items():
        if org_key == '_venue_names':
            continue
        if not isinstance(org_data, dict) or 'odds' not in org_data:
            continue
        bin_keys = list(org_data['odds'].keys())
        cache[org_key] = parse_bin_ranges(bin_keys)
    return cache


# ================== 較正ロジック ==================

K_VALUES = [0.0, 0.10, 0.20]


def get_base_rate(calib: dict, bin_cache: dict, org: str, odds_val: float | None) -> dict:
    """
    org / odds_val から base_win, base_p2, base_p3 を取得する（0〜1 の小数）。
    odds が None/0 の場合は最大ビン（最も人気薄）扱い。
    org が見つからない場合は 'ALL' にフォールバック。
    """
    # odds 補正: None/0 → 非常に大きな値（最大ビン行き）
    if odds_val is None or odds_val == 0:
        odds_val = 99999.0

    # org 優先順位
    orgs_to_try = [org, 'ALL']
    for o in orgs_to_try:
        if o not in calib or 'odds' not in calib[o]:
            continue
        if o not in bin_cache:
            continue
        bin_name = find_odds_bin(odds_val, bin_cache[o])
        if bin_name is None:
            continue
        row = calib[o]['odds'].get(bin_name)
        if row is None:
            continue
        return {
            'win': row['win'] / 100.0,
            'p2': row['place2'] / 100.0,
            'p3': row['place3'] / 100.0,
            'bin': bin_name,
            'org': o,
            'n': row.get('n', 0),
        }

    # フォールバック失敗（起こらないはずだが保険）
    return {'win': 0.10, 'p2': 0.30, 'p3': 0.50, 'bin': '?', 'org': '?', 'n': 0}


def calibrate_race(race: dict, calib: dict, bin_cache: dict) -> dict[float, list[dict]]:
    """
    1 レースの全馬に対し、k=0.0/0.10/0.20 の 3 通り較正率を計算して返す。
    返り値: {k: [{horse_no, win, p2, p3}, ...]}  (win/p2/p3 は 0〜1 の小数)
    """
    org = 'JRA' if race.get('is_jra', False) else 'NAR'
    horses = [h for h in race.get('horses', []) if not h.get('is_scratched', False)]

    if not horses:
        return {k: [] for k in K_VALUES}

    # composite z-score 計算
    composites = [h.get('composite', 50.0) or 50.0 for h in horses]
    mean_c = sum(composites) / len(composites)
    sq_diff = sum((c - mean_c) ** 2 for c in composites)
    sd_c = math.sqrt(sq_diff / len(composites)) if sq_diff > 0 else 0.0

    # 各馬の base_rate 取得と z-score 格納
    horse_data = []
    for h in horses:
        odds_val = h.get('odds')
        base = get_base_rate(calib, bin_cache, org, odds_val)
        composite_val = h.get('composite', 50.0) or 50.0
        z = (composite_val - mean_c) / sd_c if sd_c > 0 else 0.0
        horse_data.append({
            'horse_no': h.get('horse_no'),
            'base': base,
            'z': z,
        })

    # k ごとに計算
    result = {}
    for k in K_VALUES:
        adjusted = []
        for hd in horse_data:
            base = hd['base']
            z = hd['z']
            factor = 1.0 + k * z
            raw_win = max(0.0, base['win'] * factor)
            raw_p2 = max(0.0, base['p2'] * factor)
            raw_p3 = max(0.0, base['p3'] * factor)
            adjusted.append({
                'horse_no': hd['horse_no'],
                'win': raw_win,
                'p2': raw_p2,
                'p3': raw_p3,
            })

        # 正規化: win=Σ1.0, p2=Σ2.0, p3=Σ3.0
        n_horses = len(adjusted)
        sum_win = sum(a['win'] for a in adjusted)
        sum_p2 = sum(a['p2'] for a in adjusted)
        sum_p3 = sum(a['p3'] for a in adjusted)

        # ゼロ除算防止
        scale_win = 1.0 / sum_win if sum_win > 0 else 1.0
        scale_p2 = 2.0 / sum_p2 if sum_p2 > 0 else 1.0
        scale_p3 = 3.0 / sum_p3 if sum_p3 > 0 else 1.0

        normalized = []
        for a in adjusted:
            w = a['win'] * scale_win
            p2 = a['p2'] * scale_p2
            p3 = a['p3'] * scale_p3

            # 個馬の確率は 0〜1 にクリップ（place3 は「3着以内」なので最大 100%）
            w = max(0.0, min(1.0, w))
            p2 = max(0.0, min(1.0, p2))
            p3 = max(0.0, min(1.0, p3))

            # 個馬整合: win <= p2 <= p3
            w = min(w, p2, p3)
            p2 = min(p2, p3)
            p2 = max(p2, w)

            normalized.append({
                'horse_no': a['horse_no'],
                'win': w,
                'p2': p2,
                'p3': p3,
            })

        result[k] = normalized

    return result


# ================== メイン ==================

def main() -> None:
    print("=" * 80)
    print("表示率較正プレビュー (本番非改変・読み取り専用)")
    print(f"pred: {PRED_FILE}")
    print(f"calib: {CALIB_FILE}")
    print("=" * 80)

    # ファイル読み込み
    with open(PRED_FILE, encoding='utf-8') as f:
        pred_data = json.load(f)
    with open(CALIB_FILE, encoding='utf-8') as f:
        calib = json.load(f)

    bin_cache = build_bin_cache(calib)

    races = pred_data.get('races', [])
    print(f"総レース数: {len(races)}\n")

    # 各レースの最大 win_prob を計算して上位3レース選出
    race_max_wins = []
    for race in races:
        active = [h for h in race.get('horses', []) if not h.get('is_scratched', False)]
        if not active:
            continue
        max_win = max(h.get('win_prob', 0) or 0 for h in active)
        race_max_wins.append((max_win, race))

    # 過大表示が目立つ順（最大 win_prob が高い順）
    race_max_wins.sort(key=lambda x: x[0], reverse=True)
    top_races = race_max_wins[:3]

    # 各レースを出力
    for idx, (max_win_prob, race) in enumerate(top_races, 1):
        venue = race.get('venue', '?')
        race_no = race.get('race_no', '?')
        org_str = 'JRA' if race.get('is_jra', False) else 'NAR'
        field_count = race.get('field_count', '?')
        race_name = race.get('race_name', '')

        print()
        print(f"{'─' * 80}")
        print(f"[{idx}] {venue} R{race_no} {race_name}  ({org_str} / {field_count}頭)")
        print(f"{'─' * 80}")

        # 較正計算
        calib_result = calibrate_race(race, calib, bin_cache)

        # k=0 の calib を horse_no でインデックス化
        calib_by_k = {}
        for k in K_VALUES:
            calib_by_k[k] = {item['horse_no']: item for item in calib_result[k]}

        # 表示対象馬（非除外・現状 win_prob 降順・上位8頭）
        active_horses = [h for h in race.get('horses', []) if not h.get('is_scratched', False)]
        active_horses.sort(key=lambda h: h.get('win_prob', 0) or 0, reverse=True)
        display_horses = active_horses[:8]

        # ヘッダ行
        header = (
            f"{'馬番':>3} {'馬名':<12} {'オッズ':>6} {'人気':>3} {'指数':>6}"
            f" │ {'現状win':>7} {'現状p2':>7} {'現状p3':>7}"
            f" │ {'k=0.00':^20}"
            f" │ {'k=0.10':^20}"
            f" │ {'k=0.20':^20}"
        )
        sep = (
            f"{'─'*3} {'─'*12} {'─'*6} {'─'*3} {'─'*6}"
            f" ┼ {'─'*7} {'─'*7} {'─'*7}"
            f" ┼ {'─'*20}"
            f" ┼ {'─'*20}"
            f" ┼ {'─'*20}"
        )
        sub_header = (
            f"{' ':>3} {' ':<12} {' ':>6} {' ':>3} {' ':>6}"
            f" │ {' ':>7} {' ':>7} {' ':>7}"
            f" │ {'win% p2%  p3%':^20}"
            f" │ {'win% p2%  p3%':^20}"
            f" │ {'win% p2%  p3%':^20}"
        )
        print(header)
        print(sub_header)
        print(sep)

        sum_current_win = 0.0

        for h in display_horses:
            hno = h.get('horse_no', '?')
            hname = h.get('horse_name', '?')
            odds_val = h.get('odds')
            pop = h.get('popularity', '?')
            comp = h.get('composite', 0)
            cur_win = (h.get('win_prob') or 0) * 100
            cur_p2 = (h.get('place2_prob') or 0) * 100
            cur_p3 = (h.get('place3_prob') or 0) * 100
            sum_current_win += h.get('win_prob') or 0

            odds_str = f"{odds_val:.1f}" if odds_val else " ─"

            # 各 k の較正値取得
            cal_strs = []
            for k in K_VALUES:
                c = calib_by_k[k].get(hno)
                if c:
                    w = c['win'] * 100
                    p2 = c['p2'] * 100
                    p3 = c['p3'] * 100
                    cal_strs.append(f"{w:5.1f} {p2:5.1f} {p3:5.1f}")
                else:
                    cal_strs.append(f"{'─':^20}")

            row = (
                f"{hno:>3} {hname:<12.12} {odds_str:>6} {pop:>3} {comp:>6.1f}"
                f" │ {cur_win:>6.1f}% {cur_p2:>6.1f}% {cur_p3:>6.1f}%"
                f" │ {cal_strs[0]}"
                f" │ {cal_strs[1]}"
                f" │ {cal_strs[2]}"
            )
            print(row)

        # レース末尾: Σwin 比較
        sum_calib_win_k0 = sum(
            item['win']
            for item in calib_by_k[0.0].values()
        )
        print()
        print(f"  現状 Σwin = {sum_current_win:.3f}  (1.0 を超えていると過大表示の証拠)")
        print(f"  較正後 Σwin (k=0.00) = {sum_calib_win_k0:.3f}  (1.0 のはず)")

    # --- 全レース統計サマリ ---
    print()
    print("=" * 80)
    print("全レース統計サマリ")
    print("=" * 80)

    all_current_wins = []
    all_calib_wins_k0 = []
    all_calib_wins_k20 = []

    for race in races:
        active = [h for h in race.get('horses', []) if not h.get('is_scratched', False)]
        if not active:
            continue
        for h in active:
            all_current_wins.append((h.get('win_prob') or 0) * 100)

        calib_result = calibrate_race(race, calib, bin_cache)
        for item in calib_result[0.0]:
            all_calib_wins_k0.append(item['win'] * 100)
        for item in calib_result[0.20]:
            all_calib_wins_k20.append(item['win'] * 100)

    def stats(vals: list[float]) -> str:
        n = len(vals)
        mean = sum(vals) / n if n else 0
        mx = max(vals) if vals else 0
        # 中央値
        s = sorted(vals)
        med = s[n // 2] if n else 0
        return f"n={n}  mean={mean:.2f}%  median={med:.2f}%  max={mx:.2f}%"

    print(f"現状 win_prob:       {stats(all_current_wins)}")
    print(f"較正後 k=0.00:       {stats(all_calib_wins_k0)}")
    print(f"較正後 k=0.20:       {stats(all_calib_wins_k20)}")

    print()
    print("=" * 80)
    print("完了 — pred.json は一切変更していません")
    print("=" * 80)


if __name__ == '__main__':
    main()
