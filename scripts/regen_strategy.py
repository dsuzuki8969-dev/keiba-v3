# -*- coding: utf-8 -*-
"""戦略再計算スクリプト — ML推論なしで印・買い目を再生成

pred.json の評価値 (composite, win_prob 等) をそのまま使い、
印付け (assign_marks) と買い目生成だけを再実行する。

用途:
  - TEKIPAN 閾値変更
  - 印ロジック修正 (formatter.py)
  - 買い目パターン変更 (engine.py の formation/ticket 関連)
  - 確率ブレンド比率変更 (ブレンド後の確率は保存済み)

使い方:
  # 2026年全日付の印・買い目を再生成
  python scripts/regen_strategy.py --year 2026

  # 全期間 (2024-2026) を一括再生成
  python scripts/regen_strategy.py --all

  # 特定日付だけ再生成
  python scripts/regen_strategy.py --dates 20260508 20260509

  # バックテスト結果も表示
  python scripts/regen_strategy.py --year 2026 --backtest

  # ドライラン (変更を保存しない)
  python scripts/regen_strategy.py --year 2026 --dry-run

所要時間: 139日分で約 9 秒 / 868日分で約 1 分 (ML推論なし)
"""
import argparse
import glob
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import PREDICTIONS_DIR


# ──────────────────────────────────────────────
# Mock オブジェクト (assign_marks 互換)
# ──────────────────────────────────────────────

class _MockHorse:
    """assign_marks() が参照する horse 属性の最小セット"""
    def __init__(self, d: dict):
        self.horse_no = d.get("horse_no", 0)
        self.horse_name = d.get("horse_name", "?")
        self.odds = d.get("odds")
        self.tansho_odds = d.get("predicted_tansho_odds")
        self.popularity = d.get("popularity")
        self.scrape_failed = False


class _MockEvaluation:
    """assign_marks() が参照する HorseEvaluation 属性の最小セット"""
    def __init__(self, d: dict):
        self.horse = _MockHorse(d)
        self._dict = d  # 元の dict への参照

        # 偏差値・確率
        self._composite = d.get("composite", 50.0) or 50.0
        self._hybrid_total = d.get("hybrid_total", self._composite)
        self.win_prob = d.get("win_prob", 0) or 0
        self.place2_prob = d.get("place2_prob", 0) or 0
        self.place3_prob = d.get("place3_prob", 0) or 0
        self.race_relative_dev = d.get("race_relative_dev", 50.0) or 50.0

        # 特選・危険
        self.is_tokusen = d.get("is_tokusen", False)
        self.tokusen_score = d.get("tokusen_score", 0) or 0
        self.is_tokusen_kiken = d.get("is_tokusen_kiken", False)

        # 取消
        self.is_scratched = d.get("is_scratched", False)

        # EV
        self.effective_odds = d.get("odds") or d.get("predicted_tansho_odds") or 0

        # 印 (リセット用)
        from src.models import Mark
        self.mark = Mark.NONE
        self._composite_snapshot = self._composite

    @property
    def composite(self):
        return self._composite

    @property
    def hybrid_total(self):
        return self._hybrid_total


def _regen_marks_for_race(horses: list, is_jra: bool) -> list:
    """1レース分の印を再割り当て (assign_marks 呼び出し)

    Returns: 更新された horse dict リスト
    """
    from src.output.formatter import assign_marks

    # dict → MockEvaluation
    mock_evs = [_MockEvaluation(h) for h in horses]

    # assign_marks 実行
    assign_marks(mock_evs, is_jra=is_jra)

    # MockEvaluation → dict に印を書き戻し
    for mev in mock_evs:
        mev._dict["mark"] = mev.mark.value if mev.mark else "-"
        mev._dict["composite"] = round(
            max(20.0, min(100.0, getattr(mev, "_composite_snapshot", mev.composite))), 2
        )

    return horses


def _regen_tickets_for_race(race: dict) -> dict:
    """1レース分の買い目を再生成 (M'戦略 + 単勝T-4 + 三連複動的F)

    assign_marks 後の race dict を受け取り、tickets 系フィールドを更新。
    """
    from src.analytics.hybrid_summary import (
        HONMEI_MARKS, TAIKOU_MARKS, RENKA_MARKS, WIDE_MARKS, OANA_MARKS,
        MARK_PRIORITY, STAKE_PER_TICKET, HOSHI_DYNAMIC_MIN_ODDS,
    )
    from itertools import combinations

    horses = race.get("horses", [])
    active = [h for h in horses if not h.get("is_scratched")]
    if len(active) < 3:
        return race

    # 購入ルール: JRA/NAR 同一 (2026-05-23 マスター指示)
    # sanrenpuku_confidence E → 三連複スキップ、tansho_confidence E → 単勝スキップ
    # ※ 旧ルール (NAR C/D スキップ) は廃止
    s_conf = race.get("sanrenpuku_confidence", "") or ""
    t_conf = race.get("tansho_confidence", "") or ""
    overall = race.get("overall_confidence", "") or ""
    # overall E (M' skip) は全券種スキップ
    if overall in ("E", "F"):
        race["formation_tickets"] = []
        race["tickets"] = []
        if "tickets_by_mode" in race:
            for k in race["tickets_by_mode"]:
                race["tickets_by_mode"][k] = []
        race["bet_decision"] = {
            "total_stake": 0,
            "ticket_count": 0,
            "skip": True,
            "skip_reason": f"overall_{overall}_skip",
        }
        return race

    # 印別グループ
    def _by_marks(marks_set):
        return sorted(
            [h for h in active if h.get("mark", "-") in marks_set],
            key=lambda h: MARK_PRIORITY.get(h.get("mark", "-"), 99),
        )

    honmei = _by_marks(HONMEI_MARKS)
    taikou = _by_marks(TAIKOU_MARKS)
    renka = _by_marks(RENKA_MARKS)
    wide = _by_marks(WIDE_MARKS)  # △★
    oana = _by_marks(OANA_MARKS)  # ☆

    # マスター提案 5 パターン (2026-05-25): 自信度別フォーメーション (検証用)
    # SS=C(4) / S=A(7) / A=B(9) / B=D(10) / C・D=E(12)
    ho_no = [h["horse_no"] for h in honmei]
    ta_no = [h["horse_no"] for h in taikou]
    re_no = [h["horse_no"] for h in renka]
    wi_no = [h["horse_no"] for h in wide]  # △ + ★
    # ☆ 動的: オッズ条件
    oa_no = [
        h["horse_no"] for h in oana
        if (h.get("odds") or h.get("predicted_tansho_odds") or 0) >= HOSHI_DYNAMIC_MIN_ODDS
    ]

    # 自信度: sanrenpuku_confidence 優先、無ければ overall_confidence (s_conf 未設定 pred.json 対応)
    conf_for_formation = s_conf if s_conf in ("SS", "S", "A", "B", "C", "D", "E") else (overall or "B")

    # マスター承認 (2026-05-25): 30 通り検証結果ベース 純利益最大組合せ
    # SS=B / S=B / A=D / B=A / C=A / D=A (+7.86M 純利益期待)
    if conf_for_formation in ("SS", "S"):
        # B案: ◎ - 〇▲△ - 〇▲△★☆ (9 点)
        col1 = ho_no
        col2 = ta_no + re_no + wi_no[:1]
        col3 = sorted(set(ta_no + re_no + wi_no + oa_no))
    elif conf_for_formation == "A":
        # D案: ◎〇 - ◎〇▲ - ◎〇▲△★☆ (10 点)
        col1 = ho_no + ta_no
        col2 = ho_no + ta_no + re_no
        col3 = sorted(set(ho_no + ta_no + re_no + wi_no + oa_no))
    else:  # B / C / D
        # A案: ◎ - 〇▲ - 〇▲△★☆ (7 点)
        col1 = ho_no
        col2 = ta_no + re_no
        col3 = sorted(set(ta_no + re_no + wi_no + oa_no))

    # D-7/G-2 ROLLBACK (2026-05-25 緊急): skip 強化全廃で現役運用復旧
    # ---- 三連複チケット生成 (sanrenpuku_confidence E のみ skip) ----
    sanren_tickets = []
    if s_conf != "E":
        if col1 and len(col2) >= 2 and len(col3) >= 3:
            seen = set()
            for a in col1:
                for b in col2:
                    if b == a:
                        continue
                    for c in col3:
                        if c == a or c == b:
                            continue
                        combo = tuple(sorted([a, b, c]))
                        if combo not in seen:
                            seen.add(combo)
                            sanren_tickets.append({
                                "type": "三連複",
                                "combo": list(combo),
                                "stake": STAKE_PER_TICKET,
                            })

    # ---- 単勝チケット生成 (◎ 単勝のみ・1点) ----
    # D-1 ROLLBACK (2026-05-25 緊急): shobu_score TOP2 → ◎単勝 (1点) に変更
    # 真因: 戦略B は 80% 無印馬 → 実 ticket と LIVE STATS (◎単勝集計) が不一致でマスター混乱
    tansho_tickets = []
    if t_conf != "E":
        honmei_h = next((h for h in active if h.get("mark") in ("◎", "◉")), None)
        if honmei_h:
            tansho_tickets.append({
                "type": "単勝",
                "horse_no": int(honmei_h.get("horse_no", 0)),
                "mark": honmei_h.get("mark", "◎"),
                "odds": float(honmei_h.get("odds") or honmei_h.get("predicted_tansho_odds") or 0),
                "stake": 100,
            })

    # 全チケット統合
    all_tickets = sanren_tickets + tansho_tickets
    race["formation_tickets"] = sanren_tickets  # 三連複のみ (互換性)
    race["tickets"] = all_tickets
    # C-1 修正 (2026-05-25): tickets_by_mode["fixed"] も同期更新して集計バグ防止
    if isinstance(race.get("tickets_by_mode"), dict):
        race["tickets_by_mode"]["fixed"] = all_tickets

    # bet_decision 更新
    total_stake = (len(sanren_tickets) * STAKE_PER_TICKET
                   + len(tansho_tickets) * 100)
    skip_reasons = []
    if s_conf == "E":
        skip_reasons.append("sanrenpuku_confidence_E")
    if t_conf == "E":
        skip_reasons.append("tansho_confidence_E")

    race["bet_decision"] = {
        "total_stake": total_stake,
        "ticket_count": len(all_tickets),
        "skip": len(all_tickets) == 0,
        "sanren_count": len(sanren_tickets),
        "tansho_count": len(tansho_tickets),
    }
    if skip_reasons:
        race["bet_decision"]["skip_reasons"] = skip_reasons

    return race


def process_one_day(pred_path: str, dry_run: bool = False) -> dict:
    """1日分の pred.json を読み込み、印・買い目を再生成

    Returns: {"date": str, "races": int, "horses": int, "elapsed": float, "marks_changed": int}
    """
    t0 = time.time()

    with open(pred_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    total_horses = 0
    marks_changed = 0

    for race in payload.get("races", []):
        # ばんえい（帯広）除外
        vc = str(race.get("venue_code", "") or "").zfill(2)
        if vc in ("52", "65"):
            continue

        horses = race.get("horses", [])
        is_jra = race.get("is_jra", False)

        # 変更前の印を記録
        old_marks = {h.get("horse_no"): h.get("mark", "-") for h in horses}

        # 印再割り当て
        _regen_marks_for_race(horses, is_jra=is_jra)

        # 買い目再生成
        _regen_tickets_for_race(race)

        # 変更カウント
        for h in horses:
            total_horses += 1
            if h.get("mark", "-") != old_marks.get(h.get("horse_no"), "-"):
                marks_changed += 1

    elapsed = time.time() - t0
    date_str = Path(pred_path).name.replace("_pred.json", "")

    if not dry_run:
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    return {
        "date": date_str,
        "races": len(payload.get("races", [])),
        "horses": total_horses,
        "elapsed": elapsed,
        "marks_changed": marks_changed,
    }


def main():
    parser = argparse.ArgumentParser(description="戦略再計算 (ML推論なし)")
    parser.add_argument("--year", type=str, help="対象年 (例: 2026)")
    parser.add_argument("--dates", nargs="+", help="対象日付 (例: 20260508 20260509)")
    parser.add_argument("--all", action="store_true", help="全期間 (2024-2026)")
    parser.add_argument("--dry-run", action="store_true", help="変更を保存しない")
    parser.add_argument("--backtest", action="store_true", help="完了後にバックテスト実行")
    args = parser.parse_args()

    # 対象ファイル収集
    pred_files = []
    if args.dates:
        for d in args.dates:
            p = os.path.join(PREDICTIONS_DIR, f"{d}_pred.json")
            if os.path.exists(p):
                pred_files.append(p)
            else:
                print(f"  ⚠ 見つからない: {p}")
    elif args.year:
        pattern = os.path.join(PREDICTIONS_DIR, f"{args.year}*_pred.json")
        pred_files = sorted(glob.glob(pattern))
        # _prev.json, .bak を除外
        pred_files = [f for f in pred_files if "_prev" not in f and ".bak" not in f]
    elif args.all:
        # 全 pred.json を収集
        all_files = sorted([
            os.path.join(PREDICTIONS_DIR, f)
            for f in os.listdir(PREDICTIONS_DIR)
            if f.endswith("_pred.json") and "_prev" not in f and ".bak" not in f
        ])
        pred_files = all_files
    else:
        parser.error("--year, --dates, または --all を指定してください")

    if not pred_files:
        print("対象ファイルが見つかりません")
        return

    total = len(pred_files)
    print(f"=== 戦略再計算 ({total}日分) {'[DRY-RUN]' if args.dry_run else ''} ===")
    print()

    total_elapsed = 0
    total_horses = 0
    total_marks_changed = 0
    errors = []

    for i, fpath in enumerate(pred_files):
        try:
            result = process_one_day(fpath, dry_run=args.dry_run)
            total_elapsed += result["elapsed"]
            total_horses += result["horses"]
            total_marks_changed += result["marks_changed"]

            pct = (i + 1) / total * 100
            bar_len = 30
            filled = int(bar_len * (i + 1) / total)
            bar = "█" * filled + "░" * (bar_len - filled)
            changed_str = f" (印変更: {result['marks_changed']})" if result["marks_changed"] > 0 else ""
            print(
                f"  [{bar}] {pct:5.1f}% {result['date']} "
                f"{result['races']}R {result['horses']}頭 "
                f"{result['elapsed']:.1f}秒{changed_str}"
            )
        except Exception as e:
            errors.append((fpath, str(e)))
            print(f"  ❌ {Path(fpath).name}: {e}")

    # サマリ
    print()
    print(f"{'=' * 60}")
    print(f"完了: {total}日 / {total_horses}頭 / {total_elapsed:.1f}秒 ({total_elapsed / 60:.1f}分)")
    print(f"印変更: {total_marks_changed}頭")
    if errors:
        print(f"エラー: {len(errors)}件")
    if args.dry_run:
        print("[DRY-RUN] ファイルは更新されていません")
    print(f"{'=' * 60}")

    # バックテスト
    if args.backtest and not args.dry_run:
        print()
        print("=== バックテスト実行中... ===")
        try:
            from src.analytics.hybrid_summary import get_hybrid_summary
            year_filter = args.year or "all"
            result = get_hybrid_summary(year_filter, force_refresh=True)

            t4 = result.get("tansho_t4", {})
            sd = result.get("sanrenpuku_dynamic", {})
            mp = result.get("m_prime_sanrenpuku", {})

            print(f"【単勝 T-4】回収率: {t4.get('roi_pct', 0):.1f}% "
                  f"({t4.get('races_hit', 0)}/{t4.get('races_played', 0)}R)")
            print(f"【三連複 動的F】回収率: {sd.get('roi_pct', 0):.1f}% "
                  f"({sd.get('races_hit', 0)}/{sd.get('races_played', 0)}R)")
            print(f"【M' 戦略】回収率: {mp.get('roi_pct', 0):.1f}% "
                  f"({mp.get('races_hit', 0)}/{mp.get('races_played', 0)}R)")

            total_stake = (t4.get("total_stake", 0) + sd.get("total_stake", 0) + mp.get("total_stake", 0))
            total_payback = (t4.get("total_payback", 0) + sd.get("total_payback", 0) + mp.get("total_payback", 0))
            if total_stake > 0:
                print(f"【合計】回収率: {total_payback / total_stake * 100:.1f}% "
                      f"収支: {total_payback - total_stake:+,}円")
        except Exception as e:
            print(f"バックテスト失敗: {e}")


if __name__ == "__main__":
    main()
