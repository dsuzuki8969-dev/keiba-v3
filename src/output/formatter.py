"""
競馬解析マスターシステム v3.0 - HTML出力
mixin パターンで分割: css / grade_helpers / past_runs / narrative / marks / betting
"""

import re as _re
import statistics
from datetime import datetime
from html import escape as _esc
from typing import Dict, List, Optional

from config.settings import TRAINING_EMOJI
from src.log import get_logger

logger = get_logger(__name__)


def _safe(s) -> str:
    """ユーザー由来文字列のHTMLエスケープ（None安全）"""
    return _esc(str(s)) if s else ""
from data.masters.venue_master import VENUE_MAP
from src.models import (
    AnaType,
    BakenType,
    ChakusaPattern,
    HorseEvaluation,
    KikenType,
    KishuPattern,
    Mark,
    RaceAnalysis,
    RaceInfo,
    Reliability,
    Trend,
)
from src.output.betting import BettingMixin
from src.output.css import CSS
from src.output.grade_helpers import GradeMixin
from src.output.marks import MarksMixin
from src.output.narrative import NarrativeMixin
from src.output.past_runs import PastRunsMixin


def find_断層(sorted_evals: List[HorseEvaluation]) -> List[int]:
    if len(sorted_evals) < 2:
        return []
    gaps = [
        sorted_evals[i].composite - sorted_evals[i + 1].composite
        for i in range(len(sorted_evals) - 1)
    ]
    avg_gap = statistics.mean(gaps) if gaps else 0
    return [i for i, g in enumerate(gaps) if g > avg_gap * 1.5 and g > 2.0]


def _scoring_value(ev: "HorseEvaluation") -> float:
    """Plan-γ Phase 3: USE_HYBRID_SCORING フラグに応じて印付与の判定値を返す。

    USE_HYBRID_SCORING=False (default): composite を使用（従来動作完全維持）
    USE_HYBRID_SCORING=True: hybrid_total = ability_total*(1-β) + race_relative_dev*β を使用
    """
    from config.settings import USE_HYBRID_SCORING
    if USE_HYBRID_SCORING:
        return ev.hybrid_total
    return ev.composite


def assign_marks(evaluations: List[HorseEvaluation], is_jra: bool = True) -> List[HorseEvaluation]:
    """印付与: ◉◎○▲△★☆×（JRA/NAR分離閾値）

    ◉/◎: composite×ML合意ベース（合意時=composite1位、不一致時=win_prob1位を優先）
    ○▲△★: composite 2-5位（各1頭、総合指数順）
    ☆: 特選穴馬（is_tokusen の未印馬、最大2頭）
    ×: 特選危険馬（is_tokusen_kiken — ML×composite二重否定、3人気以下限定）

    Plan-γ Phase 3: USE_HYBRID_SCORING=True 時はソート・gap 判定を hybrid_total ベースに切替。
    False (default) では従来通り composite を使用。
    """
    from config.settings import (
        TEKIPAN_GAP_JRA, TEKIPAN_GAP_NAR,
        TEKIPAN_WIN_PROB_JRA, TEKIPAN_WIN_PROB_NAR,
        TEKIPAN_PLACE3_PROB_JRA, TEKIPAN_PLACE3_PROB_NAR,
        USE_HYBRID_SCORING,
    )

    # composite スナップショット（JSON出力との整合性を保証）
    comp_snapshot = {id(ev): ev.composite for ev in evaluations}
    for ev in evaluations:
        ev._composite_snapshot = comp_snapshot[id(ev)]

    # 出走取消馬を除外（オッズ確定レースでodds=Noneの馬）
    _has_odds = any(getattr(ev.horse, 'odds', None) is not None for ev in evaluations)
    _scratched = set()
    if _has_odds:
        _scratched = {id(ev) for ev in evaluations
                      if getattr(ev.horse, 'odds', None) is None
                      and getattr(ev.horse, 'tansho_odds', None) is None}

    for ev in evaluations:
        ev.mark = Mark.NONE

    # 判定値（USE_HYBRID_SCORING=False: composite / True: hybrid_total）降順でソート
    # 取消馬を除外して印付け
    sorted_ev = sorted(
        [ev for ev in evaluations if id(ev) not in _scratched],
        key=_scoring_value, reverse=True,
    )

    if not sorted_ev:
        return evaluations

    # ---- Step 0: ML合意チェック + win_prob最低閾値フィルタ ----
    # composite1位とwin_prob1位が一致するか判定
    # 不一致時はwin_prob1位を◎候補に昇格（精度+23.8pt改善）
    #
    # 追加: win_prob最低閾値
    #   ◎/鉄板候補: win_prob >= 5% 必須（0.3%で◎は意味がない）
    # ◎候補の最低win_prob（これ未満はwin_prob1位に◎を譲る）
    # ただしcomposite1位は必ず○以下の印がつく（無印にはしない）
    _MIN_WP_HONMEI = 0.05

    # sorted_ev[0] が判定値（composite or hybrid_total）の1位
    comp_top = sorted_ev[0]
    wp_top = max(evaluations, key=lambda e: e.win_prob or 0)
    ml_agrees = (comp_top.horse.horse_no == wp_top.horse.horse_no)

    # 判定値1位のwin_probが最低閾値未満 → 強制的にwin_prob1位に切り替え
    _comp_top_wp = comp_top.win_prob or 0
    if _comp_top_wp < _MIN_WP_HONMEI and not ml_agrees:
        top = wp_top
        logger.info(
            "ML合議: 判定値1位のwin_prob不足で除外 %s(wp=%.1f%% < %.0f%%) → %s(wp=%.1f%%) [hybrid=%s]",
            comp_top.horse.horse_name, _comp_top_wp * 100, _MIN_WP_HONMEI * 100,
            wp_top.horse.horse_name, (wp_top.win_prob or 0) * 100, USE_HYBRID_SCORING,
        )
    else:
        # 従来ロジック: 判定値僅差かつwin_prob大幅乖離時に入れ替え
        top = comp_top
        if not ml_agrees:
            _comp_gap_top2 = (
                _scoring_value(comp_top)
                - (_scoring_value(sorted_ev[1]) if len(sorted_ev) >= 2 else 0)
            )
            _wp_ratio = (wp_top.win_prob or 0) / max(0.01, _comp_top_wp)
            # 判定値差が2pt以内 かつ win_prob比が1.5倍以上 → win_prob1位を◎に
            if _comp_gap_top2 <= 2.0 and _wp_ratio >= 1.5:
                top = wp_top
                logger.info(
                    "ML合議: win_prob1位を◎に昇格 %s(wp=%.1f%%) > %s(score=%.1f, gap=%.1f) [hybrid=%s]",
                    wp_top.horse.horse_name, (wp_top.win_prob or 0) * 100,
                    comp_top.horse.horse_name, _scoring_value(comp_top), _comp_gap_top2,
                    USE_HYBRID_SCORING,
                )
            else:
                logger.debug(
                    "ML不一致(判定値優先): 1位=%s(%.1f) vs win_prob1位=%s(%.4f) [hybrid=%s]",
                    comp_top.horse.horse_name, _scoring_value(comp_top),
                    wp_top.horse.horse_name, wp_top.win_prob or 0, USE_HYBRID_SCORING,
                )

    # ---- Step 1: ◉ or ◎ ----
    tekipan_gap = TEKIPAN_GAP_JRA if is_jra else TEKIPAN_GAP_NAR
    tekipan_wp = TEKIPAN_WIN_PROB_JRA if is_jra else TEKIPAN_WIN_PROB_NAR
    tekipan_p3 = TEKIPAN_PLACE3_PROB_JRA if is_jra else TEKIPAN_PLACE3_PROB_NAR
    from config.settings import (
        TEKIPAN_MIN_EV_JRA, TEKIPAN_MIN_EV_NAR,
        TEKIPAN_POP_MAX_JRA, TEKIPAN_POP_MAX_NAR,
    )
    tekipan_min_ev = TEKIPAN_MIN_EV_JRA if is_jra else TEKIPAN_MIN_EV_NAR
    tekipan_pop_max = TEKIPAN_POP_MAX_JRA if is_jra else TEKIPAN_POP_MAX_NAR

    second = sorted_ev[1] if len(sorted_ev) >= 2 else None
    # gap計算は判定値（composite or hybrid_total）ベース
    gap = (_scoring_value(top) - _scoring_value(second)) if second else 99.0
    if gap < 0:
        # win_prob1位に切り替えた場合、gapは負になりうる → 判定値1位基準のgapを使用
        gap = (_scoring_value(comp_top) - _scoring_value(second)) if second else 99.0

    # EV条件（v4: 撤廃済み — TEKIPAN_MIN_EV=0.0）
    eff_odds = top.effective_odds
    top_ev = (top.win_prob or 0) * eff_odds if eff_odds and eff_odds > 0 else 1.0
    ev_ok = top_ev >= tekipan_min_ev if tekipan_min_ev > 0 else True

    # 人気条件（v4新設: 市場との合意確認）
    top_pop = top.horse.popularity or 99
    pop_ok = top_pop <= tekipan_pop_max

    is_tekipan = (
        gap >= tekipan_gap
        and (top.win_prob or 0) >= tekipan_wp
        and getattr(top, "place3_prob", 0) >= tekipan_p3
        and ev_ok
        and pop_ok
    )
    if not pop_ok and gap >= tekipan_gap and (top.win_prob or 0) >= tekipan_wp:
        logger.info(
            "◉→◎降格(人気超過): %s %d番人気 > %d (gap=%.1f wp=%.1f%%)",
            top.horse.horse_name, top_pop, tekipan_pop_max,
            gap, (top.win_prob or 0) * 100,
        )
    top.mark = Mark.TEKIPAN if is_tekipan else Mark.HONMEI

    # ---- Step 2: ○▲△★ — composite順で次の未印馬に1頭ずつ ----
    # ルール: ◎○▲△★の5印は必ず全て付与する（欠落禁止）
    # 整合性ルール: wp(勝率)が極端に低い馬は上位印(○▲△)をスキップ
    #   → composite高くてもwp低い＝穴馬(★/☆)であり対抗(○)ではない
    #   例: composite1位 wp=0.27%(10番人気)に○は不整合 → ★に回す
    _MIN_WP_TAIKOU = 0.02   # ○(対抗): wp >= 2% 必須
    _MIN_WP_TANNUKE = 0.01  # ▲(単穴): wp >= 1% 必須
    _MIN_WP_RENDASHI = 0.005  # △(連下): wp >= 0.5% 必須
    # ★(連下2): wp下限なし（composite高+wp低=穴馬推奨の受け皿）
    _wp_floors = {
        Mark.TAIKOU: _MIN_WP_TAIKOU,
        Mark.TANNUKE: _MIN_WP_TANNUKE,
        Mark.RENDASHI: _MIN_WP_RENDASHI,
        Mark.RENDASHI2: 0,
    }

    # win_prob上位なのにcomposite圏外の馬を★枠に救済
    _wp_sorted = sorted(
        [ev for ev in sorted_ev if ev.mark == Mark.NONE],
        key=lambda e: e.win_prob or 0, reverse=True,
    )
    _wp_rescue = None
    for _wp_cand in _wp_sorted[:2]:  # win_prob上位2位まで救済対象
        _wp_cand_comp_rank = next(
            (i for i, ev in enumerate(sorted_ev) if ev.horse.horse_no == _wp_cand.horse.horse_no), 99
        )
        # composite6位以下で、かつwin_prob >= 10% → ★枠で救済
        if _wp_cand_comp_rank >= 5 and (_wp_cand.win_prob or 0) >= 0.10:
            _wp_rescue = _wp_cand
            break  # 最初に見つかった圏外馬を救済

    marks_to_assign = [Mark.TAIKOU, Mark.TANNUKE, Mark.RENDASHI, Mark.RENDASHI2]
    for mark in marks_to_assign:
        # ★枠はwp_rescue馬に割り当て
        if mark == Mark.RENDASHI2 and _wp_rescue and _wp_rescue.mark == Mark.NONE:
            _wp_rescue.mark = mark
            logger.debug("★枠wp_rescue: %s", _wp_rescue.horse.horse_name)
            continue
        _wp_floor = _wp_floors.get(mark, 0)
        assigned = False
        for ev in sorted_ev:
            if ev.mark == Mark.NONE:
                # wp整合性チェック: wp不足の馬は上位印をスキップ → 後の低印で回収
                if _wp_floor > 0 and (ev.win_prob or 0) < _wp_floor:
                    logger.info(
                        "印wpガード: %s wp=%.2f%% < %.1f%% → %s見送り(後の印で回収)",
                        ev.horse.horse_name, (ev.win_prob or 0) * 100,
                        _wp_floor * 100, mark.value,
                    )
                    continue
                ev.mark = mark
                logger.debug("印付与: %s → %s (composite=%.1f wp=%.2f%%)",
                             ev.horse.horse_name, mark.value, ev.composite, (ev.win_prob or 0) * 100)
                assigned = True
                break
        if not assigned:
            logger.warning("印付与失敗: %s — wp条件を満たす未印馬がいない(sorted_ev=%d)", mark.value, len(sorted_ev))

    # ---- Step 2b: 5印完備保証（絶対ルール） ----
    # ◎○▲△★の5印は必ず全て存在しなければならない
    # Step 2で付けられなかった印がある場合、composite順で強制付与
    _all_5_marks = [Mark.TEKIPAN, Mark.HONMEI, Mark.TAIKOU, Mark.TANNUKE, Mark.RENDASHI, Mark.RENDASHI2]
    _assigned = {ev.mark for ev in sorted_ev}
    # ◎/◉は同枠（どちらかあればOK）
    _has_honmei = Mark.TEKIPAN in _assigned or Mark.HONMEI in _assigned
    _check_marks = [Mark.TAIKOU, Mark.TANNUKE, Mark.RENDASHI, Mark.RENDASHI2]
    for _req_mark in _check_marks:
        if _req_mark in _assigned:
            continue
        # 未印馬のうちcomposite最上位に強制付与
        for ev in sorted_ev:
            if ev.mark == Mark.NONE:
                ev.mark = _req_mark
                _assigned.add(_req_mark)
                logger.info(
                    "5印保証: %s に %s を強制付与 (composite=%.1f)",
                    ev.horse.horse_name, _req_mark.value, ev.composite,
                )
                break
        else:
            # 全馬に印が付いている場合でも★は必ず付ける
            # → composite最下位の印付き馬の印を入れ替えはしない（ログ警告のみ）
            logger.warning("5印保証: %s を付与する未印馬がいない", _req_mark.value)

    # ---- Step 3: ☆ — 特選穴馬（is_tokusen の未印馬、最大1頭） ----
    # マスター指示 2026-04-22: ☆ は 1頭のみ（穴馬代表）
    tokusen_cands = sorted(
        [ev for ev in sorted_ev if ev.is_tokusen and ev.mark == Mark.NONE],
        key=lambda e: e.tokusen_score,
        reverse=True,
    )
    for ev in tokusen_cands[:1]:
        ev.mark = Mark.ANA

    # ---- Step 3b: 印付与拡張（廃止） ----
    # マスター指示 2026-04-22: ☆ は 1頭のみ。補助印としての ☆ 追加付与は禁止。
    # Phase 3（三連単フォーメーション）では rank3 に「同断層内の無印馬」を自動で拾うため
    # 補助印は不要になった（generate_sanrentan_formation 参照）。

    # ---- Step 4: × — 特選危険馬（未印の馬のみ。5印を絶対に上書きしない） ----
    _protected = {Mark.TEKIPAN, Mark.HONMEI, Mark.TAIKOU, Mark.TANNUKE, Mark.RENDASHI, Mark.RENDASHI2}
    for ev in sorted_ev:
        if getattr(ev, "is_tokusen_kiken", False) and ev.mark not in _protected:
            ev.mark = Mark.KIKEN

    return evaluations


class HTMLFormatter(GradeMixin, PastRunsMixin, NarrativeMixin, MarksMixin, BettingMixin):
    def __init__(self, std_calc=None):
        self._std_calc = std_calc  # 芝含む全サーフェスの走破偏差値計算用

    def render(self, analysis: RaceAnalysis) -> str:
        import json as _json

        r = analysis.race
        # 軸馬（◉ or ◎、なければ総合1位）
        honmei_ev = next(
            (ev for ev in analysis.sorted_evaluations if ev.mark in (Mark.TEKIPAN, Mark.HONMEI)),
            analysis.sorted_evaluations[0] if analysis.sorted_evaluations else None,
        )
        meta = {
            "venue": r.venue,
            "race_no": r.race_no,
            "race_name": r.race_name or "",
            "post_time": r.post_time or "",
            "surface": getattr(r.course, "surface", "") if r.course else "",
            "distance": getattr(r.course, "distance", 0) if r.course else 0,
            "head_count": r.field_count,
            "grade": r.grade or "",
            "overall_confidence": analysis.overall_confidence.value
            if analysis.overall_confidence
            else "B",
            "confidence_score": round(analysis.confidence_score, 3),
            "honmei_no": honmei_ev.horse.horse_no if honmei_ev else 0,
            "honmei_name": honmei_ev.horse.horse_name if honmei_ev else "",
            "honmei_mark": honmei_ev.mark.value if honmei_ev else "",
            "honmei_composite": round(honmei_ev.composite, 1) if honmei_ev else 0,
            "honmei_win_pct": round(honmei_ev.win_prob * 100, 1) if honmei_ev else 0,
            "honmei_rentai_pct": round(honmei_ev.place2_prob * 100, 1) if honmei_ev else 0,
            "honmei_fukusho_pct": round(honmei_ev.place3_prob * 100, 1) if honmei_ev else 0,
        }
        meta_tag = (
            f'<script type="application/json" id="race-meta">'
            f"{_json.dumps(meta, ensure_ascii=False)}"
            f"</script>\n"
        )
        return "\n".join(
            [
                self._header(analysis.race),
                meta_tag,
                self._level1(analysis),
                self._level3(analysis),
                self._level4(analysis),
                self._level6_predicted_odds(analysis),
                self._footer(),
            ]
        )

    def _header(self, r: RaceInfo) -> str:
        return f"""<!DOCTYPE html>
<html lang="ja"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{r.venue} {r.race_no}R {r.race_name} - D-AIkeiba</title>
<style>{CSS}</style></head>
<body>
<div class="wrap">"""

    @staticmethod
    def _coverage_grade(umaren: list, sanren: list) -> tuple:
        """
        馬連+三連複 上位5点 正規化出現率合計 → SS/S/A/B/C グレード

        閾値（40/60ブレンド・91レース分布）:
          SS: 69%以上  S: 57-69%  A: 44-57%  B: 34-44%  C: 34%未満
        Returns: (grade_str, umaren_pct, sanren_pct, total_pct)
        """
        u_pct = sum(r["prob"] for r in umaren[:5]) * 100 if umaren else 0.0
        s_pct = sum(r["prob"] for r in sanren[:5]) * 100 if sanren else 0.0
        total = round(u_pct + s_pct, 2)
        if total >= 69:
            grade = "SS"
        elif total >= 57:
            grade = "S"
        elif total >= 44:
            grade = "A"
        elif total >= 34:
            grade = "B"
        else:
            grade = "C"
        return grade, u_pct, s_pct, total

    def _level6_predicted_odds(self, a: RaceAnalysis) -> str:
        """確率・出現率表示（予想オッズは非表示・内部処理のみ）"""
        has_probs = any(ev.win_prob for ev in a.evaluations)
        if not has_probs:
            return ""

        # ── 出現率ベース信頼度グレード ──
        grade, u_pct, s_pct, total_pct = self._coverage_grade(
            a.predicted_odds_umaren, a.predicted_odds_sanrenpuku
        )
        grade_cls = self._conf_cls(grade)
        grade_badge = (
            f'<span class="badge b-{grade_cls}">&nbsp;{grade}&nbsp;</span>'
            f'<span style="font-size:11px;color:var(--muted);margin-left:6px">'
            f'三連複{s_pct:.1f}% + 馬連{u_pct:.1f}% = {total_pct:.1f}%</span>'
        )

        # ── 各馬 確率テーブル（実オッズあれば表示、予想オッズは非表示）──
        has_actual_odds = any(ev.horse.odds for ev in a.evaluations)
        # 順位色用
        _w_vals = sorted([(e.win_prob or 0) for e in a.evaluations], reverse=True)
        _p2_vals = sorted([(e.place2_prob or 0) for e in a.evaluations], reverse=True)
        _p3_vals = sorted([(e.place3_prob or 0) for e in a.evaluations], reverse=True)
        _w_avg = sum(_w_vals) / len(_w_vals) if _w_vals else 0
        _p2_avg = sum(_p2_vals) / len(_p2_vals) if _p2_vals else 0
        _p3_avg = sum(_p3_vals) / len(_p3_vals) if _p3_vals else 0
        rows = ""
        sorted_ev = sorted(a.evaluations, key=lambda e: -(e.win_prob or 0))
        for ev in sorted_ev:
            wp  = (ev.win_prob    or 0) * 100
            t2p = (ev.place2_prob or 0) * 100
            t3p = (ev.place3_prob or 0) * 100
            mk  = ev.mark.value
            odds_td = ""
            if has_actual_odds:
                ao = ev.horse.odds
                odds_td = f'<td class="num">{"%.1f" % ao if ao else "—"}</td>'
            wc = self._prob_rank_color(ev.win_prob or 0, _w_vals, _w_avg)
            p2c = self._prob_rank_color(ev.place2_prob or 0, _p2_vals, _p2_avg)
            p3c = self._prob_rank_color(ev.place3_prob or 0, _p3_vals, _p3_avg)
            rows += f"""<tr>
<td><span class="m-{mk}">{mk}</span> {ev.horse.horse_no}</td>
<td>{ev.horse.horse_name}</td>
<td class="num" style="color:{wc};font-weight:700">{wp:.1f}%</td>
<td class="num" style="color:{p2c};font-weight:700">{t2p:.1f}%</td>
<td class="num" style="color:{p3c};font-weight:700">{t3p:.1f}%</td>
{odds_td}
</tr>"""

        odds_th = "<th>実ｵｯｽﾞ</th>" if has_actual_odds else ""
        tansho_table = f"""
<table class="pred-table">
<thead><tr>
<th>馬番</th><th>馬名</th>
<th>勝率</th><th>連対率</th><th>複勝率</th>
{odds_th}
</tr></thead>
<tbody>{rows}</tbody>
</table>"""

        # ── 馬連 出現率 TOP10 ──
        umaren_html = ""
        if a.predicted_odds_umaren:
            um_rows = ""
            for bet in a.predicted_odds_umaren[:10]:
                um_rows += f"""<tr>
<td>{bet['a']}-{bet['b']}</td>
<td>{bet.get('name_a','')}-{bet.get('name_b','')}</td>
<td class="num">{bet['prob']*100:.2f}%</td>
</tr>"""
            umaren_html = f"""
<div class="subsection-title">■ 馬連 出現率 TOP10</div>
<table class="pred-table compact">
<thead><tr><th>組合せ</th><th>馬名</th><th>出現率</th></tr></thead>
<tbody>{um_rows}</tbody>
</table>"""

        # ── 三連複 出現率 TOP10 ──
        sanren_html = ""
        if a.predicted_odds_sanrenpuku:
            sr_rows = ""
            for bet in a.predicted_odds_sanrenpuku[:10]:
                sr_rows += f"""<tr>
<td>{bet['a']}-{bet['b']}-{bet['c']}</td>
<td class="num">{bet['prob']*100:.3f}%</td>
</tr>"""
            sanren_html = f"""
<div class="subsection-title">■ 三連複 出現率 TOP10</div>
<table class="pred-table compact">
<thead><tr><th>組合せ</th><th>出現率</th></tr></thead>
<tbody>{sr_rows}</tbody>
</table>"""

        return f"""
<div class="card">
<div class="section-title">■ 確率・出現率 &nbsp;{grade_badge}</div>
<style>
.pred-table {{width:100%;border-collapse:collapse;font-size:13px}}
.pred-table th,.pred-table td {{padding:4px 8px;border-bottom:1px solid var(--border);text-align:left}}
.pred-table th {{background:var(--bg-alt);font-weight:600;font-size:11px}}
.pred-table .num {{text-align:right;font-variant-numeric:tabular-nums}}
.pred-table.compact td {{padding:3px 6px;font-size:12px}}
.subsection-title {{font-size:13px;font-weight:600;margin:16px 0 6px;padding-top:8px;border-top:1px solid var(--border)}}
</style>
{tansho_table}
{umaren_html}
{sanren_html}
</div>"""

    def _footer(self) -> str:
        return """<div style="text-align:center;color:var(--muted);font-size:11px;
margin-top:24px;padding:16px;border-top:1px solid var(--border)">
D-AIkeiba　|　計算層：プログラム / 分析層：AI
</div></div></body></html>"""

    @staticmethod
    def _md_to_html(text: str) -> str:
        """pace_comment の markdown風テキストをHTML化（**太字** → <b>、改行 → <br>）

        XSS対策: 入力テキストを先にHTMLエスケープしてから markdown 変換する。
        ただし元の構造を保持するため `**` パターンは escape 後の `**` をマッチ対象とする
        （escape は `*` を変換しないため安全）。
        """
        if not text:
            return ""
        text = _esc(text)
        text = _re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
        text = text.replace("\n", "<br>")
        return text

    # ---- レベル1 ----
    def _level1(self, a: RaceAnalysis) -> str:
        r = a.race
        c = a.race.course
        if c is None:
            # コース情報不完全 — 最小限の表示で返す
            return f'<div class="card"><div class="rt">{r.race_date} {r.venue} {r.race_no}R</div></div>'
        dt = r.race_date.replace("-", "/")
        dir_s = "右" if c.direction == "右" else "左"
        io_s = f"・{c.inside_outside}回り" if c.inside_outside not in ("なし", "") else ""
        # G1/G2/G3はバッジ、それ以外（OP/L/3勝/NAR等）はテキスト表示
        if r.grade in ("G1", "G2", "G3"):
            grade_b = f'<span class="badge b-S">{r.grade}</span>'
        elif r.grade:
            grade_b = f'<span class="grade-text" style="font-size:14px;color:var(--muted)">{r.grade}</span>'
        else:
            grade_b = ""

        # 馬場状態（芝・ダート両方常に表示）
        ts = f"芝：{r.track_condition_turf or '—'}" + (
            f"（含水率{r.moisture_turf:.1f}%）" if r.moisture_turf else ""
        )
        ds = f"ダート：{r.track_condition_dirt or '—'}" + (
            f"（含水率{r.moisture_dirt:.1f}%）" if r.moisture_dirt else ""
        )
        cv = f"　CV値：{r.cv_value:.1f}" if r.cv_value else ""
        track = f"""
<div class="sub-title">■ 馬場状態・トラックバイアス</div>
<div class="kv"><span class="k">馬場</span><span>{ts}　{ds}{cv}</span></div>
<div class="kv"><span class="k">有利な枠順</span><span>{a.favorable_gate or "—"}</span></div>
<div class="kv"><span class="k">有利な脚質</span><span>{a.favorable_style or "—"}</span></div>
<div class="kv"><span class="k">有利脚質の根拠</span><span style="font-size:12px;color:var(--muted)">{a.favorable_style_reason or "—"}</span></div>"""

        def nos(lst):
            return (
                "".join(f'<span class="hno">{n}</span>' for n in lst)
                or '<span style="color:var(--muted);font-size:12px">なし</span>'
            )

        pv = a.pace_type_predicted.value if a.pace_type_predicted else "不明"
        pc = a.pace_reliability.value

        # 前半3F・後半3Fの推定（実測ベースのみ表示。なければ非表示）
        has_real_3f = a.estimated_front_3f is not None and a.estimated_last_3f is not None
        front_3f_s = f"{a.estimated_front_3f:.1f}秒" if has_real_3f else "—"
        last_3f_s = f"{a.estimated_last_3f:.1f}秒" if has_real_3f else "—"

        # ネット競馬風レース情報（1行表示）
        baba_parts = []
        if r.track_condition_turf:
            baba_parts.append(f"芝:{r.track_condition_turf}")
        if r.track_condition_dirt:
            baba_parts.append(f"ダ:{r.track_condition_dirt}")
        baba_s = " ".join(baba_parts) if baba_parts else "—"
        tenkou = (
            "☀"
            if not (r.track_condition_turf or r.track_condition_dirt)
            or "良" in (r.track_condition_turf or "")
            or "良" in (r.track_condition_dirt or "")
            else "☁"
        )
        post_t = r.post_time or "—"
        post_part = f"{post_t}発走 / " if post_t != "—" else ""

        # 内/外のみ「右内回り」のように表示。「なし」のときは「右回り」にし「右な回り」を防ぐ
        io_char = (c.inside_outside if c.inside_outside in ("内", "外") else "") or ""
        race_info_line = (
            f"{post_part}{c.surface}{c.distance}m ({dir_s}{io_char}回り) / "
            f"天候:{tenkou} / 馬場:{baba_s}"
        )

        fc = getattr(c, "first_corner", None) or ""
        first_corner_disp = f"{fc}{'m' if (isinstance(fc, str) and fc.strip().isdigit()) else ''}"

        base_url = "https://race.netkeiba.com" if r.is_jra else "https://nar.netkeiba.com"
        odds_url = f"{base_url}/odds/index.html?race_id={r.race_id}"
        result_url = f"{base_url}/race/result.html?race_id={r.race_id}"
        movie_url = f"{base_url}/race/movie.html?race_id={r.race_id}"
        _btn = "display:inline-block;font-size:12px;font-weight:600;color:#fff;text-decoration:none;border-radius:4px;padding:3px 12px;margin-right:6px"
        return f"""
<div class="race-title">
  <h1>{r.race_no}R {r.race_name} {grade_b}</h1>
  <div class="race-info-line">{race_info_line}</div>
  <div class="sub">{dt} {r.condition or "—"} 出走{r.field_count}頭</div>
  <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">
    <a href="{odds_url}" target="_blank" rel="noopener" style="{_btn};background:#16a34a">オッズ取得</a>
    <a href="{result_url}" target="_blank" rel="noopener" style="{_btn};background:#1a6fa8">レース結果</a>
    <a href="{movie_url}" target="_blank" rel="noopener" style="{_btn};background:#c0392b">レース映像</a>
  </div>
</div>
<div class="card">
<div class="section-title">■ レース概要</div>
{track}
<div class="sub-title">■ コース特性</div>
<div class="kv"><span class="k">直線</span><span>{c.straight_m}m（{c.slope_type}）</span></div>
<div class="kv"><span class="k">コーナー</span><span>{c.corner_type}　×{c.corner_count}回</span></div>
<div class="kv"><span class="k">スタート〜初角</span><span>{first_corner_disp or "—"}</span></div>
<div class="sub-title">■ 展開予測 <span class="badge b-{self._conf_cls(pc)}">&nbsp;展開精度 {pc}&nbsp;</span></div>
<div class="kv"><span class="k">ペース予測</span><span class="badge b-pace">{pv}</span></div>
<div class="kv"><span class="k">前半の入り(前半3F)</span><span>{front_3f_s} 想定</span></div>
<div class="kv"><span class="k">求められる末脚(後半3F)</span><span>{last_3f_s} 想定</span></div>
<div class="pg-label">[逃げ]</div><div class="pg">{nos(a.leading_horses)}</div>
<div class="pg-label">[好位]</div><div class="pg">{nos(a.front_horses)}</div>
<div class="pg-label">[中団]</div><div class="pg">{nos(a.mid_horses)}</div>
<div class="pg-label">[後方]</div><div class="pg">{nos(a.rear_horses)}</div>
</div>"""

    # ---- レベル2 ----
    @staticmethod
    def _prob_rank_color(val: float, sorted_desc: list, avg: float) -> str:
        """確率値の順位ベース色: 1位=緑, 2位=青, 3位=赤, 平均以上=黒, 平均以下=灰"""
        if sorted_desc and val >= sorted_desc[0] - 1e-9:
            return "#16a34a"
        if len(sorted_desc) >= 2 and val >= sorted_desc[1] - 1e-9:
            return "#1a6fa8"
        if len(sorted_desc) >= 3 and val >= sorted_desc[2] - 1e-9:
            return "#c0392b"
        if val >= avg:
            return "#333"
        return "#aaa"

    def _level2(self, a: RaceAnalysis) -> str:
        _half_l2 = len(a.evaluations) // 2 if a.evaluations else 8

        rows = ""
        for ev in sorted(a.evaluations, key=lambda e: e.horse.horse_no):
            h = ev.horse
            mk = ev.mark.value
            rank = len([e for e in a.evaluations if e.composite > ev.composite]) + 1
            if h.odds:
                pop_s = f"({h.popularity}人気)" if h.popularity else ""
                odds_s = f"{h.odds:.1f}倍{pop_s}"
            elif ev.predicted_odds:
                _pred_list = sorted([e.predicted_odds for e in a.evaluations if e.predicted_odds])
                _pred_rk = (
                    _pred_list.index(ev.predicted_odds) + 1
                    if ev.predicted_odds in _pred_list
                    else "?"
                )
                odds_s = f'<span style="color:var(--muted);font-size:11px">[想定]</span>{ev.predicted_odds:.1f}倍({_pred_rk}人気)'
            else:
                odds_s = "—"
            # 騎手・性齢・斤量
            _wkg = f"{h.weight_kg:.0f}kg" if h.weight_kg else "—"
            jockey_sa = f"{h.sex}{h.age} {_wkg} {h.jockey or '—'}"
            # 枠色クラス (1-8枠)
            wk_cls = f"wk{h.gate_no}" if 1 <= h.gate_no <= 8 else "wk1"
            # 順位色 (1-3位) — .rank-1/.rank-2/.rank-3 (緑/青/赤) に統一
            rank_cls = f"rank-{rank}" if rank <= 3 else ""
            rank_s = f'<span class="{rank_cls}">{rank}位</span>' if rank_cls else f"{rank}位"
            # composite順位連動色（勝率/連対率/複勝率すべて同じ色）
            if rank == 1:
                _rc = "#16a34a"
            elif rank == 2:
                _rc = "#1a6fa8"
            elif rank == 3:
                _rc = "#c0392b"
            elif rank <= _half_l2:
                _rc = "#333"
            else:
                _rc = "#aaa"
            wc = p2c = p3c = _rc
            rows += f"""<tr>
<td style="text-align:center"><span class="waku {wk_cls}">{h.gate_no}</span></td>
<td style="text-align:center"><span class="uma {wk_cls}">{h.horse_no}</span></td>
<td style="text-align:center"><span class="m-{mk}">{mk}</span></td>
<td>{h.horse_name}</td>
<td style="font-size:12px;color:var(--muted)">{jockey_sa}</td>
<td style="font-weight:700">{ev.composite:.1f} <span style="font-size:11px;color:var(--muted);font-weight:400">({rank_s})</span></td>
<td data-live-odds="{h.horse_no}">{odds_s}</td>
<td style="color:{wc};font-weight:700">{ev.win_prob * 100:.1f}%</td><td style="color:{p2c};font-weight:700">{ev.place2_prob * 100:.1f}%</td><td style="color:{p3c};font-weight:700">{ev.place3_prob * 100:.1f}%</td>
</tr>"""
        return f"""
<div class="card">
<div class="section-title">■ 全馬一覧</div>
<table><thead><tr>
<th>枠</th><th>番</th><th>印</th><th>馬名</th><th>騎手・性齢・斤量</th>
<th>総合</th><th>オッズ(人気)</th><th>勝率</th><th>連対率</th><th>複勝率</th>
</tr></thead><tbody>{rows}</tbody></table>
</div>"""

    # ---- レベル3 ----
    def _level3(self, a: RaceAnalysis) -> str:
        items = ""
        for ev in sorted(a.evaluations, key=lambda e: e.horse.horse_no):
            mk = ev.mark
            cls = "hd-item"
            if mk == Mark.TEKIPAN:
                cls += " top"
            elif mk == Mark.HONMEI:
                cls += " honmei"
            elif mk == Mark.ANA:
                cls += " oana"
            elif ev.kiken_type != KikenType.NONE:
                cls += " kiken"
            summary = self._hcard_summary(ev, a.race.field_count, a.evaluations, a.race)
            detail = self._hcard(ev, a.race.field_count, a.evaluations, race=a.race)
            items += f'<details class="{cls}"><summary class="hd-sum">{summary}</summary><div class="hd-body">{detail}</div></details>\n'
        return f"""
<div class="card">
<div class="section-title">■ 全頭評価（馬名クリックで詳細展開）</div>
<div class="hd-list">{items}</div>
</div>"""

    def _hcard(self, ev: HorseEvaluation, fc: int, all_ev: list = None, race=None) -> str:
        h = ev.horse
        mk = ev.mark.value

        cls = "hc"
        if ev.mark == Mark.TEKIPAN:
            cls += " top"
        elif ev.mark == Mark.HONMEI:
            cls += " honmei"
        elif ev.mark == Mark.ANA:
            cls += " oana"
        elif ev.kiken_type != KikenType.NONE:
            cls += " kiken"

        rel = ev.ability.reliability.value
        tv = ev.ability.trend.value
        # トレンド: 上昇=緑, 安定/横ばい=黒, 下降=灰
        if "上昇" in tv:
            tc = "#16a34a"
        elif "下降" in tv:
            tc = "var(--muted)"
        else:
            tc = "var(--text)"
        if h.odds:
            odds_s = f"{h.odds:.1f}倍({h.popularity}人気)" if h.popularity else f"{h.odds:.1f}倍"
        elif ev.predicted_odds:
            pred_odds_list = sorted(
                [e.predicted_odds for e in (all_ev or [ev]) if e.predicted_odds], reverse=False
            )
            pred_rank = (
                pred_odds_list.index(ev.predicted_odds) + 1
                if ev.predicted_odds in pred_odds_list
                else "?"
            )
            odds_s = f'<span style="color:var(--muted);font-size:11px">[想定]</span>{ev.predicted_odds:.1f}倍({pred_rank}人気)'
        else:
            odds_s = "—"

        # 馬体重（当日確定前は前走体重でフォールバック表示）
        # 馬体重変化: ±0-4=緑, ±5-9=青, ±10+=赤
        wt_s = "ー"
        if h.horse_weight is not None and h.weight_change is not None:
            sgn = "+" if h.weight_change >= 0 else ""
            _abs_wc = abs(h.weight_change)
            if _abs_wc <= 4:
                _wc_color = "#16a34a"
            elif _abs_wc <= 9:
                _wc_color = "#2563eb"
            else:
                _wc_color = "#dc2626"
            wt_s = f'{h.horse_weight}kg(<span style="color:{_wc_color};font-weight:700">{sgn}{h.weight_change}</span>)'
        elif h.horse_weight is not None:
            wt_s = f"{h.horse_weight}kg"
        else:
            prev_wt = next((r.horse_weight for r in (h.past_runs or []) if r.horse_weight), None)
            if prev_wt:
                wt_s = f"前走{prev_wt}kg"

        # 騎手偏差値（グレード表示）— Phase 12 ハイブリッド方式
        jd = "—"
        jdv = getattr(ev, "_jockey_dev", None)
        if jdv is not None:
            mom = ev.jockey_stats.get_momentum_flag(False) if ev.jockey_stats else ""
            jg = self._dev_to_grade(jdv)
            jgc = self._grade_css(jg)
            jname_disp = h.jockey or ""
            jd_title = "算出基準：ローリング統計の複勝率ファクター加重平均偏差値"
            jd = (
                f'<span title="{jd_title}">'
                f'{jname_disp}　<span class="{jgc}" style="font-size:13px">{jg}</span>'
                f' <span style="font-size:10px;color:var(--muted)">({jdv:.1f})</span>'
                + (f"　{mom}" if mom else "")
                + "</span>"
            )
        elif ev.jockey_stats:
            jname_disp = h.jockey or ""
            jd = f'{jname_disp}　<span style="color:var(--muted)">—</span>'

        # 乗り替わり
        ch_s = ""
        if h.is_jockey_change and ev.jockey_change_pattern:
            ch_s = f"{h.prev_jockey}→{h.jockey}（{ev.jockey_change_pattern.value}）"
        elif h.is_jockey_change:
            ch_s = f"{h.prev_jockey}→{h.jockey}"

        # 調教師偏差値 — Phase 12 ハイブリッド偏差値
        tr_s = "—"
        _tdv_card = getattr(ev, "_trainer_dev", None)
        if _tdv_card is not None:
            _rk_g = self._dev_to_grade(_tdv_card)
            _rk_gc = self._grade_css(_rk_g)
            tname_disp = h.trainer or ""
            _kt = ev.trainer_stats.kaisyu_type.value if ev.trainer_stats else "—"
            tr_title = (
                f"調教師：{tname_disp}｜"
                f"偏差値：{_tdv_card:.1f}（ファクター加重平均）｜"
                f"回収タイプ：{_kt}"
            )
            tr_s = (
                f'<span title="{tr_title}">'
                f'{tname_disp}　<span class="{_rk_gc}" style="font-size:13px">{_rk_g}</span>'
                f' <span style="font-size:10px;color:var(--muted)">({_tdv_card:.1f})</span>'
                f"</span>"
            )
        elif ev.trainer_stats:
            tname_disp = h.trainer or ""
            tr_s = f'{tname_disp}　<span style="color:var(--muted)">—</span>'

        # 調教（競馬ブック風テーブル形式）
        tr_line = self._gen_training_table(ev.training_records)

        # 勝負気配
        sb_s = ""
        if ev.shobu_score >= 4:
            sb_s = '<span style="color:var(--warn);font-weight:700">🔺勝負気配</span>　'

        # 推定位置
        pos_s = "—"
        if ev.pace.estimated_position_4c is not None:
            rk = max(1, int(ev.pace.estimated_position_4c * fc))
            sd = (ev.pace.estimated_position_4c - 0.5) * fc * 0.12
            pos_s = f"4角{rk}番手（{sd:+.2f}秒差）"
        # 推定上がり3F（上位3頭は黄→青→赤で色付け。上がり3Fは低い=速い=上位）
        if ev.pace.estimated_last3f:
            l3f_vals = [
                e.pace.estimated_last3f for e in (all_ev or [ev]) if e.pace.estimated_last3f
            ]
            l3f_vals_sorted = sorted(l3f_vals)  # 小さい順（速い順）
            l3f_rank = (
                l3f_vals_sorted.index(ev.pace.estimated_last3f) + 1
                if ev.pace.estimated_last3f in l3f_vals_sorted
                else None
            )
            l3f_val_s = f"{ev.pace.estimated_last3f:.1f}秒"
            if l3f_rank == 1:
                l3f_s = f'<span class="rank-1">{l3f_val_s}</span>({l3f_rank}位)'
            elif l3f_rank == 2:
                l3f_s = f'<span class="rank-2">{l3f_val_s}</span>({l3f_rank}位)'
            elif l3f_rank == 3:
                l3f_s = f'<span class="rank-3">{l3f_val_s}</span>({l3f_rank}位)'
            elif l3f_rank:
                l3f_s = f"{l3f_val_s}({l3f_rank}位)"
            else:
                l3f_s = l3f_val_s
        else:
            l3f_s = "—"

        # バッジ
        flags = ""
        if ev.kiken_type != KikenType.NONE:
            flags = f' <span class="badge b-kiken">{ev.kiken_type.value}</span>'
        if ev.ana_type != AnaType.NONE:
            flags += f' <span class="badge b-ana">{ev.ana_type.value}</span>'

        narrative = self._gen_horse_narrative(ev, all_ev or [ev], race=race)

        # 勝負気配 HTML（f-string内backslash回避）
        shobu_html = ""
        if ev.shobu_score > 0.0:
            _sg = self._grade_html(self._normalize_to_dev(ev.shobu_score, 0.0, 6.0))
            _sv = f"{ev.shobu_score:.1f}"
            shobu_html = (
                f"<div class='kv'><span class='k'>勝負気配</span><span>"
                f"{sb_s}{_sg} <span style='font-size:10px;color:var(--muted)'>({_sv})</span>"
                f"</span></div>"
            )

        # 総合順位（レベル2と同じロジック）
        rank = len([e for e in (all_ev or [ev]) if e.composite > ev.composite]) + 1
        rank_s = f' <span style="font-size:11px;color:var(--muted);font-weight:400">({rank}位)</span>'

        # コース実績補正
        cr_val = ev.course.course_record
        cr_dev = self._normalize_to_dev(cr_val, -5.0, 5.0)
        cr_g = self._dev_to_grade(cr_dev)
        cr_gc = self._grade_css(cr_g)
        cr_label = f'<span class="{cr_gc}">{cr_g}</span> <span style="font-size:10px;color:var(--muted)">({cr_val:+.1f}pt)</span>'
        # 競馬場適性（4因子類似度）
        sc_val = ev.course.venue_aptitude
        sc_dev = self._normalize_to_dev(sc_val, -5.0, 5.0)
        sc_g = self._dev_to_grade(sc_dev)
        sc_gc = self._grade_css(sc_g)
        sc_label = f'<span class="{sc_gc}">{sc_g}</span> <span style="font-size:10px;color:var(--muted)">({sc_val:+.1f}pt)</span>'
        # 枠の有利不利
        gb_val = ev.pace.gate_bias
        gb_dev = self._normalize_to_dev(gb_val, -3.0, 3.0)
        gb_g = self._dev_to_grade(gb_dev)
        gb_gc = self._grade_css(gb_g)
        gb_label = f'<span class="{gb_gc}">{gb_g}</span> <span style="font-size:10px;color:var(--muted)">({gb_val:+.1f}pt)</span>'

        # 血統情報
        sire_s = h.sire or "—"
        dam_s = h.dam or "—"
        mgs_s = h.maternal_grandsire or "—"
        bl_adj = ev.ability.bloodline_adj
        bl_dev = self._normalize_to_dev(bl_adj, -2.5, 2.5)
        bl_g = self._dev_to_grade(bl_dev)
        bl_gc = self._grade_css(bl_g)
        if abs(bl_adj) < 0.1:
            bl_label = '<span style="color:var(--muted)">—（データ不足または中立）</span>'
        else:
            bl_label = f'<span class="{bl_gc}">{bl_g}</span> <span style="font-size:10px;color:var(--muted)">({bl_adj:+.1f}pt)</span>'

        # 血統 surface×SMILE 分解（見える化）
        sire_bd_html = self._gen_sire_breakdown_html(ev.ability.sire_breakdown, sire_s)

        # 未出走バッジ（過去走なし = 血統推定値のみ）
        is_debut = not h.past_runs
        debut_badge = (
            '<span style="background:#f59e0b;color:#fff;font-size:9px;font-weight:700;'
            'padding:1px 5px;border-radius:3px;margin-left:5px;vertical-align:middle" '
            'title="過去走データなし。血統推定値のため精度は低い">初出走</span>'
            if is_debut else ""
        )

        return f"""
<div class="{cls}">
<div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:6px;margin-bottom:10px">
  <div>
    <div class="hname"><span class="waku wk{h.gate_no}">{h.gate_no}</span> <span class="uma wk{h.gate_no}">{h.horse_no}</span> {_safe(h.horse_name)}{flags}</div>
    <div class="hmeta">{_safe(h.sex)}{h.age}　斤量{h.weight_kg}kg　馬体重：{wt_s}　騎手：<b>{_safe(h.jockey)}</b>　調教師：{_safe(h.trainer)}</div>
    <div style="font-size:11px;color:var(--muted);margin-top:3px">父：{_safe(sire_s)}{self._inline_grade(ev, '_sire_grade', '_sire_dev')}　母父：{_safe(mgs_s)}{self._inline_grade(ev, '_mgs_grade', '_mgs_dev')}</div>
  </div>
  <div class="mlarge m-{mk}">{mk}</div>
</div>
<div style="font-size:13px;color:var(--muted);margin-bottom:10px">
  総合 <strong style="font-size:19px;color:var(--text)">{ev.composite:.1f}</strong>{rank_s}　<span data-live-odds="{h.horse_no}">{odds_s}</span>
  勝率 <b>{ev.win_prob * 100:.1f}%</b>　連対率 <b>{ev.place2_prob * 100:.1f}%</b>　複勝率 <b>{ev.place3_prob * 100:.1f}%</b>
</div>
<div class="devrow">
  <div class="di"><span class="dl">能力偏差値</span>{self._grade_html(ev.ability.total, size=22)}{debut_badge}
    <span class="ds">{ev.ability.total:.1f}　平均{self._grade_html(ev.ability.wa_dev)}<span style="font-size:10px;color:var(--muted)">({ev.ability.wa_dev:.1f})</span>　最高{self._grade_html(ev.ability.max_dev)}<span style="font-size:10px;color:var(--muted)">({ev.ability.max_dev:.1f})</span>　<span class="badge b-{rel}">{rel}</span></span></div>
  <div class="di"><span class="dl">展開偏差値</span>{self._grade_html(ev.pace.total, size=22)}
    <span class="ds">{ev.pace.total:.1f}</span></div>
  <div class="di"><span class="dl">コース適性</span>{self._grade_html(ev.course.total, size=22)}
    <span class="ds">{ev.course.total:.1f}</span></div>
</div>
<div class="grid2">
<div>
  <div class="sub-title">【能力分析】</div>
  <div class="kv"><span class="k">近走パフォーマンス</span><span style="color:{tc};font-weight:700">{tv}</span></div>
  <div class="kv"><span class="k">着差パターン</span><span>{ev.ability.chakusa_pattern.value}</span></div>
  <div class="kv"><span class="k">接戦勝率</span>
    <span>{ev.ability.close_race_win_rate[0]}/{ev.ability.close_race_win_rate[1]}（{round(ev.ability.close_race_win_rate[0] / max(ev.ability.close_race_win_rate[1], 1) * 100)}%）</span></div>
  <div class="kv"><span class="k" title="勝利時の後続との差・敗戦時の上位との差を総合した安定性指標。プラスが大きいほど余力ある競馬が多い">決め手安定度</span><span>{self._chakusa_label(ev.ability.chakusa_index_avg)}<span style="color:var(--muted);font-size:10px;margin-left:4px">({ev.ability.chakusa_index_avg:+.2f})</span></span></div>
  <div class="kv"><span class="k">決め手タイプ</span><span>{ev.baken_type.value}</span></div>
  <div class="kv"><span class="k">血統適性</span><span style="font-size:12px">{bl_label}</span></div>
{sire_bd_html}</div>
<div>
  <div class="sub-title">【展開・コース分析】</div>
  <div class="kv"><span class="k">推定位置取り</span><span>{pos_s}</span></div>
  <div class="kv"><span class="k">推定上がり3F</span><span>{l3f_s}</span></div>
  <div class="kv"><span class="k" title="このコースでの過去着順を集計した実績補正値">当コース実績</span><span style="font-size:12px">{cr_label}</span></div>
  <div class="kv"><span class="k" title="コーナー数・直線長・急坂など形状から脚質との相性を評価">形状×脚質相性</span><span style="font-size:12px">{sc_label}</span></div>
  <div class="kv"><span class="k" title="この競馬場における枠番ごとの過去勝率差から有利/不利を評価">枠の有利不利</span><span style="font-size:12px">{gb_label}</span></div>
</div>
</div>
{self._gen_past_runs_table(ev)}
{self._gen_track_condition_apt(h, race)}
<div class="sub-title" style="margin-top:12px">【騎手・厩舎分析】</div>
<div class="grid2">
<div>
  <div class="kv"><span class="k" title="DB内全コース・全条件の勝率を相対評価（偏差値40〜75）">騎手偏差値</span><span>{jd}</span></div>
  {"<div class='kv'><span class='k'>乗り替わり</span><span>" + ch_s + "</span></div>" if ch_s else ""}
</div>
<div>
  <div class="kv"><span class="k" title="DB内全条件の勝率をA〜D(4段階)にランク分類。回収タイプは勝率・複勝率の傾向から判定">厩舎ランク</span><span>{tr_s}</span></div>
  {shobu_html}
</div>
</div>
<div class="cbox" style="line-height:1.9;font-size:13px;padding:12px 16px">
{narrative}
</div>
{tr_line}
</div>"""

    def _gen_training_table(self, records: list) -> str:
        """競馬ブック風テーブル形式で全追切りデータを表示。"""
        if not records:
            return ""
        valid = [
            r
            for r in records
            if (r.splits and any(v for v in r.splits.values()))
            or (r.course and r.course not in ("—", "ー", "", "不明"))
        ]
        if not valid:
            return ""

        dist_to_f = [(1200, "6F"), (1000, "5F"), (800, "4F"), (600, "3F"), (200, "1F")]
        intensity_colors = {
            "一杯": "#c0392b",
            "強め": "#e67e22",
            "馬なり": "var(--green)",
            "軽め": "var(--muted)",
            "極軽め": "var(--muted)",
        }

        rows_html = []

        first_comment = valid[0].comment if valid[0].comment else ""
        is_general = first_comment and not any(
            c in first_comment for c in ["秒先行", "秒追走", "秒遅れ", "秒先着", "同入"]
        )

        if is_general:
            rows_html.append(
                f'<tr><td colspan="7" style="text-align:center;color:var(--muted);'
                f'font-size:11px;padding:2px 0;border-bottom:1px solid var(--border)">'
                f"{first_comment}</td></tr>"
            )

        for i, rec in enumerate(valid):
            date_s = rec.date or ""
            course_s = rec.course or ""
            ic = intensity_colors.get(rec.intensity_label, "")
            em = TRAINING_EMOJI.get(rec.intensity_label, "→")
            int_html = f'<span style="color:{ic};font-weight:600">{em}{rec.intensity_label}</span>'

            splits_vals = []
            for dist, label in dist_to_f:
                v = rec.splits.get(dist) or rec.splits.get(str(dist))
                if v is not None:
                    splits_vals.append(f"<b>{v:.1f}</b>")
                else:
                    splits_vals.append("")

            td_style = 'style="padding:1px 4px;text-align:center;font-size:12px"'
            left_style = 'style="padding:1px 4px;text-align:left;font-size:12px;white-space:nowrap"'
            right_style = (
                'style="padding:1px 4px;text-align:right;font-size:12px;white-space:nowrap"'
            )

            row = (
                f'<tr style="border-bottom:1px solid var(--border)">'
                f"<td {left_style}>{date_s} {course_s} </td>"
                + "".join(f"<td {td_style}>{v}</td>" for v in splits_vals)
                + f"<td {right_style}>{int_html}</td></tr>"
            )
            rows_html.append(row)

            awase = (
                rec.comment
                if rec.comment
                and any(c in rec.comment for c in ["秒先行", "秒追走", "秒遅れ", "秒先着", "同入"])
                else ""
            )
            if awase:
                rows_html.append(
                    f'<tr><td colspan="7" style="text-align:left;font-size:11px;'
                    f'padding:0 4px 2px;color:var(--navy)">{awase}</td></tr>'
                )

        tanpyo = ""
        for rec in valid:
            c = rec.comment
            if c and not any(k in c for k in ["秒先行", "秒追走", "秒遅れ", "秒先着", "同入"]):
                if c != first_comment or not is_general:
                    tanpyo = c
                    break
        if tanpyo:
            rows_html.append(
                f'<tr><td colspan="7" style="text-align:left;font-size:11px;'
                f'padding:2px 4px;color:var(--navy);font-weight:600">'
                f"「{tanpyo}」</td></tr>"
            )

        header = (
            '<tr style="background:var(--bg2);font-size:10px;color:var(--muted)">'
            '<th style="padding:1px 4px;text-align:left">日付・コース</th>'
            '<th style="padding:1px 4px">6F</th>'
            '<th style="padding:1px 4px">5F</th>'
            '<th style="padding:1px 4px">4F</th>'
            '<th style="padding:1px 4px">3F</th>'
            '<th style="padding:1px 4px">1F</th>'
            '<th style="padding:1px 4px;text-align:right">強度</th></tr>'
        )

        return (
            f'<div class="kv" style="flex-direction:column;align-items:stretch">'
            f'<span class="k" style="margin-bottom:2px">調教</span>'
            f'<table style="width:100%;border-collapse:collapse;border:1px solid var(--border)">'
            f"{header}{''.join(rows_html)}</table></div>"
        )

    def _gen_sire_breakdown_html(self, breakdown: dict, sire_name: str) -> str:
        """父馬 surface×SMILE 別複勝率テーブルを生成（血統適性見える化）"""
        if not breakdown or (not breakdown.get("surface") and not breakdown.get("smile")):
            return ""

        SURF_LABEL = {"芝": "芝", "ダート": "ダート", "障害": "障害"}
        SMILE_LABEL = {"ss": "SS(〜1000)", "s": "S(〜1400)", "m": "M(〜1800)",
                       "i": "I(〜2200)", "l": "L(〜2600)", "e": "E(2601+)"}
        SMILE_ORDER = ["ss", "s", "m", "i", "l", "e"]

        def _pct_bar(rate: float) -> str:
            pct = rate * 100
            color = "#22c55e" if pct >= 38 else ("#f59e0b" if pct >= 28 else "#ef4444")
            return (f'<span style="color:{color};font-weight:700">{pct:.0f}%</span>'
                    f'<span style="color:var(--muted);font-size:9px"> ({rate:.3f})</span>')

        rows_surf = ""
        for surf, lbl in SURF_LABEL.items():
            v = breakdown.get("surface", {}).get(surf)
            if v:
                rows_surf += (f'<tr><td style="color:var(--muted)">{lbl}</td>'
                              f'<td>{_pct_bar(v["place_rate"])}</td>'
                              f'<td style="color:var(--muted);font-size:10px">{v["runs"]}走</td></tr>')

        rows_smile = ""
        for sm in SMILE_ORDER:
            v = breakdown.get("smile", {}).get(sm)
            if v:
                rows_smile += (f'<tr><td style="color:var(--muted)">{SMILE_LABEL[sm]}</td>'
                               f'<td>{_pct_bar(v["place_rate"])}</td>'
                               f'<td style="color:var(--muted);font-size:10px">{v["runs"]}走</td></tr>')

        if not rows_surf and not rows_smile:
            return ""

        tables = ""
        if rows_surf:
            tables += f"""<table style="font-size:11px;border-collapse:collapse;margin-right:16px">
<thead><tr><th colspan="3" style="text-align:left;color:var(--muted);font-weight:normal;padding-bottom:2px">馬場別</th></tr></thead>
<tbody>{rows_surf}</tbody></table>"""
        if rows_smile:
            tables += f"""<table style="font-size:11px;border-collapse:collapse">
<thead><tr><th colspan="3" style="text-align:left;color:var(--muted);font-weight:normal;padding-bottom:2px">距離区分別</th></tr></thead>
<tbody>{rows_smile}</tbody></table>"""

        return f"""<div style="margin:4px 0 0 8px;padding:6px 8px;background:var(--card-bg);border-radius:6px;border-left:3px solid #3b82f6">
<div style="font-size:10px;color:var(--muted);margin-bottom:4px">父 <b style="color:var(--text)">{sire_name}</b> 産駒実績（複勝率）</div>
<div style="display:flex;flex-wrap:wrap;gap:8px">{tables}</div>
</div>"""

    def _gen_track_condition_apt(self, h, race=None) -> str:
        """馬の馬場状態別適性セクション（past_runsから集計）
        表示形式: 良 1-2-3-着外(勝率%)
        """
        surface = race.course.surface if race and race.course else None
        today_cond = None
        if race:
            if surface == "芝":
                today_cond = race.track_condition_turf or None
            else:
                today_cond = race.track_condition_dirt or None

        cond_keys = ["良", "稍重", "重", "不良"]
        stats: dict = {c: {"w": 0, "p2": 0, "p3": 0, "out": 0, "runs": 0} for c in cond_keys}

        for run in h.past_runs or []:
            cond = getattr(run, "condition", None)
            if cond not in stats:
                continue
            surf = getattr(run, "surface", None)
            if surface and surf and surf != surface:
                continue
            pos = getattr(run, "finish_pos", None) or getattr(run, "finish_position", None) or 99
            stats[cond]["runs"] += 1
            if pos == 1:
                stats[cond]["w"] += 1
            elif pos == 2:
                stats[cond]["p2"] += 1
            elif pos == 3:
                stats[cond]["p3"] += 1
            else:
                stats[cond]["out"] += 1

        # 稍重+重+不良の合算
        heavy = {"w": 0, "p2": 0, "p3": 0, "out": 0, "runs": 0}
        for c in ["稍重", "重", "不良"]:
            for k in heavy:
                heavy[k] += stats[c][k]

        def _badge(cond: str, s: dict) -> str:
            w, p2, p3, out, runs = s["w"], s["p2"], s["p3"], s["out"], s["runs"]
            if runs == 0:
                return f'<span style="color:var(--muted);font-size:12px">{cond} —</span>'
            rate = w / runs * 100
            color = (
                "#c0392b"
                if rate >= 30
                else "#e67e22"
                if rate >= 15
                else "#2c6dbf"
                if w > 0
                else "var(--muted)"
            )
            is_today = today_cond == cond
            border = (
                "border:2px solid var(--warn);padding:1px 6px;" if is_today else "padding:2px 7px;"
            )
            return (
                f'<span style="display:inline-block;border-radius:4px;background:var(--card);'
                f'{border}font-size:12px;color:{color}">'
                f"{cond} <b>{w}-{p2}-{p3}-{out}</b>({rate:.1f}%)</span>"
            )

        parts = [_badge(c, stats[c]) for c in cond_keys]

        # 重馬場合算バッジ
        if heavy["runs"] > 0:
            hw, hp2, hp3, ho, hr = heavy["w"], heavy["p2"], heavy["p3"], heavy["out"], heavy["runs"]
            hrate = hw / hr * 100
            hc = "#c0392b" if hrate >= 20 else "var(--muted)"
            is_heavy = today_cond in ("稍重", "重", "不良")
            border = (
                "border:2px solid var(--warn);padding:1px 6px;" if is_heavy else "padding:2px 7px;"
            )
            heavy_badge = (
                f'<span style="display:inline-block;border-radius:4px;background:#f5f5f5;'
                f'{border}font-size:12px;color:{hc};">'
                f"重馬場計 <b>{hw}-{hp2}-{hp3}-{ho}</b>({hrate:.1f}%)</span>"
            )
        else:
            heavy_badge = '<span style="font-size:12px;color:var(--muted)">重馬場：実績なし</span>'

        surf_label = f"（{surface}）" if surface else ""
        return f"""<div class="sub-title" style="margin-top:12px">【馬場適性{surf_label}】</div>
<div style="display:flex;flex-wrap:wrap;gap:8px;padding:6px 4px;align-items:center">
{"".join(parts)}
{heavy_badge}
</div>"""

    # ------------------------------------------------------------------
    # サマリー行・短評
    # ------------------------------------------------------------------
    def _calc_kehai_dev(self, ev: HorseEvaluation) -> Optional[float]:
        """気配偏差値（30〜70）。None = 評価不可。

        ① 調教データあり かつ sigma_from_mean が計算済み（≠0）の場合:
             厩舎平均からのσを偏差値に変換。σ+2→65, σ0→50, σ-2→35。
             勝負気配スコアを補正値（最大±2.5pt）として加算。
        ② 調教データなし / ベースライン未計算（σ=0）の場合:
             勝負気配スコアのみ。スコアが一定以上なら数値を返し、低ければ None。
        """
        # 勝負気配補正（-2.5〜+2.5pt）
        shobu_adj = (ev.shobu_score / 6.0 - 0.5) * 5.0
        shobu_adj = max(-2.5, min(2.5, shobu_adj))

        if ev.training_records:
            sigma = ev.training_records[0].sigma_from_mean
            if sigma != 0.0:
                # 厩舎ベースライン比のσが計算済み → 正しい相対評価が可能
                # σ × 7.5 + 50 でスケール変換（σ2=65, σ1=57.5, σ0=50, σ-1=42.5, σ-2=35）
                dev = 50.0 + sigma * 7.5
                return max(20.0, min(100.0, dev + shobu_adj))
            # σ=0 はベースラインデータ未取得（デフォルト値）→ 強度ラベルだけでは
            # 「この厩舎にしては」の評価ができないため、気配グレードは出さない

        # 調教データなし or ベースライン未取得 → 勝負気配スコアのみで代替
        # スコアが低い場合は「判断できない」として None を返す
        if ev.shobu_score >= 2.0:
            return self._normalize_to_dev(ev.shobu_score, 0.0, 6.0)
        return None  # 評価不可

    def _gen_short_comment(self, ev: HorseEvaluation, rank: int = 0, total_horses: int = 0) -> str:
        """競馬記者風の短評（30文字程度）。
        馬の特徴・状態・展望を端的に伝える一文を生成する。
        """
        h = ev.horse
        trend = ev.ability.trend
        style = ev.pace.running_style
        style_s = style.value if style else ""
        chakusa = ev.ability.chakusa_pattern

        # 危険馬は明確に警告
        if ev.kiken_type == KikenType.KIKEN_A:
            if trend == Trend.DOWN or trend == Trend.RAPID_DOWN:
                return "人気先行の嫌い、能力過信は禁物。"
            return "実績の割に条件厳しく過信禁物。"
        if ev.kiken_type == KikenType.KIKEN_B:
            return "コース・展開ともに不向きで苦戦必至。"

        # 穴馬は妙味を強調
        if ev.ana_type == AnaType.ANA_A:
            if trend == Trend.UP or trend == Trend.RAPID_UP:
                return "上昇一途の隠れ実力馬、激走警戒。"
            return "人気の盲点、実力は上位級で侮れぬ。"
        if ev.ana_type == AnaType.ANA_B:
            if ev.course.total >= 55:
                return "条件好転で一変あり、妙味十分。"
            return "条件絶好で浮上、人気薄なら狙い目。"

        fragments = []

        # G1/G2実績
        has_big_grade = False
        for run in (h.past_runs or [])[:5]:
            grade = getattr(run, "grade", "") or ""
            pos = getattr(run, "finish_pos", None) or getattr(run, "finish_position", 99) or 99
            if grade in ("G1", "G2") and pos == 1:
                fragments.append(f"{grade}馬の格が違う")
                has_big_grade = True
                break
            elif grade == "G1" and pos <= 3:
                fragments.append(f"G1{pos}着の底力")
                has_big_grade = True
                break

        # トレンド（上昇/下降）
        if trend == Trend.RAPID_UP:
            if style_s == "逃げ":
                fragments.append("絶好調で逃げ切り警戒")
            else:
                fragments.append("勢い抜群、充実ぶり際立つ")
        elif trend == Trend.UP:
            fragments.append("状態上向きで本番向き")
        elif trend == Trend.RAPID_DOWN:
            if rank <= 3:
                fragments.append("近況不振、人気でも疑問符")
            else:
                fragments.append("近走精彩欠き苦戦濃厚")
        elif trend == Trend.DOWN:
            fragments.append("下降線で信頼度落ちる")

        # 乗り替わりパターン
        if ev.jockey_change_pattern:
            if ev.jockey_change_pattern == KishuPattern.A:
                fragments.append("鞍上強化で上積み期待")
            elif ev.jockey_change_pattern == KishuPattern.E:
                fragments.append("鞍上見切りは嫌材料")

        # 勝負気配
        if ev.shobu_score >= 5:
            fragments.append("陣営全力、勝負仕上げ")
        elif ev.shobu_score >= 3 and len(fragments) == 0:
            fragments.append("気配良く本番にらみ仕上げ")

        # 着差パターン
        if chakusa == ChakusaPattern.ASSHŌ and not has_big_grade:
            if rank <= 2:
                fragments.append("圧勝癖、嵌れば突き抜け")
            else:
                fragments.append("勝つ時は圧勝の一発型")
        elif chakusa == ChakusaPattern.MURA:
            if rank <= 3:
                fragments.append("ムラ馬だが嵌れば怖い")
            else:
                fragments.append("好凡走の差が大きく不安定")

        # 堅実型は安定感
        if chakusa == ChakusaPattern.KENJO and ev.baken_type == BakenType.ANTEI:
            if rank <= 3:
                fragments.append("堅実派で複勝軸に最適")
            else:
                fragments.append("崩れないが決め手に欠く")

        # 脚質と展開
        if style_s == "逃げ":
            if ev.pace.total >= 55:
                fragments.append("逃げ有利のペース、自在")
            elif ev.pace.total < 45:
                fragments.append("逃げが不利な流れで試練")
        elif style_s == "追込":
            if ev.pace.total >= 55:
                fragments.append("差し馬場で末脚炸裂の可能性")
            else:
                fragments.append("前残り展開は追込に逆風")
        elif style_s == "差し" and ev.pace.total >= 55:
            fragments.append("流れ向き、差し脚活きる")
        elif style_s in ("先行", "好位") and ev.pace.total >= 55:
            fragments.append("好位から早め抜け出し狙う")
        elif style_s == "中団" and ev.pace.total >= 55:
            fragments.append("中団から末脚を活かす形")

        # 能力上位なのに展開不利
        if ev.ability.total >= 55 and ev.pace.total < 45 and len(fragments) < 2:
            fragments.append("地力あるが展開が唯一の死角")

        # コース適性
        if ev.course.total >= 60 and not fragments:
            fragments.append("このコース得意、実績申し分なし")
        elif ev.course.total >= 55 and not fragments:
            fragments.append("コース適性高く舞台ベスト")
        elif ev.course.total < 42:
            if len(fragments) < 2:
                fragments.append("コース替わりに不安残る")

        # 前走内容から補足
        if not fragments and h.past_runs:
            r = h.past_runs[0]
            pos = getattr(r, "finish_pos", None) or getattr(r, "finish_position", 99) or 99
            fc_val = getattr(r, "field_count", 0) or 0
            if pos == 1:
                fragments.append("前走快勝の勢いそのまま")
            elif pos <= 3:
                fragments.append("前走好走、状態維持で前進")
            elif fc_val >= 12 and pos <= fc_val // 3:
                fragments.append("大競走でも健闘、地力十分")
            else:
                fragments.append("前走凡走も仕切り直しに期待")

        # 能力偏差値での補足（rank 1-2 で fragments 空なら）
        if not fragments:
            if rank <= 1:
                return "総合力トップ、信頼の軸候補。"
            elif rank <= 2:
                return "対抗格、本命に食い下がれる力。"
            elif rank <= 4:
                return "上位争いに加われる一頭。"
            return "上位とは力差あり、展開次第か。"

        text = "、".join(fragments[:2]) + "。"
        return text[:34]

    def _hcard_summary(self, ev: HorseEvaluation, fc: int, all_ev: list, race) -> str:
        """馬一覧サマリー行HTML（<summary>内コンテンツ）"""
        h = ev.horse
        mk = ev.mark.value
        # オッズ
        if h.odds:
            odds_s = f"{h.odds:.1f}倍"
            if h.popularity:
                odds_s += f"({h.popularity}人気)"
        elif ev.predicted_odds:
            # 全頭の想定オッズで人気順位を算出
            pred_odds_list = sorted(
                [e.predicted_odds for e in all_ev if e.predicted_odds], reverse=False
            )
            pred_rank = (
                pred_odds_list.index(ev.predicted_odds) + 1
                if ev.predicted_odds in pred_odds_list
                else "?"
            )
            odds_s = f'<span style="color:var(--muted);font-size:10px">[想定]</span>{ev.predicted_odds:.1f}倍({pred_rank}人気)'
        else:
            odds_s = "—"
        # 総合順位・色付け（1位=緑, 2位=青, 3位=赤）
        rank = len([e for e in all_ev if e.composite > ev.composite]) + 1
        if rank <= 3:
            comp_colored = f'<span class="rank-{rank}" style="font-size:13px;font-weight:700">{ev.composite:.1f}</span>'
        else:
            comp_colored = f'<span style="font-size:13px;font-weight:700">{ev.composite:.1f}</span>'
        rank_label = (
            f'<span style="font-size:10px;color:var(--muted);font-weight:400">({rank}位)</span>'
        )
        # 騎手評価グレード — Phase 12 ハイブリッド偏差値
        jockey_g = ""
        _jdv = getattr(ev, "_jockey_dev", None)
        if _jdv is not None:
            jg = self._dev_to_grade(_jdv)
            jgc = self._grade_css(jg)
            jockey_g = f'<span class="{jgc}" style="font-size:10px;margin-left:2px">{jg}</span>'
        # 調教師評価グレード — Phase 12 ハイブリッド偏差値
        trainer_g = ""
        _tdv = getattr(ev, "_trainer_dev", None)
        if _tdv is not None:
            _rk_g = self._dev_to_grade(_tdv)
            _rk_gc = self._grade_css(_rk_g)
            trainer_g = (
                f'<span class="{_rk_gc}" style="font-size:10px;margin-left:2px">{_rk_g}</span>'
            )
        # 脚質
        style_s = ev.pace.running_style.value if ev.pace.running_style else "—"
        # 所属（出馬表スクレイプ済みなら trainer_affiliation を優先、なければ競馬場名）
        if h.trainer_affiliation:
            org_s = h.trainer_affiliation
        elif race and race.is_jra:
            org_s = "JRA"
        elif race and race.venue:
            org_s = race.venue
        else:
            org_s = "地方"
        # 気配（厩舎平均比σ + 勝負気配補正 → SS〜Dグレード、評価不可なら—）
        _kehai_dev = self._calc_kehai_dev(ev)
        kehai_s = (
            self._grade_html(_kehai_dev)
            if _kehai_dev is not None
            else '<span style="color:var(--muted);font-size:11px">—</span>'
        )
        # 能力・展開・適性グレードと順位・色付け（上位3頭：黄→青→赤）
        ab_rank = len([e for e in all_ev if e.ability.total > ev.ability.total]) + 1
        pa_rank = len([e for e in all_ev if e.pace.total > ev.pace.total]) + 1
        co_rank = len([e for e in all_ev if e.course.total > ev.course.total]) + 1
        ab_g = self._dev_to_grade(ev.ability.total)
        ab_gc = self._grade_css(ab_g)
        pa_g = self._dev_to_grade(ev.pace.total)
        pa_gc = self._grade_css(pa_g)
        co_g = self._dev_to_grade(ev.course.total)
        co_gc = self._grade_css(co_g)

        def _ranked_grade(grade: str, css: str, rk: int) -> str:
            """順位色: 1位=緑, 2位=青, 3位=赤"""
            rank_tag = f'<span style="font-size:11px;color:var(--muted)">({rk}位)</span>'
            if rk <= 3:
                return f'<span class="rank-{rk}" style="font-size:15px;font-weight:700">{grade}</span>{rank_tag}'
            return f'<span class="{css}" style="font-size:14px;font-weight:700">{grade}</span>{rank_tag}'

        # 三連率（composite順位連動色: 1位=緑, 2位=青, 3位=赤, 平均以上=黒, 平均以下=灰）
        win_pct = f"{ev.win_prob * 100:.1f}"
        place2_pct = f"{ev.place2_prob * 100:.1f}"
        place3_pct = f"{ev.place3_prob * 100:.1f}"
        # composite順位で統一色（勝率/連対率/複勝率すべて同じ色）
        _half = len(all_ev) // 2 if all_ev else 8
        if rank == 1:
            _rate_c = "#16a34a"
        elif rank == 2:
            _rate_c = "#1a6fa8"
        elif rank == 3:
            _rate_c = "#c0392b"
        elif rank <= _half:
            _rate_c = "#333"
        else:
            _rate_c = "#aaa"
        wc = p2c = p3c = _rate_c
        # ML確率（正規化済み）があればニコイチ表示
        _ml_win = ev.ml_win_prob
        _ml_p3 = ev.ml_place_prob
        if _ml_win is not None and _ml_p3 is not None:
            ml_win_pct = f"{_ml_win * 100:.1f}"
            ml_p3_pct = f"{_ml_p3 * 100:.1f}"
            winrate_html = (
                f'<span class="hds-wr-label">勝</span>'
                f'<span class="hds-wr-win" style="color:{wc}">{win_pct}%</span>'
                f'<span style="font-size:9px;color:var(--muted);margin-left:1px">ML:{ml_win_pct}%</span>'
                f'<span class="hds-wr-sep">・</span>'
                f'<span class="hds-wr-label">連</span><span class="hds-wr-win" style="color:{p2c}">{place2_pct}%</span>'
                f'<span class="hds-wr-sep">・</span>'
                f'<span class="hds-wr-label">複</span>'
                f'<span class="hds-wr-win" style="color:{p3c}">{place3_pct}%</span>'
                f'<span style="font-size:9px;color:var(--muted);margin-left:1px">ML:{ml_p3_pct}%</span>'
            )
        else:
            winrate_html = (
                f'<span class="hds-wr-label">勝</span><span class="hds-wr-win" style="color:{wc}">{win_pct}%</span>'
                f'<span class="hds-wr-sep">・</span>'
                f'<span class="hds-wr-label">連</span><span class="hds-wr-win" style="color:{p2c}">{place2_pct}%</span>'
                f'<span class="hds-wr-sep">・</span>'
                f'<span class="hds-wr-label">複</span><span class="hds-wr-win" style="color:{p3c}">{place3_pct}%</span>'
            )
        # 厩舎コメント（競馬ブック厩舎の話）
        comment_s = ""
        if ev.training_records:
            kb_comment = ev.training_records[0].comment or ""
            if kb_comment and len(kb_comment) >= 2:
                comment_s = kb_comment

        waku_s = f'<span class="waku wk{h.gate_no}" style="font-size:11px;width:20px;height:20px;line-height:20px">{h.gate_no}</span>'
        uma_s = f'<span class="uma wk{h.gate_no}"  style="font-size:11px;width:20px;height:20px;line-height:20px">{h.horse_no}</span>'
        mk_s = f'<span class="m-{mk}" style="font-size:16px;min-width:18px;text-align:center">{mk}</span>'

        # 上段：枠・馬番・印・馬名・性齢・騎手(グレード)・斤量・所属・調教師(グレード)・脚質・オッズ・能/展/適グレード
        row1 = (
            f'<div class="hds-row1">'
            f"{waku_s}{uma_s}{mk_s}"
            f'<span class="hds-name">{_safe(h.horse_name)}</span>'
            f'<span style="color:var(--muted);font-size:10px">{_safe(h.sex)}{h.age}</span>'
            f'<span style="font-size:10px">{_safe(h.jockey or "—")}{jockey_g}</span>'
            f'<span style="color:var(--muted);font-size:10px">{f"{h.weight_kg:.0f}kg" if h.weight_kg else "—"}</span>'
            f'<span style="color:var(--muted);font-size:9px">{org_s}</span>'
            f'<span style="font-size:10px">{_safe(h.trainer or "—")}{trainer_g}</span>'
            f'<span style="color:#374151;font-size:10px;font-weight:600">{style_s}</span>'
            f'<span data-live-odds="{h.horse_no}" style="font-size:11px;font-weight:700;color:var(--navy)">{odds_s}</span>'
            f'<span class="hds-grades">'
            f'<span class="hds-grade-item"><span class="hds-grade-label">能力</span>{_ranked_grade(ab_g, ab_gc, ab_rank)}</span>'
            f'<span class="hds-grade-item"><span class="hds-grade-label">展開</span>{_ranked_grade(pa_g, pa_gc, pa_rank)}</span>'
            f'<span class="hds-grade-item"><span class="hds-grade-label">適性</span>{_ranked_grade(co_g, co_gc, co_rank)}</span>'
            f"</span>"
            f"</div>"
        )
        # 下段：総合指数【順位】・勝率・連対率・複勝率・短評
        # 厩舎コメント行（競馬ブック厩舎の話）
        comment_row = ""
        if comment_s:
            comment_row = (
                f'<div style="font-size:11px;color:#555;line-height:1.5;margin-top:2px;'
                f'padding:2px 0 2px 24px;border-top:1px dotted var(--border)">'
                f'<span style="color:var(--muted);font-size:10px">厩舎</span> {comment_s}</div>'
            )
        row2 = (
            f'<div class="hds-row2">'
            f'<span style="font-size:10px;color:var(--muted);margin-right:2px">総</span>'
            f"{comp_colored}{rank_label}"
            f'<span style="display:flex;align-items:center;gap:1px;margin-left:8px">{winrate_html}</span>'
            f"</div>"
            f"{comment_row}"
        )

        # Step3: SHAP寄与度バー
        shap_row = ""
        if ev.shap_groups:
            shap_row = _render_shap_bar(ev.shap_groups)

        return row1 + row2 + shap_row




# ============================================================
# Step3: SHAP寄与度バー レンダラー
# ============================================================

# グループごとの表示色 (正=青系, 負=赤系でclamping)
_SHAP_GROUP_COLORS = {
    "能力":   "#2563eb",
    "展開":   "#7c3aed",
    "騎手":   "#0891b2",
    "調教師": "#059669",
    "コース": "#d97706",
    "体型":   "#64748b",
    "血統":   "#be185d",
    "市場":   "#374151",
    "その他": "#9ca3af",
}
_SHAP_GROUP_ORDER = ["能力", "展開", "騎手", "調教師", "コース", "体型", "血統", "市場", "その他"]


def _render_shap_bar(shap_groups: dict) -> str:
    """
    SHAP寄与度グループをコンパクトなバー形式で表示する HTML を返す。
    正の寄与 = プラス方向 (青寄り)、負の寄与 = マイナス方向 (赤寄り)
    """
    if not shap_groups:
        return ""

    # 表示するグループのみ (絶対値が 0.002 以上)
    items = [
        (g, shap_groups[g])
        for g in _SHAP_GROUP_ORDER
        if g in shap_groups and abs(shap_groups[g]) >= 0.002
    ]
    if not items:
        return ""

    # スケール: 最大絶対値をバー幅 100% に対応
    max_abs = max(abs(v) for _, v in items)
    if max_abs < 1e-9:
        return ""

    base = shap_groups.get("_base", 0.0)
    base_pct = f"{base * 100:.1f}%"

    bars_html = ""
    for grp, val in items:
        pct = abs(val) / max_abs * 100
        color = _SHAP_GROUP_COLORS.get(grp, "#9ca3af")
        val_sign = "+" if val >= 0 else "−"
        val_abs = abs(val)
        # 正値: 右伸び (青) / 負値: 薄赤
        bar_color = color if val >= 0 else "#ef4444"
        bars_html += (
            f'<div style="display:flex;align-items:center;gap:3px;margin-bottom:1px">'
            f'<span style="width:32px;font-size:8.5px;color:var(--muted);text-align:right;flex-shrink:0">{grp}</span>'
            f'<div style="flex:1;background:#f1f5f9;border-radius:2px;height:7px;position:relative">'
            f'<div style="position:absolute;left:0;top:0;height:100%;width:{pct:.0f}%;'
            f'background:{bar_color};border-radius:2px;opacity:0.8"></div>'
            f'</div>'
            f'<span style="width:36px;font-size:8px;color:{bar_color};text-align:left;flex-shrink:0">'
            f'{val_sign}{val_abs:.4f}</span>'
            f'</div>'
        )

    return (
        f'<div class="hds-shap" style="margin:2px 4px 0 4px;padding:3px 4px;'
        f'background:#f8fafc;border-radius:4px;border:1px solid #e2e8f0">'
        f'<div style="display:flex;justify-content:space-between;margin-bottom:2px">'
        f'<span style="font-size:8.5px;color:var(--muted);font-weight:600">ML寄与度</span>'
        f'<span style="font-size:8px;color:var(--muted)">基準:{base_pct}</span>'
        f'</div>'
        f'{bars_html}'
        f'</div>'
    )


# ============================================================
# 日付別・全レース統合HTML（競馬場タブ + 1R～12Rタブ）
# ============================================================


def render_date_analysis_html(
    analyses_by_venue: Dict[str, Dict[int, "RaceAnalysis"]],
    date: str,
    formatter: "HTMLFormatter",
) -> str:
    """
    その日の全レースを1つのHTMLにまとめる。
    analyses_by_venue: {"東京": {1: analysis, 2: analysis, ...}, "中山": {...}}
    """
    # 中央10場→地方主要場→その他の順に固定（規則に従い全24場を同等に扱う）
    VENUE_ORDER = [
        # 中央10場
        "東京",
        "中山",
        "京都",
        "阪神",
        "中京",
        "新潟",
        "福島",
        "札幌",
        "函館",
        "小倉",
        # 地方14場（開催頻度が高い順）
        "大井",
        "川崎",
        "船橋",
        "浦和",
        "名古屋",
        "笠松",
        "園田",
        "姫路",
        "金沢",
        "盛岡",
        "水沢",
        "高知",
        "佐賀",
        "門別",
    ]
    ordered_venues = [v for v in VENUE_ORDER if v in analyses_by_venue]
    for v in sorted(analyses_by_venue.keys()):
        if v not in ordered_venues:
            ordered_venues.append(v)

    tabs_html = []
    content_html = []
    tab_idx = 0

    total_render = sum(len(r) for r in analyses_by_venue.values())
    render_count = 0

    for venue in ordered_venues:
        races = analyses_by_venue[venue]
        if not races:
            continue
        race_nos = sorted(races.keys())
        venue_id = f"v{tab_idx}"
        tabs_html.append(f'<button class="tab-venue" data-venue="{venue_id}">{venue}</button>')
        tab_idx += 1

        for rno in race_nos:
            analysis = races[rno]
            content = formatter.render(analysis)
            render_count += 1
            if render_count % 5 == 0 or render_count == total_render:
                print(f"       統合HTML: {render_count}/{total_render}", flush=True)
            # body内のwrap部分のみ抽出（ヘッダ・フッタを除く）
            if '<div class="wrap">' in content:
                start = content.find('<div class="wrap">')
                end = content.rfind("</div></body>")
                if end > 0:
                    inner = content[start:end] + "</div>"
                else:
                    inner = content
            else:
                inner = content
            panel_id = f"{venue_id}-{rno}"
            content_html.append(
                f'<div id="{panel_id}" class="tab-panel" data-venue="{venue_id}">'
                f'<div class="wrap venue-race-content">{inner}</div></div>'
            )

    # 2段階タブ用CSS・JS
    tab_css = """
.tab-container{background:var(--card);border:1px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:16px}
.tab-row{display:flex;flex-wrap:wrap;gap:4px;padding:12px 16px;background:var(--navy);align-items:center}
.tab-venue{cursor:pointer;padding:8px 16px;border:none;border-radius:6px;background:rgba(255,255,255,0.2);
  color:#fff;font-weight:600;font-size:14px;transition:background .2s}
.tab-venue:hover,.tab-venue.active{background:rgba(255,255,255,0.4)}
.tab-race{cursor:pointer;padding:6px 12px;border:none;border-radius:4px;background:rgba(255,255,255,0.15);
  color:#fff;font-size:13px;margin-left:8px;transition:background .2s}
.tab-race:hover,.tab-race.active{background:rgba(255,255,255,0.35)}
.tab-race-row{display:flex;flex-wrap:wrap;gap:4px;padding:8px 16px;background:#1a4a8a;align-items:center}
.tab-panel{display:none;padding:0}
.tab-panel.active{display:block}
.venue-race-content .race-title{border-radius:0}
"""
    # 競馬場タブ（最上位）
    venue_tabs = []
    ti = 0
    for venue in ordered_venues:
        if venue not in analyses_by_venue or not analyses_by_venue[venue]:
            continue
        vid = f"v{ti}"
        active = " active" if ti == 0 else ""
        venue_tabs.append(f'<button class="tab-venue{active}" data-venue="{vid}">{venue}</button>')
        ti += 1

    header = f"""<!DOCTYPE html>
<html lang="ja"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{date} 全レース予想 - D-AI競馬予想</title>
<style>{CSS}{tab_css}</style></head>
<body><div class="wrap">
<div class="race-title" style="margin-bottom:16px">
  <h1>{date} 中央競馬・地方競馬 全レース予想</h1>
  <div class="sub">競馬場タブ → レース番号タブで切り替え | 解析: {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
</div>
<div class="tab-container">
  <div class="tab-row">{"".join(venue_tabs)}</div>
"""
    # 初期表示: 最初の競馬場のレース行だけ表示
    for venue in ordered_venues:
        if venue not in analyses_by_venue or not analyses_by_venue[venue]:
            continue
        venue_id = f"v{ordered_venues.index(venue)}"
        # レース行の表示切り替え（競馬場タブ連動）はJSで
    # 簡易版: 全レース行を常時表示し、競馬場タブで該当行のみ表示
    # より簡単に: 競馬場タブ＋レースタブを1行に並べ、選択された競馬場のレースだけ表示
    # 再構成: 競馬場タブの下に、選択中競馬場の1R～12Rタブを出し、その下にパネル
    # パネルは競馬場+レースで一意なので、全パネルを並べてdata-venueでフィルタ
    # JSを修正して、競馬場クリック時に対応するレース行を表示し、最初のレースをアクティブに
    # シンプルに: 競馬場ごとにセクションを作り、各セクション内で1R～12Rタブ
    # これならHTMLがわかりやすい

    # よりシンプルな構造に変更: 競馬場ごとにブロック、ブロック内で1R～12Rタブ
    parts = [header]
    ti = 0
    for venue in ordered_venues:
        races = analyses_by_venue.get(venue, {})
        if not races:
            continue
        vid = f"v{ti}"
        race_nos = sorted(races.keys())
        first_rno = race_nos[0]
        first_pid = f"{vid}-{first_rno}"
        is_first_venue = ti == 0

        def _race_tab_label(r: int, analysis) -> str:
            """レース番号タブのラベル（見送り時は ×付き、印ありはマーク表示）"""
            has_buy = any(t.get("stake", 0) > 0 for t in (analysis.tickets or []))
            if not has_buy:
                return f"{r}R <span style='font-size:10px;opacity:.7'>×</span>"
            return f"{r}R"

        venue_header = f"""
<div class="tab-container venue-block" data-venue="{vid}" style="{"" if is_first_venue else "display:none"}">
  <div class="tab-row">
    <span style="color:rgba(255,255,255,0.9);font-weight:700;margin-right:12px">{venue}</span>
    {"".join(f'<button class="tab-race{" active" if (is_first_venue and r == first_rno) else ""}" data-panel="{vid}-{r}">{_race_tab_label(r, races[r])}</button>' for r in race_nos)}
  </div>
</div>
"""
        parts.append(venue_header)
        for rno in race_nos:
            analysis = races[rno]
            full_html = formatter.render(analysis)
            if "<body>" in full_html and "</body>" in full_html:
                start = full_html.find("<body>") + 6
                end = full_html.rfind("</body>")
                inner = full_html[start:end].strip()
            else:
                inner = full_html
            pid = f"{vid}-{rno}"
            show = "block" if (is_first_venue and rno == first_rno) else "none"
            parts.append(
                f'<div id="{pid}" class="tab-panel" data-venue="{vid}" style="display:{show}"><div class="wrap">{inner}</div></div>'
            )
        ti += 1

    footer = """
<script>
(function(){
  var venues = document.querySelectorAll('.tab-venue');
  var blocks = document.querySelectorAll('.venue-block');
  var panels = document.querySelectorAll('.tab-panel');
  venues.forEach(function(btn){
    btn.addEventListener('click',function(){
      var vid = btn.dataset.venue;
      venues.forEach(function(b){ b.classList.remove('active'); });
      btn.classList.add('active');
      blocks.forEach(function(b){ b.style.display = b.dataset.venue===vid ? 'block' : 'none'; });
      var firstPanel = document.querySelector('.tab-panel[data-venue="'+vid+'"]');
      if(firstPanel){
        panels.forEach(function(p){ p.style.display = 'none'; });
        document.querySelectorAll('.tab-race').forEach(function(b){ b.classList.remove('active'); });
        firstPanel.style.display = 'block';
        var r = document.querySelector('.tab-race[data-panel="'+firstPanel.id+'"]');
        if(r) r.classList.add('active');
      }
    });
  });
  document.querySelectorAll('.tab-race').forEach(function(btn){
    btn.addEventListener('click',function(){
      var pid = btn.dataset.panel;
      document.querySelectorAll('.tab-race').forEach(function(b){ b.classList.remove('active'); });
      btn.classList.add('active');
      panels.forEach(function(p){ p.style.display = p.id===pid ? 'block' : 'none'; });
    });
  });
})();
</script>
<div style="text-align:center;color:var(--muted);font-size:11px;margin-top:24px;padding:16px;border-top:1px solid var(--border)">
D-AI競馬予想　|　DATE_PLACEHOLDER 全レース一括分析
</div></div></body></html>"""
    footer = footer.replace("DATE_PLACEHOLDER", date)
    parts.append(footer)
    return "\n".join(parts)


_RE_MULTI_SPACE = _re.compile(r"[ \t]+")
_RE_BLANK_LINE = _re.compile(r"\n\s*\n")


def minify_html(html: str) -> str:
    """空白・改行を圧縮して HTML サイズを削減する（約30-40%削減）"""
    h = _RE_MULTI_SPACE.sub(" ", html)
    h = _RE_BLANK_LINE.sub("\n", h)
    h = h.replace("> <", "><")
    return h.strip()
