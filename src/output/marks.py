"""印・穴馬・危険馬セクション（レベル4） mixin"""

from html import escape as _esc

from src.models import AnaType, KikenType, Mark, RaceAnalysis


def _safe(s) -> str:
    """ユーザー由来文字列のHTMLエスケープ（None安全）"""
    return _esc(str(s)) if s else ""


class MarksMixin:
    def _level4(self, a: RaceAnalysis) -> str:
        from src.output.formatter import find_断層

        sev = sorted(a.evaluations, key=lambda e: e.composite, reverse=True)
        dan_pos = find_断層(sev)
        marked = [ev for ev in sev if ev.mark != Mark.NONE]

        mk_html = ""
        for ev in sev:
            if ev.mark == Mark.NONE:
                continue
            mk = ev.mark.value
            if ev.horse.odds is not None:
                pop = f"({ev.horse.popularity}人気)" if ev.horse.popularity else ""
                os = f"{ev.horse.odds:.1f}倍{pop}"
            elif ev.effective_odds:
                pred_list = sorted([e.effective_odds for e in sev if e.effective_odds])
                pred_rk = pred_list.index(ev.effective_odds) + 1 if ev.effective_odds in pred_list else "?"
                os = f'<span style="color:var(--muted);font-size:11px">[想定]</span>{ev.effective_odds:.1f}倍({pred_rk}人気)'
            else:
                os = "—"
            mk_html += f"""
<div style="display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--border)">
  <span class="m-{mk}" style="font-size:26px;width:32px;text-align:center">{mk}</span>
  <div>
    <strong style="font-size:15px"><span class="uma wk{ev.horse.gate_no}">{ev.horse.horse_no}</span> {_safe(ev.horse.horse_name)}</strong>
    <span data-live-odds="{ev.horse.horse_no}" style="color:var(--muted);font-size:12px;margin-left:6px">{os}</span>
  </div>
  <span style="margin-left:auto;font-size:20px;font-weight:700;color:var(--navy)">{ev.composite:.1f}</span>
</div>"""
            idx = sev.index(ev)
            if idx in dan_pos:
                gap = sev[idx].composite - sev[idx + 1].composite
                mk_html += f'<div class="danso">──── 断層　{gap:.1f}pt ────</div>'

        mark_comment = self._gen_mark_comment(marked, dan_pos, sev)

        # 特選穴馬
        tokusen = [ev for ev in a.evaluations if ev.is_tokusen]
        tokusen_html = ""
        for ev in sorted(tokusen, key=lambda e: e.tokusen_score, reverse=True):
            h = ev.horse
            os = f"{h.odds:.1f}倍" if h.odds else "—"
            pop = f"{h.popularity}人気" if h.popularity else "人気不明"
            total_count = len(a.evaluations)
            composite_rank = len([e for e in a.evaluations if e.composite > ev.composite]) + 1
            course_rank = len([e for e in a.evaluations if e.course.total > ev.course.total]) + 1

            # 理由テキスト自動生成
            reasons = []
            cr = ev.course.course_record
            if cr >= 45:
                reasons.append(
                    f"◆コース実績: コース偏差値{cr:.1f}、全馬{course_rank}位"
                )
            ct = ev.course.total
            if ct >= 52:
                reasons.append(f"◆コース適性: コース総合{ct:.1f}で高適性")
            comp = ev.composite
            if comp >= 50 and h.popularity and h.popularity > composite_rank + 1:
                reasons.append(
                    f"◆総合指数{comp:.1f}は{composite_rank}位ながら人気は{h.popularity}位 → 市場が過小評価"
                )
            from src.models import Trend
            if ev.ability.trend in (Trend.RAPID_UP, Trend.UP):
                reasons.append(f"◆近走パフォーマンス{ev.ability.trend.value}で調子上向き")
            if ev.odds_consistency_adj >= 2.0:
                reasons.append("◆オッズ整合性が高く、内部指標が人気以上の実力を示唆")

            eff_odds_str = f"{h.odds:.1f}倍" if h.odds else "オッズ未確定"
            reasons.append(f"→ {eff_odds_str}の妙味は大きい。紐に積極的に組み入れたい")
            reason_text = "\n".join(reasons)

            tokusen_html += f"""
<div style="padding:10px;border-bottom:1px solid var(--border)">
  <span class="badge b-tokusen">特選穴馬</span>
  <strong style="margin-left:6px"><span class="uma wk{h.gate_no}">{h.horse_no}</span> {_safe(h.horse_name)}</strong>
  <span style="color:var(--muted);font-size:12px;margin-left:4px">{os}・{pop}・総合{ev.composite:.1f}（{composite_rank}位/{total_count}頭）</span>
  <div style="font-size:13px;color:#555;margin-top:6px;line-height:1.6;white-space:pre-line">
【なぜ特選穴馬なのか】
{_safe(reason_text)}
  </div>
</div>"""

        # 穴馬
        ana = [ev for ev in a.evaluations if ev.ana_type != AnaType.NONE]
        ana_html = ""
        for ev in sorted(ana, key=lambda e: e.ana_score, reverse=True):
            h = ev.horse
            os = f"{h.odds:.1f}倍" if h.odds else "—"
            pop = f"{h.popularity}人気" if h.popularity else "人気不明"

            total_count = len(a.evaluations)
            composite_rank = len([e for e in a.evaluations if e.composite > ev.composite]) + 1
            ability_rank = len([e for e in a.evaluations if e.ability.total > ev.ability.total]) + 1
            pace_rank = len([e for e in a.evaluations if e.pace.total > ev.pace.total]) + 1
            course_rank = len([e for e in a.evaluations if e.course.total > ev.course.total]) + 1

            reason_parts = []
            if h.popularity and h.popularity > composite_rank + 2:
                reason_parts.append(
                    f"◆実力は{composite_rank}位/{total_count}頭相当だが人気は{h.popularity}位 → 市場が過小評価"
                )

            strengths = []
            if ev.pace.total >= 55:
                strengths.append(
                    f"展開偏差値{ev.pace.total:.1f}で全馬{pace_rank}位、今回のペースで力を発揮しやすい"
                )
            if ev.course.total >= 55:
                strengths.append(
                    f"コース偏差値{ev.course.total:.1f}で全馬{course_rank}位、このコースでの実績良好"
                )
            if ev.ability.trend.value in ("急上昇", "上昇"):
                strengths.append(f"近走パフォーマンス{ev.ability.trend.value}で調子上向き")
            if ev.ability.total >= 50:
                strengths.append(f"能力偏差値{ev.ability.total:.1f}（{ability_rank}位）で実力十分")
            if ev.shobu_score >= 4:
                strengths.append("勝負気配あり、陣営が狙っている可能性")
            if ev.jockey_change_pattern and ev.jockey_change_pattern.value == "A":
                strengths.append("騎手強化で上積みあり")

            if strengths:
                reason_parts.append("◆強み：" + "、".join(strengths[:3]))

            if len(strengths) >= 2:
                reason_parts.append("→ 人気薄だが実力・展開・コースが揃っており、激走の可能性大")
            else:
                reason_parts.append(
                    f"→ {f'{h.odds:.1f}倍' if h.odds is not None else 'オッズ未確定'}の妙味あり"
                )

            reason_text = "\n".join(reason_parts) if reason_parts else "複数の好材料あり"

            ana_html += f"""
<div style="padding:10px;border-bottom:1px solid var(--border)">
  <span class="badge b-ana">{ev.ana_type.value}</span>
  <strong style="margin-left:6px"><span class="uma wk{h.gate_no}">{h.horse_no}</span> {_safe(h.horse_name)}</strong>
  <span style="color:var(--muted);font-size:12px;margin-left:4px">{os}・{pop}・総合{ev.composite:.1f}（{composite_rank}位/{total_count}頭）</span>
  <div style="font-size:13px;color:#555;margin-top:6px;line-height:1.6;white-space:pre-line">
【なぜ穴馬なのか】
{_safe(reason_text)}
  </div>
</div>"""
        if not ana_html:
            ana_html = '<div style="color:var(--muted);padding:8px">該当なし</div>'

        # 特選危険馬
        tokusen_kiken = [ev for ev in a.evaluations if ev.is_tokusen_kiken]
        tk_html = ""
        for ev in sorted(tokusen_kiken, key=lambda e: e.tokusen_kiken_score, reverse=True):
            h = ev.horse
            os = f"{h.odds:.1f}倍" if h.odds else "—"
            pop = f"{h.popularity}人気" if h.popularity else "人気不明"

            total_count = len(a.evaluations)
            composite_rank = len([e for e in a.evaluations if e.composite > ev.composite]) + 1

            # 理由テキスト自動生成
            reasons = []

            # 前走人気降格
            if hasattr(h, "past_runs") and h.past_runs:
                prev_pop = getattr(h.past_runs[0], "popularity", None)
                cur_pop = h.popularity or 99
                if prev_pop and prev_pop > cur_pop:
                    reasons.append(
                        f"◆前走{prev_pop}人気→今回{cur_pop}人気に急浮上、実力以上の評価の可能性"
                    )

            # 多頭数
            if total_count >= 13:
                reasons.append(f"◆{total_count}頭立ての多頭数戦、上位人気でも凡走率が上昇")

            # 前走着順
            if hasattr(h, "past_runs") and h.past_runs:
                prev_fp = getattr(h.past_runs[0], "finish_pos", None)
                if prev_fp and prev_fp >= 5:
                    reasons.append(f"◆前走{prev_fp}着と凡走しており、不安材料")

            # 連続凡走
            consec = 0
            if hasattr(h, "past_runs") and h.past_runs:
                for r in h.past_runs:
                    fp = getattr(r, "finish_pos", None)
                    if fp and fp >= 4:
                        consec += 1
                    else:
                        break
            if consec >= 2:
                reasons.append(f"◆直近{consec}走連続で4着以下、スランプ気配")

            # 総合偏差値と人気の乖離
            if h.popularity and composite_rank > h.popularity + 2:
                reasons.append(
                    f"◆{h.popularity}人気だが総合指数は{composite_rank}位/{total_count}頭 → 過大評価"
                )

            reasons.append("→ 人気先行で期待値が低い、軸には危険")
            reason_text = "\n".join(reasons)

            tk_html += f"""
<div style="padding:10px;border-bottom:1px solid var(--border)">
  <span class="badge b-tokusen-kiken">特選危険馬</span>
  <strong style="margin-left:6px"><span class="uma wk{h.gate_no}">{h.horse_no}</span> {_safe(h.horse_name)}</strong>
  <span style="color:var(--muted);font-size:12px;margin-left:4px">{os}・{pop}・総合{ev.composite:.1f}（{composite_rank}位/{total_count}頭）</span>
  <div style="font-size:13px;color:#555;margin-top:6px;line-height:1.6;white-space:pre-line">
【なぜ危険なのか】
{_safe(reason_text)}
  </div>
</div>"""

        # 旧方式の危険馬（特選危険馬がいない場合のフォールバック）
        kiken = [ev for ev in a.evaluations if ev.kiken_type != KikenType.NONE]
        kk_html = ""
        if not tk_html:
            for ev in sorted(kiken, key=lambda e: e.kiken_score, reverse=True):
                h = ev.horse
                os = f"{h.odds:.1f}倍" if h.odds else "—"
                pop = f"{h.popularity}人気" if h.popularity else "人気不明"
                total_count = len(a.evaluations)
                composite_rank = len([e for e in a.evaluations if e.composite > ev.composite]) + 1

                reason_parts = []
                if h.popularity and composite_rank > h.popularity + 2:
                    reason_parts.append(
                        f"◆{h.popularity}人気（{os}）だが総合偏差値は{composite_rank}位/{total_count}頭 → 過大評価"
                    )
                reason_parts.append("→ 人気先行で期待値が低い、軸には危険")
                reason_text = "\n".join(reason_parts)

                kk_html += f"""
<div style="padding:10px;border-bottom:1px solid var(--border)">
  <span class="badge b-kiken">{ev.kiken_type.value}</span>
  <strong style="margin-left:6px"><span class="uma wk{h.gate_no}">{h.horse_no}</span> {_safe(h.horse_name)}</strong>
  <span style="color:var(--muted);font-size:12px;margin-left:4px">{os}・{pop}・総合{ev.composite:.1f}（{composite_rank}位/{total_count}頭）</span>
  <div style="font-size:13px;color:#555;margin-top:6px;line-height:1.6;white-space:pre-line">
【なぜ危険なのか】
{_safe(reason_text)}
  </div>
</div>"""

        # 統合: 特選危険馬があればそちら、なければ旧方式
        danger_html = tk_html or kk_html
        if not danger_html:
            danger_html = '<div style="color:var(--muted);padding:8px">該当なし</div>'

        return f"""
<div class="card">
<div class="section-title">■ 印・穴馬・危険な人気馬</div>
<div class="sub-title">■ 印</div>
{mk_html}
{"" if not tokusen_html else f'<div class="sub-title" style="margin-top:16px">■ 特選穴馬</div>{tokusen_html}'}
<div class="sub-title" style="margin-top:16px">■ 穴馬</div>{ana_html}
<div class="sub-title" style="margin-top:16px">■ 危険な人気馬</div>{danger_html}
</div>"""

    def _gen_mark_comment(self, marked, dan_pos, sev):
        if not marked:
            return "印を打てる馬が見当たりません。"
        top = marked[0]
        second = marked[1] if len(marked) >= 2 else None
        third = marked[2] if len(marked) >= 3 else None
        top_mk = top.mark.value

        # ---- 本命馬の武器を言語化 ----
        abi = top.ability.total
        pac = top.pace.total
        cou = top.course.total
        factors = [("能力", abi), ("展開", pac), ("コース適性", cou)]
        factors_sorted = sorted(factors, key=lambda x: x[1], reverse=True)
        best_name, best_val = factors_sorted[0]
        worst_name, worst_val = factors_sorted[2]

        # 武器の具体的な表現
        weapon_map = {
            "能力": "過去走の実績とスピード指数",
            "展開": "ペースや脚質の噛み合い",
            "コース適性": "コース形態や馬場への適性",
        }
        weapon = weapon_map.get(best_name, best_name)

        parts = [f"{top_mk}{top.horse.horse_name}は{weapon}が最大の武器。"]

        # 弱点があれば
        if best_val - worst_val >= 5:
            weakness_map = {
                "能力": "地力面",
                "展開": "今回の展開面",
                "コース適性": "コース相性",
            }
            parts.append(f"ただし{weakness_map.get(worst_name, worst_name)}に課題を残す。")

        # ---- 2位との力関係 ----
        if second:
            gap = top.composite - second.composite

            if gap >= 5:
                parts.append(
                    f"メンバー内では抜けた存在で、"
                    f"2番手の{second.horse.horse_name}とは大きな力差がある。"
                    f"頭固定の馬券で勝負できる一戦。"
                )
            elif gap >= 2:
                # 2位の武器を簡潔に
                s_factors = sorted(
                    [("能力", second.ability.total), ("展開", second.pace.total), ("コース適性", second.course.total)],
                    key=lambda x: x[1], reverse=True,
                )
                s_weapon = s_factors[0][0]
                parts.append(
                    f"対抗の{second.horse.horse_name}は{s_weapon}で勝負するタイプ。"
                    f"本命が一歩リードも、展開ひとつで逆転の目はある。"
                )
            else:
                parts.append(
                    f"{second.horse.horse_name}との力差は紙一重。"
                    f"どちらが頭でもおかしくない難解な一戦。"
                )

        # ---- 近走の勢い ----
        tv = top.ability.trend.value
        if "上昇" in tv:
            parts.append("近走は上昇カーブを描いており、勢いは十分。")
        elif "下降" in tv:
            parts.append("ただし近走はパフォーマンスが落ちており、過信は禁物。")

        # ---- 馬券の組み立て方 ----
        if second and third:
            if top.composite - (third.composite if third else 0) >= 8:
                parts.append(
                    f"相手は{second.mark.value}{second.horse.horse_name}と"
                    f"{third.mark.value}{third.horse.horse_name}に絞って厚く勝負したい。"
                )
            else:
                parts.append(
                    f"相手は{second.mark.value}{second.horse.horse_name}、"
                    f"{third.mark.value}{third.horse.horse_name}を中心に手広く構えたい。"
                )

        return "".join(parts)
