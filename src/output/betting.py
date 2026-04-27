"""買い目フォーメーション（レベル5） mixin"""

from src.models import Mark, RaceAnalysis


class BettingMixin:
    def _level5_formation(self, a: "RaceAnalysis") -> str:
        """
        K-4: フォーメーション買い目セクションのHTML生成。
        formation = {col1, col2, col3, umaren, sanrenpuku, coverage_grade, u_ratio}
        """
        fm = getattr(a, "formation", None)
        if not fm:
            return ""

        grade = fm.get("coverage_grade", "C")
        umaren_tix = fm.get("umaren", [])
        sanren_tix = fm.get("sanrenpuku", [])
        col1 = fm.get("col1", [])
        col2 = fm.get("col2", [])
        col3 = fm.get("col3", [])

        if not umaren_tix and not sanren_tix:
            return ""

        # ヘッダー
        total_stake = sum(t.get("stake", 0) for t in umaren_tix + sanren_tix)
        u_count = len([t for t in umaren_tix if t.get("stake", 0) > 0])
        s_count = len([t for t in sanren_tix if t.get("stake", 0) > 0])

        # 合成オッズ
        all_tix = umaren_tix + sanren_tix
        inv_sum = sum(
            t.get("stake", 100) / t["odds"]
            for t in all_tix
            if t.get("odds", 0) > 0
        )
        synth_odds = total_stake / inv_sum if inv_sum > 0 else 0

        html = f"""
<div class="card" style="margin-top:12px">
  <div class="section-title">■ フォーメーション買い目（自信度{grade}　合成オッズ：{synth_odds:.1f}倍）</div>
  <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px;font-size:13px">
    <span>馬連 <strong>{u_count}点</strong> + 三連複 <strong>{s_count}点</strong> ＝ 計 <strong>{u_count+s_count}点</strong></span>
    <span style="color:var(--muted)">合計 <strong>{total_stake:,}円</strong></span>
  </div>
"""

        # フォーメーション図（col1/col2/col3）
        def _col_html(evals, label):
            if not evals:
                return ""
            horses_html = "".join(
                f'<span class="m-{e.mark.value}" style="margin-right:4px">{e.mark.value}<span class="uma wk{e.horse.gate_no}">{e.horse.horse_no}</span></span>'
                for e in evals
            )
            return f'<div style="margin-bottom:6px"><span style="color:var(--muted);width:60px;display:inline-block">{label}</span>{horses_html}</div>'

        html += '<div style="background:var(--card-bg2);padding:8px;border-radius:6px;margin-bottom:10px;font-size:13px">'
        html += _col_html(col1, "1列目（軸）")
        html += _col_html(col2, "2列目（相手）")
        html += _col_html(col3, "3列目（ヒモ）")
        html += "</div>"

        # マーク付き馬番表示ヘルパー
        def _mspan(mark, no):
            return f'<span class="m-{mark}">{mark}</span><span class="uma">{no}</span>'

        # 買い目一覧（馬連）
        buy_u = [t for t in umaren_tix if t.get("stake", 0) > 0]
        if buy_u:
            html += '<div class="form-section-title" style="font-size:12px;color:var(--muted);margin-bottom:4px">馬連</div>'
            for t in buy_u[:10]:  # 最大10点表示
                ev = t.get("ev", 0)
                sig_cls = "sig-hoshi" if ev >= 300 else "sig-maru2" if ev >= 200 else "sig-maru1" if ev >= 100 else "sig-sankaku"
                no_a = t.get("a", t.get("no_a", "?"))
                no_b = t.get("b", t.get("no_b", "?"))
                mk_a = t.get("mark_a", "")
                mk_b = t.get("mark_b", "")
                combo = f"{_mspan(mk_a, no_a)} - {_mspan(mk_b, no_b)}"
                html += f"""<div class="ftkt {sig_cls}" style="font-size:12px">
  <span class="ftkt-type ftkt-type-umaren">馬連</span>
  <span class="ftkt-combo">{combo}</span>
  <span class="ftkt-ev-detail">
    <span class="ftkt-ev-label">EV</span><span class="ftkt-ev-val">{ev:.0f}%</span>
    <span class="ftkt-ev-label" style="margin-left:8px">購入</span><span class="ftkt-ev-num">{t.get('stake', 100)}円</span>
  </span>
</div>"""

        # 買い目一覧（三連複）
        buy_s = [t for t in sanren_tix if t.get("stake", 0) > 0]
        if buy_s:
            html += '<div class="form-section-title" style="font-size:12px;color:var(--muted);margin:8px 0 4px">三連複</div>'
            for t in buy_s[:15]:  # 最大15点表示
                ev = t.get("ev", 0)
                sig_cls = "sig-hoshi" if ev >= 300 else "sig-maru2" if ev >= 200 else "sig-maru1" if ev >= 100 else "sig-sankaku"
                no_a = t.get("a", t.get("no_a", "?"))
                no_b = t.get("b", t.get("no_b", "?"))
                no_c = t.get("c", t.get("no_c", "?"))
                mk_a = t.get("mark_a", "")
                mk_b = t.get("mark_b", "")
                mk_c = t.get("mark_c", "")
                combo = f"{_mspan(mk_a, no_a)} - {_mspan(mk_b, no_b)} - {_mspan(mk_c, no_c)}"
                html += f"""<div class="ftkt {sig_cls}" style="font-size:12px">
  <span class="ftkt-type ftkt-type-sanren">三連複</span>
  <span class="ftkt-combo">{combo}</span>
  <span class="ftkt-ev-detail">
    <span class="ftkt-ev-label">EV</span><span class="ftkt-ev-val">{ev:.0f}%</span>
    <span class="ftkt-ev-label" style="margin-left:8px">購入</span><span class="ftkt-ev-num">{t.get('stake', 100)}円</span>
  </span>
</div>"""

        html += "</div>"
        return html

    def _level5(self, a: RaceAnalysis) -> str:
        from src.calculator.betting import judge_confidence

        conf = judge_confidence(a.evaluations, a.pace_reliability)

        # analysis.tickets = 馬連4点 + 三連複6点 (固定10点, 各100円)
        tickets = a.tickets or []
        umaren_t     = [t for t in tickets if t.get("type") == "馬連"]
        sanrenpuku_t = [t for t in tickets if t.get("type") == "三連複"]
        has_tickets = bool(umaren_t or sanrenpuku_t)

        u_cover = sum(t.get("appearance", 0) for t in umaren_t)
        s_cover = sum(t.get("appearance", 0) for t in sanrenpuku_t)
        total_cover = u_cover + s_cover
        total_stake = sum(t.get("stake", 0) for t in tickets)

        def mspan(mark, no):
            return f'<span class="m-{mark}">{mark}</span><span class="uma">{no}</span>'

        def fmt_ticket_row(t: dict) -> str:
            ev_val  = t.get("ev", 0)
            odds    = t.get("odds", 0)
            prob    = t.get("appearance", 0)
            stake   = t.get("stake", 100)

            if ev_val >= 300.0:
                signal_mark, signal_label, sig_cls = "★", "勝負", "sig-hoshi"
            elif ev_val >= 200.0:
                signal_mark, signal_label, sig_cls = "◎", "買う", "sig-maru2"
            elif ev_val >= 100.0:
                signal_mark, signal_label, sig_cls = "○", "買う", "sig-maru1"
            elif ev_val >= 80.0:
                signal_mark, signal_label, sig_cls = "△", "検討", "sig-sankaku"
            else:
                signal_mark, signal_label, sig_cls = "", "", "sig-sankaku"

            if t.get("type") == "三連複":
                combo = (
                    f"{mspan(t.get('mark_a',''), t['a'])} - "
                    f"{mspan(t.get('mark_b',''), t['b'])} - "
                    f"{mspan(t.get('mark_c',''), t['c'])}"
                )
            else:
                combo = f"{mspan(t.get('mark_a',''), t['a'])} - {mspan(t.get('mark_b',''), t['b'])}"

            detail = (
                f'<span class="ftkt-ev-detail">'
                f'<span class="ftkt-ev-label">出現率</span>'
                f'<span class="ftkt-ev-num">{prob:.1f}%</span>'
                f'<span class="ftkt-ev-label"> × オッズ</span>'
                f'<span class="ftkt-ev-num">{odds:.1f}倍</span>'
                f'<span class="ftkt-ev-label"> ＝ 期待値</span>'
                f'<span class="ftkt-ev-val">{ev_val:.0f}%</span>'
                f'<span class="ftkt-ev-label" style="margin-left:8px">購入</span>'
                f'<span class="ftkt-ev-num">{stake:,}円</span>'
                f"</span>"
            )

            _type_cls = "ftkt-type-umaren" if t.get("type") == "馬連" else "ftkt-type-sanren" if t.get("type") == "三連複" else "ftkt-type-tansho"
            return f"""<div class="ftkt {sig_cls}">
  <span class="ftkt-type {_type_cls}">{t.get("type","")}</span>
  <span class="ftkt-combo">{combo}</span>
  {'<span class="ftkt-signal">' + signal_mark + signal_label + '</span>' if sig_cls else ''}
  {detail}
</div>"""

        _mk_pri = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "☆": 5}

        def _ticket_sort_key(t):
            a_key = _mk_pri.get(t.get("mark_a", ""), 9)
            b_key = _mk_pri.get(t.get("mark_b", ""), 9)
            c_key = _mk_pri.get(t.get("mark_c", ""), 9) if t.get("mark_c") else 0
            return (a_key, b_key, c_key)

        umaren_sorted     = sorted(umaren_t, key=_ticket_sort_key)
        sanrenpuku_sorted = sorted(sanrenpuku_t, key=_ticket_sort_key)

        umaren_rows = (
            "".join(fmt_ticket_row(t) for t in umaren_sorted)
            or '<div style="color:var(--muted);padding:6px">買い目なし</div>'
        )
        sanrenpuku_rows = (
            "".join(fmt_ticket_row(t) for t in sanrenpuku_sorted)
            or '<div style="color:var(--muted);padding:6px">買い目なし</div>'
        )

        mishon = (
            ""
            if has_tickets
            else f'<div class="mishon">🚫 <strong>見送り</strong>　相手馬（○▲△☆）が不足</div>'
        )

        # K-4: フォーメーション買い目HTML
        formation_html = self._level5_formation(a)

        # 合成オッズ算出
        inv_sum = sum(
            t.get("stake", 100) / t["odds"]
            for t in tickets
            if t.get("odds", 0) > 0
        )
        synth_odds = total_stake / inv_sum if inv_sum > 0 else 0

        return f"""
<div class="card">
<div class="section-title">■ 買い目（◎/◉軸 固定10点）</div>
{mishon}
<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px;font-size:13px;align-items:center">
  <span>自信度：<span class="badge b-{self._conf_cls(conf.value)}">&nbsp;{conf.value}&nbsp;</span></span>
  <span style="color:var(--muted)">合成オッズ：{synth_odds:.1f}倍</span>
  <span style="color:var(--muted)">{len(umaren_t)}点馬連 + {len(sanrenpuku_t)}点三連複 = 計{len(tickets)}点</span>
  <span style="color:var(--muted)">合計 {total_stake:,}円</span>
  <span style="color:var(--muted)">カバー出現率　馬連{u_cover:.1f}% + 三連複{s_cover:.1f}% = <strong>{total_cover:.1f}%</strong></span>
</div>
<div class="form-section-title">馬連　◎/◉ → ○▲△☆（{len(umaren_t)}点）</div>
{umaren_rows}
<div class="form-section-title">三連複　◎/◉軸 + 相手2頭 C(4,2)（{len(sanrenpuku_t)}点）</div>
{sanrenpuku_rows}
</div>
{formation_html}"""

    def _gen_ticket_comment(self, a, conf):
        """買い目の見解を生成"""
        if not a.tickets:
            return "買い目なし。"

        honmei = next((e for e in a.evaluations if e.mark in (Mark.TEKIPAN, Mark.HONMEI)), None)
        if not honmei:
            return "本命馬が確定していないため見送り。"

        sorted_ev = sorted(a.evaluations, key=lambda e: e.composite, reverse=True)
        second = sorted_ev[1] if len(sorted_ev) >= 2 else None

        parts = []

        if conf.value == "SS":
            parts.append(f"自信度SS：鉄板◉{honmei.horse.horse_name}で勝負。")
        elif conf.value == "S":
            if second:
                gap = honmei.composite - second.composite
                parts.append(
                    f"自信度S：本命◎{honmei.horse.horse_name}（総合{honmei.composite:.1f}）が2位{second.horse.horse_name}（{second.composite:.1f}）に{gap:.1f}pt差をつけて優位。"
                )
        elif conf.value == "A":
            parts.append(f"自信度A：本命◎{honmei.horse.horse_name}で固める。")
        elif conf.value == "B":
            if second:
                gap = honmei.composite - second.composite
                if gap < 2:
                    parts.append(
                        f"自信度B：本命◎{honmei.horse.horse_name}（総合{honmei.composite:.1f}）は確定だが、2位{second.horse.horse_name}（{second.composite:.1f}）との差{gap:.1f}ptの僅差。"
                    )
                else:
                    parts.append(
                        f"自信度B：本命◎{honmei.horse.horse_name}は確定だが、展開の不透明さなどから自信度はB。"
                    )
        elif conf.value == "C":
            parts.append("自信度C：混戦模様で自信度は低い。")
        else:
            parts.append(f"自信度{conf.value}：")

        # 固定10点の相手を列挙
        umaren_t = [t for t in (a.tickets or []) if t.get("type") == "馬連"]
        if umaren_t:
            partner_marks = [t.get("mark_b", "") for t in umaren_t[:4]]
            parts.append(f"馬連4点: {honmei.mark.value}→{'・'.join(partner_marks)}  三連複6点: 相手2頭組み合わせ。")

        return "　".join(parts)
