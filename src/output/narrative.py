"""馬の見解ナラティブ生成 mixin"""

from src.models import (
    AnaType,
    HorseEvaluation,
    KikenType,
    KishuPattern,
    Mark,
    Reliability,
    Trend,
)


class NarrativeMixin:
    def _gen_horse_narrative(self, ev: HorseEvaluation, all_ev: list, race=None) -> str:
        """各馬の見解を競馬記者のコラム風に生成。"""
        import re as _re

        h = ev.horse
        n = len(all_ev)
        abi_rank = len([e for e in all_ev if e.ability.total > ev.ability.total]) + 1
        pace_rank = len([e for e in all_ev if e.pace.total > ev.pace.total]) + 1
        co_rank = len([e for e in all_ev if e.course.total > ev.course.total]) + 1
        comp_rank = len([e for e in all_ev if e.composite > ev.composite]) + 1

        B = lambda s: f"<b>{s}</b>"
        NG = lambda s: f'<span style="color:#c0392b">{s}</span>'

        venue = race.course.venue if race else "このコース"
        surface = race.course.surface if race else ""
        dist = race.course.distance if race else 0
        run_style = ev.pace.running_style.value if ev.pace.running_style else ""
        sire = h.sire or ""

        sents = []

        # ── ① 書き出し：近走の流れから入る ──
        past = h.past_runs[:3] if h.past_runs else []
        if past:
            recent_pos = [r.finish_pos for r in past if r.finish_pos]
            big_runs = [
                r
                for r in (h.past_runs or [])[:10]
                if r.grade in ("G1", "JpnI", "G2", "JpnII", "G3", "JpnIII") and r.finish_pos <= 3
            ]
            if big_runs:
                r = big_runs[0]
                sents.append(B(f"{r.grade}{r.finish_pos}着の実績を持つ格上の存在"))
            elif len(recent_pos) >= 2:
                if all(p <= 3 for p in recent_pos):
                    sents.append(B("近走は安定して上位入着を続けており、充実ぶりが際立つ"))
                elif recent_pos[0] == 1:
                    sents.append(B("前走快勝の勢いは大きな武器"))
                elif recent_pos[0] <= 3:
                    sents.append("前走好走から引き続き好調を維持")
                elif all(p >= 5 for p in recent_pos):
                    sents.append("近走は精彩を欠く走りが続いている")
                else:
                    sents.append("近走は着順にバラつきがあり、ムラのある走りが気になる")
            elif past:
                p = past[0].finish_pos
                sents.append(
                    f"前走{past[0].venue}{past[0].distance}m{p}着"
                    if past[0].distance
                    else f"前走{p}着"
                )

        # ── ② 能力評価 ──
        trend = ev.ability.trend
        if abi_rank <= 2:
            if trend in (Trend.UP, Trend.RAPID_UP):
                sents.append(B("能力はメンバー中トップクラスで、なおかつ上昇気流に乗っている"))
            else:
                sents.append(B("持っている能力はこのメンバーでは抜けた存在"))
        elif abi_rank <= n // 3:
            if trend in (Trend.UP, Trend.RAPID_UP):
                sents.append("能力面では上位グループにつけ、状態面の上昇も加味すれば侮れない")
            else:
                sents.append("能力的には上位の一角で、地力は確か")
        elif abi_rank >= n * 2 // 3:
            if trend in (Trend.DOWN, Trend.RAPID_DOWN):
                sents.append(NG("能力面で見劣りするうえ、近走の内容も下降線で厳しい"))
            else:
                sents.append(NG("能力面ではこのメンバーに入ると苦しく、上位とは力差がある"))
        else:
            sents.append("能力的には中位の評価で、そのまま走れば圏内争いのボーダーライン上")

        # ── ③ コース適性・舞台 ──
        cr = ev.course.course_record
        sc = ev.course.shape_compatibility
        if co_rank <= 2:
            if cr >= 2.0:
                sents.append(B(f"{venue}は過去の実績からも得意舞台と言え、コース適性は申し分ない"))
            else:
                sents.append(
                    B(f"今回の{venue}{surface}{dist}mという条件設定は持ち味を活かせる舞台")
                )
        elif co_rank >= n * 2 // 3:
            if cr <= -1.0:
                sents.append(NG(f"{venue}では苦戦続きで、コース替わりが最大の不安材料"))
            else:
                sents.append(NG("コース適性の面ではやや割引が必要"))
        else:
            if cr >= 1.0:
                sents.append(f"{venue}での過去実績はまずまずで、舞台は悪くない")

        # ── ④ 血統 ──
        bl = ev.ability.bloodline_adj
        if sire and bl >= 1.0:
            sents.append(
                B(f"血統的にも父{sire}産駒は{surface}{dist}mで好走例が多く、下地は整っている")
            )
        elif sire and bl <= -1.0:
            sents.append(NG(f"父{sire}産駒はこの条件で苦戦する傾向があり、血統的な裏付けに乏しい"))
        elif sire and bl >= 0.5:
            sents.append(f"父{sire}の血統背景は悪くなく、条件的にも対応可能")

        # ── ⑤ 脚質・展開・枠順 ──
        gb = ev.pace.gate_bias
        gate = h.gate_no
        if run_style:
            if pace_rank <= 2:
                if run_style == "逃げ":
                    sents.append(
                        B(
                            "ハナを切れる見込みが高く、展開は理想的。自分の形に持ち込めれば粘り込みは十分"
                        )
                    )
                elif run_style in ("先行", "好位", "マクリ"):
                    sents.append(
                        B("好位からレースを進められる展開で、この脚質なら流れに乗りやすい")
                    )
                elif run_style == "中団":
                    sents.append(
                        B("中団から流れに乗れる形で、末脚を活かすタイミングも合いやすい")
                    )
                else:
                    sents.append(B("ペース想定が味方する形で、末脚を活かせる展開が見込める"))
            elif pace_rank >= n * 2 // 3:
                if run_style in ("差し", "追込"):
                    sents.append(
                        NG(
                            "後方からの競馬が想定されるが、今回の展開では差し届くか微妙。嵌らなければ圏外も"
                        )
                    )
                else:
                    sents.append(NG("展開的には向かない形で、自力で打開するしかない"))
            else:
                sents.append(f"{run_style}から自分なりの競馬はできる形")

        if gb >= 2.0:
            sents.append(B(f"{gate}番枠は内枠の利を活かしてロスなく立ち回れる好枠"))
        elif gb <= -2.0:
            sents.append(NG(f"{gate}番枠は外を回されるリスクがあり、枠順は恵まれたとは言い難い"))

        # ── ⑥ 騎手・調教師 ──
        if ev.jockey_change_pattern == KishuPattern.A:
            sents.append(
                B(f"鞍上{h.jockey}への乗り替わりは陣営の勝負手。テン乗りでも騎手力で補える")
            )
        elif ev.jockey_change_pattern == KishuPattern.E:
            sents.append(NG("鞍上見切りの乗り替わりは不安材料で、陣営も手詰まり感がある"))
        elif h.jockey and ev.jockey_stats:
            jdv = ev.jockey_stats.upper_long_dev
            if jdv >= 65:
                sents.append(
                    B(f"鞍上{h.jockey}はリーディング上位の実力派で、騎手の力量も大きなプラス")
                )
            elif jdv <= 45:
                sents.append(NG(f"鞍上{h.jockey}は勝ち味に遅いタイプで、鞍上の不安は否めない"))

        if ev.trainer_stats:
            tr_rank = ev.trainer_stats.rank.value
            if tr_rank == "A" and h.trainer:
                sents.append(f"{h.trainer}厩舎は仕上げ手腕に定評があり、管理馬の好走率は高い")

        # ── ⑦ 勝負気配 ──
        if ev.shobu_score >= 4:
            sents.append(B("調教の動き・陣営コメントなど複数の好材料が揃い、勝負度合いは相当高い"))
        elif ev.shobu_score >= 2.5:
            sents.append("陣営の仕上げにも抜かりはなく、勝負気配は感じ取れる")
        elif ev.shobu_score <= 0.5 and comp_rank <= n // 3:
            sents.append("ただし勝負気配が薄く、陣営の本気度に疑問符がつく")

        # ── ⑧ 穴馬・危険馬 ──
        if ev.kiken_type == KikenType.KIKEN_A:
            sents.append(NG(B("人気を背負うが、実力を過大評価されている危険な人気馬。消しも一手")))
        elif ev.kiken_type == KikenType.KIKEN_B:
            sents.append(NG("今回の条件は合わない面が多く、人気ほどの信頼は置けない"))
        if ev.ana_type == AnaType.ANA_A:
            sents.append(
                B("人気薄だが秘めた実力は上位級。配当妙味を考えれば積極的に狙いたい穴馬候補")
            )
        elif ev.ana_type == AnaType.ANA_B:
            sents.append(B("条件好転で一変の可能性あり。人気の盲点を突く面白い存在"))

        # ── ⑨ データ信頼度の注意 ──
        all_rel_c = all(e.ability.reliability == Reliability.C for e in all_ev)
        if ev.ability.reliability == Reliability.C and not all_rel_c:
            sents.append(NG("ただしキャリアが浅くデータが乏しいため、過信は禁物"))

        # ── ⑩ 締め：総合結論 ──
        if comp_rank == 1:
            sents.append(
                B("総合力ではこのメンバーで頭一つ抜けており、軸として最も信頼できる存在だ")
            )
        elif comp_rank == 2:
            sents.append(B("総合評価はメンバー中2番手。本命馬の僅かなスキを突ける対抗格"))
        elif comp_rank <= max(3, n // 4):
            sents.append("上位争いに加われる力は持っており、展開ひとつで馬券圏内は十分")
        elif comp_rank <= n // 2:
            sents.append("現状では上位勢との力差があり、好走には展開の味方が必要")
        else:
            sents.append(NG("このメンバーでは力関係的に厳しく、余程の展開利がなければ好走は難しい"))

        text = "。".join(s for s in sents if s) + "。"
        text = _re.sub(r"。{2,}", "。", text)
        text = _re.sub(r"\s{2,}", " ", text)
        return text
