"""前3走実績テーブル mixin"""

from html import escape as _esc
from typing import Optional

from data.masters.venue_master import VENUE_MAP
from src.models import HorseEvaluation


def _safe(s) -> str:
    """ユーザー由来文字列のHTMLエスケープ（None安全）"""
    return _esc(str(s)) if s else ""


class PastRunsMixin:
    @staticmethod
    def _fmt_time(sec: float) -> str:
        """秒数を競馬タイム表記（1:34.5）に変換"""
        if sec <= 0:
            return "—"
        m = int(sec // 60)
        s = sec - m * 60
        if m > 0:
            return f"{m}:{s:04.1f}"
        return f"{s:.1f}"

    @staticmethod
    def _fmt_corners(run) -> str:
        """コーナー通過順位を '2-2-3-4' 形式で返す"""
        if run.positions_corners:
            corners = run.positions_corners
            # 不正データ除外: 0を含む or 要素が1つだけ → position_4cフォールバック
            if all(c > 0 for c in corners) and len(corners) >= 2:
                return "-".join(str(p) for p in corners)
            # 要素1つで有効値 → position_4cとして扱う
            if len(corners) == 1 and corners[0] > 0:
                return str(corners[0])
        if run.position_4c and run.position_4c > 0:
            return str(run.position_4c)
        return "—"

    def _calc_cross_surface_dev(self, run) -> Optional[float]:
        """異サーフェスの走に対して走破偏差値を計算。"""
        if not self._std_calc:
            return None
        vc = VENUE_MAP.get(run.venue)
        if not vc:
            return None
        course_id = f"{vc}_{run.surface}_{run.distance}"
        try:
            st, _ = self._std_calc.calc_standard_time(
                course_id, run.grade or "", run.condition or "良", run.distance
            )
        except Exception:
            return None
        if st is None:
            return None
        from src.calculator.ability import calc_run_deviation

        try:
            return calc_run_deviation(run.finish_time_sec, st, run.distance)
        except Exception:
            return None

    @staticmethod
    def _race_level_label(race_level_dev: Optional[float]) -> str:
        """race_level_dev → 5段階レースレベル表示（前走のレースレベル評価、馬個人の評価ではない）"""
        prefix = '<span style="font-size:8px;color:var(--muted);margin-right:1px">前走Lv</span>'
        if race_level_dev is None:
            return f'{prefix}<span style="color:var(--muted);font-size:9px">—</span>'
        if race_level_dev >= 58:
            return f'{prefix}<span style="color:#c0392b;font-size:9px;font-weight:700" title="前走：勝ち馬タイムが基準を大幅上回る高速決着">★★★★★</span>'
        elif race_level_dev >= 54:
            return f'{prefix}<span style="color:#e67e22;font-size:9px;font-weight:700" title="前走：平均を上回るハイレベルな決着">★★★★</span>'
        elif race_level_dev >= 47:
            return f'{prefix}<span style="color:var(--muted);font-size:9px" title="前走：標準的なタイムレベル">★★★</span>'
        elif race_level_dev >= 43:
            return f'{prefix}<span style="color:var(--blue);font-size:9px" title="前走：平均を下回るスローな決着">★★</span>'
        else:
            return f'{prefix}<span style="color:#8e44ad;font-size:9px" title="前走：明らかにタイムレベルが低い">★</span>'

    def _gen_past_runs_table(self, ev: HorseEvaluation) -> str:
        """前3走の実績テーブルを生成。"""
        h = ev.horse
        rec_map = {
            entry[0].race_date: (entry[0], entry[1], entry[2]) for entry in ev.ability.run_records
        }

        display_runs = h.past_runs[:3]
        if not display_runs:
            return ""

        l3f_rank_map: dict = {}
        for entry in ev.ability.run_records:
            if len(entry) >= 4:
                l3f_rank_map[entry[0].race_date] = entry[3]

        rows = ""
        for i, run in enumerate(display_runs):
            matched = rec_map.get(run.race_date)
            dev = matched[1] if matched else None
            st = matched[2] if matched else None

            pos_cls = "past-win" if run.finish_pos == 1 else ""
            # 着順: 1着=緑, 2着=青, 3着=赤
            _pos = run.finish_pos
            if _pos >= 90:
                finish_s = '<span style="color:var(--muted)">取消</span>'
            elif _pos == 1:
                finish_s = '<span class="rank-1"><b>1着</b></span>'
            elif _pos == 2:
                finish_s = '<span class="rank-2"><b>2着</b></span>'
            elif _pos == 3:
                finish_s = '<span class="rank-3"><b>3着</b></span>'
            else:
                finish_s = f"{_pos}着"

            grade_s = ""
            if run.grade in ("G1", "G2", "G3"):
                g_color = {"G1": "#c0392b", "G2": "#2c6dbf", "G3": "#27ae60"}[run.grade]
                grade_s = f'<span style="color:{g_color};font-weight:700;font-size:9px">{run.grade}</span> '

            surf_char = run.surface[0] if run.surface else "?"
            surf_cls = f"past-surface-{surf_char}"
            course_s = (
                f'{grade_s}<span class="{surf_cls}">{surf_char}</span>'
                f"{run.distance}m {run.condition}"
            )

            corner_s = f'<span class="past-pos-chip">{self._fmt_corners(run)}</span>'

            f3f_s = f"{run.first_3f_sec:.1f}" if run.first_3f_sec else "—"
            if run.last_3f_sec and run.last_3f_sec > 0:
                _l3f_val = f"{run.last_3f_sec:.1f}"
                # 上がり3Fランク色: 1位=緑, 2位=青, 3位=赤
                _l3f_rank = l3f_rank_map.get(run.race_date)
                if _l3f_rank == 1:
                    l3f_s = f'<span class="l3f-r1">{_l3f_val}</span>'
                elif _l3f_rank == 2:
                    l3f_s = f'<span class="l3f-r2">{_l3f_val}</span>'
                elif _l3f_rank == 3:
                    l3f_s = f'<span class="l3f-r3">{_l3f_val}</span>'
                else:
                    l3f_s = _l3f_val
            else:
                l3f_s = "—"

            pace_s = run.pace.value if run.pace else "—"
            _pv = run.pace.value if run.pace else ""
            if _pv in ("HH", "HM"):
                pace_color = "#c0392b"
            elif _pv in ("MM",):
                pace_color = "var(--blue)"
            elif _pv in ("MS", "SS"):
                pace_color = "var(--green)"
            else:
                pace_color = "var(--muted)"

            if run.finish_pos == 1:
                mb = run.margin_behind or 0.0
                if mb <= 0:
                    margin_s = "—"
                else:
                    margin_s = f"-{min(mb, 10.0):.1f}"
            else:
                ma = run.margin_ahead or 0.0
                if 0 < ma <= 15.0:
                    margin_s = f"+{ma:.1f}"
                else:
                    margin_s = "—"

            time_s = self._fmt_time(run.finish_time_sec)
            if st:
                diff = run.finish_time_sec - st
                diff_s = f'<span style="font-size:9px;color:var(--muted)">基準{diff:+.1f}秒</span>'
            else:
                diff_s = ""

            is_cross_surface = matched is None
            if dev is None:
                dev = self._calc_cross_surface_dev(run)
            if dev is not None:
                if matched and matched[2] is None:
                    dev_s = '<span style="color:var(--muted)">—</span>'
                else:
                    _dg = self._dev_to_grade(dev)
                    _dgc = self._grade_css(_dg)
                    dev_s = f'<span class="{_dgc}" style="font-weight:700">{_dg}</span>'
            else:
                dev_s = '<span style="color:var(--muted)">—</span>'

            # 人気: 1人気=緑, 2人気=青, 3人気=赤
            _pop = getattr(run, "popularity_at_race", None)
            if _pop == 1:
                pop_s = '<span class="rank-1">1</span>'
            elif _pop == 2:
                pop_s = '<span class="rank-2">2</span>'
            elif _pop == 3:
                pop_s = '<span class="rank-3">3</span>'
            elif _pop:
                pop_s = str(_pop)
            else:
                pop_s = "—"

            # 前走リンク: race_idがあればnetkeibaのレース結果ページへリンク
            _rid = getattr(run, "race_id", "")
            if _rid and len(_rid) >= 6:
                # NAR(venue_code 30-65) vs JRA(01-10)でドメインが異なる
                _vc_link = _rid[4:6] if len(_rid) >= 6 else ""
                _vc_int = int(_vc_link) if _vc_link.isdigit() else 0
                _nk_domain = "nar.netkeiba.com" if _vc_int >= 30 else "race.netkeiba.com"
                _date_venue = f'<a href="https://{_nk_domain}/race/result.html?race_id={_esc(_rid)}" target="_blank" rel="noopener" style="color:var(--muted);text-decoration:none;border-bottom:1px dotted var(--muted)" title="レース結果を見る">{_safe(run.race_date[5:])} {_safe(run.venue)}</a>'
            else:
                _date_venue = f'{_safe(run.race_date[5:])} {_safe(run.venue)}'

            rows += f"""<tr class="{pos_cls}">
  <td style="text-align:left;color:var(--muted)">{_date_venue}</td>
  <td style="text-align:left">{course_s}</td>
  <td>{_safe(run.class_name)}</td>
  <td>{pop_s}</td>
  <td>{finish_s}/{run.field_count}頭</td>
  <td style="text-align:left;max-width:70px;overflow:hidden;white-space:nowrap">{_safe(run.jockey[:5])}</td>
  <td>{dev_s}</td>
  <td>{corner_s}</td>
  <td>{f3f_s}</td>
  <td>{l3f_s}</td>
  <td><span style="color:{pace_color};font-weight:700">{pace_s}</span></td>
  <td>{time_s}<br>{diff_s}</td>
  <td>{margin_s}</td>
</tr>"""

        return f"""
<div class="sub-title" style="margin-top:12px">【前3走実績】<span style="font-size:10px;color:var(--muted);font-weight:400;margin-left:8px">走破偏差値は当コース・馬場補正済み（基準タイムとの差から算出）</span></div>
<div style="overflow-x:auto">
<table class="past-table">
<thead><tr>
  <th>日付・競馬場</th><th>コース</th><th>クラス</th><th>人気</th><th>着/頭</th>
  <th>騎手</th><th>走破偏差値</th><th>位置取り</th>
  <th title="レース全体の前半ペース（個人タイムではない）">前半P</th>
  <th title="この馬の上がり3Fタイム（個人）">上3F</th>
  <th>ペース</th><th>タイム</th><th>着差</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>
</div>"""
