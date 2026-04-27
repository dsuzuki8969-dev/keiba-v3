"""グレード変換・偏差値ヘルパー mixin"""

from typing import Optional


class GradeMixin:
    @staticmethod
    def _dev_to_grade(dev: float) -> str:
        """偏差値 → SS/S/A/B/C/D/E の7段階グレード
        目標分布: SS=2.5%, S=7.5%, A=20%, B=40%, C=20%, D=7.5%, E=2.5%"""
        if dev >= 65.0:
            return "SS"
        if dev >= 61.0:
            return "S"
        if dev >= 56.0:
            return "A"
        if dev >= 49.0:
            return "B"
        if dev >= 44.0:
            return "C"
        if dev >= 39.0:
            return "D"
        return "E"

    @staticmethod
    def _grade_css(grade: str) -> str:
        """グレード文字列 → CSSクラス（5色体系: 緑/青/赤/黒/灰）"""
        return f"grade-{grade}" if grade in ("SS", "S", "A", "B", "C", "D", "E") else "grade-B"

    @classmethod
    def _grade_html(cls, dev: float, size: int = 15) -> str:
        """偏差値 → グレードHTMLスパン"""
        g = cls._dev_to_grade(dev)
        c = cls._grade_css(g)
        return f'<span class="{c}" style="font-size:{size}px">{g}</span>'

    @staticmethod
    def _normalize_to_dev(val: float, val_min: float, val_max: float) -> float:
        """任意スコアを20-100の偏差値スケールに正規化"""
        if val_max <= val_min:
            return 50.0
        return max(20.0, min(100.0, 20.0 + (val - val_min) / (val_max - val_min) * 80.0))

    @staticmethod
    def _chakusa_label(ci: float) -> str:
        """着差指数(chakusa_index_avg) → テキストラベル"""
        if ci >= 0.3:
            return '<span style="color:var(--green);font-weight:700">◎ 堅実</span>'
        if ci >= 0.0:
            return '<span style="color:var(--green)">〇 安定</span>'
        if ci >= -0.3:
            return '<span style="color:var(--muted)">△ 普通</span>'
        if ci >= -0.6:
            return '<span style="color:#e67e22">▲ やや甘さ傾向</span>'
        return '<span style="color:#c0392b;font-weight:700">× 詰め甘い傾向</span>'

    def _inline_grade(self, ev, grade_attr: str, dev_attr: str) -> str:
        """属性名からグレード+偏差値のインライン表示を生成"""
        g = getattr(ev, grade_attr, None) or "—"
        d = getattr(ev, dev_attr, None)
        if g == "—" or g is None:
            return ""
        gc = self._grade_css(g)
        dev_s = f"({d:.1f})" if d is not None else ""
        return f' <span class="{gc}" style="font-size:11px">{g}</span><span style="font-size:9px;color:var(--muted)">{dev_s}</span>'

    def _conf_cls(self, v):
        return {"SS": "SS", "S": "S", "A": "A2", "B": "B2", "C": "C2", "D": "D", "E": "E"}.get(v, "B2")
