# -*- coding: utf-8 -*-
"""compute_danso_columns / generate_danso_tickets unit test (2026-06-24 新仕様版)

【新仕様（compute_danso_columns / 2026-06-24 マスター確定）】
  ◉(tekipan) と ◎(honmei) は composite 1位の**同一馬**に付く別印。本命H は常に 1 頭のみ。
  軸 = ◉あれば◉、なければ◎。

  共通ゲート: g1 = comp(軸) - comp(○) ≧ 8.0 で購入。未達は None(見送り)。
              ○不在・有効6頭未満も None。

  判定順は **C先行 → A → B → 見送り**:
    present = [○,▲,△,★,☆] のうち存在する印（強さ順）
    span = comp(○) - comp(present末尾)
    g2   = comp(○) - comp(▲)

    1. C(団子・総流し): span < 5.0
         col2 = col3 = [○▲△★☆ 存在分の馬番]（総流し）
    2. A(○抜け):       g2 ≧ 5.0
         col2 = [○の馬番] / col3 = [▲△★☆ 存在分の馬番]
    3. B(○▲拮抗):      g2 < 3.0 かつ comp(▲)-comp(△) ≧ 5.0
         col2 = [○▲の馬番] / col3 = [○▲△★☆ 存在分の馬番]
    4. 谷間(g2 が 3.0〜5.0 等の非該当) → None(見送り)

    col3 には穴・抑の馬番を sorted(set(...)) で追加。

  戻り値:
    発火時 {"formation":"danso_gap","col1":[軸馬番],"col2":[...],"col3":[...],"skip_reason":None}
    見送り時 None

  ※ 旧実装は formation に "A-F1"/"A-F2"/"B-F1"/"B-F2"/"C" を返したが、
    新実装は発火時 **常に "danso_gap"**。パターン区別は col1/col2/col3 の
    馬番セットで検証する（A型=col2 が○1頭 / B型=col2 が○▲2頭 / C型=col2==col3 で全印）。

検証ケース:
  1. A型(○抜け): g1≥8 + g2≥5 + span≥5 → col2=[○] col3=[▲△★☆] (4点)
  2. B型(○▲拮抗): g1≥8 + g2<3 + ▲-△≥5 + span≥5 → col2=[○▲] col3=[○▲△★☆] (7点)
  3. C型(団子): g1≥8 + span<5 → col2==col3=[○▲△★☆] (10点)
  4. 見送り(谷間): g1≥8 + g2 が 3〜5 + span≥5 → None
  5. 見送り(ゲート未達): g1<8 → None
  6. C先行: span<5 なら（A的な g2≥5 でも）C を選ぶ
  7. A優先(span≥5): span≥5 かつ g2≥5 → A（C は span≥5 で非該当）
  8. 特選ゲート×3 / stake均等 / ticket必須フィールド / 本命◉認識 / 取消除外
"""
import sys
import os
from dataclasses import dataclass, field
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── モック用の最小限 dataclass 群 ──────────────────────────────────────────

@dataclass
class MockHorse:
    horse_no: int
    odds: Optional[float] = None
    popularity: Optional[int] = None


class MockMark:
    """Mark enum の最小モック"""
    def __init__(self, value: str):
        self.value = value


@dataclass
class MockAbility:
    total: float = 50.0


@dataclass
class MockPace:
    total: float = 50.0


@dataclass
class MockCourse:
    total: float = 50.0


@dataclass
class MockHorseEvaluation:
    horse: MockHorse
    _composite_val: float
    mark: MockMark = field(default_factory=lambda: MockMark("－"))
    win_prob: float = 0.0
    place2_prob: float = 0.0
    place3_prob: float = 0.0
    is_scratched: bool = False
    is_tokusen: bool = False
    is_tokusen_kiken: bool = False
    predicted_odds: Optional[float] = None
    ability: MockAbility = field(default_factory=MockAbility)
    pace: MockPace = field(default_factory=MockPace)
    course: MockCourse = field(default_factory=MockCourse)

    @property
    def composite(self) -> float:
        return self._composite_val

    @property
    def effective_odds(self) -> Optional[float]:
        return self.horse.odds if self.horse.odds is not None else self.predicted_odds


@dataclass
class MockRaceInfo:
    field_count: int = 16
    is_jra: bool = True


# ── ヘルパー ──────────────────────────────────────────────────────────────

def _make_eval(horse_no: int, mark_val: str, comp: float,
               odds: float = 10.0, win_prob: float = 0.06,
               is_scratched: bool = False) -> MockHorseEvaluation:
    """テスト用 HorseEvaluation モックを生成する。"""
    horse = MockHorse(horse_no=horse_no, odds=odds)
    ev = MockHorseEvaluation(
        horse=horse,
        _composite_val=comp,
        mark=MockMark(mark_val),
        win_prob=win_prob,
        place2_prob=min(win_prob * 2, 0.99),
        place3_prob=min(win_prob * 3, 0.99),
        is_scratched=is_scratched,
    )
    return ev


def _count_distinct_combos(tickets):
    """sanrenpuku チケットの distinct (昇順タプル) 数を返す。"""
    return len({tuple(sorted([t["a"], t["b"], t["c"]])) for t in tickets})


def _run_danso(evaluations, field_count=16):
    """generate_danso_tickets をモックリストで実行する。"""
    from src.calculator.betting import generate_danso_tickets as _danso
    race = MockRaceInfo(field_count=field_count)
    return _danso(evaluations, race)  # type: ignore[arg-type]


def _run_columns(evaluations, field_count=16):
    """compute_danso_columns を entries list で実行する。"""
    from src.calculator.betting import compute_danso_columns
    entries = [
        {
            "mark":        e.mark.value,
            "composite":   e.composite,
            "horse_no":    e.horse.horse_no,
            "odds":        e.horse.odds or 10.0,
            "is_scratched": e.is_scratched,
        }
        for e in evaluations
    ]
    return compute_danso_columns(entries)


# ── 標準テストシナリオ生成関数 ────────────────────────────────────────────
#
# 新ゲート g1 = comp(軸)-comp(○) ≧ 8.0 を全シナリオで確保するため、
# 軸の composite を高めに置く（◎=85, ○=75 → g1=10）。

def _make_a_evs():
    """
    A型(○抜け)シナリオ:
      軸◎=85 ○=75 → g1=10≥8 (ゲート通過)
      ○-▲ = 75-68 = 7 ≧ 5 → A 条件成立 (g2≥5)
      span = comp(○)-comp(☆) = 75-55 = 20 ≧ 5 → C 非該当 (C先行を回避)
      期待: formation=danso_gap col1=[1(◎)] col2=[2(○)] col3=[3(▲),4(△),5(★),6(☆)] → 4点
    """
    return [
        _make_eval(1, "◎", 85.0, odds=2.0, win_prob=0.20),  # 軸(本命H)
        _make_eval(2, "○", 75.0, odds=5.0, win_prob=0.12),  # g1=10
        _make_eval(3, "▲", 68.0, odds=8.0, win_prob=0.08),  # g2=7≥5 → A
        _make_eval(4, "△", 63.0, odds=12.0, win_prob=0.06),
        _make_eval(5, "★", 59.0, odds=15.0, win_prob=0.05),
        _make_eval(6, "☆", 55.0, odds=20.0, win_prob=0.04),  # span=75-55=20
        _make_eval(7, "-",  46.0, odds=30.0, win_prob=0.03),  # 無印
        _make_eval(8, "-",  44.0, odds=40.0, win_prob=0.02),
    ]


def _make_b_evs():
    """
    B型(○▲拮抗)シナリオ:
      軸◎=85 ○=75 → g1=10≥8 (ゲート通過)
      ○-▲ = 75-73 = 2 < 3 → A 非該当 (g2<5) / B の前段条件成立
      ▲-△ = 73-66 = 7 ≧ 5 → B 条件成立
      span = comp(○)-comp(☆) = 75-58 = 17 ≧ 5 → C 非該当 (C先行を回避)
      期待: formation=danso_gap col1=[1(◎)] col2=[2(○),3(▲)]
            col3=[2(○),3(▲),4(△),5(★),6(☆)] → 7点
    """
    return [
        _make_eval(1, "◎", 85.0, odds=2.0, win_prob=0.20),
        _make_eval(2, "○", 75.0, odds=5.0, win_prob=0.12),  # g1=10
        _make_eval(3, "▲", 73.0, odds=7.0, win_prob=0.09),  # g2=2<3
        _make_eval(4, "△", 66.0, odds=10.0, win_prob=0.07),  # ▲-△=7≥5 → B
        _make_eval(5, "★", 62.0, odds=14.0, win_prob=0.05),
        _make_eval(6, "☆", 58.0, odds=20.0, win_prob=0.04),  # span=75-58=17
        _make_eval(7, "-",  46.0, odds=30.0, win_prob=0.03),
        _make_eval(8, "-",  44.0, odds=40.0, win_prob=0.02),
    ]


def _make_c_evs():
    """
    C型(団子・総流し)シナリオ:
      軸◎=85 ○=75 → g1=10≥8 (ゲート通過)
      ○▲△★☆ が密集: ○=75 ▲=74 △=73 ★=72 ☆=71
      span = comp(○)-comp(☆) = 75-71 = 4 < 5 → C 先行で発火
      期待: formation=danso_gap col1=[1(◎)] col2=[2,3,4,5,6] col3=[2,3,4,5,6] → 10点
    """
    return [
        _make_eval(1, "◎", 85.0, odds=2.0, win_prob=0.20),
        _make_eval(2, "○", 75.0, odds=6.0, win_prob=0.10),  # g1=10
        _make_eval(3, "▲", 74.0, odds=7.0, win_prob=0.09),
        _make_eval(4, "△", 73.0, odds=8.0, win_prob=0.08),
        _make_eval(5, "★", 72.0, odds=9.0, win_prob=0.07),
        _make_eval(6, "☆", 71.0, odds=10.0, win_prob=0.06),  # span=75-71=4<5
        _make_eval(7, "-",  60.0, odds=20.0, win_prob=0.04),  # 無印(判定外)
        _make_eval(8, "-",  55.0, odds=30.0, win_prob=0.03),
    ]


# ── テスト本体 ────────────────────────────────────────────────────────────

def test_pattern_a_maru_nuke():
    """A型(○抜け): col1=[軸] col2=[○] col3=[▲△★☆] → 4点

    composite設計: ◎=85 ○=75(g1=10) ▲=68(g2=7≥5) △=63 ★=59 ☆=55(span=20≥5)
    分類根拠: span≥5 で C 非該当, g2≥5 で A 発火。
    """
    evs = _make_a_evs()
    res = _run_columns(evs)
    assert res is not None, "A型が発火すべきだが None が返った"
    assert res["formation"] == "danso_gap", f"formation={res['formation']} (期待=danso_gap)"
    assert res["col1"] == [1], f"col1={res['col1']} (期待=[1]・軸◎単独)"
    # A型は col2 が ○ 1頭のみ
    assert set(res["col2"]) == {2}, f"col2={res['col2']} (期待={{2}}・○のみ)"
    assert set(res["col3"]) == {3, 4, 5, 6}, f"col3={res['col3']} (期待=▲△★☆)"

    # generate_danso_tickets で点数確認 (軸×○×{▲△★☆} = 4点)
    result = _run_danso(evs, field_count=len(evs))
    tickets = result["sanrenpuku"]
    n = _count_distinct_combos(tickets)
    assert n == 4, f"A型: 期待4点, 実際{n}点"
    print(f"  PASS: A型(○抜け) → danso_gap {n}点 [OK]")


def test_pattern_b_maru_san_kinko():
    """B型(○▲拮抗): col1=[軸] col2=[○▲] col3=[○▲△★☆] → 7点

    composite設計: ◎=85 ○=75(g1=10) ▲=73(g2=2<3) △=66(▲-△=7≥5) ★=62 ☆=58(span=17≥5)
    分類根拠: span≥5 で C 非該当, g2<3 かつ ▲-△≥5 で B 発火。
    """
    evs = _make_b_evs()
    res = _run_columns(evs)
    assert res is not None, "B型が発火すべきだが None が返った"
    assert res["formation"] == "danso_gap", f"formation={res['formation']} (期待=danso_gap)"
    assert res["col1"] == [1], f"col1={res['col1']} (期待=[1]・軸◎単独)"
    # B型は col2 が ○▲ 2頭
    assert set(res["col2"]) == {2, 3}, f"col2={res['col2']} (期待={{2,3}}・○▲)"
    assert set(res["col3"]) == {2, 3, 4, 5, 6}, f"col3={res['col3']} (期待=○▲△★☆)"

    # 軸×{○,▲}×{○▲△★☆} の distinct 3頭 = 7点
    result = _run_danso(evs, field_count=len(evs))
    n = _count_distinct_combos(result["sanrenpuku"])
    assert n == 7, f"B型: 期待7点, 実際{n}点"
    print(f"  PASS: B型(○▲拮抗) → danso_gap {n}点 [OK]")


def test_pattern_c_dango():
    """C型(団子・総流し): col2==col3=[○▲△★☆] → 10点

    composite設計: ◎=85 ○=75(g1=10) ▲=74 △=73 ★=72 ☆=71(span=4<5)
    分類根拠: span<5 → C先行で発火。
    """
    evs = _make_c_evs()
    res = _run_columns(evs)
    assert res is not None, "C型が発火すべきだが None が返った"
    assert res["formation"] == "danso_gap", f"formation={res['formation']} (期待=danso_gap)"
    assert res["col1"] == [1], f"col1={res['col1']} (期待=[1]・軸◎単独)"
    # C型は col2 == col3 で全印（総流し）
    assert set(res["col2"]) == {2, 3, 4, 5, 6}, f"col2={res['col2']} (期待=○▲△★☆)"
    assert set(res["col3"]) == {2, 3, 4, 5, 6}, f"col3={res['col3']} (期待=○▲△★☆)"
    assert set(res["col2"]) == set(res["col3"]), "C型は col2==col3 のはず"

    # 軸×C(5,2) = 10点
    result = _run_danso(evs, field_count=len(evs))
    n = _count_distinct_combos(result["sanrenpuku"])
    assert n == 10, f"C型: 期待10点, 実際{n}点"
    print(f"  PASS: C型(団子) → danso_gap {n}点 [OK]")


def test_skip_gate_not_met():
    """見送り(ゲート未達): g1 = comp(軸)-comp(○) < 8.0 → None

    composite設計: ◎=78 ○=75 → g1=3<8 → 全頭密集でゲート未達。
    """
    evs = [
        _make_eval(1, "◎", 78.0, odds=5.0),  # g1 = 78-75 = 3 < 8
        _make_eval(2, "○", 75.0, odds=6.0),
        _make_eval(3, "▲", 60.0, odds=7.0),  # 以降に断層があってもゲート未達で見送り
        _make_eval(4, "△", 59.0, odds=8.0),
        _make_eval(5, "★", 58.0, odds=9.0),
        _make_eval(6, "☆", 57.0, odds=10.0),
        _make_eval(7, "-",  56.0, odds=20.0),
        _make_eval(8, "-",  55.0, odds=30.0),
    ]
    res = _run_columns(evs)
    assert res is None, f"ゲート未達(g1<8)で見送りのはずだが {res} が発火"

    result = _run_danso(evs, field_count=len(evs))
    assert result["sanrenpuku"] == [], "見送りのはず"
    print("  PASS: 見送り(ゲート未達 g1<8) → None / sanrenpuku=[] [OK]")


def test_skip_valley():
    """見送り(谷間): g1≥8 だが g2 = comp(○)-comp(▲) が 3.0〜5.0 の谷間 → None

    composite設計: ◎=85 ○=75(g1=10) ▲=71(g2=4) △=64(▲-△=7) ★=60 ☆=56(span=19≥5)
    分類根拠: span≥5 で C 非該当 / g2=4 は A(≥5)にも B(<3)にも当たらず谷間 → 見送り。
    """
    evs = [
        _make_eval(1, "◎", 85.0, odds=2.0),
        _make_eval(2, "○", 75.0, odds=5.0),  # g1=10 (ゲート通過)
        _make_eval(3, "▲", 71.0, odds=8.0),  # g2=4 → 谷間(3〜5)
        _make_eval(4, "△", 64.0, odds=12.0),
        _make_eval(5, "★", 60.0, odds=15.0),
        _make_eval(6, "☆", 56.0, odds=20.0),  # span=75-56=19≥5 → C非該当
        _make_eval(7, "-",  46.0, odds=30.0),
        _make_eval(8, "-",  44.0, odds=40.0),
    ]
    res = _run_columns(evs)
    assert res is None, f"谷間(g2=4)で見送りのはずだが {res} が発火"

    result = _run_danso(evs, field_count=len(evs))
    assert result["sanrenpuku"] == [], "見送りのはず"
    print("  PASS: 見送り(谷間 g2=4) → None / sanrenpuku=[] [OK]")


def test_priority_c_first_over_a():
    """C先行: span<5 なら A 的な g2≥5 でも C が選ばれる（新仕様の判定順）。

    旧 test_priority_a_over_c は「A優先」だったが、新仕様は C先行のため意味が反転。
    本テストは「C先行」を実証する:
      ◎=85 ○=75(g1=10) ▲=68(g2=7≥5＝A条件は満たす) △=73 ★=72 ☆=71
      span = comp(○)-comp(末尾=☆) = 75-71 = 4 < 5 → C先行で発火（A は選ばれない）。
      ※ present は印の強さ順 [○▲△★☆] なので末尾は☆(=71)。
    検証: col2==col3（C型の総流し）であること。A型なら col2=[○] になるはず。
    """
    evs = [
        _make_eval(1, "◎", 85.0, odds=2.0, win_prob=0.20),
        _make_eval(2, "○", 75.0, odds=5.0, win_prob=0.12),  # g1=10
        _make_eval(3, "▲", 68.0, odds=8.0, win_prob=0.08),  # g2=7≥5 (A条件は成立)
        _make_eval(4, "△", 73.0, odds=12.0, win_prob=0.07),
        _make_eval(5, "★", 72.0, odds=15.0, win_prob=0.05),
        _make_eval(6, "☆", 71.0, odds=20.0, win_prob=0.04),  # span=75-71=4<5
        _make_eval(7, "-",  46.0, odds=30.0, win_prob=0.03),
        _make_eval(8, "-",  44.0, odds=40.0, win_prob=0.02),
    ]
    res = _run_columns(evs)
    assert res is not None, "C先行で発火すべきだが None が返った"
    assert res["formation"] == "danso_gap", f"formation={res['formation']}"
    # C先行 → col2==col3 の総流し（A型の col2=[○] ではない）
    assert set(res["col2"]) == {2, 3, 4, 5, 6}, (
        f"C先行のはず(col2=○▲△★☆), 実際 col2={res['col2']}"
    )
    assert set(res["col2"]) == set(res["col3"]), "C先行なら col2==col3 のはず"
    print(f"  PASS: C先行 (span<5) → C が A に優先 col2={res['col2']} [OK]")


def test_priority_a_when_span_wide():
    """A優先(span≥5): C は span≥5 で非該当のため、g2≥5 の A が選ばれる。

    test_priority_c_first_over_a と同じ g2=7 でも、span を広げる(末尾☆を下げる)と
    C が外れて A になることを実証（C先行ゲートは span のみで決まる）。
      ◎=85 ○=75(g1=10) ▲=68(g2=7≥5) △=63 ★=59 ☆=55(span=20≥5) → A発火。
    検証: col2=[○] 単独（A型）であること。
    """
    evs = _make_a_evs()  # span=20≥5 / g2=7≥5
    res = _run_columns(evs)
    assert res is not None
    assert res["formation"] == "danso_gap", f"formation={res['formation']}"
    # span≥5 で C 非該当 → A 発火 → col2 は ○ 単独
    assert set(res["col2"]) == {2}, f"A優先のはず(col2=○のみ), 実際 col2={res['col2']}"
    assert set(res["col3"]) == {3, 4, 5, 6}, f"col3={res['col3']} (期待=▲△★☆)"
    print(f"  PASS: A優先 (span≥5 で C非該当) col2={res['col2']} [OK]")


def test_honmei_tekipan_recognized():
    """本命が◉(tekipan)でも正しく軸として認識される

    A型 composite に ◎ を ◉ へ差し替え。軸=◉ で同様に発火する。
    """
    evs = [
        _make_eval(1, "◉", 85.0, odds=2.0, win_prob=0.20),  # ◉が軸(本命)
        _make_eval(2, "○", 75.0, odds=5.0, win_prob=0.12),  # g1=10
        _make_eval(3, "▲", 68.0, odds=8.0, win_prob=0.08),  # g2=7≥5 → A
        _make_eval(4, "△", 63.0, odds=12.0, win_prob=0.06),
        _make_eval(5, "★", 59.0, odds=15.0, win_prob=0.05),
        _make_eval(6, "☆", 55.0, odds=20.0, win_prob=0.04),  # span=20≥5
        _make_eval(7, "-",  46.0, odds=30.0, win_prob=0.03),
        _make_eval(8, "-",  44.0, odds=40.0, win_prob=0.02),
    ]
    res = _run_columns(evs)
    assert res is not None, "◉を軸とした場合も発火すべき"
    assert res["formation"] == "danso_gap", f"formation={res['formation']}"
    # col1 に馬番1(◉)が軸として入ること
    assert res["col1"] == [1], f"◉(馬番1)がcol1(軸)であるべき, col1={res['col1']}"
    print(f"  PASS: tekipan(◉)を軸認識 → col1={res['col1']} [OK]")


def test_scratched_excluded():
    """取消馬は除外される（取消後6頭未満 → 見送り）"""
    evs = [
        _make_eval(1, "◎", 85.0, odds=2.0, is_scratched=False),
        _make_eval(2, "○", 75.0, odds=5.0, is_scratched=True),   # 取消
        _make_eval(3, "▲", 68.0, odds=8.0, is_scratched=False),
        _make_eval(4, "△", 63.0, odds=12.0, is_scratched=False),
        _make_eval(5, "★", 59.0, odds=15.0, is_scratched=True),  # 取消
        _make_eval(6, "☆", 55.0, odds=20.0, is_scratched=False),
        # 有効馬4頭のみ（<6頭）→ 見送り
    ]
    res = _run_columns(evs)
    assert res is None, "6頭未満（取消後）は見送りのはず"
    print("  PASS: 取消馬除外 → 6頭未満で見送り [OK]")


def test_tokusen_odds_gate_below_threshold():
    """特選ゲート: effective_odds=9.6 < 15.0 → is_tokusen=False"""
    from config.settings import TOKUSEN_ODDS_THRESHOLD

    class _Ev:
        def __init__(self, odds):
            self.is_tokusen = False
            self._odds = odds

        @property
        def effective_odds(self):
            return self._odds

    candidate = _Ev(odds=9.6)
    eff_odds = candidate.effective_odds
    candidate.is_tokusen = (eff_odds is not None and eff_odds >= TOKUSEN_ODDS_THRESHOLD)
    assert candidate.is_tokusen is False
    print(f"  PASS: 特選ゲート 9.6倍 → is_tokusen=False [OK]")


def test_tokusen_odds_gate_above_threshold():
    """特選ゲート: effective_odds=15.0 >= 15.0 → is_tokusen=True"""
    from config.settings import TOKUSEN_ODDS_THRESHOLD

    class _Ev:
        def __init__(self, odds):
            self.is_tokusen = False
            self._odds = odds

        @property
        def effective_odds(self):
            return self._odds

    candidate = _Ev(odds=15.0)
    eff_odds = candidate.effective_odds
    candidate.is_tokusen = (eff_odds is not None and eff_odds >= TOKUSEN_ODDS_THRESHOLD)
    assert candidate.is_tokusen is True
    print(f"  PASS: 特選ゲート 15.0倍 → is_tokusen=True [OK]")


def test_tokusen_odds_gate_no_odds():
    """特選ゲート: effective_odds=None → is_tokusen=False"""
    class _Ev:
        def __init__(self):
            self.is_tokusen = True  # 初期True でFalseに落とすことを確認

        @property
        def effective_odds(self):
            return None

    candidate = _Ev()
    eff_odds = candidate.effective_odds
    if eff_odds is not None:
        from config.settings import TOKUSEN_ODDS_THRESHOLD
        candidate.is_tokusen = (eff_odds >= TOKUSEN_ODDS_THRESHOLD)
    else:
        candidate.is_tokusen = False
    assert candidate.is_tokusen is False
    print("  PASS: 特選ゲート odds=None → is_tokusen=False [OK]")


def test_stake_flat_per_point():
    """断層三連複: 各点のstakeが DANSO_STAKE_PER_POINT(100) 均等であること"""
    from config.settings import DANSO_STAKE_PER_POINT
    evs = _make_a_evs()
    result = _run_danso(evs, field_count=len(evs))
    tickets = result["sanrenpuku"]
    assert len(tickets) > 0, "チケット生成なし"
    # _finalize_mode_tickets でtorigami判定後 stake=0になる点を除く
    active_tickets = [t for t in tickets if t.get("stake", 0) > 0]
    for t in active_tickets:
        assert t["stake"] == DANSO_STAKE_PER_POINT, (
            f"stake={t['stake']} (期待={DANSO_STAKE_PER_POINT})"
        )
    print(f"  PASS: 全点 stake={DANSO_STAKE_PER_POINT}円均等 [OK]"
          f" (torigami除く {len(active_tickets)}点)")


def test_ticket_fields_present():
    """断層三連複: ticket dict の必須フィールドが存在すること"""
    required = {
        "type", "a", "b", "c", "combo",
        "mark_a", "mark_b", "mark_c",
        "odds", "odds_source", "prob", "ev",
        "appearance", "stake", "recovery", "signal",
    }
    evs = _make_a_evs()
    result = _run_danso(evs, field_count=len(evs))
    tickets = result["sanrenpuku"]
    assert len(tickets) > 0, "チケット生成なし"
    for t in tickets[:3]:
        missing = required - set(t.keys())
        assert not missing, f"フィールド欠落: {missing}"
    print("  PASS: ticket 必須フィールド全存在 [OK]")


# ── メイン実行 ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_pattern_a_maru_nuke,
        test_pattern_b_maru_san_kinko,
        test_pattern_c_dango,
        test_skip_gate_not_met,
        test_skip_valley,
        test_priority_c_first_over_a,
        test_priority_a_when_span_wide,
        test_honmei_tekipan_recognized,
        test_scratched_excluded,
        test_tokusen_odds_gate_below_threshold,
        test_tokusen_odds_gate_above_threshold,
        test_tokusen_odds_gate_no_odds,
        test_stake_flat_per_point,
        test_ticket_fields_present,
    ]
    passed = 0
    failed = 0
    for fn in tests:
        print(f"\n[RUN] {fn.__name__}")
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*50}")
    print(f"結果: {passed} PASS / {failed} FAIL / {len(tests)} total")
    if failed > 0:
        sys.exit(1)
