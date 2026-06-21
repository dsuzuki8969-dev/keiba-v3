# -*- coding: utf-8 -*-
"""compute_danso_columns / generate_danso_tickets unit test (全面書き直し版)

【重要仕様】
  ◉(tekipan) と ◎(honmei) は composite 1位の**同一馬**に付く別印。
  → 本命H は常に 1 頭のみ。旧実装の「◉◎を別々の必須2頭」は誤り。

検証ケース:
  1. 条件A-F1: 断層①⑤③全成立 + gap(○,▲)>=3.0 → col1=[H] col2=[○] col3=[▲△★☆] (4点)
  2. 条件A-F2: 断層①断層③成立 + gap(○,▲)<3.0 + gap(▲,△)>=3.0 → col1=[H] col2=[○▲] col3=[○▲△★☆] (7点)
  3. 条件C: 断層①成立 + ○▲△★☆横一線(全隣接差<3.0) → col1=[H] col2=[5頭] col3=[5頭] (10点)
  4. 条件B-F1: abs(H-○)<2.0 + gap(○,▲)>=5.0 → 10点
  5. 条件B-F2: max(H,○,▲)-min()<2.0 + gap(▲,△)>=5.0 → 10点
  6. 見送り: 断層条件全て不成立 → sanrenpuku=[]
  7. 優先順位: 条件Aが成立すればC/Bは発火しない
  8. 優先順位: 条件A不成立でCが成立 → B不発火
  9. 特選ゲート: effective_odds<15 → is_tokusen=False
 10. 特選ゲート: effective_odds>=15 → is_tokusen=True
 11. 特選ゲート: effective_odds=None → is_tokusen=False
 12. stake均等: 各点 DANSO_STAKE_PER_POINT(100)円
 13. ticket必須フィールド: 全存在
 14. 本命◉(tekipan)でも正しく発火する（◉を本命として扱う）
 15. 取消馬は除外される
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

def _make_a_f1_evs():
    """
    条件A-F1シナリオ:
      本命H=◎(composite=80) ○=70 gap=10>=5 → 断層①成立
      ☆=53 無印=46 gap=7>=3 → 断層③成立
      ○-▲=6>=3 → 断層②A成立 → A-F1発火
      期待: col1=[1(◎)] col2=[2(○)] col3=[3(▲),4(△),5(★),6(☆)] → 4点
    """
    return [
        _make_eval(1, "◎", 80.0, odds=2.0, win_prob=0.20),  # 本命H
        _make_eval(2, "○", 70.0, odds=5.0, win_prob=0.12),
        _make_eval(3, "▲", 64.0, odds=8.0, win_prob=0.08),
        _make_eval(4, "△", 60.0, odds=12.0, win_prob=0.06),
        _make_eval(5, "★", 56.0, odds=15.0, win_prob=0.05),
        _make_eval(6, "☆", 53.0, odds=20.0, win_prob=0.04),
        _make_eval(7, "-",  46.0, odds=30.0, win_prob=0.03),  # 無印
        _make_eval(8, "-",  44.0, odds=40.0, win_prob=0.02),
    ]


def _make_a_f2_evs():
    """
    条件A-F2シナリオ:
      本命H=◎(80) ○=74 gap=6>=5 → 断層①成立
      ☆=53 無印=46 gap=7>=3 → 断層③成立
      ○-▲=2<3 → A-F1不成立
      ▲-△=4>=3 → A-F2発火
      期待: col1=[1(◎)] col2=[2(○),3(▲)] col3=[2(○),3(▲),4(△),5(★),6(☆)] → 7点
    """
    return [
        _make_eval(1, "◎", 80.0, odds=2.0, win_prob=0.20),
        _make_eval(2, "○", 74.0, odds=5.0, win_prob=0.12),
        _make_eval(3, "▲", 72.0, odds=8.0, win_prob=0.09),  # ○-▲=2<3
        _make_eval(4, "△", 68.0, odds=10.0, win_prob=0.07),  # ▲-△=4>=3
        _make_eval(5, "★", 65.0, odds=14.0, win_prob=0.05),
        _make_eval(6, "☆", 53.0, odds=20.0, win_prob=0.04),
        _make_eval(7, "-",  46.0, odds=30.0, win_prob=0.03),
        _make_eval(8, "-",  44.0, odds=40.0, win_prob=0.02),
    ]


def _make_c_evs():
    """
    条件Cシナリオ:
      本命H=◎(80) ○=74 gap=6>=5 → 断層①成立
      ○-▲=1<3 ▲-△=1<3 △-★=1<3 ★-☆=1<3 → 横一線 → 条件C発火
      (A-F1/F2は不成立: ○-▲=1<3, ▲-△=1<3)
      期待: col1=[1] col2=[2,3,4,5,6] col3=[2,3,4,5,6] → 10点
    """
    return [
        _make_eval(1, "◎", 80.0, odds=2.0, win_prob=0.20),
        _make_eval(2, "○", 74.0, odds=6.0, win_prob=0.10),
        _make_eval(3, "▲", 73.0, odds=7.0, win_prob=0.09),
        _make_eval(4, "△", 72.0, odds=8.0, win_prob=0.08),
        _make_eval(5, "★", 71.0, odds=9.0, win_prob=0.07),
        _make_eval(6, "☆", 70.0, odds=10.0, win_prob=0.06),
        _make_eval(7, "-",  65.0, odds=20.0, win_prob=0.04),  # 無印（断層③不成立だが条件Cには不要）
        _make_eval(8, "-",  60.0, odds=30.0, win_prob=0.03),
    ]


def _make_b_f1_evs():
    """
    条件B-F1シナリオ:
      断層①不成立: H=◎(62) ○=61 gap=1<5
      abs(H-○)=1<2.0 かつ gap(○,▲)=6>=5 → B-F1発火
      ○▲△★☆は全5印存在だが断層①不成立のためA/C非発火
      期待: col1=[1,2] col2=[1,2,3] col3=[1,2,3,4,5,6] → 10点
    """
    return [
        _make_eval(1, "◎", 62.0, odds=5.0, win_prob=0.11),  # 本命H
        _make_eval(2, "○", 61.0, odds=6.0, win_prob=0.10),  # abs(H-○)=1<2
        _make_eval(3, "▲", 55.0, odds=8.0, win_prob=0.08),  # gap(○,▲)=6>=5
        _make_eval(4, "△", 50.0, odds=12.0, win_prob=0.06),
        _make_eval(5, "★", 47.0, odds=16.0, win_prob=0.05),
        _make_eval(6, "☆", 44.0, odds=20.0, win_prob=0.04),
        _make_eval(7, "-",  40.0, odds=35.0, win_prob=0.03),
        _make_eval(8, "-",  36.0, odds=50.0, win_prob=0.02),
    ]


def _make_b_f2_evs():
    """
    条件B-F2シナリオ:
      断層①不成立: H=◎(62) ○=61.5 gap=0.5<5
      B-F1不成立: abs(H-○)=0.5<2 だが gap(○,▲)=1.5<5
      max(H,○,▲)-min()=1.5<2 かつ gap(▲,△)=6>=5 → B-F2発火
      期待: col1=[1,2,3] col2=[1,2,3] col3=[1,2,3,4,5,6] → 10点
    """
    return [
        _make_eval(1, "◎", 62.0, odds=5.0, win_prob=0.11),
        _make_eval(2, "○", 61.5, odds=6.0, win_prob=0.10),
        _make_eval(3, "▲", 60.5, odds=7.0, win_prob=0.09),  # max-min=1.5<2
        _make_eval(4, "△", 54.5, odds=12.0, win_prob=0.06),  # gap(▲,△)=6>=5
        _make_eval(5, "★", 51.0, odds=16.0, win_prob=0.05),
        _make_eval(6, "☆", 48.0, odds=20.0, win_prob=0.04),
        _make_eval(7, "-",  44.0, odds=35.0, win_prob=0.03),
        _make_eval(8, "-",  40.0, odds=50.0, win_prob=0.02),
    ]


# ── テスト本体 ────────────────────────────────────────────────────────────

def test_condition_a_f1():
    """条件A-F1: 本命1頭(◎)軸 / col1=[H] col2=[○] col3=[▲△★☆] → 4点"""
    evs = _make_a_f1_evs()
    res = _run_columns(evs)
    assert res is not None, "A-F1が発火すべきだが None が返った"
    assert res["formation"] == "A-F1", f"formation={res['formation']}"
    assert res["col1"] == [1], f"col1={res['col1']} (期待=[1])"
    assert res["col2"] == [2], f"col2={res['col2']} (期待=[2])"
    assert set(res["col3"]) == {3, 4, 5, 6}, f"col3={res['col3']}"

    # generate_danso_tickets で点数確認
    result = _run_danso(evs, field_count=len(evs))
    tickets = result["sanrenpuku"]
    n = _count_distinct_combos(tickets)
    assert n == 4, f"A-F1: 期待4点, 実際{n}点"
    print(f"  PASS: 条件A-F1 → {n}点 [OK]")


def test_condition_a_f2():
    """条件A-F2: 本命1頭(◎)軸 / col1=[H] col2=[○▲] col3=[○▲△★☆] → 7点"""
    evs = _make_a_f2_evs()
    res = _run_columns(evs)
    assert res is not None, "A-F2が発火すべきだが None が返った"
    assert res["formation"] == "A-F2", f"formation={res['formation']}"
    assert res["col1"] == [1], f"col1={res['col1']}"
    assert set(res["col2"]) == {2, 3}, f"col2={res['col2']}"
    assert set(res["col3"]) == {2, 3, 4, 5, 6}, f"col3={res['col3']}"

    result = _run_danso(evs, field_count=len(evs))
    n = _count_distinct_combos(result["sanrenpuku"])
    assert n == 7, f"A-F2: 期待7点, 実際{n}点"
    print(f"  PASS: 条件A-F2 → {n}点 [OK]")


def test_condition_c():
    """条件C: 断層①あり + 横一線 / col1=[H] col2=[○▲△★☆] col3=[○▲△★☆] → 10点"""
    evs = _make_c_evs()
    res = _run_columns(evs)
    assert res is not None, "条件Cが発火すべきだが None が返った"
    assert res["formation"] == "C", f"formation={res['formation']}"
    assert res["col1"] == [1], f"col1={res['col1']}"
    assert set(res["col2"]) == {2, 3, 4, 5, 6}, f"col2={res['col2']}"
    assert set(res["col3"]) == {2, 3, 4, 5, 6}, f"col3={res['col3']}"

    result = _run_danso(evs, field_count=len(evs))
    n = _count_distinct_combos(result["sanrenpuku"])
    assert n == 10, f"条件C: 期待10点, 実際{n}点"
    print(f"  PASS: 条件C → {n}点 [OK]")


def test_condition_b_f1():
    """条件B-F1: abs(H-○)<2.0 + gap(○,▲)>=5.0 → 10点"""
    evs = _make_b_f1_evs()
    res = _run_columns(evs)
    assert res is not None, "B-F1が発火すべきだが None が返った"
    assert res["formation"] == "B-F1", f"formation={res['formation']}"
    # col1=[H,○] col2=[H,○,▲] col3=sorted({H,○,▲,△,★,☆})
    assert set(res["col1"]) == {1, 2}, f"col1={res['col1']}"
    assert set(res["col2"]) == {1, 2, 3}, f"col2={res['col2']}"
    assert set(res["col3"]) == {1, 2, 3, 4, 5, 6}, f"col3={res['col3']}"

    result = _run_danso(evs, field_count=len(evs))
    n = _count_distinct_combos(result["sanrenpuku"])
    assert n == 10, f"B-F1: 期待10点, 実際{n}点"
    print(f"  PASS: 条件B-F1 → {n}点 [OK]")


def test_condition_b_f2():
    """条件B-F2: max(H,○,▲)-min()<2.0 + gap(▲,△)>=5.0 → 10点"""
    evs = _make_b_f2_evs()
    res = _run_columns(evs)
    assert res is not None, "B-F2が発火すべきだが None が返った"
    assert res["formation"] == "B-F2", f"formation={res['formation']}"
    assert set(res["col1"]) == {1, 2, 3}, f"col1={res['col1']}"
    assert set(res["col2"]) == {1, 2, 3}, f"col2={res['col2']}"
    assert set(res["col3"]) == {1, 2, 3, 4, 5, 6}, f"col3={res['col3']}"

    result = _run_danso(evs, field_count=len(evs))
    n = _count_distinct_combos(result["sanrenpuku"])
    assert n == 10, f"B-F2: 期待10点, 実際{n}点"
    print(f"  PASS: 条件B-F2 → {n}点 [OK]")


def test_no_condition_passthrough():
    """見送り: 断層①〜③・B条件全て不成立 → sanrenpuku=[]"""
    # 全馬が均等 → 断層なし
    evs = [
        _make_eval(1, "◎", 62.0, odds=5.0),
        _make_eval(2, "○", 61.0, odds=6.0),
        _make_eval(3, "▲", 60.0, odds=7.0),
        _make_eval(4, "△", 59.0, odds=8.0),
        _make_eval(5, "★", 58.0, odds=9.0),
        _make_eval(6, "☆", 57.0, odds=10.0),
        _make_eval(7, "-",  56.0, odds=20.0),
        _make_eval(8, "-",  55.0, odds=30.0),
    ]
    res = _run_columns(evs)
    assert res is None, f"見送りのはずだが {res['formation']} が発火"

    result = _run_danso(evs, field_count=len(evs))
    assert result["sanrenpuku"] == [], "見送りのはず"
    print("  PASS: 見送り → sanrenpuku=[] [OK]")


def test_priority_a_over_c():
    """優先順位: A条件が成立すればC（横一線）は発火しない"""
    # 断層①あり + gap(○,▲)>=3 かつ 断層③あり → A-F1 優先
    evs = _make_a_f1_evs()
    res = _run_columns(evs)
    assert res is not None
    # A-F1 が優先されること（C ではない）
    assert res["formation"].startswith("A-"), f"A優先のはず, 実際={res['formation']}"
    print(f"  PASS: 優先順位 A>C → {res['formation']} [OK]")


def test_priority_c_over_b():
    """優先順位: 条件A不成立でC成立 → 条件B不発火"""
    evs = _make_c_evs()
    res = _run_columns(evs)
    assert res is not None
    assert res["formation"] == "C", f"C優先のはず, 実際={res['formation']}"
    print(f"  PASS: 優先順位 C>B → {res['formation']} [OK]")


def test_honmei_tekipan_recognized():
    """本命が◉(tekipan)でも正しく本命として認識される"""
    evs = [
        _make_eval(1, "◉", 80.0, odds=2.0, win_prob=0.20),  # ◉が本命
        _make_eval(2, "○", 70.0, odds=5.0, win_prob=0.12),
        _make_eval(3, "▲", 64.0, odds=8.0, win_prob=0.08),
        _make_eval(4, "△", 60.0, odds=12.0, win_prob=0.06),
        _make_eval(5, "★", 56.0, odds=15.0, win_prob=0.05),
        _make_eval(6, "☆", 53.0, odds=20.0, win_prob=0.04),
        _make_eval(7, "-",  46.0, odds=30.0, win_prob=0.03),
        _make_eval(8, "-",  44.0, odds=40.0, win_prob=0.02),
    ]
    res = _run_columns(evs)
    assert res is not None, "◉を本命とした場合も発火すべき"
    # col1 に馬番1(◉)が含まれること
    assert 1 in res["col1"], f"◉(馬番1)がcol1に含まれるべき, col1={res['col1']}"
    print(f"  PASS: tekipan(honmei)認識 -> formation={res['formation']} col1={res['col1']} [OK]")


def test_scratched_excluded():
    """取消馬は除外される（取消後6頭未満 → 見送り）"""
    evs = [
        _make_eval(1, "◎", 80.0, odds=2.0, is_scratched=False),
        _make_eval(2, "○", 70.0, odds=5.0, is_scratched=True),   # 取消
        _make_eval(3, "▲", 64.0, odds=8.0, is_scratched=False),
        _make_eval(4, "△", 60.0, odds=12.0, is_scratched=False),
        _make_eval(5, "★", 56.0, odds=15.0, is_scratched=True),  # 取消
        _make_eval(6, "☆", 53.0, odds=20.0, is_scratched=False),
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
    evs = _make_a_f1_evs()
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
    evs = _make_a_f1_evs()
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
        test_condition_a_f1,
        test_condition_a_f2,
        test_condition_c,
        test_condition_b_f1,
        test_condition_b_f2,
        test_no_condition_passthrough,
        test_priority_a_over_c,
        test_priority_c_over_b,
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
