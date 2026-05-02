"""過去 pred.json 全件の tickets / tickets_by_mode を M' 戦略で一括書き換えるスクリプト。

M' 戦略仕様 (マスター承認済み 2026-05-03):
    SS → E  (◉◎ / ○〇 / ▲△★☆  4点)
    S  → C  (◉◎ / ○〇▲ / ○〇▲△★☆  7点)
    A  → C  (同上)
    B  → D  (◉◎○〇 / ◉◎○〇▲ / ○〇▲△★☆  10点)
    C  → D  (同上)
    D  → D  (同上)
    E  → skip

- 三連複 (unordered) のみ。単勝・馬単は廃止
- 1点100円固定
- 取消馬 (is_scratched / is_tokusen_kiken) は除外
- scripts/backtest_5patterns.py の印グループ定数・build_tickets ロジックを完全流用
- 対象: data/predictions/*_pred.json (_prev / _backup / .bak を除外)
- バックアップ: 処理前に {date}_pred_prev.json を作成 (既存スキップ)
"""
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

# ─────────────────────────────────────────────
# パス設定
# ─────────────────────────────────────────────
PRED_DIR = Path("data/predictions")

# ─────────────────────────────────────────────
# 印グループ定数 (scripts/backtest_5patterns.py から完全コピー)
# ─────────────────────────────────────────────
HONMEI_MARKS         = {"◉", "◎"}                         # 1着候補 (パターン C, E)
HONMEI_TAIKOU_MARKS  = {"◉", "◎", "○", "〇"}             # 1着候補 (パターン D)  〇と○は別字
C_2ND                = {"○", "〇", "▲"}                   # C パターン 2着
D_2ND                = {"◉", "◎", "○", "〇", "▲"}        # D パターン 2着
ABC_3RD              = {"○", "〇", "▲", "△", "★", "☆"}  # C/D パターン 3着
E_2ND                = {"○", "〇"}                         # E パターン 2着
E_3RD                = {"▲", "△", "★", "☆"}              # E パターン 3着

# ─────────────────────────────────────────────
# M' 戦略マッピング
# ─────────────────────────────────────────────
M_PRIME_STRATEGY: dict[str, str | None] = {
    "SS": "E",
    "S":  "C",
    "A":  "C",
    "B":  "D",
    "C":  "D",
    "D":  "D",
    "E":  None,   # skip
}

M_PRIME_PATTERN_MARKS: dict[str, tuple[set, set, set]] = {
    "E": (HONMEI_MARKS,        E_2ND, E_3RD),
    "C": (HONMEI_MARKS,        C_2ND, ABC_3RD),
    "D": (HONMEI_TAIKOU_MARKS, D_2ND, ABC_3RD),
}

# 印優先度 (上位ほど小さい値)
MARK_PRIORITY: dict[str, int] = {
    "◉": 0, "◎": 1, "○": 2, "〇": 2,
    "▲": 3, "△": 4, "★": 5, "☆": 6,
}


# ─────────────────────────────────────────────
# scripts/backtest_5patterns.py から流用した関数群
# ─────────────────────────────────────────────

def _filter_active(horses: list) -> list:
    """取消馬 (is_tokusen_kiken / is_scratched) を除外して有効馬リストを返す。"""
    return [h for h in horses
            if not h.get("is_tokusen_kiken") and not h.get("is_scratched")]


def _horse_mark(h: dict) -> str:
    """馬の印文字列を返す (None/未設定は空文字)。"""
    return (h.get("mark") or "").strip()


def _horses_by_marks(horses: list, marks: set) -> list[dict]:
    """指定印に一致する馬リストを返す (重複除去・印優先度昇順 + composite 降順)。"""
    cands = [h for h in horses if _horse_mark(h) in marks]
    cands.sort(key=lambda h: (MARK_PRIORITY.get(_horse_mark(h), 99), -(h.get("composite") or 0)))
    seen: set = set()
    out: list = []
    for h in cands:
        no = h.get("horse_no")
        if no and no not in seen:
            seen.add(no)
            out.append(h)
    return out


def build_tickets(
    horses: list,
    m1: set,
    m2: set,
    m3: set,
) -> list[tuple[int, int, int]]:
    """三連複フォーメーション買い目を生成する (unordered set / 馬番昇順 tuple を返す)。

    scripts/backtest_5patterns.py と完全同一ロジック。
    """
    h1 = _horses_by_marks(horses, m1)
    h2 = _horses_by_marks(horses, m2)
    h3 = _horses_by_marks(horses, m3)
    seen: set = set()
    tickets: list = []
    for ha in h1:
        a_no = ha.get("horse_no")
        for hb in h2:
            b_no = hb.get("horse_no")
            if b_no == a_no:
                continue
            for hc in h3:
                c_no = hc.get("horse_no")
                if c_no == a_no or c_no == b_no:
                    continue
                key = tuple(sorted([a_no, b_no, c_no]))
                if key in seen:
                    continue
                seen.add(key)
                tickets.append(key)
    return tickets


# ─────────────────────────────────────────────
# M' 1レース処理
# ─────────────────────────────────────────────

def regenerate_for_race(race: dict) -> tuple[list, dict]:
    """レース1件の tickets と tickets_by_mode._meta を M' 戦略で再生成する。

    Parameters
    ----------
    race : dict
        pred.json の races リスト内の 1 レース dict

    Returns
    -------
    tickets : list
        [{"type": "三連複", "combo": [a,b,c], "pattern": "M'-X",
          "stake": 100, "horse_marks": [mark_a, mark_b, mark_c]}, ...]
    meta : dict
        tickets_by_mode._meta 互換 dict
    """
    confidence: str = (race.get("overall_confidence") or "").strip()
    sub_pattern: str | None = M_PRIME_STRATEGY.get(confidence)

    # E 自信度 (skip)
    if sub_pattern is None:
        meta: dict = {
            "skipped":             True,
            "skip_reason":         f"confidence {confidence}: skip",
            "confidence":          confidence,
            "sub_pattern":         "",
            "ticket_count":        0,
            "stake_total":         0,
            "format":              "M': 自信度別 三連複 (SS=E/S=C/A=C/B/C/D=D/E=skip)",
            "tansho_count":        0,
            "sanrenpuku_count":    0,
            "formation_sanrentan": {"rank1": [], "rank2": [], "rank3": []},
        }
        return [], meta

    # 有効馬リスト取得
    all_horses: list = race.get("horses", [])
    active_horses: list = _filter_active(all_horses)

    # 印セット取得
    col1_marks, col2_marks, col3_marks = M_PRIME_PATTERN_MARKS[sub_pattern]

    # 三連複買い目生成
    combos: list[tuple[int, int, int]] = build_tickets(active_horses, col1_marks, col2_marks, col3_marks)

    # 馬番 → 印 マッピング
    mark_map: dict[int, str] = {}
    for h in active_horses:
        no = h.get("horse_no")
        if no:
            mark_map[int(no)] = _horse_mark(h)

    # ticket dict リスト組み立て
    tickets: list = []
    for combo in combos:
        horse_marks = [
            mark_map.get(combo[0], ""),
            mark_map.get(combo[1], ""),
            mark_map.get(combo[2], ""),
        ]
        tickets.append({
            "type":        "三連複",
            "combo":       list(combo),
            "pattern":     f"M'-{sub_pattern}",
            "stake":       100,
            "horse_marks": horse_marks,
        })

    ticket_count = len(tickets)
    meta = {
        "skipped":             False,
        "skip_reason":         "",
        "confidence":          confidence,
        "sub_pattern":         sub_pattern,
        "ticket_count":        ticket_count,
        "stake_total":         ticket_count * 100,
        "format":              "M': 自信度別 三連複 (SS=E/S=C/A=C/B/C/D=D/E=skip)",
        "tansho_count":        0,
        "sanrenpuku_count":    ticket_count,
        "formation_sanrentan": {"rank1": [], "rank2": [], "rank3": []},
    }
    return tickets, meta


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main() -> None:
    started = time.time()

    # pred.json ファイル一覧取得 (_prev / _backup / .bak を除外)
    all_files = sorted(PRED_DIR.glob("*_pred.json"))
    pred_files = [
        f for f in all_files
        if "_prev" not in f.name
        and "_backup" not in f.name
        and ".bak" not in f.name
    ]

    n_files = len(pred_files)
    n_backup_created = 0
    n_backup_skipped = 0
    n_races_total    = 0
    n_races_sanrenpuku = 0
    n_races_skip     = 0

    print(f"対象 pred.json: {n_files} 件")

    for fi, fp in enumerate(pred_files):
        # ── 日付文字列取得 ──
        date_str = fp.stem.split("_")[0]   # e.g. "20240101"

        # ── バックアップ作成 (_prev.json が既に存在する場合はスキップ) ──
        prev_path = fp.parent / f"{date_str}_pred_prev.json"
        if not prev_path.exists():
            shutil.copy2(fp, prev_path)
            n_backup_created += 1
        else:
            n_backup_skipped += 1

        # ── pred.json 読み込み ──
        try:
            pred = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  [WARN] {fp.name} の読み込みに失敗しました: {exc}")
            continue

        races: list = pred.get("races", [])

        # ── 各レースを M' で書き換え ──
        for race in races:
            n_races_total += 1
            tickets, meta = regenerate_for_race(race)

            race["tickets"] = tickets
            race["tickets_by_mode"] = {
                "fixed":    tickets,
                "accuracy": [],
                "balanced": [],
                "recovery": [],
                "_meta":    meta,
            }

            if meta.get("skipped"):
                n_races_skip += 1
            else:
                n_races_sanrenpuku += len(tickets)

        # ── 書き戻し ──
        fp.write_text(json.dumps(pred, ensure_ascii=False, indent=2), encoding="utf-8")

        # ── 進捗表示 (100件ごと) ──
        if (fi + 1) % 100 == 0 or (fi + 1) == n_files:
            elapsed = time.time() - started
            print(f"  {fi + 1}/{n_files} ({date_str}) elapsed={elapsed:.1f}s", flush=True)

    # ── 完了レポート ──
    elapsed_total = time.time() - started
    print()
    print("=" * 60)
    print("M' 戦略 tickets 再生成 完了レポート")
    print("=" * 60)
    print(f"集計対象 pred 日数         : {n_files}")
    print(f"バックアップ作成           : {n_backup_created} 件")
    print(f"バックアップ スキップ (既存): {n_backup_skipped} 件")
    print(f"書き換え対象 race          : {n_races_total}")
    print(f"  三連複生成 (点数合計)    : {n_races_sanrenpuku}")
    print(f"  skip (E 自信度)          : {n_races_skip}")
    print(f"Total elapsed              : {elapsed_total:.1f}s")


if __name__ == "__main__":
    main()
