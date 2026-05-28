# -*- coding: utf-8 -*-
"""payout 形式統一モジュール (F-2 2026-05-25)

JRA dict 形式 / NAR list 形式の混在を解消し、全 payout を以下の統一形式に変換:

    {
        'tansho': [{'combo': '1', 'payout': 240, 'popularity': 1}, ...],
        'sanrenpuku': [{'combo': '1-2-3', 'payout': 1500, 'popularity': 5}],
        ...
    }

ticket_type は romaji (tansho, fukusho, umaren, umatan, wide, sanrenpuku, sanrentan) に正規化。
combo は文字列形式 ('1-2-3' for sanrenpuku, '1' for tansho)、payout は int (per 100 円)。

# 利用箇所
- scripts/verify_all_tickets.py
- scripts/analyze_r1_ticket_roi.py
- (将来) 他の集計スクリプト
"""
from typing import Any, Dict, List

# 日本語キー / romaji キーを romaji に統一
TICKET_TYPE_KEY_MAP: Dict[str, str] = {
    "単勝": "tansho", "tansho": "tansho",
    "複勝": "fukusho", "fukusho": "fukusho",
    "馬連": "umaren", "umaren": "umaren",
    "馬単": "umatan", "umatan": "umatan",
    "ワイド": "wide", "wide": "wide",
    "三連複": "sanrenpuku", "3連複": "sanrenpuku", "sanrenpuku": "sanrenpuku",
    "三連単": "sanrentan", "3連単": "sanrentan", "sanrentan": "sanrentan",
}

# 順序重要券種 (combo マッチング時に sorted せず完全一致)
ORDERED_TICKET_TYPES = frozenset(["sanrentan", "umatan"])


def normalize_payouts(raw_payouts: Any) -> Dict[str, List[Dict[str, Any]]]:
    """JRA dict / NAR list 混在の payouts を統一 list 形式に正規化

    Args:
        raw_payouts: scraper や DB 由来の生 payouts (dict or なんでも)
    Returns:
        {ticket_type_romaji: [{'combo': str, 'payout': int, 'popularity': int}, ...]}
    """
    if not isinstance(raw_payouts, dict):
        return {}

    result: Dict[str, List[Dict[str, Any]]] = {}
    for key, val in raw_payouts.items():
        normalized_key = TICKET_TYPE_KEY_MAP.get(key)
        if not normalized_key:
            continue

        if isinstance(val, list):
            entries = [item for item in val if isinstance(item, dict)]
        elif isinstance(val, dict):
            entries = [val]
        else:
            continue

        result.setdefault(normalized_key, []).extend(entries)

    return result


def combo_match(combo_a: Any, combo_b: Any, ticket_type: str) -> bool:
    """ticket combo と payout combo の一致判定

    Args:
        combo_a: ticket 側 (list of int/str)
        combo_b: payout 側 (str '1-2-3' or '210' or list)
        ticket_type: romaji ticket type (sanrentan/umatan は順序保持)

    Notes (2026-05-30 修正):
        results.json は scraper バージョンで以下 2 形式が混在する:
          - 区切り入り: "6-12" (2024-12 等で確認)
          - 区切り無し連結: "48" / "210" / "911" (2024-09 / 2025 全月で確認)
        前者は split("-") で正しく分解されるが、後者は 1 要素になり
        馬連/ワイド/三連系で永久不一致になる。フォールバックで救済する。
    """
    if not combo_a or not combo_b:
        return False

    if isinstance(combo_a, list):
        ca = [str(x) for x in combo_a]
    else:
        ca = str(combo_a).replace("=", "-").split("-")

    if isinstance(combo_b, str):
        cb = combo_b.replace("=", "-").replace("→", "-").replace(" ", "").split("-")
    else:
        cb = [str(x) for x in (combo_b or [])]

    if not ca or not cb:
        return False

    if ticket_type in ORDERED_TICKET_TYPES:
        if ca == cb:
            return True
    else:
        if sorted(ca) == sorted(cb):
            return True

    # フォールバック: payout 側が区切り無し連結 (例: "48" / "210") の場合
    # ticket 側 ca を数値順で連結して再判定
    if isinstance(combo_b, str) and len(cb) == 1 and cb[0].isdigit():
        try:
            ca_ints = [int(x) for x in ca]
            if ticket_type in ORDERED_TICKET_TYPES:
                joined_ca = "".join(str(x) for x in ca_ints)
            else:
                joined_ca = "".join(str(x) for x in sorted(ca_ints))
            return joined_ca == cb[0]
        except (ValueError, TypeError):
            pass

    return False


def get_payout_for_combo(
    normalized_payouts: Dict[str, List[Dict[str, Any]]],
    ticket_type: str,
    combo: Any,
) -> int:
    """指定 combo の payout を取得 (combo マッチング込み)

    Args:
        normalized_payouts: normalize_payouts() の出力
        ticket_type: romaji ticket type
        combo: ticket 側 combo (list or str)
    Returns:
        per-100yen payout (int)。マッチなしは 0。
    """
    entries = normalized_payouts.get(ticket_type, [])
    for entry in entries:
        if combo_match(combo, entry.get("combo", ""), ticket_type):
            return int(entry.get("payout", 0) or 0)
    return 0


def get_first_payout(
    normalized_payouts: Dict[str, List[Dict[str, Any]]],
    ticket_type: str,
) -> int:
    """券種の最初の payout を取得 (combo 確認なし・同着除外)

    verify_all_tickets 互換用 (top3 一致時に三連複払戻をそのまま採用するケース)。
    """
    entries = normalized_payouts.get(ticket_type, [])
    if not entries:
        return 0
    return int(entries[0].get("payout", 0) or 0)
