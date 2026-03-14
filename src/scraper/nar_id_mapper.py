"""
NAR ID 統一マスタ構築スクリプト

NAR公式サイトのID（騎手30xxx系、調教師11xxx系の短い数字）と
netkeiba ID（騎手05xxx、調教師01xxx等の英数字）を名前マッチングで紐付け、
data/nar_id_map.json にマッピングを保存する。

使い方:
    python -m src.scraper.nar_id_mapper
    python src/scraper/nar_id_mapper.py
"""

import json
import os
import sys
from collections import defaultdict
from datetime import date
from typing import Dict, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import DATA_DIR, PERSONNEL_DB_PATH, PREDICTIONS_DIR
from src.log import get_logger
from src.scraper.personnel import _normalize_name

logger = get_logger(__name__)

# NAR競馬場コード一覧
NAR_VENUE_CODES: Set[str] = {
    "30", "35", "36", "42", "43", "44", "45",
    "46", "47", "48", "50", "51", "54", "55",
}

# 出力先パス
NAR_ID_MAP_PATH = os.path.join(DATA_DIR, "nar_id_map.json")


def _is_nar_id(id_str: str) -> bool:
    """
    NAR公式IDかどうかを判定する。

    NAR公式ID: 数値のみで4桁以下（"450", "1234" など）
    netkeiba ID: 5桁数値（"05203"）、a+4桁英数（"a025d"）、B+4桁（"B0063"）等
    """
    return id_str.isdigit() and len(id_str) <= 4


def _is_nar_race(race_id: str) -> bool:
    """race_idからNARレースかどうかを判定する"""
    if len(race_id) < 6:
        return False
    venue_code = race_id[4:6]
    return venue_code in NAR_VENUE_CODES


def _load_existing_map() -> dict:
    """既存のマッピングファイルを読み込む（差分更新用）"""
    if not os.path.exists(NAR_ID_MAP_PATH):
        return {"jockey": {}, "trainer": {}, "metadata": {}}
    try:
        with open(NAR_ID_MAP_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 必須キーの存在を保証
        for key in ("jockey", "trainer", "metadata"):
            if key not in data:
                data[key] = {}
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"既存マッピングファイルの読み込みに失敗: {e}")
        return {"jockey": {}, "trainer": {}, "metadata": {}}


def _load_personnel_db() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    personnel_db.json から netkeiba ID → 名前 のマッピングを取得する。

    Returns:
        (jockey_nk: {nk_id: name}, trainer_nk: {nk_id: name})
    """
    if not os.path.exists(PERSONNEL_DB_PATH):
        logger.error(f"personnel_db.json が見つかりません: {PERSONNEL_DB_PATH}")
        return {}, {}

    with open(PERSONNEL_DB_PATH, "r", encoding="utf-8") as f:
        db = json.load(f)

    jockey_nk: Dict[str, str] = {}
    for jid, info in db.get("jockeys", {}).items():
        name = info.get("jockey_name", "")
        if name:
            jockey_nk[jid] = name

    trainer_nk: Dict[str, str] = {}
    for tid, info in db.get("trainers", {}).items():
        name = info.get("trainer_name", "")
        if name:
            trainer_nk[tid] = name

    logger.info(f"personnel_db 読み込み完了: 騎手 {len(jockey_nk)}件, 調教師 {len(trainer_nk)}件")
    return jockey_nk, trainer_nk


def _build_name_to_nkid_map(
    nk_map: Dict[str, str],
) -> Tuple[Dict[str, str], Set[str]]:
    """
    正規化名前 → netkeiba ID の逆引きマップを構築する。
    同姓同名が存在する場合は衝突セットに記録してマッピングから除外する。

    Returns:
        (name_to_nkid: {正規化名前: nk_id}, collisions: {衝突した正規化名前})
    """
    # 正規化名前 → [nk_id, ...] を収集
    name_candidates: Dict[str, list] = defaultdict(list)
    for nk_id, name in nk_map.items():
        norm = _normalize_name(name)
        name_candidates[norm].append(nk_id)

    name_to_nkid: Dict[str, str] = {}
    collisions: Set[str] = set()

    for norm_name, nk_ids in name_candidates.items():
        if len(nk_ids) == 1:
            name_to_nkid[norm_name] = nk_ids[0]
        else:
            # 同姓同名衝突
            collisions.add(norm_name)

    return name_to_nkid, collisions


def _scan_predictions() -> Tuple[
    Dict[str, str], Dict[str, str], int
]:
    """
    data/predictions/ 配下の全 *_pred.json を走査し、
    NARレースからNAR ID → 名前ペアを収集する。

    Returns:
        (nar_jockeys: {nar_id: name}, nar_trainers: {nar_id: name}, file_count)
    """
    if not os.path.exists(PREDICTIONS_DIR):
        logger.error(f"predictions ディレクトリが見つかりません: {PREDICTIONS_DIR}")
        return {}, {}, 0

    # NAR ID → 名前（複数ファイルで出現する場合は最後の値で上書き）
    nar_jockeys: Dict[str, str] = {}
    nar_trainers: Dict[str, str] = {}
    file_count = 0
    error_count = 0

    pred_files = sorted(
        f for f in os.listdir(PREDICTIONS_DIR)
        if f.endswith("_pred.json")
    )

    for fname in pred_files:
        fpath = os.path.join(PREDICTIONS_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            file_count += 1
        except (json.JSONDecodeError, OSError) as e:
            error_count += 1
            logger.debug(f"JSONファイル読み込みスキップ: {fname} ({e})")
            continue

        for race in data.get("races", []):
            race_id = race.get("race_id", "")
            if not _is_nar_race(race_id):
                continue

            for horse in race.get("horses", []):
                # 騎手
                jid = horse.get("jockey_id", "")
                jname = horse.get("jockey", "")
                if jid and jname and _is_nar_id(jid):
                    nar_jockeys[jid] = jname

                # 調教師
                tid = horse.get("trainer_id", "")
                tname = horse.get("trainer", "")
                if tid and tname and _is_nar_id(tid):
                    nar_trainers[tid] = tname

    if error_count > 0:
        logger.warning(f"JSONファイル読み込みエラー: {error_count}件")
    logger.info(
        f"prediction走査完了: {file_count}ファイル, "
        f"NAR騎手ID {len(nar_jockeys)}件, NAR調教師ID {len(nar_trainers)}件"
    )
    return nar_jockeys, nar_trainers, file_count


def _match_nar_to_nk(
    nar_map: Dict[str, str],
    name_to_nkid: Dict[str, str],
    collisions: Set[str],
    role: str,
) -> Dict[str, dict]:
    """
    NAR ID → 名前 と 正規化名前 → netkeiba ID を突合してマッピングを構築する。

    Args:
        nar_map: {nar_id: 名前}
        name_to_nkid: {正規化名前: nk_id}
        collisions: 同姓同名で衝突した正規化名前のセット
        role: "騎手" or "調教師"（ログ用）

    Returns:
        {nar_id: {"nk_id": "...", "name": "..."}}
    """
    result: Dict[str, dict] = {}
    unmatched = 0
    collision_hits = 0

    for nar_id, name in nar_map.items():
        norm = _normalize_name(name)

        if norm in collisions:
            collision_hits += 1
            logger.warning(
                f"同姓同名衝突のためマッピングスキップ: {role} NAR_ID={nar_id}, "
                f"名前='{name}' (正規化='{norm}')"
            )
            continue

        nk_id = name_to_nkid.get(norm)
        if nk_id:
            result[nar_id] = {"nk_id": nk_id, "name": name}
        else:
            unmatched += 1
            logger.debug(f"{role}マッチング失敗: NAR_ID={nar_id}, 名前='{name}'")

    logger.info(
        f"{role}マッチング結果: 成功 {len(result)}件, "
        f"未マッチ {unmatched}件, 同姓同名衝突 {collision_hits}件"
    )
    return result


def build_nar_id_map() -> dict:
    """
    NAR ID → netkeiba ID のマッピングを構築して保存する。

    既存のマッピングがあれば読み込んで差分更新（追記）する。

    Returns:
        構築されたマッピング辞書
    """
    logger.info("=== NAR ID統一マスタ構築開始 ===")

    # 1. 既存マッピングを読み込み
    existing = _load_existing_map()
    logger.info(
        f"既存マッピング: 騎手 {len(existing['jockey'])}件, "
        f"調教師 {len(existing['trainer'])}件"
    )

    # 2. personnel_db.json から netkeiba ID → 名前を取得
    jockey_nk, trainer_nk = _load_personnel_db()
    if not jockey_nk and not trainer_nk:
        logger.error("personnel_db が空のため処理を中断します")
        return existing

    # 3. 正規化名前 → netkeiba ID の逆引きマップを構築
    j_name_to_nkid, j_collisions = _build_name_to_nkid_map(jockey_nk)
    t_name_to_nkid, t_collisions = _build_name_to_nkid_map(trainer_nk)

    if j_collisions:
        logger.warning(
            f"騎手の同姓同名衝突: {len(j_collisions)}件 "
            f"({', '.join(sorted(j_collisions)[:5])}...)"
        )
    if t_collisions:
        logger.warning(
            f"調教師の同姓同名衝突: {len(t_collisions)}件 "
            f"({', '.join(sorted(t_collisions)[:5])}...)"
        )

    # 4. prediction JSONからNAR IDを収集
    nar_jockeys, nar_trainers, file_count = _scan_predictions()

    # 5. 名前マッチングでNAR ID → netkeiba IDを構築
    new_j_map = _match_nar_to_nk(nar_jockeys, j_name_to_nkid, j_collisions, "騎手")
    new_t_map = _match_nar_to_nk(nar_trainers, t_name_to_nkid, t_collisions, "調教師")

    # 6. 既存マッピングに差分追記（新規のみ追加、既存は上書きしない）
    added_j = 0
    for nar_id, info in new_j_map.items():
        if nar_id not in existing["jockey"]:
            existing["jockey"][nar_id] = info
            added_j += 1

    added_t = 0
    for nar_id, info in new_t_map.items():
        if nar_id not in existing["trainer"]:
            existing["trainer"][nar_id] = info
            added_t += 1

    logger.info(f"新規追加: 騎手 {added_j}件, 調教師 {added_t}件")

    # 7. メタデータ更新
    existing["metadata"] = {
        "created": date.today().isoformat(),
        "prediction_files_scanned": file_count,
        "jockey_mappings": len(existing["jockey"]),
        "trainer_mappings": len(existing["trainer"]),
    }

    # 8. 保存
    os.makedirs(os.path.dirname(NAR_ID_MAP_PATH), exist_ok=True)
    with open(NAR_ID_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

    logger.info(
        f"保存完了: {NAR_ID_MAP_PATH} "
        f"(騎手 {len(existing['jockey'])}件, "
        f"調教師 {len(existing['trainer'])}件)"
    )
    logger.info("=== NAR ID統一マスタ構築完了 ===")
    return existing


def lookup_nk_id(
    nar_id: str, role: str = "jockey"
) -> Optional[str]:
    """
    NAR IDからnetkeiba IDを引く便利関数。

    Args:
        nar_id: NAR公式ID（"450" など）
        role: "jockey" or "trainer"

    Returns:
        netkeiba ID。マッピングが見つからない場合は None
    """
    if not os.path.exists(NAR_ID_MAP_PATH):
        return None
    try:
        with open(NAR_ID_MAP_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        entry = data.get(role, {}).get(nar_id)
        if entry:
            return entry.get("nk_id")
    except (json.JSONDecodeError, OSError):
        pass
    return None


if __name__ == "__main__":
    result = build_nar_id_map()
    print(
        f"\n完了: 騎手 {len(result['jockey'])}件, "
        f"調教師 {len(result['trainer'])}件 → {NAR_ID_MAP_PATH}"
    )
