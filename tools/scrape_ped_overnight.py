#!/usr/bin/env python3
"""
夜間血統スクレイピングスクリプト
================================
ML JSONファイルに登場する全馬のped HTMLを取得し、horse_sire_map.pkl を更新する。

使い方:
    python tools/scrape_ped_overnight.py [--test-bprefix] [--dry-run] [--limit N]

オプション:
    --test-bprefix   B-prefix馬を1頭だけ試してから本実行するか確認
    --dry-run        実際には取得せず、取得対象数だけ報告
    --limit N        N頭だけ処理して停止（デバッグ用）
    --no-rebuild     取得後にhorse_sire_map.pklを再構築しない

想定実行時間: ~21,000頭 × 1.5秒 ≒ 8.75時間
進捗ログ: 100頭ごとに速度・残り時間を表示
中断再開: キャッシュ済みはスキップするのでいつでも再開可能
"""

import sys
import os
import json
import time
import pickle
import logging
import argparse
import re
from typing import Dict, Tuple, List, Optional, Set

# プロジェクトルートをsys.pathに追加
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from src.scraper.netkeiba import NetkeibaClient, PedigreeParser, clean_horse_name

# ============================================================
# 設定
# ============================================================
ML_DATA_DIR   = os.path.join(ROOT, "data", "ml")
CACHE_DIR     = os.path.join(ROOT, "data", "cache")
MODEL_DIR     = os.path.join(ROOT, "data", "models")
SIRE_MAP_PATH = os.path.join(MODEL_DIR, "horse_sire_map.pkl")

REQUEST_INTERVAL = 1.8   # 秒（礼儀ある間隔、1.5より少し余裕を持たせる）
SAVE_INTERVAL    = 500   # 何頭ごとにpkl保存するか
LOG_INTERVAL     = 100   # 何頭ごとに進捗ログを出すか

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(ROOT, "tools", "scrape_ped.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ============================================================
# ユーティリティ
# ============================================================

def _cache_key(horse_id: str) -> str:
    """horse_idからキャッシュキーを計算（NetkeibaClient._cache_key と同じ計算）"""
    url = f"db.netkeiba.com/horse/ped/{horse_id}/"
    return url.replace("/", "_").replace("?", "_")[:200]


def _is_cached(horse_id: str, cache_dir: str = CACHE_DIR) -> bool:
    """このhorse_idのped HTMLがすでにキャッシュされているかチェック"""
    key = _cache_key(horse_id)
    base = os.path.join(cache_dir, key + ".html")
    return os.path.exists(base + ".lz4") or os.path.exists(base)


def collect_all_horse_ids() -> Set[str]:
    """全MLジェイソンファイルから horse_id を収集"""
    ids: Set[str] = set()
    if not os.path.isdir(ML_DATA_DIR):
        logger.warning("ML_DATA_DIRが見つかりません: %s", ML_DATA_DIR)
        return ids

    files = sorted(f for f in os.listdir(ML_DATA_DIR)
                   if f.endswith(".json") and not f.startswith("_"))
    logger.info("MLジェイソンファイル数: %d", len(files))

    for fname in files:
        try:
            with open(os.path.join(ML_DATA_DIR, fname), "r", encoding="utf-8") as f:
                data = json.load(f)
            for race in data.get("races", []):
                for horse in race.get("horses", []):
                    hid = horse.get("horse_id")
                    if hid:
                        ids.add(str(hid))
        except Exception as e:
            logger.debug("スキップ %s: %s", fname, e)

    logger.info("MLデータ内ユニーク馬数: %d", len(ids))
    return ids


def load_existing_sire_map() -> Dict[str, Tuple[str, str]]:
    """既存のhorse_sire_map.pklを読み込む"""
    if os.path.exists(SIRE_MAP_PATH):
        with open(SIRE_MAP_PATH, "rb") as f:
            d = pickle.load(f)
        logger.info("既存sire_map読み込み: %d頭", len(d))
        return d
    logger.info("既存sire_mapなし → 新規作成")
    return {}


def save_sire_map(sire_map: Dict[str, Tuple[str, str]]) -> None:
    """horse_sire_map.pkl を保存"""
    os.makedirs(MODEL_DIR, exist_ok=True)
    with open(SIRE_MAP_PATH, "wb") as f:
        pickle.dump(sire_map, f)
    logger.info("sire_map保存: %d頭 → %s", len(sire_map), SIRE_MAP_PATH)


def rebuild_sire_name_map(sire_map: Dict[str, Tuple[str, str]], cache_dir: str = CACHE_DIR) -> None:
    """sire_name_map.pkl を再構築（sire_id → 名前、horse_id → (sire_name, bms_name)）"""
    try:
        import lz4.frame as _lz4
        _HAS_LZ4 = True
    except ImportError:
        _HAS_LZ4 = False

    from bs4 import BeautifulSoup

    id_to_name: Dict[str, str] = {}
    horse_names: Dict[str, Tuple[str, str]] = {}

    logger.info("sire_name_map再構築中（キャッシュHTMLからパース）...")
    count = 0
    for horse_id, (sire_id, bms_id) in sire_map.items():
        key = _cache_key(horse_id)
        html_path = os.path.join(cache_dir, key + ".html")
        html = None
        if _HAS_LZ4 and os.path.exists(html_path + ".lz4"):
            try:
                with open(html_path + ".lz4", "rb") as f:
                    html = _lz4.decompress(f.read()).decode("utf-8")
            except Exception:
                pass
        if html is None and os.path.exists(html_path):
            try:
                with open(html_path, "r", encoding="utf-8") as f:
                    html = f.read()
            except Exception:
                pass
        if not html:
            continue

        soup = BeautifulSoup(html, "lxml")
        table = (soup.select_one("table.db_heredity")
                 or soup.select_one("table.pedigree_table")
                 or soup.select_one("div#db-main-column table")
                 or soup.select_one("table"))
        if not table:
            continue

        sire_name, bms_name = "", ""
        tds = table.select("td")

        # ヘルパー: tdからhorse IDと名前を抽出
        def _ext_link(td_elem):
            for a in td_elem.select("a[href*='/horse/']"):
                h = a.get("href", "")
                if any(x in h for x in ("/horse/ped/", "/horse/sire/", "/horse/mare/")):
                    continue
                lm = re.search(r"/horse/([0-9a-zA-Z]+)/?", h)
                if lm and a.get_text(strip=True):
                    return lm.group(1), clean_horse_name(a.get_text(strip=True))
            return "", ""

        # 父のrowspanを基準値として取得
        father_rowspan = int(tds[0].get("rowspan", "1")) if tds else 1

        # 父: 最初の[SIRE]リンクtd
        for td in tds:
            if td.select_one("a[href*='/horse/sire/']"):
                _, sname = _ext_link(td)
                if sname:
                    sire_name = sname
                    if sire_id:
                        id_to_name[sire_id] = sname
                    break

        # 母: 父と同じrowspanの[MARE]td → 次セル=母父(BMS)
        for i, td in enumerate(tds):
            rs = int(td.get("rowspan", "1"))
            if rs != father_rowspan:
                continue
            if not td.select_one("a[href*='/horse/mare/']"):
                continue
            if i + 1 < len(tds):
                _, bname = _ext_link(tds[i + 1])
                if bname:
                    bms_name = bname
                    if bms_id:
                        id_to_name[bms_id] = bname
            break

        if sire_name or bms_name:
            horse_names[horse_id] = (sire_name, bms_name)
        count += 1

    sire_name_map = {
        "id_to_name": id_to_name,
        "name_to_id": {v: k for k, v in id_to_name.items()},
        "horse_names": horse_names,
    }
    name_map_path = os.path.join(MODEL_DIR, "sire_name_map.pkl")
    with open(name_map_path, "wb") as f:
        pickle.dump(sire_name_map, f)
    logger.info("sire_name_map保存: sire_ids=%d, horse_names=%d → %s",
                len(id_to_name), len(horse_names), name_map_path)


# ============================================================
# メイン処理
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="血統ped HTMLを夜間バックグラウンドで取得")
    parser.add_argument("--test-bprefix", action="store_true",
                        help="B-prefix馬1頭テスト後に本実行の確認を求める")
    parser.add_argument("--dry-run", action="store_true",
                        help="取得対象を数えるだけ（実際には取得しない）")
    parser.add_argument("--limit", type=int, default=0,
                        help="最大N頭まで処理（0=無制限）")
    parser.add_argument("--no-rebuild", action="store_true",
                        help="終了後にsire_map再構築をスキップ")
    args = parser.parse_args()

    # ① 対象horse_id収集
    all_ids = collect_all_horse_ids()

    # ② キャッシュ済みを除外
    missing = [hid for hid in sorted(all_ids) if not _is_cached(hid)]
    normal_missing = [hid for hid in missing if not hid.startswith("B")]
    b_prefix_missing = [hid for hid in missing if hid.startswith("B")]

    logger.info("=" * 60)
    logger.info("スクレイピング対象サマリー")
    logger.info("  全馬数:           %d", len(all_ids))
    logger.info("  キャッシュ済み:    %d", len(all_ids) - len(missing))
    logger.info("  未取得（通常）:    %d", len(normal_missing))
    logger.info("  未取得（B-prefix）:%d", len(b_prefix_missing))
    logger.info("  想定所要時間:      %.1f時間", len(missing) * REQUEST_INTERVAL / 3600)
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("--dry-run モード: ここで終了")
        logger.info("B-prefixサンプル(先頭5件): %s", b_prefix_missing[:5])
        return

    if not missing:
        logger.info("全馬のped HTMLが取得済みです。")
        if not args.no_rebuild:
            sire_map = load_existing_sire_map()
            rebuild_sire_name_map(sire_map)
        return

    # ③ NetkeibaClientを初期化（ignore_ttl=Trueでキャッシュを常に優先）
    client = NetkeibaClient(cache_dir=CACHE_DIR, ignore_ttl=True,
                            request_interval=REQUEST_INTERVAL)
    ped_parser = PedigreeParser(client)
    sire_map = load_existing_sire_map()

    # ④ B-prefixテスト（--test-bprefix オプション時）
    if args.test_bprefix and b_prefix_missing:
        test_id = b_prefix_missing[0]
        logger.info("B-prefix テスト馬: %s", test_id)
        result = ped_parser.parse(test_id)
        sire_id, sire_name, _, _, bms_id, bms_name = result
        if sire_id:
            logger.info("  ✅ 取得成功: 父=%s(%s), 母父=%s(%s)",
                        sire_name, sire_id, bms_name, bms_id)
            sire_map[test_id] = (sire_id, bms_id)
        else:
            logger.info("  ❌ 取得失敗（B-prefix馬はdb.netkeiba.comに存在しない可能性あり）")
            logger.info("  B-prefix馬のスクレイピングをスキップします")
            b_prefix_missing = []  # B-prefixは全てスキップ

        ans = input("B-prefixテスト完了。本実行を続けますか？ [y/N]: ").strip().lower()
        if ans != "y":
            logger.info("中断しました")
            return
    elif b_prefix_missing:
        # --test-bprefix なしの場合は自動でB-prefix1頭テストを試みる
        test_id = b_prefix_missing[0]
        logger.info("B-prefix自動テスト: %s", test_id)
        result = ped_parser.parse(test_id)
        sire_id_test = result[0]
        if not sire_id_test:
            logger.info("  → B-prefix馬はdb.netkeiba.comに存在しないため、全B-prefix馬をスキップ")
            b_prefix_missing = []
        else:
            logger.info("  → B-prefix馬も取得可能（sire_id=%s）", sire_id_test)
            sire_map[test_id] = (result[0], result[4])

    # ⑤ 本実行: 通常馬 + B-prefix馬（スキップしなかった場合）
    # B-prefix以外を先に処理し、B-prefixは最後
    to_process = normal_missing + b_prefix_missing
    if args.limit > 0:
        to_process = to_process[:args.limit]
        logger.info("--limit %d: %d頭に絞って処理", args.limit, len(to_process))

    logger.info("処理開始: %d頭", len(to_process))
    logger.info("Ctrl+C で中断可能（次回は自動でレジューム）")

    fetched = 0
    skipped = 0
    errors  = 0
    start_time = time.time()

    try:
        for idx, horse_id in enumerate(to_process, 1):
            # キャッシュ済みなら再確認してスキップ
            if _is_cached(horse_id):
                skipped += 1
                # sire_mapにも追加（キャッシュはあるがmapにない場合）
                if horse_id not in sire_map:
                    result = ped_parser.parse(horse_id)
                    if result[0]:  # sire_id
                        sire_map[horse_id] = (result[0], result[4])
                continue

            # ped取得＆パース（client.get()が内部でキャッシュ保存）
            try:
                result = ped_parser.parse(horse_id)
                sire_id, sire_name, _, _, bms_id, _ = result
                if sire_id:
                    sire_map[horse_id] = (sire_id, bms_id)
                    fetched += 1
                else:
                    errors += 1
                    logger.debug("sire_id取得失敗: %s", horse_id)
            except Exception as e:
                errors += 1
                logger.warning("エラー %s: %s", horse_id, e)

            # 進捗ログ
            if idx % LOG_INTERVAL == 0:
                elapsed = time.time() - start_time
                rate = idx / elapsed if elapsed > 0 else 0
                remaining = (len(to_process) - idx) / rate if rate > 0 else 0
                logger.info(
                    "[%d/%d] 取得=%d スキップ=%d エラー=%d "
                    "速度=%.1f頭/分 残り=%.1f時間",
                    idx, len(to_process), fetched, skipped, errors,
                    rate * 60, remaining / 3600
                )

            # 定期保存
            if idx % SAVE_INTERVAL == 0:
                save_sire_map(sire_map)

    except KeyboardInterrupt:
        logger.info("\n中断されました（Ctrl+C）")

    # ⑥ 最終保存
    save_sire_map(sire_map)
    elapsed_total = time.time() - start_time
    logger.info("=" * 60)
    logger.info("完了サマリー")
    logger.info("  処理馬数:   %d", len(to_process))
    logger.info("  新規取得:   %d", fetched)
    logger.info("  スキップ:   %d（キャッシュ済み）", skipped)
    logger.info("  エラー:     %d", errors)
    logger.info("  所要時間:   %.1f分", elapsed_total / 60)
    logger.info("  sire_map:  %d頭", len(sire_map))
    logger.info("=" * 60)

    # ⑦ sire_name_mapも再構築
    if not args.no_rebuild:
        logger.info("sire_name_map再構築中...")
        rebuild_sire_name_map(sire_map)
        logger.info("sire_name_map再構築完了")

    logger.info("全処理完了。次のステップ: python tools/experiment_no_market.py を実行してください")


if __name__ == "__main__":
    main()
