"""
キャッシュ済みHTMLから各種DBを構築するスクリプト。
ネットアクセスなしで data/cache/ のファイルのみを使用。

対応DB:
  1. 血統DB (bloodline_db.json)  ← 馬戦績+血統キャッシュから父馬別成績を集計
  2. 調教師ベースラインDB (trainer_baseline_db.json) ← 将来Keibabookキャッシュが増えれば対応

使い方:
  python scripts/build_dbs_from_cache.py
  python scripts/build_dbs_from_cache.py --bloodline   # 血統DBのみ
"""

import sys
import os
import re
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    CACHE_DIR, BLOODLINE_DB_PATH, TRAINER_BASELINE_DB_PATH, DATA_DIR
)
from src.models import Horse


# ────────────────────────────────────────────────────
# 馬IDをキャッシュから列挙
# ────────────────────────────────────────────────────

def _list_cached_horse_ids(cache_dir: str) -> list:
    """
    data/cache/ から馬IDを収集。
    戦績ページ(result)があれば最優先、なければトップページを使用。
    .html と .html.lz4 の両方に対応。
    """
    result_ids = set()
    top_ids    = set()
    for fname in os.listdir(cache_dir):
        m = re.match(r"db\.netkeiba\.com_horse_result_(\d+)_\.html(\.lz4)?$", fname)
        if m:
            result_ids.add(m.group(1))
            continue
        m2 = re.match(r"db\.netkeiba\.com_horse_(\d+)_\.html(\.lz4)?$", fname)
        if m2:
            top_ids.add(m2.group(1))
    # result があれば result、なければ top で補完
    all_ids = list(result_ids | (top_ids - result_ids))
    return sorted(all_ids)


# ────────────────────────────────────────────────────
# 血統DB構築
# ────────────────────────────────────────────────────

def _resolve_cache_path(base_path: str) -> str:
    """キャッシュファイルパスを解決。.html.lz4 があればそちらを優先"""
    if os.path.exists(base_path):
        return base_path
    lz4_path = base_path + ".lz4"
    if os.path.exists(lz4_path):
        return lz4_path
    return base_path


def _parse_html_direct(html_path: str):
    """BeautifulSoupで直接HTMLを読む（キャッシュ期限を無視、lz4対応）"""
    from bs4 import BeautifulSoup
    resolved = _resolve_cache_path(html_path)
    try:
        if resolved.endswith(".lz4"):
            import lz4.frame
            with open(resolved, "rb") as f:
                data = lz4.frame.decompress(f.read())
            return BeautifulSoup(data.decode("utf-8"), "lxml")
        else:
            with open(resolved, "r", encoding="utf-8") as f:
                return BeautifulSoup(f.read(), "lxml")
    except Exception:
        return None


def _parse_pedigree_from_soup(soup) -> tuple:
    """血統ページSoupから (sire_id, sire_name, mgs_id, mgs_name) を取得"""
    sire_id, sire_name, mgs_id, mgs_name = "", "", "", ""
    table = (
        soup.select_one("table.db_heredity") or
        soup.select_one("table.pedigree_table") or
        soup.select_one("table")
    )
    if not table:
        return sire_id, sire_name, mgs_id, mgs_name

    dam_found = False
    tds = table.select("td")
    for i, td in enumerate(tds):
        sire_link = td.select_one("a[href*='/horse/sire/']")
        mare_link = td.select_one("a[href*='/horse/mare/']")
        main_links = [a for a in td.select("a[href*='/horse/']")
                      if "/horse/ped/" not in a.get("href", "")
                      and "/horse/sire/" not in a.get("href", "")
                      and "/horse/mare/" not in a.get("href", "")]
        if not main_links:
            continue
        a = main_links[0]
        m = re.search(r"/horse/([0-9a-zA-Z]+)/?", a.get("href", ""))
        if not m:
            continue
        lid, lname = m.group(1), a.get_text(strip=True)
        if not lname:
            continue
        if sire_link and not sire_id:
            sm = re.search(r"/horse/sire/([0-9a-zA-Z]+)", sire_link.get("href", ""))
            if sm and sm.group(1) == lid:
                sire_id, sire_name = lid, lname
        elif mare_link and not dam_found:
            mm = re.search(r"/horse/mare/([0-9a-zA-Z]+)", mare_link.get("href", ""))
            if mm and mm.group(1) == lid:
                dam_found = True
                if i + 1 < len(tds):
                    next_links = [na for na in tds[i + 1].select("a[href*='/horse/']")
                                  if "/horse/ped/" not in na.get("href", "")
                                  and "/horse/mare/" not in na.get("href", "")]
                    for na in next_links[:1]:
                        nm = re.search(r"/horse/([0-9a-zA-Z]+)/?", na.get("href", ""))
                        if nm:
                            mgs_id, mgs_name = nm.group(1), na.get_text(strip=True)

    # 父が取れない場合、1行目1列目から
    if not sire_id and tds:
        for a in tds[0].select("a[href*='/horse/']"):
            h = a.get("href", "")
            if "/horse/ped/" in h or "/horse/sire/" in h or "/horse/mare/" in h:
                continue
            fm = re.search(r"/horse/([0-9a-zA-Z]+)/?", h)
            if fm:
                sire_id, sire_name = fm.group(1), a.get_text(strip=True)
                break
    return sire_id, sire_name, mgs_id, mgs_name


def _parse_result_from_soup(soup) -> list:
    """戦績ページSoupからPastRunリストを生成"""
    from src.models import PastRun
    runs = []
    for row in soup.select("table.db_h_race_results tr")[1:]:
        try:
            cells = [td.get_text(strip=True) for td in row.select("td")]
            if len(cells) < 12:
                continue
            # 大まかなカラム: 0=日付 1=開催 2=天気 3=R 4=レース名 5=映像 6=頭数 7=枠 8=馬番
            # 9=オッズ 10=人気 11=着順 12=騎手 13=斤量 14=コース 15=馬場 16=馬場指数
            # 17=タイム 18=着差 19=ラスト3F ... (列数は可変)
            date_str = cells[0]  # YYYY/MM/DD
            finish_raw = cells[11] if len(cells) > 11 else ""
            try:
                finish_pos = int(finish_raw)
            except (ValueError, TypeError):
                finish_pos = 99

            # コース文字列から距離・芝ダ取得（例: "芝1600"）
            course_raw = cells[14] if len(cells) > 14 else ""
            m_dist = re.search(r"(\d{3,4})", course_raw)
            distance = int(m_dist.group(1)) if m_dist else 0
            surface = "芝" if "芝" in course_raw else ("ダ" if "ダ" in course_raw else "")

            if not date_str or distance == 0:
                continue

            # 馬場状態（カラム16: 良/稍/重/不）
            cond_raw = cells[16] if len(cells) > 16 else "良"
            cond_map = {"良": "良", "稍": "稍重", "重": "重", "不": "不良"}
            condition = cond_map.get(cond_raw, cond_raw) if cond_raw else "良"

            # 斤量
            wt_raw = cells[13] if len(cells) > 13 else ""
            try:
                weight_kg = float(wt_raw)
            except (ValueError, TypeError):
                weight_kg = 55.0

            run = PastRun(
                race_date=date_str.replace("/", "-"),
                venue=cells[1] if len(cells) > 1 else "",
                course_id="",
                distance=distance,
                surface=surface,
                condition=condition,
                class_name=cells[4] if len(cells) > 4 else "",
                grade="",
                field_count=int(cells[6]) if len(cells) > 6 and cells[6].isdigit() else 0,
                gate_no=int(cells[7]) if len(cells) > 7 and cells[7].isdigit() else 0,
                horse_no=int(cells[8]) if len(cells) > 8 and cells[8].isdigit() else 0,
                jockey=cells[12] if len(cells) > 12 else "",
                weight_kg=weight_kg,
                position_4c=0,
                finish_pos=finish_pos,
                finish_time_sec=0.0,
                last_3f_sec=None,
                margin_behind=0.0,
                margin_ahead=0.0,
            )
            runs.append(run)
        except Exception:
            continue
    return runs


def build_bloodline_from_cache(cache_dir: str, cache_path: str, verbose: bool = True):
    """
    キャッシュ済みHTMLを直接読み解析（CACHE_MAX_AGE_SEC を無視）して血統DBを構築。
    ネットアクセス一切なし。
    """
    from src.scraper.improvement_dbs import build_bloodline_db

    horse_ids = _list_cached_horse_ids(cache_dir)
    if verbose:
        print(f"  対象馬: {len(horse_ids)}頭（キャッシュより）")

    horses = []
    skipped = 0
    for i, horse_id in enumerate(horse_ids, 1):
        try:
            # 血統ページ直接読み込み
            ped_file = os.path.join(cache_dir, f"db.netkeiba.com_horse_ped_{horse_id}_.html")
            result_file = os.path.join(cache_dir, f"db.netkeiba.com_horse_result_{horse_id}_.html")

            sire_id, sire_name, mgs_id, mgs_name = "", "", "", ""
            ped_resolved = _resolve_cache_path(ped_file)
            if os.path.exists(ped_resolved):
                ped_soup = _parse_html_direct(ped_file)
                if ped_soup:
                    sire_id, sire_name, mgs_id, mgs_name = _parse_pedigree_from_soup(ped_soup)

            past_runs = []
            result_resolved = _resolve_cache_path(result_file)
            if os.path.exists(result_resolved):
                res_soup = _parse_html_direct(result_file)
                if res_soup:
                    past_runs = _parse_result_from_soup(res_soup)

            if not sire_id and not sire_name:
                skipped += 1
                continue

            h = Horse(
                horse_id=horse_id,
                horse_name=f"cache_{horse_id}",
                sex="不明", age=0,
                color="", trainer="", trainer_id="",
                owner="", breeder="",
                sire=sire_name, dam="",
                sire_id=sire_id,
                maternal_grandsire=mgs_name,
                maternal_grandsire_id=mgs_id,
                past_runs=past_runs,
            )
            horses.append(h)
            if verbose and i % 100 == 0:
                print(f"  [{i}/{len(horse_ids)}] 解析済み (血統取得: {len(horses)}, スキップ: {skipped})")
        except Exception as e:
            skipped += 1
            if verbose:
                print(f"  [WARN] {horse_id}: {e}")

    if verbose:
        print(f"  解析完了: {len(horses)}頭で血統データあり（スキップ: {skipped}）")

    if not horses:
        print("  [WARN] 有効な馬データなし。スキップ。")
        return {}

    db = build_bloodline_db(horses, netkeiba_client=None, cache_path=cache_path)
    n_sire = len(db.get("sire", {}))
    n_bms  = len(db.get("bms", {}))
    if verbose:
        print(f"  血統DB: 父馬 {n_sire}件 / 母父馬 {n_bms}件")

    # build_bloodline_db は netkeiba_client=None のとき自動保存しないので明示保存
    if cache_path and (n_sire > 0 or n_bms > 0):
        import json
        from src.scraper.improvement_dbs import _tuple_key_to_str
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        # タプルキーを文字列に変換して保存
        serializable = {}
        for role in ("sire", "bms"):
            serializable[role] = {}
            for bid, entry in db.get(role, {}).items():
                serializable[role][bid] = {
                    field: _tuple_key_to_str(data) if isinstance(data, dict) else data
                    for field, data in entry.items()
                }
        tmp = cache_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False)
        os.replace(tmp, cache_path)
        if verbose:
            print(f"  保存完了: {cache_path}")
    return db


# ────────────────────────────────────────────────────
# 調教師ベースラインDB構築（KBキャッシュ + Netkeiba出走表）
# ────────────────────────────────────────────────────

# KBの場コード(2桁) → Netkeibaの場コード(2桁) の逆引き
_KB_TO_NK_VENUE = {
    "04": "05",  # 東京
    "02": "06",  # 中山
    "01": "09",  # 阪神
    "03": "10",  # 小倉
}


def _kb_race_id_to_netkeiba(kb_id: str) -> str:
    """KBのrace_id → Netkeibaのrace_id に変換（4桁VVコードを逆変換）"""
    if len(kb_id) < 6:
        return kb_id
    kb_venue = kb_id[4:6]
    nk_venue = _KB_TO_NK_VENUE.get(kb_venue, kb_venue)
    return kb_id[:4] + nk_venue + kb_id[6:]


def _list_kb_race_ids(kb_cache_dir: str) -> list:
    """KBキャッシュから race_id 一覧を抽出"""
    ids = []
    for fname in os.listdir(kb_cache_dir):
        m = re.search(r"_(cyuou|chihou)_cyokyo_(\d{12})\.html$", fname)
        if m:
            ids.append((m.group(1), m.group(2), fname))
    return ids  # [(kind, kb_race_id, filename), ...]


def _parse_shutuba_trainer_map(html_path: str) -> dict:
    """
    Netkeiba出走表HTMLから {馬名: trainer_id} を返す。
    出走表HTMLのテーブル構成: 枠|馬番|馬名|性齢|斤量|騎手|調教師|...
    """
    from bs4 import BeautifulSoup
    result = {}
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "lxml")
        for a in soup.select("a[href*='/trainer/']"):
            trainer_id_m = re.search(r"/trainer/([0-9a-zA-Z]+)", a.get("href", ""))
            if not trainer_id_m:
                continue
            tid = trainer_id_m.group(1)
            # 馬名は同じ行のhorse linkから
            row = a.find_parent("tr")
            if not row:
                continue
            horse_a = row.select_one("a[href*='/horse/']")
            if horse_a:
                hname = horse_a.get_text(strip=True)
                if hname:
                    result[hname] = tid
    except Exception:
        pass
    return result


def _parse_kb_training_records(html_path: str) -> dict:
    """
    KBキャッシュHTMLから {馬名: [TrainingRecord]} を返す。
    既存の KeibabookTrainingScraper._parse_training_table を流用。
    """
    from bs4 import BeautifulSoup
    from src.scraper.keibabook_training import KeibabookTrainingScraper, KeibabookClient
    result = {}
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "lxml")
        # KeibabookClient のインスタンスは不要（_parse_training_table は self 以外不使用）
        dummy = object.__new__(KeibabookTrainingScraper)
        result = dummy._parse_training_table(soup)
    except Exception:
        pass
    return result


def build_trainer_baseline_from_cache(
    cache_dir: str,
    kb_cache_dir: str,
    save_path: str,
    verbose: bool = True,
) -> dict:
    """
    KBキャッシュ（調教HTML）とNetkeibaキャッシュ（出走表）を突き合わせ
    trainer_baseline_db を構築・保存する。
    """
    import json, statistics
    from collections import defaultdict
    from src.scraper.race_results import (
        load_trainer_baseline_db, merge_trainer_baseline, save_trainer_baseline_db
    )

    kb_races = _list_kb_race_ids(kb_cache_dir)
    if verbose:
        print(f"  KBキャッシュ: {len(kb_races)}レース分")

    # trainer_id × course → [3F time]
    by_trainer_course: dict = defaultdict(lambda: defaultdict(list))
    matched_races = 0

    for kind, kb_id, fname in kb_races:
        kb_path = os.path.join(kb_cache_dir, fname)
        nk_id   = _kb_race_id_to_netkeiba(kb_id)
        # 対応するNetkeiba出走表キャッシュを探す
        prefix = "race" if kind == "cyuou" else "nar"
        shutuba_fname = f"{prefix}.netkeiba.com_race_shutuba.html_race_id={nk_id}.html"
        shutuba_path  = os.path.join(cache_dir, shutuba_fname)
        if not os.path.exists(shutuba_path):
            # NARは同一IDなのでそのままも試す
            shutuba_path2 = os.path.join(cache_dir, f"nar.netkeiba.com_race_shutuba.html_race_id={kb_id}.html")
            if os.path.exists(shutuba_path2):
                shutuba_path = shutuba_path2
            else:
                continue

        trainer_map   = _parse_shutuba_trainer_map(shutuba_path)
        training_recs = _parse_kb_training_records(kb_path)

        if not trainer_map or not training_recs:
            continue

        matched_races += 1
        for horse_name, records in training_recs.items():
            tid = trainer_map.get(horse_name, "")
            if not tid:
                continue
            for rec in records:
                t3f = rec.splits.get(600) or rec.splits.get("3F") or rec.splits.get(600.0)
                if t3f is None:
                    # 200m単位で最短距離 = 200m のタイムから推算
                    t3f = rec.splits.get(200) or rec.splits.get(400)
                if t3f is not None and 10.0 <= float(t3f) <= 20.0:
                    course = rec.course or "不明"
                    by_trainer_course[tid][course].append(float(t3f))

    if verbose:
        print(f"  マッチしたレース: {matched_races}/{len(kb_races)}")

    # 既存DBを読んでマージ
    existing = load_trainer_baseline_db(save_path)
    new_data: dict = {}
    for tid, courses in by_trainer_course.items():
        new_data[tid] = {}
        for course, times in courses.items():
            if len(times) >= 1:
                std = statistics.stdev(times) if len(times) >= 2 else 0.5
                new_data[tid][course] = {
                    "mean_3f": statistics.mean(times),
                    "std_3f":  max(0.1, std),
                    "_n":      len(times),
                }

    merged = merge_trainer_baseline(new_data, existing)
    save_trainer_baseline_db(save_path, merged)

    n_new  = len(new_data)
    n_total = len(merged)
    if verbose:
        print(f"  調教師ベースライン: 新規 {n_new}件 追加 → 合計 {n_total}件")
    return merged


# ────────────────────────────────────────────────────
# main
# ────────────────────────────────────────────────────

def main(bloodline_only: bool = False):
    import time
    from config.settings import KEIBABOOK_CACHE_DIR
    t0 = time.time()

    print("=" * 55)
    print("  DBキャッシュ構築スクリプト")
    print("=" * 55)

    # ── 血統DB ───────────────────────────────────────────
    print("\n[1] 血統DB 構築 ...")
    db = build_bloodline_from_cache(CACHE_DIR, BLOODLINE_DB_PATH, verbose=True)
    n = len(db.get("sire", {}))
    if n:
        print(f"  → {BLOODLINE_DB_PATH}  ({n} 父馬)")
    else:
        print("  → データなし（キャッシュが不十分）")

    # ── 調教師ベースラインDB ──────────────────────────────
    if not bloodline_only and os.path.isdir(KEIBABOOK_CACHE_DIR):
        print("\n[2] 調教師ベースラインDB 構築 ...")
        build_trainer_baseline_from_cache(
            CACHE_DIR, KEIBABOOK_CACHE_DIR, TRAINER_BASELINE_DB_PATH, verbose=True
        )
    else:
        print("\n[2] Keibabookキャッシュなし → スキップ")

    print(f"\n完了 ({time.time()-t0:.1f}秒)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bloodline", action="store_true")
    args = ap.parse_args()
    main(bloodline_only=args.bloodline)
