"""
カルテット・マルチソースオーケストレーター

5つのデータソースを統合してフォールバックチェーンを構築する:
  1. netkeiba        — 出馬表・過去走（メイン）
  2. JRA公式         — オッズ・馬体重・公式ID・結果・ラップタイム
  3. NAR公式         — オッズ・馬体重・結果・通過順
  4. 競馬ブック       — 調教データ・結果・過去走
  5. 楽天競馬        — NAR結果・通過順（最詳細）

フォールバック優先度:
  出馬表:   netkeiba → JRA/NAR公式 → ブック
  ID取得:   netkeiba → JRA公式直接変換 → 馬番突合
  オッズ:   JRA/NAR公式 → ブック → netkeiba
  馬体重:   JRA/NAR公式 → ブック → netkeiba
  結果取得: 公式 → netkeiba → ブック → 楽天競馬(NAR)
  ラップ:   JRA公式(ハロンタイム) → netkeiba
  通過順:   公式 → netkeiba → ブック → 楽天競馬(NAR)
  過去走:   netkeiba → ブック
  調教:     ブック (専有)
"""

import logging
from typing import Dict, List, Optional, Tuple

from src.models import Horse, RaceInfo

logger = logging.getLogger("keiba.multi_source")


class MultiSourceEnricher:
    """
    netkeiba で取得済みの Horse[] に対して、
    JRA/NAR公式 および 競馬ブック のデータを重ね合わせるエンリッチャー。

    使い方:
        enricher = MultiSourceEnricher(official_odds_scraper)
        enricher.enrich(race_id, race_info, horses)
    """

    def __init__(
        self,
        official_odds=None,        # OfficialOddsScraper instance
        keibabook_client=None,     # KeibabookClient instance (optional)
    ):
        self._official = official_odds
        self._kb_client = keibabook_client

    # ================================================================
    # メインエンリッチメント
    # ================================================================

    def enrich(
        self,
        race_id: str,
        race_info: RaceInfo,
        horses: List[Horse],
        *,
        fetch_odds: bool = True,
        fetch_weights: bool = True,
        fetch_ids: bool = True,
    ) -> Dict[str, int]:
        """
        各ソースからデータを取得して Horse[] に重ね合わせる。

        Returns:
            {"odds": N, "weights": N, "ids": N, "owners": N}  各項目の更新馬数
        """
        stats = {"odds": 0, "weights": 0, "ids": 0, "owners": 0}
        venue_code = race_id[4:6] if len(race_id) >= 6 else ""

        if not self._official:
            return stats

        # ── 1. オッズ (JRA/NAR公式優先) ──
        if fetch_odds:
            try:
                odds_data = self._official.get_tansho(race_id)
                for h in horses:
                    if h.horse_no in odds_data:
                        h.odds, h.popularity = odds_data[h.horse_no]
                        stats["odds"] += 1
            except Exception as e:
                logger.debug("公式オッズ取得失敗: %s", e)

        # ── 2. 馬体重 + 馬主 + 公式ID (JRA/NAR公式) ──
        if fetch_weights or fetch_ids:
            try:
                weight_data = self._official.get_weights(race_id)
                for h in horses:
                    info = weight_data.get(h.horse_no, {})
                    if not info:
                        continue

                    # 馬体重
                    if fetch_weights and info.get("weight"):
                        h.horse_weight = info["weight"]
                        h.weight_change = info.get("weight_change", 0)
                        stats["weights"] += 1

                    # 馬主
                    if info.get("owner"):
                        if not h.owner:
                            h.owner = info["owner"]
                            stats["owners"] += 1

                    # JRA 公式 ID（netkeiba IDと直接対応）
                    if fetch_ids:
                        stats["ids"] += self._apply_official_ids(h, info)

            except Exception as e:
                logger.debug("公式weight/ID取得失敗: %s", e)

        if any(v > 0 for v in stats.values()):
            logger.info(
                "マルチソース補完: %s — odds=%d, weight=%d, id=%d, owner=%d",
                race_id, stats["odds"], stats["weights"],
                stats["ids"], stats["owners"],
            )
        return stats

    # ================================================================
    # JRA 公式 ID → netkeiba ID 変換・適用
    # ================================================================

    @staticmethod
    def _apply_official_ids(horse: Horse, info: dict) -> int:
        """
        JRA公式ページから抽出した ID を Horse に適用する。

        JRA公式 → netkeiba の変換規則:
          - horse_id:   10桁数値 → そのまま netkeiba horse_id
          - jockey_id:  4桁数値 → 5桁ゼロパッド = netkeiba jockey_id
          - trainer_id: 4桁数値 → 5桁ゼロパッド = netkeiba trainer_id

        Returns: 1 if any ID was applied, 0 otherwise
        """
        applied = 0

        # 馬ID: JRA公式の10桁 = netkeibaの10桁 (完全一致)
        official_horse_id = info.get("horse_id", "")
        if official_horse_id and len(official_horse_id) == 10:
            if not horse.horse_id or horse.horse_id != official_horse_id:
                logger.debug(
                    "horse_id 補完: %s (%s → %s)",
                    horse.horse_name, horse.horse_id, official_horse_id,
                )
                horse.horse_id = official_horse_id
                applied = 1

        # 騎手ID: 5桁ゼロパッド = netkeiba jockey_id
        # 公式ソースは権威的なので、既存値があっても常に上書き（騎手変更対応）
        official_jockey_id = info.get("jockey_id", "")
        if official_jockey_id and len(official_jockey_id) == 5:
            if horse.jockey_id != official_jockey_id:
                if horse.jockey_id:
                    logger.info("騎手ID更新: %s (%s → %s)",
                                horse.horse_name, horse.jockey_id, official_jockey_id)
                horse.jockey_id = official_jockey_id
                applied = 1
        # 騎手名: 公式ソースの名前で上書き（代替騎手対応）
        official_jockey_name = info.get("jockey_name", "")
        if official_jockey_name and horse.jockey != official_jockey_name:
            if horse.jockey:
                logger.info("騎手名更新: %s (%s → %s)",
                            horse.horse_name, horse.jockey, official_jockey_name)
            horse.jockey = official_jockey_name

        # 調教師ID: 5桁ゼロパッド = netkeiba trainer_id
        # 公式ソースは権威的なので、既存値があっても常に上書き（転厩対応）
        official_trainer_id = info.get("trainer_id", "")
        if official_trainer_id and len(official_trainer_id) == 5:
            if horse.trainer_id != official_trainer_id:
                if horse.trainer_id:
                    logger.info("調教師ID更新: %s (%s → %s)",
                                horse.horse_name, horse.trainer_id, official_trainer_id)
                horse.trainer_id = official_trainer_id
                applied = 1
        # 調教師名: 公式ソースの名前で上書き（転厩対応）
        official_trainer_name = info.get("trainer_name", "")
        if official_trainer_name and horse.trainer != official_trainer_name:
            if horse.trainer:
                logger.info("調教師名更新: %s (%s → %s)",
                            horse.horse_name, horse.trainer, official_trainer_name)
            horse.trainer = official_trainer_name

        return applied

    # ================================================================
    # ID 相互変換ユーティリティ
    # ================================================================

    @staticmethod
    def jra_to_netkeiba_horse_id(jra_id: str) -> str:
        """JRA公式の馬ID (10桁) → netkeiba horse_id (同一)"""
        return jra_id

    @staticmethod
    def jra_to_netkeiba_jockey_id(jra_id: str) -> str:
        """JRA公式の騎手ID (4桁) → netkeiba jockey_id (5桁ゼロパッド)"""
        return jra_id.zfill(5)

    @staticmethod
    def jra_to_netkeiba_trainer_id(jra_id: str) -> str:
        """JRA公式の調教師ID (4桁) → netkeiba trainer_id (5桁ゼロパッド)"""
        return jra_id.zfill(5)


# ============================================================
# データソースメタ情報
# ============================================================

DATASOURCE_INFO = {
    "netkeiba": {
        "name": "ネット競馬",
        "url": "https://race.netkeiba.com",
        "capabilities": ["entry", "past_runs", "odds", "results"],
        "coverage": ["JRA", "NAR"],
    },
    "jra_official": {
        "name": "JRA公式",
        "url": "https://www.jra.go.jp",
        "capabilities": ["odds", "weights", "ids", "entry", "results", "lap_times", "corners"],
        "coverage": ["JRA"],
    },
    "nar_official": {
        "name": "NAR公式",
        "url": "https://www.keiba.go.jp",
        "capabilities": ["odds", "weights", "entry", "results", "corners"],
        "coverage": ["NAR"],
    },
    "keibabook": {
        "name": "競馬ブック",
        "url": "https://s.keibabook.co.jp",
        "capabilities": ["training", "entry", "results", "past_runs"],
        "coverage": ["JRA", "NAR"],
    },
    "rakuten_keiba": {
        "name": "楽天競馬",
        "url": "https://keiba.rakuten.co.jp",
        "capabilities": ["results", "corners"],
        "coverage": ["NAR"],
    },
}
