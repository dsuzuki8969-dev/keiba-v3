"""
データベース構築ツール
過去のレース結果から各種DBを自動構築
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from typing import List, Dict
from src.scraper.race_results import RaceResultScraper


def build_standard_time_db(race_results: List[Dict], output_path: str = "data/standard_times.csv"):
    """
    基準タイムDBを構築
    
    各コースの基準タイム（良馬場の平均タイム）を計算
    """
    print("\n📊 基準タイムDBを構築中...")
    
    records = []
    
    for result in race_results:
        # 1着馬のタイムを抽出
        first_place = next((h for h in result['horses'] if h['finish'] == 1), None)
        if not first_place or first_place['time'] == 0:
            continue
        
        # 良馬場のみ
        if result['track_condition'] != "良":
            continue
        
        records.append({
            'venue': result['venue'],
            'surface': result['surface'],
            'distance': result['distance'],
            'time': first_place['time'],
            'date': result['date'],
        })
    
    if not records:
        print("⚠️  データが不足しています")
        return
    
    df = pd.DataFrame(records)
    
    # コースごとに平均タイムを計算
    standard_times = df.groupby(['venue', 'surface', 'distance']).agg({
        'time': ['mean', 'std', 'count']
    }).reset_index()
    
    standard_times.columns = ['venue', 'surface', 'distance', 'standard_time', 'std_dev', 'sample_count']
    standard_times = standard_times[standard_times['sample_count'] >= 3]  # 最低3サンプル
    
    # 保存
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    standard_times.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"✅ 基準タイムDB作成完了: {len(standard_times)}コース")
    print(f"   保存先: {output_path}")


def build_jockey_stats_db(race_results: List[Dict], output_path: str = "data/jockey_stats.csv"):
    """
    騎手統計DBを構築
    
    各騎手の勝率・連対率・複勝率を計算
    """
    print("\n🏇 騎手統計DBを構築中...")
    
    # TODO: netkeibaから騎手名を取得する必要がある
    # 現在のrace_resultsには騎手情報が含まれていないため、
    # 別途スクレイピングが必要
    
    print("⚠️  騎手統計DBは別途実装が必要です（騎手名のスクレイピング）")
    print("   現在は基準タイムDBのみ構築されます")


def build_trainer_stats_db(race_results: List[Dict], output_path: str = "data/trainer_stats.csv"):
    """
    厩舎統計DBを構築
    
    各厩舎の勝率・連対率・複勝率を計算
    """
    print("\n🏠 厩舎統計DBを構築中...")
    
    # TODO: netkeibaから厩舎名を取得する必要がある
    # 現在のrace_resultsには厩舎情報が含まれていないため、
    # 別途スクレイピングが必要
    
    print("⚠️  厩舎統計DBは別途実装が必要です（厩舎名のスクレイピング）")
    print("   現在は基準タイムDBのみ構築されます")


def main():
    """メイン処理"""
    print("="*60)
    print("📦 競馬解析システム - データベース構築ツール")
    print("="*60)
    
    # サンプルレースIDリスト（2024年G1レース）
    sample_race_ids = [
        # 2024年有馬記念
        "202406050811",
        # 2024年ジャパンカップ
        "202406040811",
        # 2024年天皇賞（秋）
        "202405040811",
        # 2024年菊花賞
        "202405030811",
        # 2024年秋華賞
        "202405030710",
    ]
    
    print(f"\n🔍 {len(sample_race_ids)}件のレース結果を取得します...")
    print("   （サーバー負荷を考慮して1秒間隔で取得）")
    
    # レース結果を取得
    scraper = RaceResultScraper()
    race_results = scraper.scrape_multiple(sample_race_ids, delay=1.0)
    
    print(f"\n✅ {len(race_results)}件のレース結果を取得しました")
    
    if not race_results:
        print("\n⚠️  レース結果が取得できませんでした")
        print("   - インターネット接続を確認してください")
        print("   - netkeibaにアクセスできるか確認してください")
        return
    
    # 各DBを構築
    build_standard_time_db(race_results)
    build_jockey_stats_db(race_results)
    build_trainer_stats_db(race_results)
    
    print("\n" + "="*60)
    print("✅ データベース構築が完了しました")
    print("="*60)
    print("\n📝 次のステップ:")
    print("   1. data/standard_times.csv を確認")
    print("   2. python demo.py でシステムをテスト")
    print("   3. より多くのレースデータを追加して精度向上")
    print()


if __name__ == "__main__":
    main()
