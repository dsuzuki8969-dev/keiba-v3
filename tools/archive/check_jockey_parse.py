"""
騎手スクレイパーのパース動作確認
"""
import sys, io
sys.path.insert(0, '.')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from src.scraper.netkeiba import NetkeibaClient
from src.scraper.personnel import JockeyScraper

client = NetkeibaClient()

# 川田のIDでテスト
jid = "01143"
scraper = JockeyScraper(client)

# 直接fetchしてデータを確認
print(f"=== 川田(jid={jid})の取得テスト ===")
stats = scraper.fetch(jid, "川田")
print(f"upper_long_dev: {stats.upper_long_dev:.1f}")
print(f"lower_long_dev: {stats.lower_long_dev:.1f}")
print(f"upper_short_dev: {stats.upper_short_dev:.1f}")
print(f"lower_short_dev: {stats.lower_short_dev:.1f}")

# _fetch_period_statsの戻り値を確認
print("\n=== 長期成績(12ヶ月)の生データ ===")
long_data = scraper._fetch_period_stats(jid, months=12)
print(f"long_data: {long_data}")
print("\n=== 短期成績(2ヶ月)の生データ ===")
short_data = scraper._fetch_period_stats(jid, months=2)
print(f"short_data: {short_data}")
