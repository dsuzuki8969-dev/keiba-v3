"""newspaper.html の構造確認"""
import sys, os
sys.path.insert(0, ".")
from src.scraper.netkeiba import NetkeibaClient, RACE_URL, get_venue_code_from_race_id
from data.masters.venue_master import JRA_CODES

client = NetkeibaClient(no_cache=True)
race_id = "202509050811"
base = "https://nar.netkeiba.com" if get_venue_code_from_race_id(race_id) not in JRA_CODES else RACE_URL
url = f"{base}/race/newspaper.html"
soup = client.get(url, params={"race_id": race_id})
if not soup:
    print("fetch failed")
else:
    horses = soup.select('a[href*="/horse/"]')
    print("horse links:", len(horses))
    for a in horses[:5]:
        print(" ", a.get("href"), "|", repr(a.get_text(strip=True)[:30]))
    tables = soup.select("table")
    print("tables:", len(tables))
