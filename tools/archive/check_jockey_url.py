"""
騎手ページのURL調査
"""
import sys, io
sys.path.insert(0, '.')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from src.scraper.netkeiba import NetkeibaClient

client = NetkeibaClient()

# 川田のID 01143
jid = "01143"

# 複数のURL試行
urls = [
    f"https://db.netkeiba.com/jockey/result/recent/{jid}/",
    f"https://db.netkeiba.com/jockey/{jid}/",
    f"https://db.netkeiba.com/jockey/result/{jid}/",
    f"https://www.netkeiba.com/jockey/result/{jid}/",
    f"https://race.netkeiba.com/jockey/result/{jid}/",
]

for url in urls:
    try:
        import requests
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        print(f"[{r.status_code}] {url}")
        if r.status_code == 200:
            print(f"  -> OK, content length: {len(r.text)}")
    except Exception as e:
        print(f"[ERROR] {url}: {e}")
