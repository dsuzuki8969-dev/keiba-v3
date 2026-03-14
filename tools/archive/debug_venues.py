from src.scraper.netkeiba import NetkeibaClient
import re

RACE_URL = "https://race.netkeiba.com"
client = NetkeibaClient(no_cache=True)

date_key = "20260222"

# Step1: group取得
date_url = f"{RACE_URL}/top/race_list_get_date_list.html"
date_soup = client.get(date_url, params={"kaisai_date": date_key, "encoding": "UTF-8"})

print("=== date_list の li[date] タグ ===")
for li in date_soup.select("li[date]"):
    print(f"  date={li.get('date')}  group={li.get('group')}  class={li.get('class')}")

group = ""
for li in date_soup.select("li[date][group]"):
    if li.get("date") == date_key:
        group = li.get("group", "")
        break

print(f"\n取得したgroup: '{group}'")

# Step2: race_list_sub
sub_url = f"{RACE_URL}/top/race_list_sub.html"
params = {"kaisai_date": date_key, "encoding": "UTF-8"}
if group:
    params["current_group"] = group
print(f"リクエストURL: {sub_url} {params}")

sub_soup = client.get(sub_url, params=params)

race_ids = []
for a_tag in sub_soup.select("a[href*='race_id=']"):
    href = a_tag.get("href", "")
    m = re.search(r"race_id=(\d{12})", href)
    if m:
        race_ids.append(m.group(1))

# ユニークな場コードだけ表示
from data.masters.venue_master import get_venue_code_from_race_id, get_venue_name
seen = set()
print("\n=== 取得競馬場 ===")
for rid in race_ids:
    vc = get_venue_code_from_race_id(rid)
    if vc and vc not in seen:
        seen.add(vc)
        print(f"  code={vc}  name={get_venue_name(vc)}  例: {rid}")
