from src.scraper.netkeiba import NetkeibaClient

RACE_URL = "https://race.netkeiba.com"
client = NetkeibaClient(no_cache=True)
date_key = "20260222"

sub_url = f"{RACE_URL}/top/race_list_sub.html"
params = {"kaisai_date": date_key, "encoding": "UTF-8", "current_group": "1020260221"}
sub_soup = client.get(sub_url, params=params)

# 競馬場名ラベルを探す
with open('debug_html.txt', 'w', encoding='utf-8') as f:
    # 各セクションのタイトル・ヘッダーを探す
    for tag in sub_soup.select('.RaceList_DataItem, .Keibajo, [class*="keibajo"], [class*="place"], [class*="venue"], h3, h4, dt'):
        text = tag.get_text(strip=True)
        if text:
            f.write(f'{tag.name} class={tag.get("class")}: {text[:60]}\n')
    
    # すべてのテキストでリンクのhrefも確認
    f.write('\n=== 競馬場関連リンク ===\n')
    for a in sub_soup.select('a[href*="jyo"]'):
        f.write(f'{a.get("href")}  text={a.get_text(strip=True)[:30]}\n')

    # HTML全体（最初の5000文字）
    f.write('\n\n=== HTML（先頭5000字） ===\n')
    f.write(str(sub_soup)[:5000])

print('debug_html.txt に書き込みました')
