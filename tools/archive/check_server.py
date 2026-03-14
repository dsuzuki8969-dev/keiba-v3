import urllib.request
res = urllib.request.urlopen('http://127.0.0.1:5051/dash')
html = res.read().decode('utf-8')

with open('check_result.txt', 'w', encoding='utf-8') as f:
    start = html.find('id="home-venues"')
    if start >= 0:
        f.write(html[start:start+1500])
    else:
        f.write('home-venues が見つかりません')

print('完了')
