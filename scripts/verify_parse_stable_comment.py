#!/usr/bin/env python3
"""
T-025 parseStableComment 検証スクリプト
旧パターン vs 新パターン bullet 比較
"""
import json
import re
import sys

# ---------- 旧パターン (T-022 後・師のみ) ----------
def parse_old(raw: str) -> list:
    if not raw or not raw.strip():
        return []
    cleaned = raw.strip()
    cleaned = re.sub(r'^[○●◯◎▲△★☆×]\S+[（(][^）)]*[）)][^\s—―－\-]*\s*[—―－\-]+\s*', '', cleaned)
    cleaned = re.sub(r'^[○●◯◎▲△★☆×]?[぀-ゟ゠-ヿ一-鿿ｦ-ﾟA-Za-z]+【[^】]+】', '', cleaned)
    # 旧: 師のみ
    cleaned = re.sub(r'(^|\n)[\s　]*[^\s　\n。．]*?師[—―－\-]+\s*', r'\1', cleaned)
    if cleaned == raw.strip():
        cleaned = re.sub(r'^[^。．\n]*[—―－\-]+\s*', '', cleaned)
    sentences = [s.strip() for s in re.split(r'[。．\n]', cleaned)]
    sentences = [re.sub(r'^[。．\s　]+', '', s).strip() for s in sentences]
    sentences = [s for s in sentences if len(s) >= 5]
    sentences = [s for s in sentences if not re.match(r'^[○●◯◎▲△★☆×]\S+[（(][^）)]+[）)]$', s)]
    sentences = [s for s in sentences if not re.match(r'^[○●◯◎▲△★☆×][^\s（(]+$', s)]
    return sentences

# ---------- 新パターン (T-025: 厩務員/助手/マネジャー + 言い切り化) ----------
ASSERTIVE_RULES = [
    (r'(と思う|と思います|と感じる|と感じます|気がする|気もする|印象だ|印象です|ように見える|ように映る|ようだ|みたいだ|だろうか|でしょうか)$', ''),
    (r'(かな|かも|かもしれない|かもしれません|かも知れない)$', ''),
    (r'(だよ|だね|だな|だよね)$', 'だ'),
    (r'(でしょう|だろう)$', ''),
    (r'(ですね|ですよ|だぜ|なのよ)$', ''),
    (r'(かもね|かもよ)$', ''),
]

def to_assertive(s: str) -> str:
    t = re.sub(r'[。．、,]+$', '', s.strip()).strip()
    for _ in range(3):
        before = t
        for pat, rep in ASSERTIVE_RULES:
            t = re.sub(pat, rep, t).strip()
        if t == before:
            break
    return t

def parse_new(raw: str) -> list:
    if not raw or not raw.strip():
        return []
    cleaned = raw.strip()
    cleaned = re.sub(r'^[○●◯◎▲△★☆×]\S+[（(][^）)]*[）)][^\s—―－\-]*\s*[—―－\-]+\s*', '', cleaned)
    cleaned = re.sub(r'^[○●◯◎▲△★☆×]?[぀-ゟ゠-ヿ一-鿿ｦ-ﾟA-Za-z]+【[^】]+】', '', cleaned)
    # 新: 師|厩務員|助手|マネジャー
    cleaned = re.sub(r'(^|\n)[\s　]*[^\s　\n。．]*?(師|厩務員|助手|マネジャー)[—―－\-]+\s*', r'\1', cleaned)
    if cleaned == raw.strip():
        cleaned = re.sub(r'^[^。．\n]*[—―－\-]+\s*', '', cleaned)
    sentences = [s.strip() for s in re.split(r'[。．\n]', cleaned)]
    sentences = [re.sub(r'^[。．\s　]+', '', s).strip() for s in sentences]
    sentences = [s for s in sentences if len(s) >= 5]
    sentences = [s for s in sentences if not re.match(r'^[○●◯◎▲△★☆×]\S+[（(][^）)]+[）)]$', s)]

    def apply_p6(s):
        return re.sub(r'^[\s　]*[^\s　\n。．]*?(師|厩務員|助手|マネジャー)[—―－\-]+\s*', '', s).strip()
    sentences = [apply_p6(s) for s in sentences]
    sentences = [s for s in sentences if len(s) >= 5]
    sentences = [s for s in sentences if not re.match(r'^[○●◯◎▲△★☆×][^\s（(]+$', s)]
    sentences = [to_assertive(s) for s in sentences]
    sentences = [s for s in sentences if len(s) >= 5]
    seen = []
    for s in sentences:
        if s not in seen:
            seen.append(s)
    return seen


print("=== A. パターン4/6 ダッシュ4種 + 人名拡張 ===")
tests_p4 = [
    # (入力, 期待結果説明)
    ("眞島師——きっかけ掴めれば良好", "真島師——きっかけ掴めば良好 → 削除"),
    ("平松師――状態上々", "平松師――状態上々 → 削除"),
    ("担当廂務員――状態上々", "担当厩務員――状態上々 → 削除"),
    ("大林助手－－レースに向いている", "大林助手－－レースに向いている → 削除"),
    ("○○マネジャー--問題なし", "○○マネジャー--問題なし → 削除"),
]
for raw, label in tests_p4:
    old_r = parse_old(raw)
    new_r = parse_new(raw)
    ok = len(new_r) > 0 and new_r[0] and (len(old_r) == 0 or old_r != new_r or len(new_r) > 0)
    # 検証: 新パターンで prefix が消えていること
    prefix_gone = all('師' not in s[:3] and '厩務員' not in s[:4] and '助手' not in s[:2] and 'マネジャー' not in s[:4] for s in new_r)
    print(f"  [{label}]")
    print(f"    旧({len(old_r)}): {old_r}")
    print(f"    新({len(new_r)}): {new_r}")
    print(f"    prefix除去: {'OK' if prefix_gone else 'FAIL'}")

print()
print("=== A-3. 言い切り化 ===")
assertive_tests = [
    ("距離は問題なくこなせると思う", "距離は問題なくこなせる"),
    ("休み明けを2連勝した後も好調だよ", "休み明けを2連勝した後も好調だ"),
    ("今後のためにもいい経験にしたいところ", "今後のためにもいい経験にしたいところ"),
    ("前走は展開に恵まれなかった", "前走は展開に恵まれなかった"),
    ("徐々に状態は良くなっている", "徐々に状態は良くなっている"),
]
for inp, expected in assertive_tests:
    result = to_assertive(inp)
    status = "OK" if result == expected else f"FAIL got={result!r}"
    print(f"  [{status}] '{inp}' -> '{result}'")

print()
print("=== B. 実データ 20260428_pred.json ===")
pred_path = r"C:\Users\dsuzu\keiba\keiba-v3\data\predictions\20260428_pred.json"
with open(pred_path, encoding='utf-8') as f:
    pred = json.load(f)

# 大井 10R
oi10 = None
for race in pred.get('races', []):
    if '大井' in race.get('venue', '') and race.get('race_no') == 10:
        oi10 = race
        break

if oi10:
    for h in oi10.get('horses', []):
        if h.get('horse_name') in ['ソルベット', 'エースアビリティ', 'ミエノサンダー']:
            tr = (h.get('training_records') or [{}])[0]
            sc = tr.get('stable_comment', '') or ''
            sb = tr.get('stable_comment_bullets') or []
            inp = '\n'.join(sb) if sb else sc
            old_b = parse_old(inp)
            new_b = parse_new(inp)
            print(f"  【{h['horse_name']}】 sc={len(sc)}字 bullets={len(sb)}件")
            print(f"  raw50: {inp[:80]!r}")
            print(f"  旧({len(old_b)}): {old_b}")
            print(f"  新({len(new_b)}): {new_b}")
else:
    print("  大井10R なし。大井レース一覧:")
    for race in pred.get('races', []):
        if '大井' in race.get('venue', ''):
            print(f"    R{race.get('race_no')} {race.get('race_name','')}")

# 園田 10R チャンチャン
so10 = None
for race in pred.get('races', []):
    if '園田' in race.get('venue', '') and race.get('race_no') == 10:
        so10 = race
        break
if so10:
    for h in so10.get('horses', []):
        if 'チャンチャン' in h.get('horse_name', ''):
            tr = (h.get('training_records') or [{}])[0]
            sc = tr.get('stable_comment', '') or ''
            sb = tr.get('stable_comment_bullets') or []
            inp = '\n'.join(sb) if sb else sc
            old_b = parse_old(inp)
            new_b = parse_new(inp)
            print(f"\n  【{h['horse_name']}】 sc={len(sc)}字 bullets={len(sb)}件")
            print(f"  raw80: {inp[:80]!r}")
            print(f"  旧({len(old_b)}): {old_b}")
            print(f"  新({len(new_b)}): {new_b}")

# 「担当厩務員」grep
print()
print("=== 「担当厩務員」含む馬 grep ===")
found = 0
for race in pred.get('races', []):
    for h in race.get('horses', []):
        tr = (h.get('training_records') or [{}])[0]
        sc = tr.get('stable_comment', '') or ''
        sb = tr.get('stable_comment_bullets') or []
        all_text = sc + ' '.join(sb)
        if '厩務員' in all_text:
            inp = '\n'.join(sb) if sb else sc
            new_b = parse_new(inp)
            print(f"  {race.get('venue')} R{race.get('race_no')} {h.get('horse_name')}")
            print(f"  raw80: {inp[:80]!r}")
            print(f"  新bullets: {new_b}")
            found += 1
if found == 0:
    print("  なし（20260428 予想データに 厩務員 含む馬なし）")

print("\nDone.")
