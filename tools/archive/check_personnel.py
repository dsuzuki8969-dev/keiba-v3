import sys, io
sys.path.insert(0, '.')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from src.scraper.personnel import PersonnelDBManager

mgr = PersonnelDBManager()
checks = [
    ('01143', '川田'),
    ('01088', 'ルメール'),
    ('05339', '坂井'),
    ('01116', '戸崎圭'),
    ('01014', '岩田康'),
]
print("=== 騎手DBキャッシュ確認 ===")
for jid, name in checks:
    j = mgr.get_jockey(jid)
    if j:
        print(f"{name}({jid}): upper_long={j.upper_long_dev:.1f} lower_long={j.lower_long_dev:.1f}")
    else:
        print(f"{name}({jid}): キャッシュなし")

print("\n=== 調教師DBキャッシュ確認 ===")
trainer_checks = [('01070', '高木登'), ('01023', '国枝栄')]
for tid, name in trainer_checks:
    t = mgr.get_trainer(tid)
    if t:
        print(f"{name}({tid}): rank={t.rank.value} recovery={t.recovery_break}")
    else:
        print(f"{name}({tid}): キャッシュなし")
