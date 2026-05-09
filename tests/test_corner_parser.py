"""Corner_Numパーサーの動作テスト"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lz4.frame
from scripts.rebuild_race_log_corners import parse_corner_table, get_position_for_horse

cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'cache')
race_id = '202605010106'
path = os.path.join(cache_dir, f'race.netkeiba.com_race_result.html_race_id={race_id}.html.lz4')

with open(path, 'rb') as f:
    compressed = f.read()
html = lz4.frame.decompress(compressed).decode('utf-8', errors='replace')

corners = parse_corner_table(html)
print(f'Corner_Num: {corners}')
print()

for hno in [9, 12, 2, 3, 11, 1, 5]:
    positions, pos_4c = get_position_for_horse(corners, hno)
    print(f'  hno={hno:2d}: positions={positions} 4c={pos_4c}')

print()
print('Expected from screenshot:')
print('  hno= 9 (1着リアライズリバティ): 2,2')
print('  hno=12 (10着ワイルドミュール): 12,10')
print('  hno= 2 (3着トランスマーレ): 10,10')
print('  hno=11 (11着マジックオーラ): 12,10')
print('  hno= 3 (13着モンサンドール): 12,14')
