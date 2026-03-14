"""
競馬解析マスターシステム v3.0 - 競馬場マスタ
中央10場 + 地方14場 = 24場
personnel・calibration・その他で共通利用
"""

# 競馬場名（netkeiba表記）→ 場コード（course_id の先頭部分）
# course_master.py と整合。netkeiba の騎手リーディング等は「東京・芝1600」形式
VENUE_NAME_TO_CODE: dict[str, str] = {
    # 中央競馬 10場
    "札幌": "03",
    "函館": "04",
    "福島": "01",
    "新潟": "02",
    "東京": "05",
    "中山": "06",
    "中京": "07",
    "京都": "08",
    "阪神": "09",
    "小倉": "10",
    # 地方競馬 14場
    "帯広": "52",
    "門別": "30",
    "盛岡": "35",
    "水沢": "36",
    "浦和": "42",
    "船橋": "43",
    "大井": "44",
    "川崎": "45",
    "金沢": "46",
    "笠松": "47",
    "名古屋": "48",
    "園田": "49",
    "姫路": "51",
    "高知": "54",
    "佐賀": "55",
}

# 逆引き: 場コード → 競馬場名
VENUE_CODE_TO_NAME: dict[str, str] = {v: k for k, v in VENUE_NAME_TO_CODE.items()}
# netkeiba race_id では別コードが使われるケースがある
VENUE_CODE_TO_NAME["50"] = "園田"   # race_id上は50、SPAT4等では49
VENUE_CODE_TO_NAME["65"] = "帯広"   # race_id上は65、SPAT4等では52

# 中央競馬の場コード集合
JRA_VENUE_CODES: frozenset[str] = frozenset(
    ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]
)


def is_jra_venue(venue_code: str) -> bool:
    """場コードが中央競馬か"""
    return venue_code in JRA_VENUE_CODES


def get_venue_code(name: str) -> str | None:
    """競馬場名から場コードを取得。マスタにない場合は None"""
    return VENUE_NAME_TO_CODE.get(name)


def get_venue_name(code: str) -> str | None:
    """場コードから競馬場名を取得"""
    return VENUE_CODE_TO_NAME.get(code)


# 他モジュール互換用エイリアス
VENUE_MAP = VENUE_NAME_TO_CODE
JRA_CODES = JRA_VENUE_CODES
JRA_VENUES = JRA_VENUE_CODES
is_jra = is_jra_venue


def get_venue_code_from_race_id(race_id: str) -> str | None:
    """race_id から場コードを抽出。例: 202501050511 -> 05"""
    if not race_id or len(race_id) < 10:
        return None
    # JRA: 202501050511 (開催ID) -> 5-6桁目が場
    # 地方: 形式が異なる場合あり
    try:
        if len(race_id) >= 12:
            return race_id[4:6] if race_id[4:6].isdigit() else None
        return None
    except Exception:
        return None


def is_banei(venue_code: str) -> bool:
    """場コードがばんえい（帯広）か。race_id上は65、SPAT4等では52"""
    return venue_code in ("52", "65")


BANEI_VENUE_CODES = frozenset(("52", "65"))
