from json import load
from pathlib import Path

from pandas import read_csv

cwd = Path(__file__).parent
inputs = cwd / "../inputs"
outputs = cwd / "../outputs"
stac = cwd / "../stac"

ADMIN_LEVEL_MAX = 5
WGS84 = 4326

l1 = outputs / "level-1"
l1.mkdir(parents=True, exist_ok=True)
l1a = outputs / "level-1a"
l1a.mkdir(parents=True, exist_ok=True)
l2 = outputs / "level-2"
l2.mkdir(parents=True, exist_ok=True)
l3 = outputs / "level-3"
l3.mkdir(parents=True, exist_ok=True)
processing_levels = {"1": l1, "1a": l1a, "2": l2, "3": l3}

with Path.open(inputs / "level_1a.json") as f:
    level_1a_fixes = load(f)
unterm = {x["iso3"]: x for x in read_csv(inputs / "unterm.csv").to_dict("records")}
m49 = {x["iso3"]: x for x in read_csv(inputs / "m49.csv").to_dict("records")}

invisible_chars = [
    "U+0009",
    "U+000A",
    "U+000D",
    "U+00A0",
    "U+200C",
    "U+200E",
    "U+200F",
    "U+FEFF",
]
apostrophe_chars = ["U+0060", "U+2019", "U+2032"]
quote_chars = ["U+201C", "U+201D"]
