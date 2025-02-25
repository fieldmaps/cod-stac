from json import load
from os import getenv
from pathlib import Path

from dotenv import load_dotenv
from pandas import read_csv
from pycountry import countries

load_dotenv(override=True)

cwd = Path(__file__).parent
inputs = cwd / "../inputs"
outputs = cwd / "../outputs"
gdb = cwd / "../gdb"
extended = cwd / "../extended"
stac = cwd / "../stac"

ADMIN_LEVEL_MAX = 5
WGS84 = 4326

l1 = outputs / "level-1"
l1.mkdir(parents=True, exist_ok=True)
l1a = outputs / "level-1a"
l1a.mkdir(parents=True, exist_ok=True)
l1b = outputs / "level-1b"
l1b.mkdir(parents=True, exist_ok=True)
l2 = outputs / "level-2"
l2.mkdir(parents=True, exist_ok=True)
l2l = outputs / "level-2-lines"
l2l.mkdir(parents=True, exist_ok=True)
l3 = outputs / "level-3"
l3.mkdir(parents=True, exist_ok=True)
l3l = outputs / "level-3-lines"
l3l.mkdir(parents=True, exist_ok=True)
processing_levels = {
    "1": l1,
    "1a": l1a,
    "1b": l1b,
    "2": l2,
    "2l": l2l,
    "3": l3,
    "3l": l3l,
}

e1 = extended / "pre"
e1.mkdir(parents=True, exist_ok=True)
e2 = extended / "post"
e2.mkdir(parents=True, exist_ok=True)

l3l = outputs / "level-3-lines"
l3l.mkdir(parents=True, exist_ok=True)

with Path.open(inputs / "level_1a.json") as f:
    level_1a_fixes = load(f)
with Path.open(inputs / "level_2.json") as f:
    level_2_fixes = load(f)
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

iso3_list = [
    x.upper().strip() for x in getenv("ISO3", "").split(",") if x.strip() != ""
]

countries.add_entry(
    alpha_2="XI",
    alpha_3="XIK",
    name="India-administered Kashmir",
    numeric="900",
)
countries.add_entry(
    alpha_2="XP",
    alpha_3="XPK",
    name="Pakistan-administered Kashmir",
    numeric="901",
)
countries.add_entry(
    alpha_2="XS",
    alpha_3="XSG",
    name="Siachen Glacier",
    numeric="902",
)
