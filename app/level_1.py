from pandas import NaT
from tqdm import tqdm

from .config import ADMIN_LEVEL_MAX, countries, iso3_list
from .utils import read_parquet, to_parquet


def main() -> None:
    """Applies all issues in fix.json to files."""
    pbar = tqdm(countries)
    for country in pbar:
        iso3 = country.alpha_3
        pbar.set_postfix_str(iso3)
        if len(iso3_list) and iso3 not in iso3_list:
            continue
        sources = ["hdx", "itos"]
        for admin_level in range(ADMIN_LEVEL_MAX, -1, -1):
            gdf = read_parquet(sources, iso3, admin_level)
            if gdf is not None:
                if "validTo" in gdf:
                    gdf["validTo"] = NaT
                    gdf["validTo"] = gdf["validTo"].astype("date32[pyarrow]")
                to_parquet(gdf, iso3, admin_level, "1")
