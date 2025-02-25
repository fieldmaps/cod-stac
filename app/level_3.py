from geopandas import GeoDataFrame, read_parquet
from tqdm import tqdm

from .config import ADMIN_LEVEL_MAX, countries, inputs, iso3_list, l2
from .utils import to_parquet


def clip_and_save(gdf: GeoDataFrame, iso3: str, admin_level: int):
    if iso3.startswith("X"):
        adm0 = read_parquet(inputs / "un/bnda_dsp.parquet")
    else:
        adm0 = read_parquet(inputs / "un/bnda_cty.parquet")
    cty = adm0[adm0["iso3cd"] == iso3]
    gdf = gdf.clip(cty, keep_geom_type=True)
    gdf["validto"] = gdf["validto"].astype("date32[pyarrow]")
    gdf = gdf.reset_index()
    to_parquet(gdf, iso3, admin_level, "3")


def main() -> None:
    """Applies all issues in fix.json to files."""
    pbar = tqdm(countries)
    for country in pbar:
        iso3 = country.alpha_3
        pbar.set_postfix_str(iso3)
        if len(iso3_list) and iso3 not in iso3_list:
            continue
        for admin_level in range(ADMIN_LEVEL_MAX, -1, -1):
            file_path = l2 / f"{iso3.lower()}_adm{admin_level}.parquet"
            if file_path.exists():
                gdf = read_parquet(file_path)
                if gdf is not None:
                    clip_and_save(gdf, iso3, admin_level)
