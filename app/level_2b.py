from geopandas import GeoDataFrame, read_parquet
from tqdm import tqdm

from .config import ADMIN_LEVEL_MAX, countries, e2, iso3_list
from .utils import to_parquet


def dissolve_and_save(gdf: GeoDataFrame, iso3: str, admin_levels: int):
    for admin_level in range(admin_levels, -1, -1):
        columns = []
        for level in range(admin_level, -1, -1):
            columns += [x for x in gdf.columns if x.startswith(f"adm{level}_name")]
            columns += [f"adm{level}_pcode"]
        columns += [x for x in gdf.columns if x.startswith("lang")]
        columns += ["date", "validon", "validto", gdf.active_geometry_name]
        gdf = gdf.dissolve(f"adm{admin_level}_pcode", as_index=False)
        gdf = gdf[columns]
        gdf = gdf.sort_values(by=[f"adm{admin_level}_pcode"])
        to_parquet(gdf, iso3, admin_level, "2")


def main() -> None:
    """Applies all issues in fix.json to files."""
    pbar = tqdm(countries)
    for country in pbar:
        iso3 = country.alpha_3
        pbar.set_postfix_str(iso3)
        if len(iso3_list) and iso3 not in iso3_list:
            continue
        for admin_level in range(ADMIN_LEVEL_MAX, -1, -1):
            file_path = e2 / f"{iso3.lower()}_adm{admin_level}.parquet"
            if file_path.exists():
                gdf = read_parquet(file_path)
                if gdf is not None:
                    dissolve_and_save(gdf, iso3, admin_level)
                    break
