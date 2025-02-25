from geopandas import GeoDataFrame, read_parquet
from pandas import concat
from tqdm import tqdm

from .config import ADMIN_LEVEL_MAX, countries, iso3_list, l2, l2l


def clip_dissolve_and_save(
    child: GeoDataFrame,
    parent: GeoDataFrame,
    iso3: str,
    admin_level: int,
):
    child.geometry = child.boundary
    parent.geometry = parent.boundary
    lines = child.overlay(parent, how="difference").dissolve()
    lines = lines[[lines.active_geometry_name]]
    lines["bdytyp"] = 10 + admin_level
    lines["iso3cd"] = iso3
    return lines


def main() -> None:
    """Applies all issues in fix.json to files."""
    pbar = tqdm(countries)
    for country in pbar:
        iso3 = country.alpha_3
        pbar.set_postfix_str(iso3)
        if len(iso3_list) and iso3 not in iso3_list:
            continue
        cty_lines = GeoDataFrame()
        for admin_level in range(ADMIN_LEVEL_MAX, 0, -1):
            child_path = l2 / f"{iso3.lower()}_adm{admin_level}.parquet"
            if child_path.exists():
                child = read_parquet(child_path)
                parent = read_parquet(
                    l2 / f"{iso3.lower()}_adm{admin_level - 1}.parquet",
                )
                adm_line = clip_dissolve_and_save(child, parent, iso3, admin_level)
                cty_lines = concat([cty_lines, adm_line], ignore_index=True)
        if cty_lines.active_geometry_name and not cty_lines.empty:
            cty_lines.to_parquet(
                l2l / f"{iso3.lower()}.parquet",
                compression="zstd",
                geometry_encoding="geoarrow",
                write_covering_bbox=True,
            )
