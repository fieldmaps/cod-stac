from geopandas import GeoDataFrame, read_parquet
from pandas import NaT, concat
from tqdm import tqdm

from .config import (
    ADMIN_LEVEL_MAX,
    WGS84,
    countries,
    e1,
    inputs,
    iso3_list,
    l1b,
    level_2_fixes,
)


def add_remove_split(gdf: GeoDataFrame, iso3: str, admin_level: int):
    additions_path = inputs / f"un/{iso3.lower()}_adm{admin_level}.parquet"
    if additions_path.exists():
        additions = read_parquet(additions_path).to_crs(WGS84)
        columns = gdf.columns
        gdf = concat([gdf, additions], ignore_index=True)[columns]
        gdf["validto"] = NaT
        gdf["validto"] = gdf["validto"].astype("date32[pyarrow]")
    if iso3 in level_2_fixes:
        pcode_level = level_2_fixes[iso3]["adm"]
        pcode = f"adm{pcode_level}_pcode"
        for name, (switch, *args) in level_2_fixes[iso3]["layers"].items():
            if switch == "==":
                gdf_part = gdf[gdf[pcode].isin(args)]
            elif switch == "!=":
                gdf_part = gdf[~gdf[pcode].isin(args)]
            gdf_part.to_parquet(
                e1 / f"{name}_adm{admin_level}.parquet",
                compression="zstd",
                geometry_encoding="geoarrow",
                write_covering_bbox=True,
            )
    else:
        gdf.to_parquet(
            e1 / f"{iso3.lower()}_adm{admin_level}.parquet",
            compression="zstd",
            geometry_encoding="geoarrow",
            write_covering_bbox=True,
        )


def main() -> None:
    """Applies all issues in fix.json to files."""
    pbar = tqdm(countries)
    for country in pbar:
        iso3 = country.alpha_3
        pbar.set_postfix_str(iso3)
        if len(iso3_list) and iso3 not in iso3_list:
            continue
        for admin_level in range(ADMIN_LEVEL_MAX, -1, -1):
            file_path = l1b / f"{iso3.lower()}_adm{admin_level}.parquet"
            if file_path.exists():
                gdf = read_parquet(file_path)
                if gdf is not None:
                    add_remove_split(gdf, iso3, admin_level)
                    break
