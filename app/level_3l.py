from geopandas import GeoDataFrame, read_parquet
from pandas import concat
from tqdm import tqdm

from .config import countries, inputs, iso3_list, l2l, l3l


def clip_lines(gdf: GeoDataFrame, iso3: str):
    if iso3.startswith("X"):
        adm0 = read_parquet(inputs / "un/bnda_dsp.parquet")
    else:
        adm0 = read_parquet(inputs / "un/bnda_cty.parquet")
    cty = adm0[adm0["iso3cd"] == iso3]
    gdf = gdf.clip(cty, keep_geom_type=True)
    gdf = gdf.dissolve(by=["bdytyp", "iso3cd"], as_index=False)
    return gdf.reset_index().drop(columns=["index"])


def main() -> None:
    """Applies all issues in fix.json to files."""
    lines = read_parquet(inputs / "un/bndl.parquet")
    lines["iso3cd"] = lines["iso3cd"].fillna("")
    lines = lines[~lines["bdytyp"].isin([6, 7])]
    lines = lines[["bdytyp", "iso3cd", lines.active_geometry_name]]
    lines = lines.dissolve(by=["bdytyp", "iso3cd"], as_index=False)
    pbar = tqdm(countries)
    for country in pbar:
        iso3 = country.alpha_3
        pbar.set_postfix_str(iso3)
        if len(iso3_list) and iso3 not in iso3_list:
            continue
        file_path = l2l / f"{iso3.lower()}.parquet"
        if file_path.exists():
            gdf = read_parquet(file_path)
            if gdf is not None:
                gdf = clip_lines(gdf, iso3)
                cty_lines = lines[lines["iso3cd"].str.contains(iso3)]
                adm_lines = concat([cty_lines, gdf], ignore_index=True)
                adm_lines.to_parquet(
                    l3l / f"{iso3.lower()}.parquet",
                    compression="zstd",
                    geometry_encoding="geoarrow",
                    write_covering_bbox=True,
                )
