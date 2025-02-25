import re

from geopandas import GeoDataFrame, read_parquet
from tqdm import tqdm

from .config import ADMIN_LEVEL_MAX, countries, iso3_list, l1a
from .utils import to_parquet


def get_langs(gdf: GeoDataFrame, admin_level: int) -> list[str]:
    """Gets a list of language codes.

    Args:
        gdf: Current layer's GeoDataFrame.
        admin_level: Current layer's admin level.

    Returns:
        List of languages in gdf
    """
    columns = list(gdf.columns)
    p = re.compile(rf"^adm{admin_level}_\w{{2}}$")
    langs = [x.split("_")[1] for x in columns if p.search(x)]
    return list(dict.fromkeys(langs))


def refactor_columns(gdf: GeoDataFrame, admin_level: int) -> GeoDataFrame:
    """Convert names like ADM2_EN to adm2_name with additional lang column."""
    gdf.columns = [x.lower() for x in gdf.columns]
    langs = get_langs(gdf, admin_level)
    for index, lang in enumerate(langs):
        for level in range(admin_level, -1, -1):
            old_col = f"adm{level}_{lang}"
            new_col = f"adm{level}_name{'' if index == 0 else index}"
            if old_col in gdf.columns:
                gdf = gdf.rename(columns={old_col: new_col})
        lang_col = f"lang{'' if index == 0 else index}"
        gdf[lang_col] = lang
    return gdf


def main() -> None:
    """Applies all issues in fix.json to files."""
    pbar = tqdm(countries)
    for country in pbar:
        iso3 = country.alpha_3
        pbar.set_postfix_str(iso3)
        if len(iso3_list) and iso3 not in iso3_list:
            continue
        for admin_level in range(ADMIN_LEVEL_MAX, -1, -1):
            file_path = l1a / f"{iso3.lower()}_adm{admin_level}.parquet"
            if file_path.exists():
                gdf = read_parquet(file_path)
                if gdf is not None:
                    gdf = refactor_columns(gdf, admin_level)
                    to_parquet(gdf, iso3, admin_level, "1b")
