from typing import Literal

import geopandas as gpd
from geopandas import GeoDataFrame
from pandas import read_csv

from .config import inputs, m49, processing_levels, unterm


def read_parquet(
    sources: list[str],
    iso3: str,
    admin_level: int,
) -> GeoDataFrame | None:
    """Gets the preferred file path for a particular iso3 and admin level combination.

    Args:
        sources: list of sources
        iso3: country iso3
        admin_level: admin level

    Returns:
        GeoDataFrame of admin boundaries if they exist.
    """
    file_name = f"{iso3.lower()}_adm{admin_level}.parquet"
    for source in sources:
        file_path = inputs / source / file_name
        if file_path.exists():
            return gpd.read_parquet(file_path)
    return None


def to_parquet(
    gdf: GeoDataFrame,
    iso3: str,
    admin_level: int,
    processing_level: str,
) -> None:
    """Gets the preferred file path for a particular iso3 and admin level combination.

    Args:
        gdf: GeoDataFrame
        iso3: country iso3
        admin_level: admin level
        processing_level: processing level
    """
    file_name = f"{iso3.lower()}_adm{admin_level}.parquet"
    file_path = processing_levels[processing_level] / file_name
    gdf.to_parquet(
        file_path,
        compression="zstd",
        geometry_encoding="geoarrow",
        write_covering_bbox=True,
    )


def get_epsg_ease(min_lat: float, max_lat: float) -> Literal[6931, 6932, 6933]:
    """Gets the code for appropriate Equal-Area Scalable Earth grid based on latitude.

    Args:
        min_lat: Minimum latitude of geometry from bounds.
        max_lat: Maximum latitude of geometry from bounds.

    Returns:
        EPSG for global EASE grid if area touches neither or both poles, otherwise a
        north or south grid if the area touches either of those zones.
    """
    latitude_poles = 80
    latitude_equator = 0
    epsg_ease_north = 6931
    epsg_ease_south = 6932
    epsg_ease_global = 6933
    if max_lat >= latitude_poles and min_lat >= latitude_equator:
        return epsg_ease_north
    if min_lat <= -latitude_poles and max_lat <= latitude_equator:
        return epsg_ease_south
    return epsg_ease_global


def get_adm0_name(iso3: str, lang: str):
    """Checks if Admin 0 name is invalid.

    Args:
        column: Admin 0 column to check.
        name: value of column.
        iso3: ISO-3 country code.

    Returns:
        UNTERM or M49 name.
    """
    if iso3 in unterm:
        return unterm[iso3][f"{lang}_short"]
    if iso3 in m49:
        return m49[iso3][f"{lang}_short"]
    return ""


def get_metadata() -> list[dict]:
    """Load the metadata table and create a list with every COD admin layer to download.

    For example, returns entries for AFG_ADM0, AFG_ADM1, AFG_ADM2, AGO_ADM0, etc.

    Returns:
        List containing the following information to download each COD: ISO-3 code,
        admin level, URL and layer index of the COD on the ArcGIS server.
    """
    metadata = read_csv(inputs / "metadata.csv")
    return metadata.to_dict("records")
