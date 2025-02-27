from pathlib import Path
from subprocess import DEVNULL, run

from geopandas import read_parquet
from tqdm import tqdm

from .config import iso3_list, outputs


def to_geojsonl(file: Path) -> None:
    """Save file as GeoJSONSeq."""
    gdf = read_parquet(file)
    gdf.to_file(file.with_suffix(".geojsonl"))


def to_pmtiles(file: Path) -> None:
    """Save file as PMTiles."""
    run(
        [
            "tippecanoe",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--force",
            "--maximum-zoom=g",
            "--read-parallel",
            "--simplify-only-low-zooms",
            f"--output={file.with_suffix('.pmtiles')}",
            file.with_suffix(".geojsonl"),
        ],
        check=False,
        stderr=DEVNULL,
    )


def main() -> None:
    """Main function, runs all modules in sequence."""
    files = sorted(outputs.rglob("*.parquet"))
    pbar = tqdm(files)
    for file in pbar:
        pbar.set_postfix_str(file.stem)
        iso3 = file.stem.split("_")[0].upper()
        if len(iso3_list) and iso3 not in iso3_list:
            continue
        to_geojsonl(file)
        to_pmtiles(file)
        file.with_suffix(".geojsonl").unlink()


if __name__ == "__main__":
    main()
