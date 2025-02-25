from pathlib import Path
from shutil import rmtree
from subprocess import run

import geopandas as gpd

cwd = Path(__file__).parent

output_dir_tmp = cwd / "gdb/level-3-tmp.gdb"
output_dir = cwd / "gdb/level-3.gdb"
output_file = cwd / "gdb/level-3.gdb.zip"


def get_fields(adm_lvl: int) -> str:
    """Get fields for the given admin level."""
    result = []
    for i in range(adm_lvl, -1, -1):
        if adm_lvl >= 4:
            result.extend(
                [f"adm{i}_name", f"adm{i}_name1", f"adm{i}_pcode"],
            )
        else:
            result.extend(
                [f"adm{i}_name", f"adm{i}_name1", f"adm{i}_name2", f"adm{i}_pcode"],
            )
    if adm_lvl >= 4:
        result.extend(["lang", "lang1", "date", "validon", "validto", "layer"])
    else:
        result.extend(["lang", "lang1", "lang2", "date", "validon", "validto", "layer"])
    return ",".join(result)


def run_lines() -> None:
    """Merge all lines into a single GDB."""
    input_dir = cwd / "outputs/level-3-lines"
    run(
        [
            "ogrmerge",
            "-progress",
            "-single",
            *["--config", "OGR_ORGANIZE_POLYGONS", "ONLY_CCW"],
            *["-of", "OpenFileGDB"],
            *["-nln", "lines"],
            *["-lco", "TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER"],
            *["-o", output_dir_tmp],
            *sorted(input_dir.glob("*.parquet")),
        ],
        check=False,
    )


def run_polygons() -> None:
    """Merge all polygons into a single GDB."""
    input_dir = cwd / "outputs/level-3"
    for adm_lvl in range(5):
        run(
            [
                "ogrmerge",
                "-progress",
                "-single",
                "-update",
                *["--config", "OGR_ORGANIZE_POLYGONS", "ONLY_CCW"],
                *["-nln", f"adm{adm_lvl}"],
                *["-src_layer_field_name", "layer"],
                *["-lco", "TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER"],
                *["-o", output_dir_tmp],
                *sorted(input_dir.glob(f"*_adm{adm_lvl}.parquet")),
            ],
            check=False,
        )


def deduplicate_lines() -> None:
    """Deduplicate lines."""
    gdf = gpd.read_file(output_dir_tmp, layer="lines", use_arrow=True)
    gdf = gdf.drop_duplicates(subset=["bdytyp", "iso3cd"])
    gdf.to_file(
        output_dir,
        layer="lines",
        driver="OpenFileGDB",
        layer_options={"TARGET_ARCGIS_VERSION": "ARCGIS_PRO_3_2_OR_LATER"},
        use_arrow=True,
    )


def transfer_polygons() -> None:
    """Transfer polygons to the final GDB."""
    for adm_lvl in range(5):
        run(
            [
                "ogr2ogr",
                "-update",
                *["--config", "OGR_ORGANIZE_POLYGONS", "ONLY_CCW"],
                *["-select", get_fields(adm_lvl)],
                *["-nln", f"adm{adm_lvl}"],
                *["-lco", "TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER"],
                output_dir,
                *[output_dir_tmp, f"adm{adm_lvl}"],
            ],
            check=False,
        )


if __name__ == "__main__":
    """Merge all outputs into a single GDB and upload it to S3."""
    rmtree(output_dir_tmp, ignore_errors=True)
    rmtree(output_dir, ignore_errors=True)
    output_file.unlink(missing_ok=True)
    run_lines()
    run_polygons()
    deduplicate_lines()
    transfer_polygons()
    rmtree(output_dir_tmp, ignore_errors=True)
    run(
        ["sozip", "--recurse-paths", "--junk-paths", output_file, output_dir],
        check=False,
    )
    rmtree(output_dir, ignore_errors=True)
    run(
        [
            "rclone",
            "copyto",
            "--progress",
            "--s3-chunk-size=100M",
            output_file,
            f"r2://fieldmaps-data-cod/gdb/{output_file.name}",
        ],
        check=False,
    )
