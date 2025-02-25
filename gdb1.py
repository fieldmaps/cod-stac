from pathlib import Path
from shutil import rmtree
from subprocess import run

cwd = Path(__file__).parent

if __name__ == "__main__":
    for processing_level in ["1", "1a", "1b"]:
        input_dir = cwd / f"outputs/level-{processing_level}"
        output_dir = cwd / f"gdb/level-{processing_level}.gdb"
        rmtree(output_dir, ignore_errors=True)
        output_file = cwd / f"gdb/level-{processing_level}.gdb.zip"
        output_file.unlink(missing_ok=True)
        run(
            [
                "ogrmerge",
                "-progress",
                *["--config", "OGR_ORGANIZE_POLYGONS", "ONLY_CCW"],
                *["-of", "OpenFileGDB"],
                *["-lco", "TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER"],
                *["-o", output_dir],
                *sorted(input_dir.glob("*.parquet")),
            ],
            check=False,
        )
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
