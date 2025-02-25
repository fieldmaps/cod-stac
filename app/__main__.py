from . import (
    images,
    level_1,
    level_1a,
    level_1b,
    level_2a,
    level_2b,
    level_2l,
    level_3,
    level_3l,
    pmtiles,
    stac,
)


def main() -> None:
    """Main function, runs all modules in sequence."""
    if False:
        level_1.main()
        level_1a.main()
        level_1b.main()
        level_2a.main()
        level_2b.main()
        level_2l.main()
        level_3.main()
        level_3l.main()
        images.main()
        pmtiles.main()
        stac.main()
    stac.main()


if __name__ == "__main__":
    main()
