from . import images, level_1, level_1a, pmtiles, stac


def main() -> None:
    """Main function, runs all modules in sequence."""
    if False:
        level_1.main()
        level_1a.main()
        pmtiles.main()
        images.main()
        stac.main()
    stac.main()


if __name__ == "__main__":
    main()
