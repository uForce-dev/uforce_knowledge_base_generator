from config import settings


def initialize() -> None:
    settings.temp_dir.mkdir(exist_ok=True)


def main() -> None:
    initialize()
    print(settings)


if __name__ == "__main__":
    main()
