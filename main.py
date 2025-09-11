import sys
from pathlib import Path

from src.cli import main

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

if __name__ == "__main__":
    main()
