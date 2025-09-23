# tests/conftest.py
import sys
import pathlib

# Put the project root (the folder that contains `utils/`) on sys.path
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))